
extern crate std;
extern crate chrono;
extern crate scraper;
extern crate curl;
extern crate serde;
extern crate reqwest;
use reqwest::header::HeaderMap;
use scraper::{Html, Selector};
use tokio::sync::watch::error;
use std::{env::args, fmt::Error,str};
use chrono::Utc;
use serde::{Serialize, Deserialize};
use serde_json;

use curl::easy::Easy;

static DEBUG: bool = false;
//Purpose of this module is to pull a table observations
// and parse the html into json (later protobufs)

    //Brainstorm how to make more dynamic (schemaless?)
    #[derive(Debug, Serialize, Deserialize)]
    pub struct UnparsedContract {
        pub timestamp: i64,
        pub contract_name: String,
        pub last_trade_date: String,
        pub strike: String,
        pub last_price: String,
        pub bid: String,
        pub ask: String,
        pub change: String,
        pub percent_change: String,
        pub volume: String,
        pub open_interest: String,
        pub implied_volatility: String,
    }

    impl UnparsedContract {

        pub fn new() -> UnparsedContract {
            UnparsedContract{
                timestamp: Utc::now().timestamp(),
                contract_name: "".to_string(),
                last_trade_date: "".to_string(),
                strike: "".to_string(),
                last_price: "".to_string(),
                bid: "".to_string(),
                ask: "".to_string(),
                change: "".to_string(),
                percent_change: "".to_string(),
                volume: "".to_string(),
                open_interest: "".to_string(),
                implied_volatility: "".to_string(),
            }
        }


        pub fn keys(&self) -> [&str; 12] {
            [
            "timestame",
            "contract_name",
            "last_trade_date",
            "strike",
            "last_price",
            "bid",
            "ask",
            "change",        
            "percent_change",    
            "volume",            
            "open_interest",        
            "implied_volatility",
            ]
        }

        pub fn idx_to_key(&mut self, index: usize, value: String) {
            match index {
                1 => {
                    self.contract_name = value;
                },
                2 => {
                    self.last_trade_date = value;
                },
                3 => {
                    self.strike = value;
                },
                4 => {
                    self.last_price = value;
                },
                5 => {
                    self.bid = value;
                },
                6 => {
                    self.ask = value;
                },
                7 => {
                    self.change = value;
                },
                8 => {
                    self.percent_change = value;
                },
                9 => {
                    self.volume = value;
                },
                10 => {
                    self.open_interest = value;
                },
                11 => {
                    self.implied_volatility = value;
                }
                _ => panic!("invalid Index"),
            }
        }
    }


    #[derive(Debug, Serialize, Deserialize)]
    pub struct TimeSeries {
        pub data: Vec<UnparsedContract>,
    }


    

    //pub fn scrape(url: &str) -> std::io::Result<String> {
    pub fn scrape(url: &str) -> Result<String, Box<dyn std::error::Error>> {
    
        //Instantiate Easy instance for scraping
        let mut easy = Easy::new();
    
        
        //Instantiate string to store raw html scraped
        let mut stringed = String::new();
    
    
        //Need to make this more reproducible (also input symbol)
        //TODO: build request url dynamically using format!
        match easy.url(url) {
            Ok(_) => (),
            Err(e) => {
                let msg = format!("options_scraper::scrape: error while running easy.url: {}", e);
                println!("{}", msg);
                return Err(msg.into());
            },
        };
        //Scope declared here in order to transfer ownership of stringed back to main function
        {
            let mut transfer = easy.transfer();
            match transfer.write_function(|data|{

                let stringed_bytes = match str::from_utf8(data).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("options_scraper::scrape: Error while reading scraped bytes to string {}", e))
                }) {
                    Ok(stringed) => stringed,
                    Err(e) => {
                        let msg = format!("options_scraper::scrape: error while stringing bytes within transfer.write_function: {}", e);
                        //Handle error below
                        println!("{}", msg);
                        ""
                    },
                };
    
                stringed.push_str(stringed_bytes);
    
                
                Ok(data.len())
            }) {
                Ok(_) => (),
                Err(e) => {
                    let msg = format!("options_scraper::scrape: error while running transfer.write_function: {}", e);
                    println!("{}", msg);
                    return Err(msg.into());
                },
            }
            //transfer.perform().unwrap();
            match transfer.perform() {
                Ok(_) => (),
                Err(e) => {
                    println!("{}", e);
                    let msg = format!("options_scraper::scrape: error while running transfer.perform: {}", e);
                    return Err(msg.into());
                },
            }
    
        }

        //process_bytes here
        let ts = match process_bytes(stringed) {
            Ok(x) => x,
            Err(e) => {
                println!("{}", e);
                return Err(e);
            },
        };
        
        //let serialized = serde_json::to_string(&ts).unwrap();

        let serialized = match serde_json::to_string(&ts) {
            Ok(x) => x,
            Err(_) => {
                //Handle error
                let msg = format!("options_scraper::scrape: error on json serialization");
                println!("{}", "error on serialization");
                return Err("".to_string().into());
            },
        };
    
        Ok(serialized)
        
    }

    pub async fn async_scrape(url: &str) -> Result<TimeSeries, Box<dyn std::error::Error>> {
        //TODO: Build url dynamically here:
        //let mut headers = HeaderMap::new();
        //Cookie?
        //Sec-Ch-Ua
        //Sec-Ch-Ua-Platform
        //Sec-Fetch-Dest
        //User-Agent
        //headers.insert(key, value.parse().unwrap());
        let client = reqwest::Client::new();
        let resp = match client.get(url)
        .header("Sec-Ch-Ua", r#""Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123""#)
        .header("Sec-Ch-Ua-Platform", r#""macOS""#)
        .header("Sec-Fetch-Dest", "document")
        .header("Sec-Fetch-Mode", "navigate")
        .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
        .send().await {
            Ok(x) => x,
            Err(e) => {
                let msg = format!("options_scraper::async_scrape: error on reqwest to url: {} - {}", url, e); 
                println!("{}", msg);
                //Handle error
                return Err(msg.into());
            },
        };
    
    
        let text = resp.text().await?;

        println!("Length of resp: {:?}", text.clone().len());
   
        let ts = match process_bytes(text) {
            Ok(x) => x,
            Err(e) => {
                //Handle error
                return Err(e);
            },
        };
        Ok(ts)
    }
    
    fn process_bytes(stringed: String) -> Result<TimeSeries, Box<dyn std::error::Error>> {
        //Instantiate list for storing parsed data
        let mut scraped_elements = Vec::new();
        let mut contracts:Vec<UnparsedContract> = Vec::new();

        if DEBUG {
            println!("Stringed Document {:?}", stringed.clone());
        }
    
        // parsing block
        let dom = Html::parse_document(stringed.as_str());
    
        let td_selector = match Selector::parse(r#"table > tbody > tr > td"#) {
            Ok(x) => x,
            Err(e) => {
                let msg = format!("options_scraper::process_bytes: error while parsing td selector: {} ", e);
                println!("{}", msg);
                return Err(msg.into());
            },
        };
    
        for element in dom.select(&td_selector) {
                let mut scraped_element = element.inner_html();
                //TODO: Fix this hard coded value to something either inputted or part of a removeList 
                if scraped_element.contains("<span class=\"svelte-12t6atp\"></span>") {
                    scraped_element = scraped_element.replace("<span class=\"svelte-12t6atp\"></span>", "");
                }
                scraped_elements.push(scraped_element.clone());
        }

    
        
        //May need a better way here to detect if html is still present in the string
        let mut count = 1;
        let mut contract: Vec<String> = vec!["".to_string(); 12];
        contract[0] = "".to_string();
        let scraped_length = scraped_elements.len();
        for (idx, element) in scraped_elements.iter_mut().enumerate() {

            ///TODO: Remove this hard-coded value: LOL
            if count >= 12 || idx == scraped_length - 1  {
                let mut i = 1;
                let mut new_contract = UnparsedContract::new();
                while i < 12 {
                    new_contract.idx_to_key(i, contract[i].to_string());
                    i += 1;
                }
                contracts.push(new_contract);

                count = 1;
                
            }
    
            //dynamically parsing html tags still present within the <td> tags
            let fragment = Html::parse_fragment(element.as_str()); 
            let a_selector = match Selector::parse("a") {
                Ok(x) => x,
                Err(e) => {
                    let msg = format!("options_scraper::process_bytes: error while parsing <a> selector: {} ", e);
                    println!("{}", msg);
                    return Err(msg.into());
                },
            };
            let span_selector = match Selector::parse("span") {
                Ok(x) => x,
                Err(e) => {
                    let msg = format!("options_scraper::process_bytes: error while parsing <span> selector: {} ", e);
                    println!("{}", msg);
                    return Err(msg.into());
                },
            };
            //Select out span + a elements
            let a = fragment.select(&a_selector).next().ok_or("Nil");
            let span = fragment.select(&span_selector).next().ok_or("Nil");
    
            //If a exists in inner html add the inner_html of a instead 
            if a.is_ok() {
                let a_element = a.unwrap().inner_html();
                contract[count] = a_element;
                count += 1;
                continue;
            }
            //If span exists in inner html add the inner_html of span instead 
            if span.is_ok() {
                let span_element = span.unwrap().inner_html();
                contract[count] = span_element;
                count += 1;
                continue;
            }

            if DEBUG {
                dbg!("element: {:?}\n", element.clone());
            }
            

            contract[count] = element.to_string();
    
    
            //contract.idx_to_key(count, element);
            count += 1;
        } 
        
        Ok(TimeSeries{
            data: contracts,
        })
    }

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use dotenv::dotenv;


    #[ignore = "Used for testing external endpoint"]
    #[tokio::test]
    async fn test_async_scrape() {
        dotenv().ok();
    
        let symbol = match env::var("SYMBOL") {
            Ok(v) => {
                if v != "" {
                    v
                } else {
                    panic!("SYMBOL was empty in environment")
                }
            },
            Err(e) => panic!("SYMBOL not found in environment"),
        };
    
        let url = format!(r#"https://{}"#, symbol);

        let output_ts = match async_scrape(url.as_str()).await {
            Ok(x) => x,
            Err(e) => {
                let msg = format!("parsing_queue.AsyncConsumer.consume - Error occurred while scraping: {}", e);
                println!("{}", msg);
                //Panic! here?
                TimeSeries {
                    data: Vec::new(),
                }
            },
        }; 
        for i in output_ts.data.iter() {
            dbg!("Contract: {:?}\n", i);
        }


    }
}


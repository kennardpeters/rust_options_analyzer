
extern crate std;
extern crate chrono;
extern crate scraper;
extern crate curl;
extern crate serde;
extern crate reqwest;
use scraper::{Html, Selector};
use std::{process, str};
use chrono::Utc;
use serde::{Serialize, Deserialize};

use curl::easy::Easy;

//Purpose of this module is to pull a table observations
// and parse the html into json (later protobufs)

    //Brainstorm how to make more dynamic (schemaless?)
    #[derive(Debug, Serialize, Deserialize)]
    pub struct Contract {
        timestamp: i64,
        contract_name: String,
        last_trade_name: String,
        strike: String,
        last_price: String,
        bid: String,
        ask: String,
        change: String,
        percent_change: String,
        volume: String,
        open_interest: String,
        implied_volatility: String,
    }

    impl Contract {

        pub fn new() -> Contract {
            Contract{
                timestamp: Utc::now().timestamp(),
                contract_name: "".to_string(),
                last_trade_name: "".to_string(),
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
            "last_trade_name",
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
                    self.last_trade_name = value;
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
        data: Vec<Contract>,
    }


    

    pub fn scrape(url: &str) -> std::io::Result<TimeSeries> {
        // TODO: Change to json instead of saving to a file once queue is made
    
        //Instantiate Easy instance for scraping
        let mut easy = Easy::new();
    
        
        //Instantiate string to store raw html scraped
        let mut stringed = String::new();
    
    
        //Need to make this more reproducible (also input symbol)
        //TODO: build request url dynamically using format!
        easy.url(url).unwrap();
        //Scope declared here in order to transfer ownership of stringed back to main function
        {
            let mut transfer = easy.transfer();
            transfer.write_function(|data|{
                let stringed_bytes = match str::from_utf8(data) {
                    Ok(stringed) => stringed,
                    Err(e) => {
                        println!("Stringing bytes failed!");
                        println!("{}", e);
                        //Handle error below
                        panic!("{}", e);
                    },
                };
    
                stringed.push_str(stringed_bytes);
    
                
                Ok(data.len())
            }).unwrap();
            transfer.perform().unwrap();
    
        }

        //process_bytes here
        let ts = process_bytes(stringed);
    
        Ok(ts)
        
    }

    pub async fn async_scrape(url: &str) -> Result<TimeSeries, Box<dyn std::error::Error>> {
        //TODO: Build url dynamically here:
        let resp = match reqwest::get(url).await {
            Ok(x) => x,
            Err(_) => {
                println!("{}", "error on random request");
                process::exit(0x0100);
            },
        };
    
    
        let text = resp.text().await?;
    
        //TODO: Call process_bytes here
        let ts = process_bytes(text);
        //return timeSeries
        Ok(ts)
    }
    
    fn process_bytes(stringed: String) -> TimeSeries {
        //Instantiate list for storing parsed data
        let mut scraped_elements = Vec::new();
        let mut contracts:Vec<Contract> = Vec::new();
    
        // parsing block
        let dom = Html::parse_document(stringed.as_str());
    
        let td_selector = Selector::parse(r#"table > tbody > tr > td"#).unwrap();
    
        for element in dom.select(&td_selector) {
                scraped_elements.push(element.inner_html());
        }
    
        
        //May need a better way here to detect if html is still present in the string
        let mut count = 1;
        //let contract = Contract::new().borrow_mut();
        let mut contract: Vec<String> = vec!["".to_string(); 12];
        contract[0] = "".to_string();
        for element in scraped_elements {

            if count >= 12 {
                let mut i = 1;
                let mut new_contract = Contract::new();
                while i < 12 {
                    new_contract.idx_to_key(i, contract[i].to_string());
                    i += 1;
                }
                contracts.push(new_contract);

                count = 1;
                
            }
    
            //dynamically parsing html tags still present within the <td> tags
            let fragment = Html::parse_fragment(element.as_str()); 
            let a_selector = Selector::parse("a").unwrap();
            let span_selector = Selector::parse("span").unwrap();
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

            contract[count] = element;
    
    
            //contract.idx_to_key(count, element);
            count += 1;
        } 
        
        TimeSeries{
            data: contracts,
        }
    }

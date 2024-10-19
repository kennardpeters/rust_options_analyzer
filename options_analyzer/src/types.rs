extern crate serde;
extern crate chrono;
extern crate tokio;
extern crate sqlx;

use std::ops::Deref;
use std::str::FromStr;

pub use crate::options_scraper::UnparsedContract;
use serde::{Serialize, Deserialize};
use chrono::{DateTime, NaiveDateTime, NaiveTime, TimeZone, Utc};
use chrono::format::ParseError;
use sqlx::types::time::Time;
use sqlx::Row;
use tokio::sync::{mpsc::Sender, oneshot};
use sqlx::postgres::PgRow;

 #[derive(Debug, Serialize, Deserialize, Clone)]
 pub struct Contract {
     pub timestamp: DateTime<Utc>,
     pub contract_name: String,
     pub last_trade_date: String, //string to date
     pub strike: f64, //string to float
     pub last_price: f64,
     pub bid: f64,
     pub ask: f64,
     pub change: f64,
     pub percent_change: f64,
     pub volume: i64,
     pub open_interest: i64,
     pub implied_volatility: f64,
 }
//Possibly implement dereference trait here

impl Contract {
    pub fn new() -> Self {
        Self {
            timestamp: Utc::now(),
            contract_name: "".to_string(),
            last_trade_date: "".to_string(),
            strike: 0.0,
            last_price: 0.0,
            bid: 0.0,
            ask: 0.0,
            change: 0.0,
            percent_change: 0.0,
            volume: 0,
            open_interest: 0,
            implied_volatility: 0.0,
        }
    }
    pub fn new_from_unparsed(unparsed: &UnparsedContract) -> Self {
        let mut contract = Contract::new();
        contract.parse(unparsed);
        contract
    }
    pub fn parse(&mut self, unparsed: &UnparsedContract) {
        self.timestamp = Utc::now();
        self.contract_name = unparsed.contract_name.clone();
        //need to parse this out using chronos package
        //self.last_trade_date = NaiveDateTime::parse_from_str(unparsed.last_trade_date.replace("EDT", "").clone(), "%Y-%m-%d %H:%M").unwrap().format("%Y-%m-%d").to_string(); //unparsed.last_trade_date;
        self.last_trade_date = unparsed.last_trade_date.clone();
        self.strike = match unparsed.strike.replace("-", "0").parse::<f64>() {
            Ok(v) => v,
            Err(e) => {
                println!("types.Contract::parse: Parsing Strike {} caused the following error: {:?}", unparsed.strike, e);
                0.0
            }
        };
        self.last_price = match unparsed.last_price.replace("-", "0").parse::<f64>() {
            Ok(v) => v,
            Err(e) => {
                println!("types.Contract::parse: Parsing Last_price {} caused the following error: {:?}", unparsed.last_price, e);
                0.0
            }
        };
        self.bid = match unparsed.bid.replace("-", "0").parse::<f64>() {
            Ok(v) => v,
            Err(e) => {
                println!("types.Contract::parse: Parsing Bid {} caused the following error: {:?}", unparsed.bid, e);
                0.0
            }
        };
        self.ask = match unparsed.ask.parse::<f64>() {
            Ok(v) => v,
            Err(e) => {
                println!("types.Contract::parse: Parsing Ask {} caused the following error: {:?}", unparsed.ask, e);
                0.0
            }
        };
        self.change = match unparsed.change.replace("<!-- -->", "").parse::<f64>() {
            Ok(v) => v,
            Err(e) => {
                println!("types.Contract::parse: Parsing change {} caused the following error: {:?}", unparsed.change, e);
                0.0
            }
        };
        self.percent_change = match unparsed.percent_change.replace("%", "").replace("<!-- -->", "").parse::<f64>() {
            Ok(v) => v,
            Err(e) => {
                println!("types.Contract::parse: Parsing Percent change {} caused the following error: {:?}", unparsed.percent_change, e);
                0.0
            }
        };
        let volume = unparsed.volume.replace(",", "").replace("-", "0");
        self.volume = match volume.parse::<i64>() {
            Ok(v) => v,
            Err(e) => {
                println!("types.Contract::parse: Parsing Volume {} caused the following error: {:?}",unparsed.volume, e);
                0
            }
        };
        self.open_interest = match unparsed.open_interest.replace(",", "").parse::<i64>() {
            Ok(v) => v,
            Err(e) => {
                println!("types.Contract::parse: Parsing Open_interest {} caused the following error: {:?}", unparsed.open_interest, e);
                0
            }
        };
        self.implied_volatility = match unparsed.implied_volatility.replace("%", "").parse::<f64>() {
            Ok(v) => v,
            Err(e) => {
                println!("types.Contract::parse: Parsing Implied_volatility {} caused the following error: {:?}", unparsed.implied_volatility, e);
                0.0
            }
        };
    }
}

impl From<PgRow> for Contract {
    fn from(row: PgRow) -> Self {
        //parse rows 
        //flag used to indicate parsing error that should be bubbled up to the caller
        let mut parsing_error: bool = false;

        let timestamp: DateTime<Utc> = match row.try_get("time") {
            Ok(v) => v,
            Err(e) => {
                println!("types.Contract::from: Parsing Timestamp SQL row caused the following error: {:?}", e);
                parsing_error = true;
                Utc.with_ymd_and_hms(0, 0, 0, 0, 0, 0).unwrap()
            }
        };
        
        let mut contract_name: String = match row.try_get("contract_name") {
            Ok(v) => v,
            Err(e) => {
                parsing_error = true;
                println!("types.Contract::from: Parsing Contract_name SQL row caused the following error: {:?}", e);
                "".to_string()
            }
        };

        let last_trade_date: String = match row.try_get("last_trade_date") {
            Ok(v) => v,
            Err(e) => {
                println!("types.Contract::from: Parsing Last_trade_date SQL row caused the following error: {:?}", e);
                "".to_string()
            }
        };

        let strike: f64 = match row.try_get("strike") {
            Ok(v) => v,
            Err(e) => {
                parsing_error = true;
                println!("types.Contract::from: Parsing Strike from SQL row caused the following error: {:?}", e);
                0.0
            }
        };

        let last_price: f64 = match row.try_get("last_price") {
            Ok(v) => v,
            Err(e) => {
                parsing_error = true;
                println!("types.Contract::from: Parsing last_price from SQL row caused the following error: {:?}", e);
                0.0
            },
        };

        let bid: f64 = match row.try_get("bid") {
            Ok(v) => v,
            Err(e) => {
                parsing_error = true;
                println!("types.Contract::from: Parsing `bid` from SQL row caused the following error: {:?}", e);
                0.0
            }
        };

        let ask: f64 = match row.try_get("ask") {
            Ok(v) => v,
            Err(e) => {
                parsing_error = true;
                println!("types.Contract::from: Parsing `ask` from SQL row caused the following error: {:?}", e);
                0.0
            }
        };

        let change: f64 = match row.try_get("change") {
            Ok(v) => v,
            Err(e) => {
            parsing_error = true;
            println!("types.Contract::from: Parsing `change` from SQL row caused the following error: {:?}", e);
            0.0
            }
        };

        let percent_change: f64 = match row.try_get("percent_change") {
            Ok(v) => v,
            Err(e) => {
                parsing_error = true;
                println!("types.Contract::from: Parsing `percent_change` from SQL row caused the following error: {:?}", e);
                0.0
            }
        };

        let volume_32: i32 = match row.try_get("volume") {
            Ok(v) => v,
            Err(e) => {
                parsing_error = true;
                println!("types.Contract::from: Parsing `volume` from SQL row caused the following error: {:?}", e);
                0
            }
        };

        let open_interest_32: i32 = match row.try_get("open_interest") {
            Ok(v) => v,
            Err(e) => {
                parsing_error = true;
                println!("types.Contract::from: Parsing `open_interest` from SQL row caused the following error: {:?}", e);
                i32::from(0)
            }
        };

        let mut implied_volatility: f64 = match row.try_get("implied_volatility") {
            Ok(v) => v,
            Err(e) => {
                println!("types.Contract::from: Parsing Implied_volatility caused the following error: {:?}", e);
                0.0
            }
        };

        //flags to the caller that the contract is invalid
        if parsing_error {
            contract_name = "".to_string();
        }

        Contract {
            timestamp,
            contract_name,
            last_trade_date,
            strike,
            last_price,
            bid,
            ask,
            change,
            percent_change,
            volume: volume_32 as i64,
            open_interest: open_interest_32 as i64,
            implied_volatility,
        }

    }
}

pub type Responder<T, E> = oneshot::Sender<Result<T, E>>;

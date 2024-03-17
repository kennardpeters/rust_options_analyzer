extern crate serde;
extern crate chrono;
extern crate tokio;

use std::ops::Deref;

pub use crate::options_scraper::UnparsedContract;
use serde::{Serialize, Deserialize};
use chrono::{NaiveDateTime, DateTime, Utc};
use chrono::format::ParseError;
use tokio::sync::{mpsc::Sender, oneshot};

 #[derive(Debug, Serialize, Deserialize, Clone)]
 pub struct Contract {
     pub timestamp: i64,
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
            timestamp: 0,
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
        self.timestamp = unparsed.timestamp;
        self.contract_name = unparsed.contract_name.clone();
        //need to parse this out using chronos package
        //self.last_trade_date = NaiveDateTime::parse_from_str(unparsed.last_trade_date.replace("EDT", "").clone(), "%Y-%m-%d %H:%M").unwrap().format("%Y-%m-%d").to_string(); //unparsed.last_trade_date;
        self.last_trade_date = unparsed.last_trade_date.clone();
        self.strike = match unparsed.strike.parse::<f64>() {
            Ok(v) => v,
            Err(e) => {
                println!("Parsing Strike {} caused the following error: {:?}", unparsed.strike, e);
                0.0
            }
        };
        self.last_price = match unparsed.last_price.parse::<f64>() {
            Ok(v) => v,
            Err(e) => {
                println!("Parsing Last_price {} caused the following error: {:?}", unparsed.last_price, e);
                0.0
            }
        };
        self.bid = match unparsed.bid.parse::<f64>() {
            Ok(v) => v,
            Err(e) => {
                println!("Parsing Bid {} caused the following error: {:?}", unparsed.bid, e);
                0.0
            }
        };
        self.ask = match unparsed.ask.parse::<f64>() {
            Ok(v) => v,
            Err(e) => {
                println!("Parsing Ask {} caused the following error: {:?}", unparsed.ask, e);
                0.0
            }
        };
        self.change = match unparsed.change.replace("<!-- -->", "").parse::<f64>() {
            Ok(v) => v,
            Err(e) => {
                println!("Parsing change {} caused the following error: {:?}", unparsed.change, e);
                0.0
            }
        };
        self.percent_change = match unparsed.percent_change.replace("%", "").replace("<!-- -->", "").parse::<f64>() {
            Ok(v) => v,
            Err(e) => {
                println!("Parsing Percent change {} caused the following error: {:?}", unparsed.percent_change, e);
                0.0
            }
        };
        let volume = unparsed.volume.replace("-", "0");
        self.volume = match volume.parse::<i64>() {
            Ok(v) => v,
            Err(e) => {
                println!("Parsing Open_interest {} caused the following error: {:?}",unparsed.open_interest, e);
                0
            }
        };
        self.open_interest = match unparsed.open_interest.parse::<i64>() {
            Ok(v) => v,
            Err(e) => {
                println!("Parsing Open_interest {} caused the following error: {:?}", unparsed.open_interest, e);
                0
            }
        };
        self.implied_volatility = match unparsed.implied_volatility.replace("%", "").parse::<f64>() {
            Ok(v) => v,
            Err(e) => {
                println!("Parsing Implied_volatility {} caused the following error: {:?}", unparsed.implied_volatility, e);
                0.0
            }
        };
    }
}

pub type Responder<T, E> = oneshot::Sender<Result<T, E>>;

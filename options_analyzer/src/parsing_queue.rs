extern crate amqprs;
extern crate std;
extern crate serde_json;
extern crate async_trait;
extern crate tokio;

use amqprs::{
    channel::{BasicAckArguments, BasicConsumeArguments, Channel},
    consumer::AsyncConsumer,
    BasicProperties,
    Deliver,
};
use std::{borrow::BorrowMut, ops::{Deref, DerefMut}, str, sync::Arc, time::Duration, future::Future};
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::{Mutex, mpsc::Sender, oneshot};

pub use crate::mq::{Queue, publish_to_queue};
pub use crate::options_scraper;
use crate::scraped_cache;
pub use crate::types::Contract;
pub use crate::scraped_cache::{ScrapedCache, Command};

pub struct ParsingQueue<'a> {
    pub name: &'a str, //Current name of queue
    routing_key: &'a str, // queue name for publishing to the next queue
    exchange_name: &'a str, //Exchange name used for publishing to the next queue
    parsing_consumer: Option<ParsingConsumer>,
    tx: tokio::sync::mpsc::Sender<Command>,
}

impl<'a> ParsingQueue<'a> {
    pub fn new(queue_name: &'a str, routing_key: &'a str, exchange_name: &'a str, tx: Sender<Command>) -> Self { 
        Self {
            name: queue_name,
            routing_key,
            exchange_name,
            parsing_consumer: None,
            tx,
        }
    }

    pub fn new_args(name: &str) -> BasicConsumeArguments {
        BasicConsumeArguments::new(
            name, 
            "example_basic_pub_sub",
        )
    }
    
    //Start a consumer on channel
    //NEED a separate consumer struct here
    //TODO: Add Result type as return to handle errors
    pub async fn process_queue(&mut self, channel: &mut Channel) {
        let args = BasicConsumeArguments::new(
            &self.name,
            "parser",
        );

        self.parsing_consumer = Some(ParsingConsumer::new(args.no_ack, self.tx.clone()));

        //TODO: Handle unwrap here
        //Possible declare a new consumer here with cache on it
        let consumer_tag = channel
            .basic_consume(self.parsing_consumer.clone().unwrap(), args)
            .await
            .unwrap();

        dbg!(consumer_tag);
        
    }
}

#[async_trait]
impl<'a> Queue for ParsingQueue<'a> {

    fn queue_name(&self) -> &str {
        self.name
    }
    

    fn args(&self) -> BasicConsumeArguments {
        BasicConsumeArguments::new(
            self.name, 
            "example_basic_pub_sub",
        )
    } 
    
}

#[derive(Clone)] //Copy
struct ParsingConsumer {
    no_ack: bool,
    tx: Sender<Command>,
    //tx: Arc<Mutex<mpsc::Sender<Command>>>,
    //additional fields as needed
}

impl ParsingConsumer {
    pub fn new(no_ack: bool, tx: Sender<Command>) -> Self {
        Self { 
            tx, 
            no_ack 
        }
    }
}

#[async_trait]
impl AsyncConsumer for ParsingConsumer {

    //TODO: Add error handling to function 
    async fn consume(
        &mut self,
        channel: &Channel,
        deliver: Deliver,
        _basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        //unserialize content
        let stringed_bytes = match str::from_utf8(&content) {
            Ok(stringed) => stringed,
            Err(e) => {
                println!("Stringing bytes failed!");
                ""
            },
        };
        let unserialized_content: Value = serde_json::from_str(&stringed_bytes).unwrap();
        println!("Unserialized Content: {:?}", unserialized_content);
        if unserialized_content["symbol"].is_null() {
            println!("Symbol is null! for the following delivery: {}",unserialized_content);
            let args = BasicAckArguments::new(deliver.delivery_tag(), false);

            channel.basic_ack(args).await.unwrap();

        } else {
        let symbol = unserialized_content["symbol"].to_string().replace("\"", "");
        println!("SYMBOL: {:?}\n", symbol);
        let url = format!(r#"https://finance.yahoo.com/quote/{}/options?p={}"#, symbol, symbol);
        println!("URL: {:?}\n", url);
        let output_ts = options_scraper::async_scrape(url.as_str()).await.expect("Scrape Failed!");

        println!("Serialized Object LENGTH: {:?}", output_ts.data.len());

        //Parse out fields of time series objects from string => correct datatype
        let mut contracts: Vec<Contract> = Vec::new();

        for i in output_ts.data.iter() {
            let (resp_tx, resp_rx) = oneshot::channel();
            println!("Contract: {:?}\n", i.clone());
            let contract = Contract::new_from_unparsed(i);
            let command = Command::Set{
                key: contract.contract_name.clone(),
                value: contract,
                resp: resp_tx,
            };
            self.tx.send(command).await.unwrap();
            let resp = resp_rx.await;
            dbg!(resp);
        }

        dbg!(contracts);


        //Wait until channel logic is fixed to run the commented out code below
        //let e_content = String::from(
        //    r#"
        //        {
        //            "publisher": "parsing",
        //            "data": "Hello, from Parsing Queue"
        //        }
        //    "#,
        //).into_bytes();
        //Insert into next queue (need to find channel based on queue name and send it through that
        //channel)
        //publish_to_queue(channel, "amq.direct", "amqprs.example", e_content).await;

        let args = BasicAckArguments::new(deliver.delivery_tag(), false);

        channel.basic_ack(args).await.unwrap();
        println!("DELIVERY TAG: {}", deliver.delivery_tag())
        }

    }
}

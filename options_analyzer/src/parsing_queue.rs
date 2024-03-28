extern crate amqprs;
extern crate std;
extern crate serde_json;
extern crate async_trait;
extern crate tokio;

use amqprs::{
    channel::{BasicAckArguments, BasicCancelArguments, BasicConsumeArguments, Channel, ConsumerMessage},
    consumer::AsyncConsumer,
    BasicProperties,
    Deliver,
};
use std::{borrow::BorrowMut, future::Future, ops::{Deref, DerefMut}, str, sync::Arc, time::Duration};
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
    pub async fn process_queue(&mut self, channel: &mut Channel, pub_channel: &mut Channel) -> Result<(), Box<dyn std::error::Error>> {
        let args = BasicConsumeArguments::new(
            &self.name,
            "parser",
        ).manual_ack(false)
        .finish();

        self.parsing_consumer = Some(ParsingConsumer::new(args.no_ack, self.tx.clone()));

        //consuming behavior defined here
        let (ctag, mut messages_rx) = channel.basic_consume_rx(args.clone()).await?;
        while let Some(deliver) = messages_rx.recv().await {
           //need to convert to another function
           let content = match deliver.content {
               Some(x) => x,
               None => continue,
           };


           //unserialize content
           let stringed_bytes = match str::from_utf8(&content) {
               Ok(stringed) => stringed,
               Err(e) => {
                   let msg = format!("parsing_queue::process_queue - stringing content bytes failed {}", e);
                   println!("{}", msg);
                   ""
               },
           };
           let unserialized_content: Value = match serde_json::from_str(&stringed_bytes) {
               Ok(unser_con) => unser_con,
               Err(e) => {
                   let msg = format!("parsing_queue::process_queue - unserializing content into json failed {}", e);
                   println!("{}", msg);
                   //Panic!
                   Value::Null

               }
           };
           println!("Unserialized Content: {:?}", unserialized_content);
           if unserialized_content.is_null() || unserialized_content["symbol"].is_null() {
               println!("Symbol is null! for the following delivery: {}",unserialized_content);
               let args = BasicAckArguments::new(deliver.deliver.unwrap().delivery_tag(), false);

               match channel.basic_ack(args).await {
                   Ok(_) => {}
                   Err(e) => {
                       let msg = format!("parsing_queue::process_queue - Error occurred while acking message after null content: {}", e);
                       println!("{}", msg);
                   },
               }

           } else {
                //main consumer logic to be retried later
                self.process_func(pub_channel, unserialized_content).await?;

                let args = BasicAckArguments::new(deliver.deliver.unwrap().delivery_tag(), false);

                match channel.basic_ack(args).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg = format!("parsing_queue.AsyncConsumer.consume - Error occurred while acking message: {}", e);
                        println!("{}", msg);
                    },
                };

           }
        }

        if let Err(e) = channel.basic_cancel(BasicCancelArguments::new(&ctag)).await {
            let msg = format!("parsing_queue::process_queue - Error occurred while cancelling consumer: {}", e);
            println!("{}", msg);
        }

        dbg!(ctag);
        Ok(())
        
    }
    
    async fn process_func(&mut self, pub_channel: &mut Channel, unserialized_content: Value) -> Result<(), Box<dyn std::error::Error>> {

        let symbol = unserialized_content["symbol"].to_string().replace("\"", "");
        let url = format!(r#"https://finance.yahoo.com/quote/{}/options?p={}"#, symbol, symbol);
        println!("URL: {:?}\n", url);

        let output_ts = match options_scraper::async_scrape(url.as_str()).await {
            Ok(x) => x,
            Err(e) => {
                let msg = format!("parsing_queue::process_func - Error occurred while scraping: {}", e);
                println!("{}", msg);
                //Return err here?
                options_scraper::TimeSeries {
                    data: Vec::new(),
                }
            },
        }; 


        println!("Serialized Object LENGTH: {:?}", output_ts.data.len());

        for i in output_ts.data.iter() {
            let (resp_tx, resp_rx) = oneshot::channel();
            println!("Contract: {:?}\n", i.clone());
            let contract = Contract::new_from_unparsed(i);
            let next_key = contract.contract_name.clone();
            let command = Command::Set{
                key: contract.contract_name.clone(),
                value: contract,
                resp: resp_tx,
            };
            match self.tx.send(command).await {
                Ok(_) => {

                },
                Err(e) => {
                    let msg = format!("parsing_queue::process_func - Error occurred while sending contract to cache: {}", e);
                    println!("{}", msg);
                },
            };
            let resp = match resp_rx.await {
                Ok(x) => x,
                Err(e) => {
                    let msg = format!("parsing_queue::process_func - Error occurred while receiving result of sending contract to cache: {}", e);
                    println!("{}", msg);
                    Err(()) 
                },
            };
            let e_content = String::from(
                format!(
                    r#"
                        {{
                            "publisher": "parsing",
                            "key": {:?}
                        }}
                    "#,
                    next_key 
                )
            ).into_bytes();
            publish_to_queue(pub_channel, "amq.direct", "writing_queue", e_content).await;
            //TODO: remove after verfication on separate queue
            dbg!(resp);
        }

        Ok(())
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

//Deprecated
#[derive(Clone)] 
struct ParsingConsumer {
    no_ack: bool,
    tx: Sender<Command>,
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
                let msg = format!("parsing_queue.AsyncConsumer.consume - stringing content bytes failed {}", e);
                println!("{}", msg);
                ""
            },
        };
        let unserialized_content: Value = match serde_json::from_str(&stringed_bytes) {
            Ok(unser_con) => unser_con,
            Err(e) => {
                let msg = format!("parsing_queue.AsyncConsumer.consume - unserializing content into json failed {}", e);
                println!("{}", msg);
                //Panic!
                Value::Null

            }
        };
        println!("Unserialized Content: {:?}", unserialized_content);
        if unserialized_content.is_null()|| unserialized_content["symbol"].is_null() {
            println!("Symbol is null! for the following delivery: {}",unserialized_content);
            let args = BasicAckArguments::new(deliver.delivery_tag(), false);

            match channel.basic_ack(args).await {
                Ok(_) => {}
                Err(e) => {
                    let msg = format!("parsing_queue.AsyncConsumer.consume - Error occurred while acking message after null content: {}", e);
                    println!("{}", msg);
                },
            }

        } else {
            let symbol = unserialized_content["symbol"].to_string().replace("\"", "");
            println!("SYMBOL: {:?}\n", symbol);
            let url = format!(r#"https://finance.yahoo.com/quote/{}/options?p={}"#, symbol, symbol);
            println!("URL: {:?}\n", url);
            let output_ts = match options_scraper::async_scrape(url.as_str()).await {
                Ok(x) => x,
                Err(e) => {
                    let msg = format!("parsing_queue.AsyncConsumer.consume - Error occurred while scraping: {}", e);
                    println!("{}", msg);
                    //Panic! here?
                    options_scraper::TimeSeries {
                        data: Vec::new(),
                    }
                },
            }; 
        

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
                match self.tx.send(command).await {
                    Ok(_) => {

                    },
                    Err(e) => {
                        let msg = format!("parsing_queue.AsyncConsumer.consume - Error occurred while sending contract to cache: {}", e);
                        println!("{}", msg);
                    },
                };
                let resp = match resp_rx.await {
                    Ok(x) => x,
                    Err(e) => {
                        let msg = format!("parsing_queue.AsyncConsumer.consume - Error occurred while receiving result of sending contract to cache: {}", e);
                        println!("{}", msg);
                        Err(()) 
                    },
                };
                dbg!(resp);
            }

            //dbg!(contracts);


            //Wait until channel logic is fixed to run the commented out code below
            let e_content = String::from(
                r#"
                    {
                        "publisher": "parsing",
                        "data": "Hello, from Parsing Queue"
                    }
                "#,
            ).into_bytes();
            //Insert into next queue (need to find channel based on queue name and send it through that channel)
            publish_to_queue(channel, "amq.direct", "amqprs.example", e_content).await;

            let args = BasicAckArguments::new(deliver.delivery_tag(), false);

            match channel.basic_ack(args).await {
                Ok(_) => {}
                Err(e) => {
                    let msg = format!("parsing_queue.AsyncConsumer.consume - Error occurred while acking message: {}", e);
                    println!("{}", msg);
                },
            };
            println!("DELIVERY TAG: {}", deliver.delivery_tag())
        }

    }
}

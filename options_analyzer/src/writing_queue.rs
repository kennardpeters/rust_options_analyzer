extern crate tokio;
extern crate amqprs;
extern crate serde_json;
extern crate std;
extern crate sqlx;

use crate::mq::Queue;
use crate::types::Contract;
use crate::scraped_cache::{ScrapedCache, Command};
use sqlx::{Pool, Postgres, Row};

use tokio::sync::mpsc::Sender;
use tokio::sync::{oneshot, Mutex};
use std::str;
use std::sync::Arc;

use amqprs::{
    channel::{BasicAckArguments, BasicConsumeArguments, Channel, ConsumerMessage, BasicCancelArguments},
    consumer::AsyncConsumer,
    BasicProperties,
    Deliver,
};
use serde_json::Value;


pub struct WritingQueue<'a> {
    pub name: &'a str, //Current name of queue
    routing_key: &'a str, // queue name for publishing to the next queue
    exchange_name: &'a str, //Exchange name used for publishing to the next queue
    pool: Arc<Mutex<Pool<Postgres>>>,
    tx: Sender<Command>, //use to communicate with caching thread
}

impl<'a> WritingQueue<'a> {
    pub fn new(queue_name: &'a str, routing_key: &'a str, exchange_name: &'a str, pool: Arc<Mutex<Pool<Postgres>>>, tx: Sender<Command>) -> Self { 
        Self {
            name: queue_name,
            routing_key,
            exchange_name,
            pool,
            tx,
        }
    }

    pub async fn process_queue(&mut self, channel: &mut Channel, pub_channel: &mut Channel) -> Result<(), Box<dyn std::error::Error>> {
        let args = BasicConsumeArguments::new(
            &self.name,
            "writer",
        ).manual_ack(false)
        .finish();

        let (ctag, mut messages_rx) = channel.basic_consume_rx(args.clone()).await?;
        while let Some(deliver) = messages_rx.recv().await {
            let content = match deliver.content {
                Some(x) => x,
                None => continue,
            };

            //unserialize content
            let stringed_bytes = match str::from_utf8(&content) {
                Ok(stringed) => stringed,
                Err(e) => {
                    let msg = format!("writing_queue::process_queue - stringing content bytes failed {}", e);
                    println!("{}", msg);
                    ""
                },
            };
            let unserialized_content: Value = match serde_json::from_str(&stringed_bytes) {
                Ok(unser_con) => unser_con,
                Err(e) => {
                    let msg = format!("writing_queue::process_queue - unserializing content into json failed {}", e);
                    println!("{}", msg);
                    //Panic!
                    Value::Null
                }
            };
            println!("Unserialized Content: {:?}", unserialized_content);
            if unserialized_content.is_null() || unserialized_content["key"].is_null() {
                println!("Key is null! for the following delivery: {}",unserialized_content);
                let args = BasicAckArguments::new(deliver.deliver.unwrap().delivery_tag(), false);

                match channel.basic_ack(args).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg = format!("writing_queue::process_queue - Error occurred while acking message after null content: {}", e);
                        println!("{}", msg);
                    },
                };
            } else {
                //main consumer logic to be retried later
                match self.process_func(pub_channel, unserialized_content).await {
                    Ok(_) => {},
                    Err(e) => {
                        let msg = format!("writing_queue::process_queue - Error occurred while processing message: {}", e);
                        println!("{}", msg);
                    },
                };
                let args = BasicAckArguments::new(deliver.deliver.unwrap().delivery_tag(), false);
                match channel.basic_ack(args).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg = format!("writing_queue::process_queue - Error occurred while acking message after null content: {}", e);
                        println!("{}", msg);
                    },
                };
            }
        }

        if let Err(e) = channel.basic_cancel(BasicCancelArguments::new(&ctag)).await {
            let msg = format!("writing_queue::process_queue - Error occurred while cancelling consumer: {}", e);
            println!("{}", msg);
        }

        Ok(())
    }

    async fn process_func(&mut self, pub_channel: &mut Channel, unserialized_content: Value) -> Result<(), Box<dyn std::error::Error>> {

        //Grab a contract names from the queue item and pull them from cache
        let contract_name = unserialized_content["key"].to_string().replace("\"", "");

        let (resp_tx, resp_rx) = oneshot::channel();
        let command = Command::Get{
            key: contract_name.clone(),
            resp: resp_tx,
        };
        match self.tx.send(command).await {
            Ok(_) => {},
            Err(e) => {
                let msg = format!("writing_queue::process_func - Error occurred while requesting contract from cache: {}", e);
                println!("{}", msg);
            },
        };
        let resp = match resp_rx.await {
            Ok(x) => x,
            Err(e) => {
                let msg = format!("writing_queue::process_func - Error occurred while receiving contract from cache: {}", e);
                println!("{}", msg);
                Err(()) 
            },
        };
        //TODO: Fix unwrapping
        let contract = match resp {
            Ok(x) => {
                if x.is_some() {
                    x.unwrap()
                } else {
                    println!("Unwrapped None! from Cache");
                    return Err("Unwrapped None! from Cache".into());
                }
            },
            Err(e) => {
                let msg = format!("writing_queue::process_func - Error occurred while receiving contract from cache: {:?}", e);
                println!("{}", msg);
                Contract::new()
            },
        };

        //Write to the postgres database
        //insert sqlx code here
        //Pull out sqlx code into separate package
        let pool = self.pool.lock().await.clone();
                //RETURNING time
        let result = sqlx::query(
            r#"
                INSERT INTO contracts
                (time, contract_name, last_trade_date, strike, last_price, bid, ask, change, percent_change, volume, open_interest) Values (NOW(), $1, $2, $3, $4, $5, $6, $7,  $8, $9, $10)
                Returning time
            "#,
        )
            .bind(contract_name)
            .bind(contract.last_trade_date)
            .bind(contract.strike)
            .bind(contract.last_price)
            .bind(contract.bid)
            .bind(contract.ask)
            .bind(contract.change)
            .bind(contract.percent_change)
            .bind(contract.volume)
            .bind(contract.open_interest)
            .fetch_one(&pool).await;
            //.execute(pool).await;
        match result {
            Ok(v) => {
                println!("Successfully inserted into postgres!");
                //dbg!(v);
            },
            Err(e) => {
                let msg = format!("writing_queue::process_func - Error occurred while inserting into postgres: {}", e);
                println!("{}", msg);
            },
        };





        
        //Insert into next queue
        
        Ok(())
    }
}

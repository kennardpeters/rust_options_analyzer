extern crate tokio;
extern crate amqprs;
extern crate serde_json;
extern crate std;
extern crate sqlx;

use crate::mq::{self, future_err, publish_to_queue, MQConnection, Queue};
use crate::types::Contract;
use crate::scraped_cache::{ScrapedCache, Command};
use crate::db::DBConnection;
use async_trait::async_trait;
use mq::{PubChannelCommand, SubChannelCommand};
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
    db_connection: Arc<Mutex<DBConnection<'a>>>, //struct used for communticating with the database
    sub_tx: Sender<SubChannelCommand>, //tx used to subscribe from the queue 
    pub_tx: Sender<PubChannelCommand>, //tx used to content to publish to queue
    cache_tx: Sender<Command>, //use to communicate with caching thread
}

impl<'a> WritingQueue<'a> {
    pub fn new(queue_name: &'a str, routing_key: &'a str, exchange_name: &'a str, db_connection: Arc<Mutex<DBConnection<'a>>>, sub_tx: Sender<SubChannelCommand>, pub_tx: Sender<PubChannelCommand>, cache_tx: Sender<Command>) -> Self { 
        Self {
            name: queue_name,
            routing_key,
            exchange_name,
            db_connection,
            sub_tx,
            pub_tx,
            cache_tx,
        }
    }

    pub async fn process_queue(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let args = BasicConsumeArguments::new(
            &self.name,
            "writer",
        ).manual_ack(true)
        .finish();

        match mq::consume_from_queue(args, self.sub_tx.clone(), self).await {
            Ok(()) => (),
            Err(e) => {
                let msg = format!("writing_queue::process_queue - error returned from  consume_from_queue: {}", e);
                return Err(Box::from(msg));
            }
        };

        Ok(())

        //let (ctag, mut messages_rx) = channel.basic_consume_rx(args.clone()).await?;
        //while let Some(deliver) = messages_rx.recv().await {
        //    let content = match deliver.content {
        //        Some(x) => x,
        //        None => continue,
        //    };

        //    //unserialize content
        //    let stringed_bytes = match str::from_utf8(&content) {
        //        Ok(stringed) => stringed,
        //        Err(e) => {
        //            let msg = format!("writing_queue::process_queue - stringing content bytes failed {}", e);
        //            println!("{}", msg);
        //            ""
        //        },
        //    };
        //    let unserialized_content: Value = match serde_json::from_str(&stringed_bytes) {
        //        Ok(unser_con) => unser_con,
        //        Err(e) => {
        //            let msg = format!("writing_queue::process_queue - unserializing content into json failed {}", e);
        //            println!("{}", msg);
        //            //Panic!
        //            Value::Null
        //        }
        //    };
        //    println!("Unserialized Content: {:?}", unserialized_content);
        //    if unserialized_content.is_null() || unserialized_content["key"].is_null() {
        //        println!("Key is null! for the following delivery: {}",unserialized_content);

        //        let delivery = match deliver.deliver {
        //            Some(x) => x,
        //            None => {
        //                println!("writing_queue::process_queue - deliver was None");
        //                continue;
        //            },

        //        };

        //        let args = BasicAckArguments::new(delivery.delivery_tag(), false);

        //        match channel.basic_ack(args).await {
        //            Ok(_) => {}
        //            Err(e) => {
        //                let msg = format!("writing_queue::process_queue - Error occurred while acking message after null content: {}", e);
        //                println!("{}", msg);
        //            },
        //        };
        //        continue;
        //    } else {
        //        //main consumer logic to be retried later
        //        match self.process_func(pub_channel, unserialized_content).await {
        //            Ok(_) => {},
        //            Err(e) => {
        //                let msg = format!("writing_queue::process_queue - Error occurred while processing message: {}", e);
        //                println!("{}", msg);
        //            },
        //        };

        //        let delivery = match deliver.deliver {
        //            Some(x) => x,
        //            None => {
        //                println!("writing_queue::process_queue - deliver was None");
        //                continue;
        //            },

        //        };

        //        let args = BasicAckArguments::new(delivery.delivery_tag(), false);
        //        match channel.basic_ack(args).await {
        //            Ok(_) => {}
        //            Err(e) => {
        //                let msg = format!("writing_queue::process_queue - Error occurred while acking message after db insertion: {}", e);
        //                println!("{}", msg);
        //            },
        //        };
        //        continue;
        //    }
        //}
        //
        //if !channel.is_open() {
        //    println!("writing_queue::process_queue - Channel closed");
        //    dbg!(ctag);
        //    return Ok(());
        //    
        //} else if let Err(e) = channel.basic_cancel(BasicCancelArguments::new(&ctag)).await {
        //    let msg = format!("writing_queue::process_queue - Error occurred while cancelling consumer: {}", e);
        //    println!("{}", msg);
        //    return Err(msg.into());
        //} else {
        //    println!("writing_queue::process_queue - done");
        //    Ok(())
        //}
    }

    //async fn process_func(&mut self, pub_channel: &mut Channel, unserialized_content: Value) -> Result<(), Box<dyn std::error::Error>> {

    //    //Grab a contract names from the queue item and pull them from cache
    //    let contract_name = unserialized_content["key"].to_string().replace("\"", "");

    //    let (resp_tx, resp_rx) = oneshot::channel();
    //    let command = Command::Get{
    //        key: contract_name.clone(),
    //        resp: resp_tx,
    //    };
    //    match self.cache_tx.send(command).await {
    //        Ok(_) => {},
    //        Err(e) => {
    //            let msg = format!("writing_queue::process_func - Error occurred while requesting contract from cache: {}", e);
    //            println!("{}", msg);
    //        },
    //    };
    //    let resp = match resp_rx.await {
    //        Ok(x) => x,
    //        Err(e) => {
    //            let msg = format!("writing_queue::process_func - Error occurred while receiving contract from cache: {}", e);
    //            println!("{}", msg);
    //            Err(()) 
    //        },
    //    };
    //    //TODO: Fix unwrapping
    //    let contract = match resp {
    //        Ok(x) => {
    //            if x.is_some() {
    //                x.unwrap()
    //            } else {
    //                println!("Unwrapped None! from Cache");
    //                return Err("writing_queue::process_func - Unwrapped None! from Cache".into());
    //            }
    //        },
    //        Err(e) => {
    //            let msg = format!("writing_queue::process_func - Error occurred while receiving contract from cache: {:?}", e);
    //            println!("{}", msg);
    //            Contract::new()
    //        },
    //    };
    //    if contract.strike == 0.0 {
    //        return Err("writing_queue::process_func - invalid strike (0.0) => skipping commit to database".into());
    //    }


    //    //Write to the postgres database
    //    //insert sqlx code here
    //    let mut db_connection = self.db_connection.lock().await;
    //    match db_connection.open().await {
    //        Ok(()) => (),
    //        Err(e) => {
    //            println!("writing_queue::process_func - error while opening db_connection {}", e);
    //        },
    //    };
    //    let result = db_connection.insert_contract(&contract).await;
    //    match result {
    //        Ok(v) => {
    //            println!("Successfully inserted into postgres!");
    //        },
    //        Err(e) => {
    //            let msg = format!("writing_queue::process_func - Error occurred while inserting into postgres: {}", e);
    //            return Err(future_err(msg));
    //        },
    //    };
    //    let next_key = contract.contract_name;
    //    let e_content = String::from(
    //        format!(
    //            r#"
    //                {{
    //                    "publisher": "writing",
    //                    "key": {:?}
    //                }}
    //            "#,
    //            next_key 
    //        )
    //    ).into_bytes();

    //    //TODO: publish using pub_tx instead
    //    publish_to_queue(pub_channel, "amq.direct", "calc_queue", e_content).await;
    //    
    //    
    //    
    //    Ok(())
    //}
}

#[async_trait]
impl<'a> Queue for WritingQueue<'a> {

    fn queue_name(&self) ->  &str {
        self.name
    }

    fn args(&self) -> BasicConsumeArguments {
        BasicConsumeArguments::new(
            self.name,
            "writing"
        )
    }

    async fn process_func(&self, deliver: &ConsumerMessage) -> Result<(), Box<dyn std::error::Error + Send>> {

        let content = match &deliver.content {
            Some(x) => x,
            None => {
                let msg = format!("writing_queue::process_func - content was None!");
                return Err(future_err(msg));
            },
        };

        //unserialize content
        let stringed_bytes = match str::from_utf8(&content) {
            Ok(stringed) => stringed,
            Err(e) => {
                let msg = format!("writing_queue::process_queue - stringing content bytes failed {}", e);
                return Err(future_err(msg));
            },
        };
        let unserialized_content: Value = match serde_json::from_str(&stringed_bytes) {
            Ok(unser_con) => unser_con,
            Err(e) => {
                let msg = format!("writing_queue::process_queue - unserializing content into json failed {}", e);
                return Err(future_err(msg));

            }
        };
        //Grab a contract names from the queue item and pull them from cache
        let contract_name = unserialized_content["key"].to_string().replace("\"", "");

        let (resp_tx, resp_rx) = oneshot::channel();
        let command = Command::Get{
            key: contract_name.clone(),
            resp: resp_tx,
        };
        match self.cache_tx.send(command).await {
            Ok(_) => {},
            Err(e) => {
                let msg = format!("writing_queue::process_func - Error occurred while requesting contract from cache: {}", e);
                return Err(future_err(msg));
            },
        };
        let resp = match resp_rx.await {
            Ok(x) => x,
            Err(e) => {
                let msg = format!("writing_queue::process_func - Error occurred while receiving contract from cache: {}", e);
                return Err(future_err(msg));
            },
        };
        let contract = match resp {
            Ok(x) => {
                if x.is_some() {
                    x.unwrap()
                } else {
                    let msg = format!("writing_queue::process_func - Unwrapped None! from Cache for key: {}", contract_name.clone());
                    println!("{}", msg);
                    //return ok here in order to skip invalid
                    return Ok(())
                    //return Err(future_err(msg));
                }
            },
            Err(e) => {
                let msg = format!("writing_queue::process_func - Error occurred while receiving contract from cache: {:?}", e);
                return Err(future_err(msg))
            },
        };
        if contract.strike == 0.0 {
            let msg = format!("writing_queue::process_func - invalid strike (0.0) => skipping commit to database");
            return Err(future_err(msg));
        }


        //Write to the postgres database
        //insert sqlx code here
        let mut db_connection = self.db_connection.lock().await;
        //TODO: Move this statement to main to reduce the amount of open clients
        //match db_connection.open().await {
        //    Ok(()) => (),
        //    Err(e) => {
        //        let msg = format!("writing_queue::process_func - error while opening db_connection {}", e);
        //        return Err(future_err(msg));
        //    },
        //};
        let result = db_connection.insert_contract(&contract).await;
        match result {
            Ok(v) => {
                dbg!("Successfully inserted into postgres!");
            },
            Err(e) => {
                let msg = format!("writing_queue::process_func - Error occurred while inserting into postgres: {}", e);
                return Err(future_err(msg));
            },
        };
        //TODO: Insert into next queue once created
        println!("{} next_key on writing thread", contract.contract_name.clone());
        let next_key = contract.contract_name;
        let e_content = String::from(
            format!(
                r#"
                    {{
                        "publisher": "writing",
                        "key": {:?}
                    }}
                "#,
                next_key 
            )
        ).into_bytes();

        println!("content created on writing queue");



        let (pub_resp_tx, pub_resp_rx) = oneshot::channel(); 

        let cmd = mq::PubChannelCommand::Publish { 
            queue_name: String::from(self.queue_name()),
            content: e_content, 
            resp: pub_resp_tx,
        };

        match self.pub_tx.send(cmd).await {
            Ok(()) => (),
            Err(e) => {
                let msg = format!("writing_queue::process_func - Error occurred while sending content to the next queue from queue: {} - {}", self.queue_name(), e);
                return Err(future_err(msg));
            }
        }

        let resp = match pub_resp_rx.await {
            Ok(x) => x,
            Err(e) => {
                let msg = format!("writing_queue::process_func - Error occurred while awaiting result of send item to the next queue: {}", e);
                return Err(future_err(msg));
            }
        };
        println!("writing_queue::process_func - content sent to next queue");

        Ok(())

    }
}

#[cfg(test)]
mod tests {
    use std::future;

    use super::*;
    use tokio::sync::mpsc;
    use crate::mq::publish_to_queue;

    //#[tokio::test]
    //async fn test_process() {
    //    //Create an mq instance
    //    //Setup using the MQConnection struct
    //    let mut mq_connection = Arc::new(Mutex::new(MQConnection::new("localhost", 5672, "guest", "guest")));
    //    let mut mq_connection_p = mq_connection.clone();
    //    //publishing logic
    //    let conn_result = match mq_connection_p.lock().await.open().await {
    //        Ok(v) => v,
    //        Err(e) => {
    //            println!("writing_queue::test_process - Error occurred while opening connection: {}", e);
    //            return;
    //        }
    //    };
    //    assert_eq!(conn_result, ());
    //    let mut pub_channel = match mq_connection_p.lock().await.add_channel(Some(2)).await {
    //        Ok(v) => Some(v),
    //        Err(e) => {
    //            println!("writing_queue::test_process - Error occurred while adding channel: {}", e);
    //            None
    //        }
    //    };
    //    assert!(pub_channel.is_some());
    //    //Create fake contract
    //    let mut contract = Contract::new();
    //    contract.contract_name = "TEST_CONTRACT".to_string();
    //    contract.last_price = 100.0;

    //    
    //    //caching channel
    //    let (tx, mut rx) = mpsc::channel::<Command>(32);
    //    //Instantiate cache
    //    let mut scraped_cache = ScrapedCache::new(100);

    //    //Insert fake contract into cache
    //    scraped_cache.set("TEST_CONTRACT".to_string(), contract.clone());
    //    //Instantiate db_connection
    //    let db_connection = Arc::new(Mutex::new(DBConnection::new("localhost", 5432, "postgres", "postgres", "postgres")));


    //    //Create a writing queue
    //    let mut writing_queue = Arc::new(Mutex::new(WritingQueue::new("writing_queue", "writing_queue", "amq.direct", db_connection.clone(), tx.clone())));
    //    //Process the queue
    //    
    //    let mut mq_connection_w = mq_connection.clone();
    //    tokio::spawn(async move {
    //        let w_exchange_name = "amq.direct";
    //        let writing_routing_key = "writing_queue"; 
    //        let queue_name = "writing_queue";

    //        let mut mq_connection_w = mq_connection_w.lock().await;

    //        //declare new channel for background thread
    //        let w_channel_id = Some(6);
    //        let mut sub_channel = match mq_connection_w.add_channel(w_channel_id).await {
    //            Ok(c) => Some(c),
    //            Err(e) => {
    //                eprintln!("main: Error occurred while adding sub channel w/ id {} in writing thread: {}", w_channel_id.unwrap(), e);
    //                None
    //            }
    //        };
    //        println!("sub Channel Created on Writing Thread");

    //        //declare a new channel for publishing from background thread
    //        let pfw_channel_id = Some(7);
    //        let mut pub_channel = match mq_connection_w.add_channel(pfw_channel_id).await {
    //            Ok(c) => Some(c),
    //            Err(e) => {
    //                eprintln!("main: Error occurred while adding pub channel w/ id {} in writing thread: {}", w_channel_id.unwrap(), e);
    //                None
    //            }
    //        };
    //        assert!(sub_channel.is_some() == true);

    //        let queue_added = match mq_connection_w.add_queue(sub_channel.as_mut().unwrap(), queue_name, writing_routing_key, w_exchange_name).await {
    //            Ok(_) => {},
    //            Err(e) => {
    //                eprintln!("main: Error occurred while adding queue w/ name {}: {}", queue_name, e);
    //            }
    //        };
    //        assert_eq!(queue_added, ());
    //        println!("Queue Created on Writing Thread");

    //        let writing_queue = writing_queue.clone();
    //        match writing_queue.lock().await.process_queue(sub_channel.as_mut().unwrap(), pub_channel.as_mut().unwrap()).await {
    //            Ok(_) => {},
    //            Err(e) => {
    //                eprintln!("writing_queue::test_process - Error occurred while WritingQueue::processing queue: {}", e);
    //                assert!(false);
    //                
    //            }
    //        };
    //    });
    //    //Create fake queue item
    //    let e_content = String::from(
    //        format!(
    //            r#"
    //                {{
    //                    "publisher": "writing unit test",
    //                    "key": {:?}
    //                }}
    //            "#,
    //            contract.contract_name 
    //        )
    //    ).into_bytes();
    //    publish_to_queue(pub_channel.as_mut().unwrap(), "amq.direct", "writing_queue", e_content).await;

    //    //await processing of queue item
    //    
    //    //Verify the contract has been inserted into the db
    //    let observed_contract = match db_connection.lock().await.select_contract("TEST_CONTRACT").await {
    //        Ok(x) => {
    //            x
    //        },
    //        Err(e) => {
    //            println!("writing_queue::process_func - Error occurred while receiving contract from cache: {:?}", e);
    //            return;
    //        },
    //    };
    //    assert_eq!(observed_contract.last_price, contract.last_price);
    //    //delete the fake contract from the db
    //    let deleted_contract = db_connection.lock().await.delete_contract("TEST_CONTRACT").await;
    //    assert!(deleted_contract.is_ok());
    //}
}

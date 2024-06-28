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
    publish_next_queue: bool,
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
            publish_next_queue: true,
        }
    }

    pub fn toggle_publishing(&mut self) {
        self.publish_next_queue = !self.publish_next_queue;
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

    }

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
                let msg = "writing_queue::process_func - content was None!".to_string();
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
                if let Some(x) = x {
                    x
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

        if self.publish_next_queue {
            let next_key = contract.contract_name;
            let e_content = format!(
                    r#"
                        {{
                            "publisher": "writing",
                            "key": {:?}
                        }}
                    "#,
                    next_key 
            ).into_bytes();

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
        }

        Ok(())

    }
}

#[cfg(test)]
mod tests {
    use std::{default, error::Error, future};

    use super::*;
    use tokio::sync::mpsc;
    use crate::{mq::publish_to_queue, scraped_cache, err_loc};

    #[tokio::test(flavor = "multi_thread", worker_threads=3)]
    async fn test_process_func() {
        //Create an mq instance
        //Setup using the MQConnection struct
        let mut mq_connection = Arc::new(Mutex::new(MQConnection::new("localhost", 5672, "guest", "guest")));
        //open connection
        let conn_result = match mq_connection.lock().await.open().await {
            Ok(v) => v,
            Err(e) => {
                panic!("parsing_queue::test_process - Error occurred while opening connection: {} - {}", e, err_loc!());
                return;
            }
        };
        assert_eq!(conn_result, ());

        //Create publishing mpsc channel
        let (pub_tx, mut pub_rx) = mpsc::channel::<PubChannelCommand>(128);
        //Create subription mpsc channel
        let (sub_tx, mut sub_rx) = mpsc::channel::<SubChannelCommand>(128);
        // create a cache channel
        let (cache_tx, mut cache_rx) = mpsc::channel::<Command>(32);


        //Create publishing thread
        let mq_connection_p = mq_connection.clone();
        let t1 = tokio::spawn(async move {
            while let Some(cmd) = pub_rx.recv().await {
                match cmd {
                    PubChannelCommand::Publish { queue_name, content, resp } => {
                        //pass in current queue name and content to publish_to_next_queue func
                        //open a channel
                        //send content and return result of send
                        let response: Result<(), Box<dyn std::error::Error + Send>> = match mq_connection_p.lock().await.publish_to_next_queue(&queue_name, content).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                let msg = format!("Error while publishing from queue: {}", queue_name);
                                panic!("{} - {}", msg, err_loc!())
                            }
                        };
                        continue;
                    
                    }
                }
            }
        });
        
        //Create subscription thread
        let mq_connection_s = mq_connection.clone();
        let t2 = tokio::spawn(async move {
            while let Some(cmd) = sub_rx.recv().await {
                match cmd {
                    SubChannelCommand::Open { queue_name, resp } => {
                        //open a channel
                        //declare a queue on the channel
                        let channel: Result<Channel, Box<dyn std::error::Error + Send>> = match mq_connection_s.lock().await.add_sub_channel_and_queue(&queue_name).await {
                            Ok(v) => Ok(v),
                            Err(e) => {
                                panic!("Error while opening channel and adding queue: {} - {}", e, err_loc!());
                            }

                        };
                        //send channel back to sender
                        let res = match resp.send(channel) {
                            Ok(()) => (),
                            Err(e) => {
                                panic!("subscription_thread: Error while sending channel to queue: {} for subscribing - {}", &queue_name, err_loc!())

                            },
                        };
                        continue;
                    }
                    SubChannelCommand::Close { queue_name, channel, resp } => {
                        //close the channel passed in
                        let response: Result<(), Box<dyn std::error::Error + Send>> = match mq_connection_s.lock().await.close_channel(channel).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                panic!("writing_queue::test_process_func - Error while opening channel and adding queue: {} - {} - {}", queue_name, e, err_loc!());
                            }
                        };
                        continue;

                    }
                }
           }
        });


        //Create fake contract
        let mut contract = Contract::new();
        contract.contract_name = "TEST_CONTRACT".to_string();
        contract.last_price = 100.0;
        contract.strike = 140.0;

        
        //Instantiate cache
        let mut scraped_cache = Arc::new(tokio::sync::Mutex::new(ScrapedCache::new(100)));
        //Create caching thread
        tokio::spawn(async move {
           while let Some(cmd) = cache_rx.recv().await {
                match cmd {
                    scraped_cache::Command::Get { key, resp } => {

                        let res = scraped_cache.lock().await.get(&key).await.cloned();
                        //Switch out later

                        let respx = resp.send(Ok(res)); 
                        dbg!(respx);
                    }
                    scraped_cache::Command::Set { key, value, resp } => {
                        let res = scraped_cache.lock().await.set(key, value).await;

                        let _ = resp.send(Ok(()));

                    }
                }
               
           } 
        });

        let (cache_resp_tx, cache_resp_rx) = oneshot::channel();

        //Insert fake contract into cache
        let cmd = scraped_cache::Command::Set { 
            key: contract.contract_name.clone(), 
            value: contract.clone(), 
            resp: cache_resp_tx,
        };

        match cache_tx.send(cmd).await {
            Ok(_) => (),
            Err(e) => {
                panic!("writing_queue::test_process_func - Error while setting contract - {} - {}", e, err_loc!());
            }
        }

        // create a queue item and publish to the queue
        let queue_name = "writing_queue";
        let exchange_name = "amq.direct";

        //Instantiate db_connection
        let db_connection = Arc::new(Mutex::new(DBConnection::new("localhost", 5444, "postgres", "postgres", "scraped")));

        //open connection to db
        match db_connection.lock().await.open().await {
            Ok(_) => {}
            Err(e) => {
                panic!("{}, error: main: Error while opening connection to database: {}", err_loc!(), e);
            }
        }

        //Create fake queue item
        let content = String::from(
            format!(
                r#"
                    {{
                        "publisher": "writing_queue::test_process_func",
                        "key": {:?}
                    }}
                "#,
                contract.contract_name.clone() 
            )
        ).into_bytes();


        let (resp_tx, _) = oneshot::channel();

        //publish content to writing_queue NOTE: we pass in parsing queue here in order to send to
        //correct queue based on config
        let cmd = PubChannelCommand::Publish {
            queue_name: String::from("parsing_queue"),
            content,
            resp: resp_tx,

        };
        

        match pub_tx.send(cmd).await {
            Ok(_) => (),
            Err(e) => {
                panic!("parsing_queue::test_process - Error occurred while publishing to queue: {} - {}", e, err_loc!()); 
            }
        }



        //Process the queue
        let writing_pub_tx = pub_tx.clone();
        let writing_sub_tx = sub_tx.clone();
        let writing_cache_tx = cache_tx.clone();
        let writing_db = db_connection.clone();
        let writing_thread = tokio::spawn(async move {

            //Create a writing queue
            let mut writing_queue = WritingQueue::new(queue_name, "", exchange_name, writing_db, writing_sub_tx, writing_pub_tx, writing_cache_tx);

            //turn publishing to next queue off
            writing_queue.toggle_publishing();


            match writing_queue.process_queue().await {
                Ok(_) => {},
                Err(e) => {
                    panic!("writing_queue::test_process_func - Error occurred while WritingQueue::processing queue: {} - {}", e, err_loc!());
                }
            };
        });

        //await processing of queue item
        
        //Verify the contract has been inserted into the db
        let mut observed_contract: Option<Contract> = None;
        loop {
            let db_contract = match db_connection.lock().await.select_contract("TEST_CONTRACT").await {
                Ok(x) => {
                    x
                },
                Err(e) => {
                    let msg = format!("writing_queue::process_func - Error occurred while querying contract from database: {:?} - {}", e, err_loc!());
                    match e {
                        sqlx::Error::RowNotFound => {
                            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                            continue;
                        },
                        default => {
                            panic!("{}", msg);
                        }
                    }
                },
            };
            observed_contract = Some(db_contract);
            break;
            //handle error here
            //if db_contract.is_some() {
            //    break;
            //} else {
            //    tokio::time::sleep(Duration::from_millis(50)).await;
            //    continue;
            //}
        }
        let obs = observed_contract.unwrap();
        assert_eq!(obs.last_price, contract.last_price);
        //delete the fake contract from the db
        let deleted_contract = db_connection.lock().await.delete_contract("TEST_CONTRACT").await;
        assert!(deleted_contract.is_ok());
    }
}

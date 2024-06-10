extern crate amqprs;
extern crate tokio;
extern crate serde_json;
extern crate std;
extern crate async_trait;

use amqprs::channel::{BasicConsumeArguments, Channel, BasicAckArguments, BasicCancelArguments, ConsumerMessage};
use tokio::sync::{mpsc::Sender, oneshot, Mutex};
use serde_json::Value;
use std::{str, sync::Arc};
use async_trait::async_trait;

use crate::mq::{PubChannelCommand, SubChannelCommand};
pub use crate::scraped_cache::{ScrapedCache, Command};
use crate::db::DBConnection;
use crate::mq::{Queue, consume_from_queue, future_err};


pub struct CalcQueue<'a> {
    pub name: &'a str,  //Current name of the queue
    next_routing_key: &'a str, //queue name for publishing to the next queue
    next_exchange_name: &'a str, //Exchange name used for publishing to the next queue
    db_connection: Arc<Mutex<DBConnection<'a>>>,
    sub_tx: Sender<SubChannelCommand>,
    pub_tx: Sender<PubChannelCommand>,
    cache_tx: Sender<Command>, //tx used for sending /receiving contracts from cache
}

impl<'a> CalcQueue<'a> {
    pub fn new(queue_name: &'a str, next_routing_key: &'a str, next_exchange_name: &'a str, db_connection: Arc<Mutex<DBConnection<'a>>>, sub_tx: Sender<SubChannelCommand>, pub_tx: Sender<PubChannelCommand>, cache_tx: Sender<Command>) -> Self {
        Self {
            name: queue_name,
            next_routing_key,
            next_exchange_name,
            db_connection,
            pub_tx,
            sub_tx,
            cache_tx,
        }
    }

    pub async fn process_queue(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let args = BasicConsumeArguments::new(
            &self.name,
            "calc",
        )
        .manual_ack(true)
        .finish();

        match consume_from_queue(args, self.sub_tx.clone(), self).await {
              Ok(()) => (),
              Err(e) => {
                  let msg = format!("calc_queue::process_queue - error returned from
                  consume_from_queue: {}", e);
                  return Err(Box::from(msg));
              },
        };
        
        Ok(())
        
        //consuming behavior defined here
        //let (ctag, mut messages_rx) = channel.basic_consume_rx(args.clone()).await?;
        //while let Some(deliver) = messages_rx.recv().await {
        //    let content = match deliver.content {
        //        Some(x) => x,
        //        None => {
        //            println!("calc_queue::process_queue - None unwrapped from content");
        //            continue;
        //        },
        //    };

        //    //unserialize delivery
        //    let stringed_bytes = match str::from_utf8(&content) {
        //        Ok(stringed) => stringed,
        //        Err(e) => {
        //            let msg = format!("calc_queue::process_queue - stringing content bytes failed {}", e);
        //            println!("{}", msg);
        //            return Err(msg.into());
        //        },
        //    };

        //    let unserialized_content: Value = match serde_json::from_str(&stringed_bytes) {
        //        Ok(v) => v,
        //        Err(e) => {
        //            let msg = format!("calc_queue::process_queue - unserializing content into json failed {}", e);
        //            return Err(msg.into());
        //        },
        //    };


        //    println!("calc_queue::process_queue - Unserialized Content: {:?}", unserialized_content);

        //    if unserialized_content.is_null() || unserialized_content["key"].is_null() {
        //        println!("calc_queue::process_queue - key is null! for the following delivery: {}", unserialized_content);
        //        
        //        let delivery = match deliver.deliver {
        //            Some(x) => x,
        //            None => {
        //                println!("calc_queue::process_queue - deliver was None");
        //                continue;
        //            },

        //        };
        //        let args = BasicAckArguments::new(delivery.delivery_tag(), false);

        //        match channel.basic_ack(args).await {
        //            Ok(_) => {},
        //            Err(e) => {
        //                let msg = format!("calc_queue::process_queue - Error occurred while acking message after null content: {}", e);
        //                println!("{}", msg);
        //            }
        //        };
        //        continue;

        //    } else {
        //        //Add main consumer logic here
        //        
        //        //process_func()
        //        
        //        let delivery = match deliver.deliver {
        //            Some(x) => x,
        //            None => {
        //                println!("calc_queue::process_queue - deliver was None");
        //                continue;
        //            },

        //        };
        //        
        //        let args = BasicAckArguments::new(delivery.delivery_tag(), false);
        //        match channel.basic_ack(args).await {
        //            Ok(_) => {}
        //            Err(e) => {
        //                let msg = format!("calc_queue::process_queue - Error occurred while acking message after processing message: {}", e);
        //                println!("{}", msg);
        //            },
        //        };
        //        continue;
        //    }


        //}
        //if !channel.is_open() {
        //    println!("calc_queue::process_queue - Channel closed");
        //    dbg!(ctag);
        //    return Ok(());
        //    
        //} else if let Err(e) = channel.basic_cancel(BasicCancelArguments::new(&ctag)).await {
        //    let msg = format!("calc_queue::process_queue - Error occurred while cancelling consumer: {}", e);
        //    println!("{}", msg);
        //    return Err(msg.into());
        //} else {
        //    println!("calc_queue::process_queue - done");
        //    Ok(())
        //}

        
    }
}

#[async_trait]
impl<'a> Queue for CalcQueue<'a> {

    fn queue_name(&self) -> &str {
        self.name
    }

    fn args(&self) -> BasicConsumeArguments {
        BasicConsumeArguments::new(
            self.name,
            "calc",
        )
    }

    async fn process_func(&self, deliver: &ConsumerMessage) -> Result<(), Box<dyn std::error::Error + Send>> {
        //TODO: Implement once we figure out initial statistical model
        let content = match &deliver.content {
            Some(x) => x,
            None => {
                let msg = format!("calc_queue::process_func - content was None!");
                return Err(future_err(msg));
            },
        };

        //unserialize content
        let stringed_bytes = match str::from_utf8(&content) {
            Ok(stringed) => stringed,
            Err(e) => {
                let msg = format!("calc_queue::process_func - stringing content bytes failed {}", e);
                return Err(future_err(msg));
            },
        };
        let unserialized_content: Value = match serde_json::from_str(&stringed_bytes) {
            Ok(unser_con) => unser_con,
            Err(e) => {
                let msg = format!("calc_queue::process_func - unserializing content into json failed {}", e);
                return Err(future_err(msg));

            }
        };
        //Grab a contract names from the queue item and pull them from cache
        let contract_name = unserialized_content["key"].to_string().replace("\"", "");
        let msg = format!("calc_queue::process_func - contract name made it to calc queue: {}", contract_name.as_str());
        dbg!(msg);
        Ok(())
    }
}

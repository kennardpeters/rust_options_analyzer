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
                let msg = "calc_queue::process_func - content was None!".to_string();
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

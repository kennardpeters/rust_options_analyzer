extern crate amqprs;

use amqprs::channel::{BasicConsumeArguments, Channel};

pub use crate::scraped_cache::{ScrapedCache, Command};


pub struct CalcQueue<'a> {
    pub name: &'a str,  //Current name of the queue
    next_routing_key: &'a str, //queue name for publishing to the next queue
    next_exchange_name: &'a str, //Exchange name used for publishing to the next queue
    tx: tokio::sync::mpsc::Sender<Command> //tx used for sending /receiving contracts from cache
}

impl<'a> ParsingQueue<'a> {
    pub fn new(queue_name: &'a str, next_routing_key: &'a str, next_exchange_name: &'a str, tx: Sender<Command>) -> Self {
        Self {
            name: queue_name,
            next_routing_key,
            next_exchange_name,
            tx,
        }
    }

    //NEEDED?
    pub fn new_args(name: &str) -> BasicConsumeArguments {
        BasicConsumeArguments::new(
            name,
            "example_basic_pub_sub",
        )
    }

    pub async fn process_queue(&mut self, channel: &mut Channel, pub_channel: &mut Channel) -> Result<(), Box<dyn std::error::Error>> {
        let args = BasicConsumeArguments::new(
            name,
            "calc",
        ).manual_ack(false)
        .finish();

        //consuming behavior defined here
        let (ctag, mut messages_rx) = channel.basic_consume_rx(args.clone()).await?;
        while let Some(delivery) = messages_rx.recv().await {
            let content = match delivery.content {
                Some(x) => x,
                None => continue,
            };

            //unserialize delivery
            let stringed_bytes = match str::from_utf8(&content) {
                Ok(stringed) => stringed,
                Err(e) => {
                    let msg = format!("calc_queue::process_queue - stringing content bytes failed {}", e);
                    println!("{}", msg);
                    return Err(msg.into());
                },
            };

            let unserialized_content: Value = match serde_json::from_str(&stringed_bytes) {
                Ok(v) => v,
                Err(e) => {
                    let msg = format!("calc_queue::process_queue - unserializing content into json failed {}", e);
                    return Err(msg.into());
                },
            };


            println!("calc_queue::process_queue - Unserialized Content: {:?}", unserialized_content);

            if unserialized_content.is_null() || unserialized_content["key"].is_null() {
                println!("calc_queue::process_queue - key is null! for the following delivery: {}" unserialized_content);
                let args = BasicAckArguments::new(delivery.deliver.unwrap().delivery_tag(), false);

                match channel.basic_ack(args).await {
                    Ok(_) => {},
                    Err(e) => {
                        let msg = format!("calc_queue::process_queue - Error occurred while acking message after null content: {}" e);
                        println!("{}", msg);
                    }
                };
            } else {
                //Add main consumer logic here
                
                //process_func()
                

                let args = BasicAckArguments::new(deliver.deliver.unwrap().delivery_tag(), false);
                match channel.basic_ack(args).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg = format!("calc_queue::process_queue - Error occurred while acking message after processing message: {}", e);
                        println!("{}", msg);
                    },
                };
            }



        }
        
    }
}

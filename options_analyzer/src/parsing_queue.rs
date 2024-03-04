extern crate amqprs;
extern crate std;
extern crate serde_json;
extern crate async_trait;

use amqprs::{
    channel::{BasicAckArguments, BasicConsumeArguments, Channel},
    consumer::AsyncConsumer,
    BasicProperties,
    Deliver,
};
use std::{borrow::BorrowMut, ops::DerefMut, str};
use async_trait::async_trait;
use serde_json::Value;


pub use crate::mq::{Queue, publish_to_queue};

#[derive(Copy, Clone)]
pub struct ParsingQueue<'a> {
    pub name: &'a str, //Current name of queue
    routing_key: &'a str, // queue name for publishing to the next queue
    exchange_name: &'a str, //Exchange name used for publishing to the next queue
    parsing_consumer: Option<ParsingConsumer>,
}

impl<'a> ParsingQueue<'a> {
    pub fn new(queue_name: &'a str, routing_key: &'a str, exchange_name: &'a str) -> Self {
        Self {
            name: queue_name,
            routing_key,
            exchange_name,
            parsing_consumer: None,
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
    pub async fn process_queue(&mut self, channel: &mut Channel) {
        let args = BasicConsumeArguments::new(
            &self.name,
            "example_basic_pub_sub",
        );

        self.parsing_consumer = Some(ParsingConsumer::new(args.no_ack));


        //TODO: Handle unwrap here
        let consumer_tag = channel
            .basic_consume(self.parsing_consumer.unwrap(), args)
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

#[derive(Copy, Clone)]
struct ParsingConsumer {
    no_ack: bool,
    //additional fields as needed
}

impl ParsingConsumer {
    pub fn new(no_ack: bool) -> Self {
        Self { no_ack }
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
                println!("Stringing bytes failed!");
                ""
            },
        };
        //TODO: Handle unwrap here
        let unserialized_content: Value = serde_json::from_str(&stringed_bytes).unwrap();
        //Process func
        //parse data
        let e_content = String::from(
            r#"
                {
                    "publisher": "example",
                    "data": "Hello, from Example Queue"
                }
            "#,
        ).into_bytes();
        
        println!("The following data {} was received from {}", unserialized_content["data"], unserialized_content["publisher"]);
        dbg!(unserialized_content);

        //Insert into next queue
        publish_to_queue(channel, "amq.topic", "amqprs.example", e_content).await;

        //TODO: Move this to callback?

        let args = BasicAckArguments::new(deliver.delivery_tag(), false);

        channel.basic_ack(args).await.unwrap();
        println!("DELIVERY TAG: {}", deliver.delivery_tag())

    }
}

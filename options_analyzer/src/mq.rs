extern crate amqprs;
extern crate tracing_subscriber;
extern crate async_trait;
extern crate tracing;
extern crate serde_json;
extern crate std;

use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback}, channel::{
        BasicAckArguments, BasicConsumeArguments, BasicPublishArguments, Channel, QueueBindArguments, QueueDeclareArguments
    }, connection::{Connection, OpenConnectionArguments}, consumer::AsyncConsumer, AmqpChannelId, BasicProperties, Deliver
};
use tokio::time;
use std::{borrow::BorrowMut, str, sync::Arc};


use async_trait::async_trait;
use serde_json::Value;
#[cfg(feature = "traces")]
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::parsing_queue::{self, ParsingQueue};

const QUEUE_LIST: &[&str] = &["parse_queue"];

pub struct MQConnection<'a> {
    connection: Option<Connection>,
    pub host: &'a str,
    pub port: u16,
    pub username: &'a str,
    pub password: &'a str
}

impl<'a> MQConnection<'a> {
    pub fn new(
        host: &'a str,
        port: u16,
        username: &'a str,
        password: &'a str,
    ) -> MQConnection<'a> {
        Self { 
            connection: None,
            host,
            port,
            username,
            password,
        }

    }
    pub async fn open(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let connection = Connection::open(&OpenConnectionArguments::new(
            self.host,
            self.port,
            self.username,
            self.password,
        ))
        .await?;

        connection.register_callback(DefaultConnectionCallback)
        .await?;

        self.connection = Some(connection);

        //TODO: Make better return
        Ok(())
    }

    //TODO: Change return to Option or Result type
    //pub async fn add_channel(&self, channel_id: Option<AmqpChannelId>) -> Option<Channel> {
    pub async fn add_channel(&self, channel_id: Option<AmqpChannelId>) -> Result<Channel, Box<dyn std::error::Error>> {
        let connection = self.connection.clone();

        if connection.is_some() {

            let mut channel = connection.unwrap().open_channel(channel_id).await?;
    
            channel
                .register_callback(DefaultChannelCallback)
                .await?;
                //.unwrap();

            //return Some(channel);
            Ok(channel)
        } else {
            Err("Connection was None".into())
        }
    }

    pub async fn open_channel(&mut self) -> Arc<Option<Channel>> {
       //return channel on connection struct in order to run queues on separate channels
       ///Need to come up with a way to assign channel ID
       return Arc::new(Some(self.connection.as_mut().unwrap().open_channel(Some(1)).await.unwrap()))
    }

    //add queue method
    //TODO: Add error handling (return result type here)
    pub async fn add_queue(&mut self, channel: &mut Channel, queue_name: &str, routing_key: &str, exchange_name: &str) {
        //Declare queue
        let (queue_name, _, _) = channel
        .queue_declare(QueueDeclareArguments::durable_client_named(queue_name))
            .await
            .unwrap()
            .unwrap();
        

        channel
            .queue_bind(QueueBindArguments::new(
                &queue_name,
                exchange_name,
                routing_key,
            ))
            .await
            .unwrap();
    }



    //TODO: Add error handling (return result type here)
    pub async fn close_connections(&self) {
        self.connection.clone().expect("Connection was None").close().await.unwrap();
    }
}


pub struct ExampleConsumer {
   no_ack: bool, 
}

impl ExampleConsumer {
    pub fn new(no_ack: bool) -> Self {
        Self { no_ack }
    }
}

#[async_trait]
impl AsyncConsumer for ExampleConsumer {
    async fn consume(
        &mut self,
        channel: &Channel,
        deliver: Deliver,
        _basic_properities: BasicProperties,
        content: Vec<u8>,
    ) {
        #[cfg(feature = "traces")]
        info!(
            "consume delivery {} on channel {}, content: {}",
            deliver,
            channel,
            content,
        );
        let stringed_bytes = match str::from_utf8(&content) {
                    Ok(stringed) => stringed,
                    Err(e) => {
                        println!("Stringing bytes failed!");
                        println!("{}", e);
                        //Handle error below
                        panic!("{}", e);
                    },
                };
        let unserialized_content: Value = match serde_json::from_str(&stringed_bytes) {
            Ok(unser_con) => unser_con,
            Err(e) => {
                panic!("{}", e);
            }

        };

        println!("The following data {} was received in Example Consumer from {}", unserialized_content["data"], unserialized_content["publisher"]);
        

        dbg!(unserialized_content);


        let args = BasicAckArguments::new(deliver.delivery_tag(), false);

        channel.basic_ack(args).await.unwrap();

        //Insert into next queue
    }
}


pub async fn publish_example() {

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .try_init()
        .ok();

    
    //Open a connection (Migrate to new connection struct inside new method)
    let connection = Connection::open(&OpenConnectionArguments::new(
        "localhost",
        //"host.docker.internal",
        5672,
        "guest",
        "guest",
    ))
    .await
    .unwrap();

    connection.register_callback(DefaultConnectionCallback)
    .await
    .unwrap();

    //open a channel to the connection
    let channel = connection.open_channel(None).await.unwrap();
    channel
        .register_callback(DefaultChannelCallback)
        .await
        .unwrap();

    // declare a queue (Inside new queue)
    let (queue_name, _, _) = channel
        .queue_declare(QueueDeclareArguments::durable_client_named(
            "amqprs.examples.basic",
        ))
        .await
        .unwrap()
        .unwrap();

    // bind the queue to exchange
    let routing_key = "amqprs.example";
    let exchange_name = "amq.topic";
    channel
        .queue_bind(QueueBindArguments::new(
            &queue_name,
            exchange_name,
            routing_key,
        ))
        .await
        .unwrap();
    ///////////////////////////////////////////////
    // start consumer with given name
    let args = BasicConsumeArguments::new(
        &queue_name,
        "example_basic_pub_sub"
    );

    //Insert custom consumer into basic_consume
    channel
        //.basic_consume(DefaultConsumer::new(args.no_ack), args)
        .basic_consume(ExampleConsumer::new(args.no_ack), args)
        .await
        .unwrap();

    // publish message
    let content = String::from(
        r#"
            {
                "publisher": "example",
                "data": "Hello, amqprs!"
            }
        "#,
    ).into_bytes();

    //create arguments for basic_publish
    let args = BasicPublishArguments::new(exchange_name, routing_key);

    channel
        .basic_publish(BasicProperties::default(), content, args)
        .await
        .unwrap();


    time::sleep(time::Duration::from_secs(1)).await;
    //explicitly close
    channel.close().await.unwrap();
    connection.close().await.unwrap();
}

//Trait to implement for each queue step (Must implement AsyncConsumer Trait as well
#[async_trait]
pub trait Queue {
    //Declare a new queue
    //fn new()

    fn queue_name(&self) -> &str;
    //open new connection?
    //register callback
    //Open channel + defaultChannelCallback

    //fn new(channel)
    //declare the queue from the channel
    //bind the queue to the exchange
    //fn new(&mut self, queue_name: &str, channel: Channel, routing_key: &str, exchange_name: &str) -> Queue;

    fn args(&self) -> BasicConsumeArguments;

    //ProcessQueue()
    //Start a consumer on channel?
    //and run in the background
    //async fn process_queue(&mut self);


    //Consume func (Do we need this?
    //async fn consume(
    //    &mut self, 
    //    channel: &Channel, 
    //    deliver: Deliver, 
    //    _basic_properties: BasicProperties, 
    //    content: Vec<u8>
    //);
    //unserialize content
    //Process func
    //Insert into next queue
}



pub async fn publish_to_queue(channel: &Channel, exchange_name: &str, routing_key: &str, content: Vec<u8>) {
    //TODO: move this to the MQ connection struct in order to grab correct channel for publishing
    let args = BasicPublishArguments::new(exchange_name, routing_key);

    channel
        .basic_publish(BasicProperties::default(), content, args)
        .await
        .unwrap();
}

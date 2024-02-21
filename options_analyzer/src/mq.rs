extern crate amqprs;
extern crate tracing_subscriber;
extern crate async_trait;
extern crate tracing;
extern crate serde_json;
extern crate std;

use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback},
    channel::{
        self, BasicConsumeArguments, BasicPublishArguments, Channel, QueueBindArguments, QueueDeclareArguments
    },
    connection::{self, Connection, OpenConnectionArguments},
    consumer::AsyncConsumer,//DefaultConsumer, 
    BasicProperties,
    Deliver,
};
use tokio::time;
use std::str;


use async_trait::async_trait;
use serde_json::Value;
#[cfg(feature = "traces")]
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

const QUEUE_LIST: &[&str] = &["parse_queue"];

pub struct MQConnection {
    connection: Connection,
    channel: Channel, 
}

impl MQConnection {
    pub async fn new(
        host: &str,
        port: u16,
        username: &str,
        password: &str,
    ) -> Self {
        let connection = Connection::open(&OpenConnectionArguments::new(
            host,
            port,
            username,
            password,
        ))
        .await
        .unwrap();

        connection.register_callback(DefaultConnectionCallback)
        .await
        .unwrap();

        let channel = connection.open_channel(None).await.unwrap();
        channel
            .register_callback(DefaultChannelCallback)
            .await
            .unwrap();
        Self { 
            connection,
            channel,
        }

    }

    //add queue method
    async fn add_queue(&mut self, queue_name: &str) {
        //Declare queue
        let (queue_name, _, _) = self.channel
        .queue_declare(QueueDeclareArguments::durable_client_named(queue_name))
            .await
            .unwrap()
            .unwrap();
        
        
        //bind queue to exchange
        let routing_key = "amqprs.example";
        let exchange_name = "amq.topic";

        self.channel
            .queue_bind(QueueBindArguments::new(
                &queue_name,
                exchange_name,
                routing_key,
            ))
            .await
            .unwrap();
    }

    async fn drop(self) {
        self.channel.close().await.unwrap();
        self.connection.close().await.unwrap();
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
        let unserialized_content: Value = serde_json::from_str(&stringed_bytes).unwrap();

        println!("The following data {} was received from {}", unserialized_content["data"], unserialized_content["publisher"]);
        

        dbg!(unserialized_content);

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
trait Queue: AsyncConsumer {
    //Declare a new queue
    //fn new()
    //open new connection?
    //register callback
    //Open channel + defaultChannelCallback

    //fn new(channel)
    //declare the queue from the channel
    //bind the queue to the exchange
    fn new(&self);

    //ProcessQueue()
    //Start a consumer on channel?
    //and run in the background
    fn process_queue(&self);


    //Consume func
    async fn consume(
        &mut self, 
        channel: &Channel, 
        deliver: Deliver, 
        _basic_properties: BasicProperties, 
        content: Vec<u8>
    );
    //unserialize content
    //Process func
    //Insert into next queue
}



pub async fn publish_to_queue(channel: Channel, exchange_name: &str, routing_key: &str, content: Vec<u8>) {
    let args = BasicPublishArguments::new(exchange_name, routing_key);

    channel
        .basic_publish(BasicProperties::default(), content, args)
        .await
        .unwrap();
    //Close connection & Channel?
}

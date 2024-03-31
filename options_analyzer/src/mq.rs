extern crate amqprs;
extern crate tracing_subscriber;
extern crate async_trait;
extern crate tracing;
extern crate serde_json;
extern crate std;

use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback}, channel::{
        BasicAckArguments, BasicConsumeArguments, BasicPublishArguments, Channel, QueueBindArguments, QueueDeclareArguments
    }, 
    connection::{Connection, OpenConnectionArguments}, 
    consumer::AsyncConsumer, 
    AmqpChannelId, 
    BasicProperties, 
    Deliver
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

//MQConnection is a struct to handle storing and opening connections to the mq server
pub struct MQConnection<'a> {
    connection: Option<Connection>,
    pub host: &'a str,
    pub port: u16,
    pub username: &'a str,
    pub password: &'a str
}

impl<'a> MQConnection<'a> {
    //new method is a constructor for creating a new mq connection
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

    //open method for opening a connection to the mq server based on the host, port, username, and
    //password passed into the mq struct
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

    //add channel method for adding a channel to an mq connection and returning the new channel
    pub async fn add_channel(&self, channel_id: Option<AmqpChannelId>) -> Result<Channel, Box<dyn std::error::Error>> {
        let connection = self.connection.clone();

        if connection.is_some() {

            let mut channel = connection.unwrap().open_channel(channel_id).await?;
    
            channel
                .register_callback(DefaultChannelCallback)
                .await?;

            Ok(channel)
        } else {
            Err("mq::add_channel: Connection was None".into())
        }
    }

    //add queue method for adding a queue to the current channel
    pub async fn add_queue(&mut self, channel: &mut Channel, queue_name: &str, routing_key: &str, exchange_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        //Declare queue
        let option_args = match channel.queue_declare(QueueDeclareArguments::durable_client_named(queue_name)).await {            
            Ok(v) => {
                if v.is_some() {
                    v
                } else {
                   None 
                }
            },
            Err(e) => {
                eprintln!("mq::add_queue - Error occurred while declaring queue: {}", e);
                None
            } 
        };
        if option_args.is_none() {
            return Err("mq::add_queue - Queue was None after declaring".into()); 
        }
        

        channel
            .queue_bind(QueueBindArguments::new(
                &queue_name,
                exchange_name,
                routing_key,
            ))
            .await?;

        Ok(())
    }
    
    //close_connections closes all connections to the MQ server
    pub async fn close_connections(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.connection.clone().expect("mq::close_connections - Connection was None while closing").close().await?;

        Ok(())
    }
}

///publish_to_queue publishes a message to a queue using the channel, exchange, and routing key
pub async fn publish_to_queue(channel: &Channel, exchange_name: &str, routing_key: &str, content: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
    //TODO: move this to the MQ connection struct in order to grab correct channel for publishing
    let args = BasicPublishArguments::new(exchange_name, routing_key);

    match channel
        .basic_publish(BasicProperties::default(), content, args)
        .await {
            Ok(_) => {}
            Err(e) => {
                let msg = format!("mq::publish_to_queue - Error occurred while publishing message: {}", e);
                return Err(msg.into());
            },
        }

    Ok(())
}

//Trait to implement for each queue step (Must implement AsyncConsumer Trait as well
//TODO: Refactor this trait to new structure
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

//Requires RabbitMQ to be running
#[cfg(test)]
mod tests {
    use amqprs::consumer::DefaultConsumer;

    use super::*;
    #[tokio::test]
    async fn test_add_queue() {
        //Setup using the MQConnection struct
        let mut mq = MQConnection::new("localhost", 5672, "guest", "guest");
        let conn_result = match mq.open().await {
            Ok(v) => v,
            Err(e) => {
                println!("mq::test_add_queue - Error occurred while opening connection: {}", e);
                return;
            }
        };
        assert_eq!(conn_result, ());
        let mut channel = match mq.add_channel(Some(2)).await {
            Ok(v) => v,
            Err(e) => {
                println!("mq::test_add_queue - Error occurred while adding channel: {}", e);
                return;
            }
        };

        

        //Main function we are testing
        let res = match mq.add_queue(&mut channel, "test_queue", "test_routing_key", "test_exchange").await {
            Ok(v) => v,
            Err(e) => {
                println!("mq::test_add_queue - Error occurred while adding queue: {}", e);
                return;
            }
        };
        assert_eq!(res, ());


        //verify that queue exists on channel/exchange
        let mut client_channel = match mq.add_channel(None).await {
            Ok(v) => v,
            Err(e) => {
                println!("mq::test_add_queue - Error occurred while adding channel: {}", e);
                return;
            }
        };

        // Attempt to declare the queue with passive option
        let queue_name = "test_queue";
        let client_args = QueueDeclareArguments::durable_client_named(queue_name).passive(true).finish();
        let queue_res = match client_channel.queue_declare(client_args).await {
            Ok(_) => println!("Queue {} exists", queue_name),
            Err(e) => {
                println!("Queue {} does not exist: {}", queue_name, e);
            }
        };
        assert_eq!(queue_res, ());

        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        channel.close().await.unwrap();
        client_channel.close().await.unwrap();
        mq.close_connections().await.unwrap();

        
    }
    
    #[tokio::test]
    async fn test_publish_to_queue() {
        //TODO:
        let mut mq = MQConnection::new("localhost", 5672, "guest", "guest");

        let conn_result = match mq.open().await {
            Ok(v) => v,
            Err(e) => {
                println!("mq::test_add_queue - Error occurred while opening connection: {}", e);
                return;
            }
        };
        assert_eq!(conn_result, ());
        let mut channel = match mq.add_channel(Some(2)).await {
            Ok(v) => v,
            Err(e) => {
                println!("mq::test_add_queue - Error occurred while adding channel: {}", e);
                return;
            }
        };

        

        //Main function we are testing
        let res = match mq.add_queue(&mut channel, "test_queue", "test_routing_key", "test_exchange").await {
            Ok(v) => v,
            Err(e) => {
                println!("mq::test_add_queue - Error occurred while adding queue: {}", e);
                return;
            }
        };

        let args = BasicConsumeArguments::new(
            "test_queue",
            "test_consumer_tag",
        );

        let test_content = String::from(
            r#"
                {
                    "publisher": "cargo test",
                    "data": "test_junk"
                }
            "#,
        ).into_bytes();

        let _consumer = match channel.basic_consume(TestConsumer::new(args.no_ack, test_content.clone()), args).await {
            Ok(v) => v,
            Err(e) => {
                println!("mq::test_add_queue - Error occurred while adding consumer: {}", e);
                return;
            }
        };

        // publish message



        let pub_result = publish_to_queue(&mut channel, "test_exchange", "test_routing_key", test_content).await.unwrap();
        assert_eq!(pub_result, ());
        //check if mq was published using a fake consumer
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        channel.close().await.unwrap();
        mq.close_connections().await.unwrap();
    }

}

//Test Consumer used for mq testing
pub struct TestConsumer {
    no_ack: bool,
    expected_content: Vec<u8>, 
}

impl TestConsumer {
    pub fn new(no_ack: bool, expected_content: Vec<u8>) -> Self {
        Self { 
            no_ack, 
            expected_content
        }

    }
}

#[async_trait]
impl AsyncConsumer for TestConsumer {
    async fn consume(
        &mut self,
        channel: &Channel,
        deliver: Deliver,
        _basic_properities: BasicProperties,
        content: Vec<u8>,
    ) {
        assert_eq!(self.expected_content, content);

        let args = BasicAckArguments::new(deliver.delivery_tag(), false);

        channel.basic_ack(args).await.unwrap();

    }
}

//Deprecated Struct to implement previous consumer pattern
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

///publish_example is an example of how to publish and consume from rabbitmq using the amqprs library
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


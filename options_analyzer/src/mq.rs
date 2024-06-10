extern crate amqprs;
extern crate tracing_subscriber;
extern crate async_trait;
extern crate tracing;
extern crate serde_json;
extern crate std;
extern crate futures;

use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback}, 
    channel::{
        BasicAckArguments, BasicCancelArguments, BasicConsumeArguments, BasicNackArguments, BasicPublishArguments, Channel, ConsumerMessage, QueueBindArguments, QueueDeclareArguments
    }, 
    connection::{Connection, OpenConnectionArguments}, 
    consumer::AsyncConsumer, 
    AmqpChannelId, 
    BasicProperties, 
    Deliver
};
use futures::{channel::mpsc::SendError, executor::block_on};
use tokio::{sync::{futures as tokio_futures, mpsc::Sender, oneshot}, time};
use std::{borrow::BorrowMut, error::Error, fmt::{Display,Formatter}, str, sync::Arc, fs, collections::HashMap, future::Future};


use async_trait::async_trait;
use serde_json::Value;
#[cfg(feature = "traces")]
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

//TODO: Figure out tracing here:
//#[cfg(feature = "traces")]
//use tracing::info;
//use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::{config_parse, parsing_queue::{self, ParsingQueue}};
use crate::types::Responder;

//TODO: Replaced with config file
const DEFAULT_EXCHANGE: &str = "amq.direct";


//SubChannelCommand enum is used to opening subscription channels, declaring queues and closing subscription channels to the mq server
pub enum SubChannelCommand {
    Open {
        queue_name: String,
        resp: Responder<Channel, Box<dyn std::error::Error + Send>>
    },
    Close {
        queue_name: String,
        channel: Channel,
        resp: Responder<(), Box<dyn std::error::Error + Send>>
    },
}

//PubChannelCommand enum is used to open and close publishing channels to the mq server
pub enum PubChannelCommand {
    Publish {
        queue_name: String,
        content: Vec<u8>, //Possibly change to bytes
        resp: Responder<(), Box<dyn std::error::Error + Send>>,
    },
}

//Error wrapping for thread safety
//#[derive(Debug, Clone)]
//struct AsyncError {
//    source: Option<Box<Self>>,
//    desc: String,
//}
//
//impl <E: Error + ?Sized> From<&E> for AsyncError {
//    fn from(err: &E) -> Self {
//        AsyncError {
//            source: err.source().map(|x| Box::new(x.into())),
//            desc: format!("{err}")
//        }
//    }
//}
//
//impl <E: amqprs::error::Error + ?Sized> From<&E> for AsyncError {
//    fn from(err: &E) -> Self {
//        AsyncError {
//            source: err.source()
//        }
//    }
//}
//
//impl Error for AsyncError {
//    fn source(&self) -> Option<&(dyn Error + 'static)> {
//       self.source().as_ref().map(|x| &**x as &dyn Error) 
//    }
//
//    fn description(&self) -> &str {
//        self.desc.as_str()
//    }
//
//    fn cause(&self) -> Option<&dyn Error> {
//        self.source().as_ref().map(|x| &**x as &dyn Error)
//    }
//}
//
//impl Display for AsyncError {
//    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
//        write!(f, "{}", self.desc)
//    }
//}

//MQConnection is a struct to handle storing and opening connections to the mq server
pub struct MQConnection<'a> {
    connection: Option<Connection>,
    queue_sequence: Option<HashMap<i64, String>>,
    //map of pub_channels or just one here?
    pub host: &'a str,
    pub port: u16,
    pub username: &'a str,
    pub password: &'a str
    //sub_rx?
    //pub_rx?
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
            queue_sequence: None,
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

        //Grab queue list from config file
        let queue_sequence: HashMap<String, String> = match config_parse::get_queue_sequence() {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("mq::open(): Error while getting queue sequence - {}", e);
                return Err(Box::from(msg));
            },
        };
        let mut parsed_queue_sequence: HashMap<i64, String> = HashMap::new(); 
        //TODO: change hackey structure to more refined approach
        for (key, value) in queue_sequence.into_iter() {
            if key.contains('0') {
                parsed_queue_sequence.insert(0, value.clone());
            }
            if key.contains('1') {
               parsed_queue_sequence.insert(1, value.clone());
            }
            if key.contains('2') {
                parsed_queue_sequence.insert(2, value.clone());
            }
            if key.contains('3') {
                parsed_queue_sequence.insert(3, value.clone());
            }

            
        }
        dbg!(&parsed_queue_sequence);
        self.queue_sequence = Some(parsed_queue_sequence);

        self.connection = Some(connection);

        Ok(())
    }

    //add_sub_channel_and_queue is used by an mq consumer thread to open a channel to the
    //MQConnection, to declare a queue to consume from, and to return the channel to be used by the
    //consumer
    pub async fn add_sub_channel_and_queue(&self, queue_name: &str) -> Result<Channel, Box<dyn std::error::Error + Send>> {
        let connection = match self.connection.clone() {
            Some(x) => x,
            None => {
                //TODO: Add backtrace macro here
                let msg = format!("mq::add_sub_channel_and_queue: Connection was None while adding channel for Queue: {}", queue_name);
                return Err(future_err(msg));
            },
        };

        let exchange: &str = DEFAULT_EXCHANGE;
        
        let mut channel = match connection.open_channel(None).await {
            Ok(v) => v,
            Err(e) => {
                    //TODO: Add backtrace macro here
                    let msg = format!("mq::add_sub_channel_and_queue - Error occurred while opening channel: {}", e);
                    return Err(future_err(msg));
            },
        };
        
        match channel
            .register_callback(DefaultChannelCallback)
            .await {
            Ok(_) => (),
            Err(e) => {
                let msg = format!("mq::add_sub_channel_and_queue - Error occurred while registering callback: {}", e);
                return Err(future_err(msg));
            },
        };
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
                eprintln!("mq::add_sub_channel_and_queue - Error occurred while declaring queue: {}", e);
                None
            } 
        };
        if option_args.is_none() {
            let msg = format!("mq::add_sub_channel_and_queue - Queue: {} was None after declaring", queue_name);
            return Err(future_err(msg)); 
        }
        

        match channel
            .queue_bind(QueueBindArguments::new(
                &queue_name,
                exchange,
                &queue_name,
            ))
            .await {
                Ok(()) => (),
                Err(e) => {
                    let msg = format!("mq::add_sub_channel_and_queue - Error while binding queue: {} to exchange: {}", queue_name, exchange);
                    return Err(future_err(msg)); 
                }
            };
        

        Ok(channel)
    }
    
    //add_pub_channel_and_queue is used by an mq publishing thread to open a channel to the
    //MQConnection, to declare a queue to publish to, and to return the channel to be used by the
    //publisher
    pub async fn add_pub_channel_and_queue(&self, queue_name: &str) -> Result<Channel, Box<dyn std::error::Error + Send>> {
        let connection = match self.connection.clone() {
            Some(x) => x,
            None => {
                //TODO: Add backtrace macro here
                let msg = format!("mq::add_sub_channel_and_queue: Connection was None while adding channel for Queue: {}", queue_name);
                return Err(future_err(msg));
            },
        };

        let exchange: &str = DEFAULT_EXCHANGE;
        
        let mut channel = match connection.open_channel(None).await {
            Ok(v) => v,
            Err(e) => {
                    //TODO: Add backtrace macro here
                    let msg = format!("mq::add_sub_channel_and_queue - Error occurred while opening channel: {}", e);
                    return Err(future_err(msg));
            },
        };
        
        match channel
            .register_callback(DefaultChannelCallback)
            .await {
            Ok(_) => (),
            Err(e) => {
                let msg = format!("mq::add_sub_channel_and_queue - Error occurred while registering callback: {}", e);
                return Err(future_err(msg));
            },
        };
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
                eprintln!("mq::add_sub_channel_and_queue - Error occurred while declaring queue: {}", e);
                None
            } 
        };
        if option_args.is_none() {
            let msg = format!("mq::add_sub_channel_and_queue - Queue: {} was None after declaring", queue_name);
            return Err(future_err(msg)); 
        }
        

        //match channel
        //    .queue_bind(QueueBindArguments::new(
        //        &queue_name,
        //        exchange,
        //        &queue_name,
        //    ))
        //    .await {
        //        Ok(()) => (),
        //        Err(e) => {
        //            let msg = format!("mq::add_sub_channel_and_queue - Error while binding queue: {} to exchange: {}", queue_name, exchange);
        //            return Err(future_err(msg)); 
        //        }
        //    };
        

        Ok(channel)
    }

    ///add_channel method for adding a channel to an mq connection and returning the new channel
    pub async fn add_channel(&self, channel_id: Option<AmqpChannelId>) -> Result<Channel, Box<dyn std::error::Error>> {
        let connection = match self.connection.clone() {
            Some(x) => x,
            None => {
                return Err("mq::add_channel: Connection was None".into())
            },
        };

        let mut channel = match connection.open_channel(channel_id).await {
            Ok(v) => v,
            Err(e) => {
                    let msg = format!("mq::add_channel - Error occurred while opening channel: {}", e);
                    return Err(msg.into());
            },
        };
    
        match channel
            .register_callback(DefaultChannelCallback)
            .await {
            Ok(_) => (),
            Err(e) => {
                let msg = format!("mq::add_channel - Error occurred while registering callback: {}", e);
                return Err(msg.into());
            }
        };
        
        Ok(channel)
    }

    //add queue method for adding a queue to the current channel
    pub async fn add_queue(&mut self, channel: &Channel, queue_name: &str, routing_key: &str, exchange_name: &str) -> Result<(), Box<dyn std::error::Error>> {
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

    ///publish_to_queue publishes a message to a queue using the channel, exchange, and routing key
    //Refactor to take in a Queue trait and publish based off shared properties
    pub async fn publish_to_next_queue(&self, queue_name: &str, content: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        //TODO: implement function to grab publishing channel

        let exchange_name = DEFAULT_EXCHANGE;
        let next_queue = match self.get_next_queue(queue_name) {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("mq::MQConnection::publish_to_next_queue() - error getting next queue from sequence: {}", e);
                return Err(Box::from(msg));
            }
        };
        let args = BasicPublishArguments::new(exchange_name, next_queue);

        //TODO: implement grabbing publishing channel if it doesn't exist for each queue
        let channel = match self.add_pub_channel_and_queue(next_queue).await {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("mq::MQConnection::publish_to_next_queue() - error getting channel for publishing: {}", e);
                return Err(Box::from(msg));
            }

        };
    
        match channel
            .basic_publish(BasicProperties::default(), content, args)
            .await {
                Ok(_) => {},
                Err(e) => {
                    let msg = format!("mq::publish_to_queue - Error occurred while publishing message: {}", e);
                    return Err(msg.into());
                },
            }

        match self.close_channel(channel).await {
            Ok(_) => {},
            Err(e) => {
                let msg = format!("mq::publish_to_queue - Error occurred while closing publishing channel: {}", e);
                return Err(msg.into());
            }
        };

    
        Ok(())
    }

    //close_channel closes the channel passed in as an argument (note the channel is moved)
    pub async fn close_channel(&self, channel: Channel) -> Result<(), Box<dyn std::error::Error + Send>> {
        let res = match channel.close().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                let msg = format!("mq::MQConnection::close_channel() - Error while closing channel: {}", e);
                return Err(future_err(msg))
            }
        };
    }

    //close_connections closes all connections to the MQ server
    pub async fn close_connections(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.connection.clone().expect("mq::close_connections - Connection was None while closing").close().await?;

        Ok(())
    }
    // get_next_queue returns the next queue in a sequence given the current queue_name
    fn get_next_queue(&self, current_queue: &str) -> Result<&str, Box<dyn std::error::Error>> {
        let queue_sequence = match &self.queue_sequence {
            Some(v) => v,
            None => {
                let err = Box::from("mq::get_next_queue - Queue sequence not found!");
                return Err(err);
            }
        };
        let seq_len: i64 = queue_sequence.len() as i64;
        for (key, value) in queue_sequence.into_iter() {
            if value.as_str() == current_queue && *key != seq_len - 1 {
                let next_key = *key + 1;
                let next_queue = match queue_sequence.get(&next_key) {
                    Some(v) => v,
                    None => {
                        return Err(Box::from("mq::get_next_queue - Next queue was unwrapped as None"));
                    },
                };
                //let msg = format!("{}'s next queue: {}", current_queue, next_queue);
                //dbg!(msg);
                return Ok(next_queue.as_str());
            } else if value.as_str() == current_queue {
                println!("mq::get_next_queue - last queue in sequence");
                return Ok("")
            }
        }

        let err = Box::from("mq::get_next_queue - current_queue not found in queue sequence");
        Err(err)
    }
}


//TODO: Convert into more generic struct: Bandaid to create errors which implements the SEND trait 
pub fn future_err(msg: String) -> Box<futures::io::Error> {
    //TODO: Add backtrace macro here
    let custom_error = std::io::Error::other(msg);
    let fut_err: futures::io::Error = futures::io::Error::from(custom_error);
    Box::new(fut_err)
}

///publish_to_queue publishes a message to a queue using the channel, exchange, and routing key
//Refactor to take in a Queue trait and publish based off shared properties
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

//consume_from_queue graps deliveries from mq receiver channel 
// and inserts them into the given process_func
// handle acking/nacking here depending on result of process_func
//pub async fn consume_from_queue<F, Fut>(consumer_args: BasicConsumeArguments, sub_tx: Sender<SubChannelCommand>, process_func: impl FnMut(&ConsumerMessage) -> Fut) -> Result<(), Box<dyn std::error::Error>> 
//where 
//    Fut: Future<Output = Result<(), Box<dyn std::error::Error>>>,
//{
pub async fn consume_from_queue(consumer_args: BasicConsumeArguments, sub_tx: Sender<SubChannelCommand>, queue: &impl Queue) -> Result<(), Box<dyn std::error::Error>>  {

    //open channel here
    //Possibly convert to helper function //input (sub_tx, consumer_args) //output: Result<Channel,
    //Error)
    let (resp_tx, resp_rx) = oneshot::channel();
    //Convert MQ_connection to communicate via mpsc instead?
    let cmd = SubChannelCommand::Open { 
        queue_name: consumer_args.queue.clone(), 
        resp: resp_tx,
    };
    match sub_tx.send(cmd).await {
        Ok(()) => (),
        Err(e) => {
            let msg = format!("mq::consume_from_queue: Error while sending subscription thead: {}", e);
            return Err(msg.into());
        },
    };
    let resp = match resp_rx.await {
        Ok(x) => x,
        Err(e) => {
            let msg = format!("mq::consume_from_queue: Error while receiving from subscription channel: {}", e);
            return Err(msg.into());
        }

    };

    let channel = match resp {
        Ok(channel) => channel,
        Err(e) => {
            let msg = format!("mq::consume_from_queue: Error received from subscription channel: {}", e);
            return Err(msg.into());
        }
    };
    /////



    //declare while loop consuming from messages_rx
    //consuming behavior defined here
    let (ctag, mut messages_rx) = match channel.basic_consume_rx(consumer_args).await {
        Ok(v) => v,
        Err(e) => {
            return Err(e.into())
        },
    };

    //Should I convert or wrap it in a select statement?
    while let Some(deliver) = messages_rx.recv().await {
        //TODO: Add await once we figure out how to pass an async function as arg
        match queue.process_func(&deliver).await {
            Ok(()) => {
                //ack delivery 
                match ack_delivery(deliver, &channel).await {
                    Ok(()) => (),
                    Err(e) => {
                        let msg = format!("mq::consume_from_queue: Error while acking delivery {}",e);
                        return Err(Box::from(msg));
                    }
                }
            },
            Err(e) => {
                //nack delivery (TODO: requeue after logic is hammered out)
                let msg = format!("mq::consume_from_queue: Error return from consuming queue - {}", e);
                println!("{}", msg);
                match nack_delivery(deliver, &channel).await {
                    Ok(()) => (),
                    Err(e) => {
                        let msg = format!("mq::consume_from_queue: Error while nacking delivery {}",e);
                        return Err(Box::from(msg));
                    }
                }
            },
        };
    }

    //time::sleep(time::Duration::from_secs(30)).await;

    //dbg

    //Need a context like struct here
    if let Err(e) = channel.basic_cancel(BasicCancelArguments::new(&ctag)).await {
        let msg = format!("parsing_queue::process_queue - Error occurred while cancelling consumer: {}", e);
        println!("{}", msg);
        return Err(msg.into());
    }
    
    let msg = format!("{} queue closed: may need to need to wrap consumption in another loop", queue.queue_name());

    println!("{}", msg);

    Ok(())
}

async fn ack_delivery(deliver: ConsumerMessage, channel: &Channel) -> Result<(), Box<dyn std::error::Error>> {

    //Unwrap delivery
    let delivery = match deliver.deliver {
        Some(x) => x,
        None => {
            return Err(Box::from("mq::ack_delivery - deliver was None"))
        },

    };

    let args = BasicAckArguments::new(delivery.delivery_tag(), false);

    match channel.basic_ack(args).await {
        Ok(_) => {},
        Err(e) => {
            let msg = format!("mq::ack_delivery - Error occurred while acking message: {}", e);
            return Err(Box::from(msg));
        },
    };

    Ok(())
}


async fn nack_delivery(deliver: ConsumerMessage, channel: &Channel) -> Result<(), Box<dyn std::error::Error>> {

    //Unwrap delivery
    let delivery = match deliver.deliver {
        Some(x) => x,
        None => {
            return Err(Box::from("mq::nack_delivery - deliver was None"))
        },

    };

    //Requeue = 3rd argument: TODO set to true here after fixing logic
    let args = BasicNackArguments::new(delivery.delivery_tag(), false, false);

    //Perform nack
    match channel.basic_nack(args).await {
        Ok(_) => {},
        Err(e) => {
            let msg = format!("mq::nack_delivery - Error occurred while nacking message: {}", e);
            return Err(Box::from(msg));
        },
    };

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
    async fn process_func(&self, deliver: &ConsumerMessage) -> Result<(), Box<dyn std::error::Error + Send>>;


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

//Requires RabbitMQ container to be running
#[cfg(test)]
mod tests {
    use amqprs::consumer::DefaultConsumer;

    use super::*;

    use reqwest::Client;
    use serde_json::Value;
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
        let res = match mq.add_queue(&mut channel, "test_queue", "test_routing_key", "amq.direct").await {
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

        let client = Client::new();

        //Should be formatted like this: http://localhost:15672/api/queues/%2F/test_queue/bindings
        let url = format!("http://localhost:15672/api/queues/%2F/{}/bindings", queue_name);

        // Verify that the queue exists via management API
        let res = client.get(&url)
            .basic_auth("guest", Some("guest"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        let res_json: Value = match serde_json::from_str(&res) {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("Error while unserializing into Value: {:?}", e);
                eprintln!("{:?}", msg);
                panic!("{:?}", msg);
            }
        };
        let bindings = match res_json.as_array() {
            Some(v) => v,
            None => {
                let msg = format!("Unwrapped none while converting json to array");
                eprintln!("{:?}", msg);
                panic!("{:?}", msg);
            }
        };
        let is_bound = bindings.iter().any(|b| {
            b["source"].as_str().unwrap() == "amq.direct"
        });
        assert!(is_bound);

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
        let res = match mq.add_queue(&mut channel, "test_queue", "test_routing_key", "amq.direct").await {
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



        let pub_result = publish_to_queue(&mut channel, "amq.direct", "test_routing_key", test_content).await.unwrap();
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


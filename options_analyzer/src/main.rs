#![allow(unused)]
use std::{borrow::{Borrow, BorrowMut}, env, ops::Deref, process, sync::{Arc, Mutex}, time::Instant};

use tokio::{sync::oneshot, time::{self, interval, Duration}};
use amqprs::channel::Channel;
use mq::MQConnection;
use parsing_queue::ParsingQueue;
use tokio::signal;
use tokio::sync::{mpsc, Notify};
use crate::scraped_cache::ScrapedCache;
use serde_json::Value;
use amqprs::channel::{BasicAckArguments, BasicCancelArguments};
use sqlx::postgres::PgPool;
use dotenv::dotenv;
use futures::{executor::block_on, future::join_all};
use tracing::{debug, error, info, warn};


pub mod scraped_cache;
pub mod db;
pub mod mq;
pub mod types;
pub mod options_scraper;
pub mod writing_queue;
pub mod parsing_queue;
pub mod calc_queue;
pub mod stream_queue;
pub mod config_parse;
pub use mq::Queue;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let now = Instant::now();

    //TODO: Possibly move setting/parsing of env files to another function/file
    env::set_var("RUST_BACKTRACE", "full");
    env::set_var("RUST_LOG", "debug");

    //Grab symbol from environment variables
    dotenv().ok();

    let symbol = match env::var("SYMBOL") {
        Ok(v) => {
            if v != "" {
                v
            } else {
                panic!("{}, error: {}", err_loc!(), "SYMBOL was empty in environment");
            }
        },
        Err(e) => panic!("SYMBOL not found in environment"),
    };
    //// end of env region

    //"host.docker.internal" 
    //Declare threadsafe mq connection struct to handle connecting and publishing to rabbitmq
    let mut mq_connection = Arc::new(tokio::sync::Mutex::new(MQConnection::new("localhost", 5672, "guest", "guest")));
    println!("MQ Connection Created");

    //Declare thread-safe db connection struct to handle database calls
    let mut db_connection = Arc::new(tokio::sync::Mutex::new(db::DBConnection::new("localhost", 5444, "postgres", "postgres", "scraped")));
    println!("DB Pool Created");


    //Caching channel (need to clone tx for each additional thread)
    let (cache_tx, mut cache_rx) = mpsc::channel::<scraped_cache::Command>(128);

    //MQ channel for opening/ closing mq channels for subscribing
    let (sub_tx, mut sub_rx) = mpsc::channel::<mq::SubChannelCommand>(128);

    //MQ channel for opening/ closing mq channels for publishing 
    let (pub_tx, mut pub_rx) = mpsc::channel::<mq::PubChannelCommand>(128);

    let mut contract_cache = Arc::new(tokio::sync::Mutex::new(scraped_cache::ScrapedCache::new(1000)));
    println!("Scraped Cache Created");


    match mq_connection.lock().await.open().await {
        Ok(_) => {}
        Err(e) => {
            eprintln!("{}, error: main: Error while opening connection to rabbitmq: {}", err_loc!(), e);
            process::exit(1);
        }
    }
    println!("MQ connection opened");

    match db_connection.lock().await.open().await {
        Ok(_) => {}
        Err(e) => {
            eprintln!("{}, error: main: Error while opening connection to database: {}", err_loc!(), e);
            process::exit(1);
        }
    }
    println!("DB connection opened");
    
    //TODO: Convert this to another form of input (Cmd line arg, csv, or initiated by front-end?) 
    let content = String::from(
        format!(r#"
            {{
                "publisher": "main",
                "symbol": {:?} 
            }}
        "#,
        symbol)
    ).into_bytes();
    println!("content created");


    //Caching thread
    let t1 = tokio::spawn(async move {
       while let Some(cmd) = cache_rx.recv().await {
            match cmd {
                scraped_cache::Command::Get { key, resp } => {

                    //Perform action
                    let res = contract_cache.lock().await.get(&key).await.cloned();

                    //Send response of action back to sender via tx passed in
                    let respx = match resp.send(Ok(res.clone())) {
                        Ok(()) => (),
                        Err(e) => {
                            let msg = format!("caching_thread: Error while sending response: {:?} to get request", res);
                            print!("{}", msg);
                            process::exit(1);
                        }
                    };
                    continue;
                }
                scraped_cache::Command::Set { key, value, resp } => {

                    //Perform action
                    let res = contract_cache.lock().await.set(key, value).await;

                    //Send response of action back to sender via tx passed in
                    let respx = match resp.send(Ok(())) {
                        Ok(()) => (),
                        Err(e) => {
                            let msg = format!("caching_thread: Error while sending response: {:?} to set request", res);
                            print!("{}", msg);
                            process::exit(1);
                        }

                    };
                    continue;
                }
            }
           
       } 
    });

    //Intention is to only share the MQ connection struct between the following two tokio threads
    let mq_connection_s = mq_connection.clone();
    let t5 = tokio::spawn(async move {
        while let Some(cmd) = sub_rx.recv().await {
            match cmd {
                mq::SubChannelCommand::Open { queue_name, resp } => {
                    //open a channel
                    //declare a queue on the channel
                    let channel: Result<Channel, Box<dyn std::error::Error + Send>> = match mq_connection_s.lock().await.add_sub_channel_and_queue(&queue_name).await {
                        Ok(v) => Ok(v),
                        Err(e) => {
                            let msg = format!("Error while opening channel and adding queue: {}", e);
                            Err(mq::future_err(msg))
                        }

                    };
                    //send channel back to sender
                    let res = match resp.send(channel) {
                        Ok(()) => (),
                        Err(e) => {
                            let msg = format!("subscription_thread: Error while sending channel to queue: {} for subscribing", &queue_name);
                            println!("{}", msg);

                            process::exit(1);

                        },
                    };
                    continue;
                }
                mq::SubChannelCommand::Close { queue_name, channel, resp } => {
                    //close the channel passed in
                    let response: Result<(), Box<dyn std::error::Error + Send>> = match mq_connection_s.lock().await.close_channel(channel).await {
                        Ok(()) => Ok(()),
                        Err(e) => {
                            let msg = format!("Error while opening channel and adding queue: {} - {}", queue_name, e);
                            Err(mq::future_err(msg))
                        }
                    };
                    //TODO: send result back to sender
                    continue;

                }
            }
       }
    });
    
    let mq_connection_p = mq_connection.clone();
    let t6 = tokio::spawn(async move {
        while let Some(cmd) = pub_rx.recv().await {
            match cmd {
                mq::PubChannelCommand::Publish { queue_name, content, resp } => {
                    //pass in current queue name and content to publish_to_next_queue func
                    //open a channel
                    //send content and return result of send
                    let response: Result<(), Box<dyn std::error::Error + Send>> = match mq_connection_p.lock().await.publish_to_next_queue(&queue_name, content).await {
                        Ok(()) => Ok(()),
                        Err(e) => {
                            let msg = format!("Error while publishing from queue: {}", queue_name);
                            Err(mq::future_err(msg))
                        }
                    };
                    //send result back to sender
                    let _ = match resp.send(response) {
                        Ok(()) => (),
                        Err(e) => {
                            let msg = format!("Error while sending result of publish to queue: {}", &queue_name);
                            println!("{}", msg);
                            process::exit(1);
                        }
                    };
                    continue;
                
                }
            }
        }
    });
    

    //Parsing queue thread
    let parsing_pub_tx = pub_tx.clone();
    let parsing_sub_tx = sub_tx.clone();
    let parsing_cache_tx = cache_tx.clone();
    let t2 = tokio::spawn(async move {

        let mut parsing_queue = Arc::new(tokio::sync::Mutex::new(ParsingQueue::new("parsing_queue", "", "", parsing_sub_tx.clone(), parsing_pub_tx.clone(), parsing_cache_tx.clone())));

        match parsing_queue.lock().await.process_queue().await {
            Ok(_) => {},
            Err(e) => {
                eprintln!("main: Error occurred while ParsingQueue::processing queue: {}", e);
                process::exit(1);
            }
        };

    });

    //Writing queue thread
    let writing_sub_tx = sub_tx.clone();
    let writing_pub_tx = pub_tx.clone();
    let writing_cache_tx = cache_tx.clone();
    let writing_db_connection = db_connection.clone();
    let t3 = tokio::spawn(async move {

        let mut writing_queue = Arc::new(tokio::sync::Mutex::new(writing_queue::WritingQueue::new("writing_queue", "", "", writing_db_connection, writing_sub_tx, writing_pub_tx, writing_cache_tx.clone())));
        
        println!("Queue Created on Writing Thread");

        match writing_queue.lock().await.process_queue().await {
            Ok(_) => {},
            Err(e) => {
                eprintln!("main: Error occurred while WritingQueue::processing queue: {}", e);
                process::exit(1);
            }
        };
        println!("Queue finished on Writing Thread");
    });

    
    //Add Calculation step
    //let mut mq_connection_ca = mq_connection.clone();
    let calc_sub_tx = sub_tx.clone();
    let calc_pub_tx = pub_tx.clone();
    let calc_cache_tx = cache_tx.clone();
    let calc_db_connection = db_connection.clone();
    let t4 = tokio::spawn(async move {

        let mut calc_queue = Arc::new(tokio::sync::Mutex::new(calc_queue::CalcQueue::new("calc_queue", "", "", calc_db_connection, calc_sub_tx, calc_pub_tx, calc_cache_tx)));

        println!("Calc queue created in thread");

        //Replace with calculation queue
        match calc_queue.lock().await.process_queue().await {
            Ok(_) => {},
            Err(e) => {
                eprintln!("main: Error occurred while CalcQueue::processing queue: {}", e);
                process::exit(1);
            }
        };
        
        println!("Calc queue finished in thread");
        
    });
    


    //Add Streaming step 

    //Publish message to parsing queue
    let (resp_tx, resp_rx) = oneshot::channel();
    let cmd = mq::PubChannelCommand::Publish { 
        queue_name: "".to_string(), 
        content, 
        resp: resp_tx 
    };
    match pub_tx.send(cmd).await {
        Ok(_) => {},
        Err(e) => {
            eprintln!("main: Error occurred while publishing message: {}", e);
            process::exit(1);
        } 
    }
    let resp = match resp_rx.await {
        Ok(x) => x,
        Err(e) => {
            eprintln!("main: Error returned from result of response");
            process::exit(1);
        },
    };
    
    println!("Item Published! from main");
   
   //let future = async {

       // Define a periodic interval
       //let mut interval = interval(Duration::from_secs(15));

       ////loop indefinitely
       //loop {
           // Wait for the next tick of the interval
           //interval.tick().await;

           // publish message
          //mq::publish_to_queue(&mq_connection.channel, exchange_name, routing_key, content).await;
       //}

       //mq::publish_example().await;

   //};
   //
   //future.await;

   //Join handles
   let mut handles = Vec::with_capacity(6);
   handles.push(t1);
   handles.push(t2);
   handles.push(t3);
   handles.push(t4);
   handles.push(t5);
   handles.push(t6);
   block_on(join_all(handles));

   let elapsed_time = now.elapsed();
   println!("Elapsed time: {:.2?}", elapsed_time);
   
   //close connection after publishing 
   match block_on(signal::ctrl_c()) {
           Ok(()) => {
               match mq_connection.lock().await.close_connections().await {
                   Ok(_) => {},
                   Err(e) => {
                       eprintln!("main: Error occurred while closing connections: {}", e);
                       process::exit(1);
                   }
               }
           },
           Err(err) => {
               eprint!("Unable to listen for shutdown signal: {}", err);
               match mq_connection.lock().await.close_connections().await {
                   Ok(_) => {},
                   Err(e) => {
                       eprintln!("main: Error occurred while closing connections: {}", e);
                       process::exit(1);
                   }
               }
           }
   }
   process::exit(0);
   println!("Process Exited {}", err_loc!());

}

//Do we want to return an error here? or keep it limited to a string
#[macro_export]
macro_rules! err_loc {
    ( $x:expr ) => {
       format!("file: {}, line: {}, column: {}, error: {}", file!(), line!(), column!(), $x) 
    };
    () => {
       format!("file: {}, line: {}, column: {}", file!(), line!(), column!()) 
    };
}

#![allow(unused)]
use core::time;
use std::{borrow::{Borrow, BorrowMut}, process, sync::{Arc, Mutex}, env};

use tokio::{sync::oneshot, time::{interval, Duration}};
use mq::MQConnection;
use parsing_queue::ParsingQueue;
use tokio::signal;
use tokio::sync::{mpsc, Notify};
use crate::scraped_cache::ScrapedCache;
use serde_json::Value;
use amqprs::channel::{BasicAckArguments, BasicCancelArguments};
use sqlx::postgres::PgPool;
use dotenv::dotenv;


pub mod scraped_cache;
pub mod db;
pub mod mq;
pub mod types;
pub mod options_scraper;
pub mod writing_queue;
pub mod parsing_queue;
pub use mq::Queue;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    //Grab symbol from environment variables
    dotenv().ok();

    let symbol = match env::var("SYMBOL") {
        Ok(v) => {
            if v != "" {
                v
            } else {
                panic!("SYMBOL was empty in environment")
            }
        },
        Err(e) => panic!("SYMBOL not found in environment"),
    };

    //"host.docker.internal" 
    //Declare threadsafe mq connection struct to handle connecting and publishing to rabbitmq
    let mut mq_connection = Arc::new(tokio::sync::Mutex::new(MQConnection::new("localhost", 5672, "guest", "guest")));
    println!("MQ Connection Created");

    //Declare thread-safe db connection struct to handle database calls
    let mut db_connection = Arc::new(tokio::sync::Mutex::new(db::DBConnection::new("localhost", 5444, "postgres", "postgres", "scraped")));
    println!("DB Pool Created");


    //Caching channel (need to clone tx for each additional thread)
    let (tx, mut rx) = mpsc::channel(32);

    let mut contract_cache = Arc::new(tokio::sync::Mutex::new(scraped_cache::ScrapedCache::new(100)));
    println!("Scraped Cache Created");

    //Parsing Queue Declaration
    let parsing_routing_key = "parsing_queue";
    let exchange_name = "amq.direct";
    let queue_name = "parsing_queue"; //next queue
    let mut parsing_queue = Arc::new(tokio::sync::Mutex::new(ParsingQueue::new(queue_name, parsing_routing_key, exchange_name, tx.clone())));
    println!("Parsing Queue Created");

    //Writing Queue Declaration
    let writing_routing_key = "writing_queue";
    let w_exchange_name = "amq.direct";
    let queue_name = "writing_queue";
    let mut writing_queue = Arc::new(tokio::sync::Mutex::new(writing_queue::WritingQueue::new(queue_name, writing_routing_key, w_exchange_name, db_connection.clone(), tx.clone())));
    println!("Writing Queue Created");

    let mut mq_connection_p = mq_connection.clone();
    match mq_connection_p.lock().await.open().await {
        Ok(_) => {}
        Err(e) => {
            eprintln!("main: Error while opening connection to rabbitmq: {}", e);
            process::exit(1);
        }
    }
    //Block below needed? 
    let pub_channel_id = Some(3);
    let mut pub_channel = match mq_connection_p.lock().await.add_channel(pub_channel_id).await {
        Ok(c) => Some(c),
        Err(e) => {
            eprintln!("main: Error occurred while adding channel 3: {}", e);
            process::exit(1);
        }
    };
    println!("Pub Channel Created");
    
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
    tokio::spawn(async move {
       while let Some(cmd) = rx.recv().await {
            match cmd {
                scraped_cache::Command::Get { key, resp } => {

                    let res = contract_cache.lock().await.get(&key).await.cloned();
                    //Switch out later

                    let respx = resp.send(Ok(res)); 
                    dbg!(respx);
                }
                scraped_cache::Command::Set { key, value, resp } => {
                    let res = contract_cache.lock().await.set(key, value).await;

                    let _ = resp.send(Ok(()));

                }
            }
           
       } 
    });

    //Parsing queue thread
    let mut mq_connection_c = mq_connection.clone();
    tokio::spawn(async move {
        let mut mq_connection_c = mq_connection_c.lock().await;

        //declare new channel for background thread
        let p_channel_id = Some(2);
        let mut sub_channel = match mq_connection_c.add_channel(p_channel_id).await {
            Ok(c) => Some(c),
            Err(e) => {
                eprintln!("main: Error occurred while adding channel w/ id {} in parsing thread: {}", p_channel_id.unwrap(), e);
                process::exit(1);
            }
        };
        println!("Sub Channel Created on Parsing Thread");

        //declare new channel for publishing from background thread
        let pfs_channel_id = Some(5);
        let mut pub_from_sub_channel = match mq_connection_c.add_channel(pfs_channel_id).await {
            Ok(c) => Some(c),
            Err(e) => {
                eprintln!("main: Error occurred while adding channel w/ id {} in parsing thread: {}", p_channel_id.unwrap(), e);
                process::exit(1);
            }
        };
        println!("Pub from Sub Channel Created on Parsing Thread");

        let queue_name = "parsing_queue";

        //Add queue to background thread 
        let _ = match mq_connection_c.add_queue(sub_channel.as_mut().unwrap(), queue_name, parsing_routing_key, "amq.direct").await {
            Ok(_) => {},
            Err(e) => {
                eprintln!("main: Error occurred while adding queue w/ name {}: {}", queue_name, e);
                process::exit(1);
            }
        };
        println!("Queue Created on Parsing Thread");
        let parsing_queue = parsing_queue.clone();
        match parsing_queue.lock().await.process_queue(sub_channel.as_mut().unwrap(), pub_from_sub_channel.as_mut().unwrap()).await {
            Ok(_) => {},
            Err(e) => {
                eprintln!("main: Error occurred while ParsingQueue::processing queue: {}", e);
                process::exit(1);
            }
        };

    });

    //Writing queue thread
    let mut mq_connection_w = mq_connection.clone();
    tokio::spawn(async move {
        let w_exchange_name = "amq.direct";
        let writing_routing_key = writing_routing_key; 
        let queue_name = "writing_queue";

        let mut mq_connection_w = mq_connection_w.lock().await;

        //declare new channel for background thread
        let w_channel_id = Some(6);
        let mut sub_channel = match mq_connection_w.add_channel(w_channel_id).await {
            Ok(c) => Some(c),
            Err(e) => {
                eprintln!("main: Error occurred while adding sub channel w/ id {} in writing thread: {}", w_channel_id.unwrap(), e);
                process::exit(1);
            }
        };
        println!("sub Channel Created on Writing Thread");

        //declare a new channel for publishing from background thread
        let pfw_channel_id = Some(7);
        let mut pub_channel = match mq_connection_w.add_channel(pfw_channel_id).await {
            Ok(c) => Some(c),
            Err(e) => {
                eprintln!("main: Error occurred while adding pub channel w/ id {} in writing thread: {}", w_channel_id.unwrap(), e);
                process::exit(1);
            }
        };

        let _ = match mq_connection_w.add_queue(sub_channel.as_mut().unwrap(), queue_name, writing_routing_key, w_exchange_name).await {
            Ok(_) => {},
            Err(e) => {
                eprintln!("main: Error occurred while adding queue w/ name {}: {}", queue_name, e);
                process::exit(1);
            }
        };
        println!("Queue Created on Writing Thread");

        let writing_queue = writing_queue.clone();
        match writing_queue.lock().await.process_queue(sub_channel.as_mut().unwrap(), pub_channel.as_mut().unwrap()).await {
            Ok(_) => {},
            Err(e) => {
                eprintln!("main: Error occurred while WritingQueue::processing queue: {}", e);
                process::exit(1);
            }
        };
    });


    mq::publish_to_queue(pub_channel.as_mut().unwrap(), exchange_name, parsing_routing_key, content).await?;
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
    
    //close connection after publishing TODO: possibly move to end?
    match signal::ctrl_c().await {
            Ok(()) => {
                match mq_connection_p.lock().await.close_connections().await {
                    Ok(_) => {},
                    Err(e) => {
                        eprintln!("main: Error occurred while closing connections: {}", e);
                        process::exit(1);
                    }
                }
            },
            Err(err) => {
                eprint!("Unable to listen for shutdown signal: {}", err);
                match mq_connection_p.lock().await.close_connections().await {
                    Ok(_) => {},
                    Err(e) => {
                        eprintln!("main: Error occurred while closing connections: {}", e);
                        process::exit(1);
                    }
                }
            }
    }
    process::exit(0);

}


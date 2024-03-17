#![allow(unused)]
use core::time;
use std::{borrow::{Borrow, BorrowMut}, process, sync::{Arc, Mutex}};

use tokio::time::{interval, Duration};
use mq::MQConnection;
use parsing_queue::ParsingQueue;
use tokio::signal;
use tokio::sync::mpsc;
use crate::scraped_cache::ScrapedCache;


pub mod options_scraper;
pub mod parsing_queue;
pub mod scraped_cache;
pub mod mq;
pub mod types;
pub use mq::Queue;


#[tokio::main]
async fn main() {
   //"host.docker.internal" 
    let mut mq_connection = Arc::new(tokio::sync::Mutex::new(MQConnection::new("localhost", 5672, "guest", "guest")));

    //Caching channel (need to clone tx for each additional thread)
    let (tx, mut rx) = mpsc::channel(32);

    let mut contract_cache = Arc::new(tokio::sync::Mutex::new(scraped_cache::ScrapedCache::new(100)));

    let routing_key = "parsing_queue";
    //let exchange_name = "";
    let exchange_name = "amq.direct";
    //let queue_name = "amqprs.examples.basic"; //next queue
    let queue_name = "parsing_queue"; //next queue
    let mut parsing_queue = Arc::new(tokio::sync::Mutex::new(ParsingQueue::new(queue_name, routing_key, exchange_name, tx.clone())));

    let mut mq_connection_p = mq_connection.clone();
    //add channel
    mq_connection_p.lock().await.open().await;
    let mut channel = Some(mq_connection_p.lock().await.add_channel(Some(3)).await.unwrap());
    //add queue
    mq_connection_p.lock().await.add_queue(channel.as_mut().unwrap(), "parsing_queue", routing_key, exchange_name).await;
    
    //TODO: Convert this to another form of input (Cmd line arg or csv) 
    let content = String::from(
        r#"
            {
                "publisher": "main",
                "data": "Hello, amqprs!",
                "symbol": "SPY"
            }
        "#,
    ).into_bytes();


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


    //Example queue thread
    let mut mq_connection_ce = mq_connection.clone();
    tokio::spawn(async move {
        let routing_key = "amqprs.example";
        let exchange_name = "amq.direct";
        let mut mq_connection_ce = mq_connection_ce.lock().await;
        mq_connection_ce.open().await;


        let args = amqprs::channel::BasicConsumeArguments::new(
            &queue_name,
            "example_basic_pub_sub"
        );
        let mut channel = Some(mq_connection_ce.add_channel(Some(1)).await.unwrap());
        let _ = mq_connection_ce.add_queue(channel.as_mut().unwrap(), "amqprs.examples.basic", routing_key, exchange_name).await;
        channel.as_mut().unwrap() 
        .basic_consume(mq::ExampleConsumer::new(args.no_ack), args)
        .await
        .unwrap();


    });

    //Parsing queue thread
    let mut mq_connection_c = mq_connection.clone();
    tokio::spawn(async move {
        let mut mq_connection_c = mq_connection_c.lock().await;
        mq_connection_c.open().await;
        let mut channel = Some(mq_connection_c.add_channel(Some(2)).await.unwrap());
        let _ = mq_connection_c.add_queue(channel.as_mut().unwrap(), "parsing_queue", "parsing_queue", "amq.direct").await;
        let parsing_queue = parsing_queue.clone();
        parsing_queue.lock().await.process_queue(channel.as_mut().unwrap()).await;


    });

    mq::publish_to_queue(channel.as_mut().unwrap(), exchange_name, routing_key, content).await;

    //tokio::time::sleep(Duration::from_secs(5)).await;
    

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
                mq_connection_p.lock().await.close_connections().await; 
            },
            Err(err) => {
                eprint!("Unable to listen for shutdown signal: {}", err);
                mq_connection_p.lock().await.close_connections().await; 
            }
    }
    process::exit(0);

}


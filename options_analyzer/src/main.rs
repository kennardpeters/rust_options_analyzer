#![allow(unused)]
use std::{borrow::{Borrow, BorrowMut}, process, sync::{Arc, Mutex}};

use tokio::time::{interval, Duration};
use mq::MQConnection;
use parsing_queue::ParsingQueue;
use tokio::signal;


mod options_scraper;
pub mod parsing_queue;
pub mod mq;
pub use mq::Queue;

#[tokio::main]
async fn main() {
    
    let mut mq_connection = Arc::new(tokio::sync::Mutex::new(MQConnection::new("localhost", 5672, "guest", "guest")));

    let routing_key = "parsing_queue";
    //let exchange_name = "";
    let exchange_name = "amq.topic";
    //let queue_name = "amqprs.examples.basic"; //next queue
    let queue_name = "parsing_queue"; //next queue
    let mut parsing_queue = Arc::new(tokio::sync::Mutex::new(ParsingQueue::new(queue_name, routing_key, exchange_name)));
    //

    let mut mq_connection_p = mq_connection.clone();
    mq_connection_p.lock().await.open().await;
    mq_connection_p.lock().await.add_queue("parsing_queue", routing_key, exchange_name).await;
    
    
    let content = String::from(
        r#"
            {
                "publisher": "example",
                "data": "Hello, amqprs!"
            }
        "#,
    ).into_bytes();
    mq::publish_to_queue(&mq_connection_p.lock().await.channel.clone().expect("Channel was None"), exchange_name, routing_key, content).await;

    let mut mq_connection_c = mq_connection.clone();
    tokio::spawn(async move {
        let mut mq_connection_c = mq_connection_c.lock().await;
        mq_connection_c.open().await;
        let _ = mq_connection_c.add_queue("parsing_queue", routing_key, exchange_name).await;
        let parsing_queue = parsing_queue.clone();
        parsing_queue.lock().await.process_queue(mq_connection_c.channel.as_mut().unwrap()).await;

        match signal::ctrl_c().await {
            Ok(()) => {
                mq_connection_c.close_connections().await; 
            },
            Err(err) => {
                eprint!("Unable to listen for shutdown signal: {}", err);
                mq_connection_c.close_connections().await; 
            }
        }
        

    });

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
    //mq_connection_p.lock().await.close_connections().await;
    

    //let future = async {

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
    
    // Define a periodic interval
    //let mut interval = interval(Duration::from_secs(15));

        let url = "";
    ////loop indefinitely
    //loop {
    //    // Wait for the next tick of the interval
    //    interval.tick().await;
    //    let output_object = options_scraper::scrape(url).expect("Scrape Failed!");
    //    println!("Serialized Object: {:?}", output_object);
    //}
    process::exit(0);

}


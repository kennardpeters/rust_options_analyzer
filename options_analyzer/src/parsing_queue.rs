extern crate amqprs;
extern crate std;
extern crate serde_json;
extern crate async_trait;
extern crate tokio;

use amqprs::{
    channel::{BasicAckArguments, BasicCancelArguments, BasicConsumeArguments, Channel, ConsumerMessage},
    consumer::AsyncConsumer,
    BasicProperties,
    Deliver,
};
use std::{borrow::BorrowMut, future::Future, ops::{Deref, DerefMut}, str, sync::Arc, time::Duration};
use async_trait::async_trait;
use serde_json::Value;
use tokio::{sync::{Mutex, mpsc::Sender, oneshot}, time::sleep};

pub use crate::mq::{Queue, publish_to_queue, SubChannelCommand, PubChannelCommand};
pub use crate::options_scraper;
use crate::{mq::{self, future_err}, scraped_cache};
pub use crate::types::Contract;
pub use crate::scraped_cache::{ScrapedCache, Command};

pub struct ParsingQueue<'a> {
    pub name: &'a str, //Current name of queue
    routing_key: &'a str, // queue name for publishing to the next queue
    exchange_name: &'a str, //Exchange name used for publishing to the next queue
    sub_tx: Sender<SubChannelCommand>, //tx used to subscribe from queue
    pub_tx: Sender<PubChannelCommand>, //tx used to publish content to next queue
    cache_tx: Sender<Command>, //tx used for sending /receiving contracts from cache
}

static CONSUMER_TAG: &str = "parser";

impl<'a> ParsingQueue<'a> {
    pub fn new(queue_name: &'a str, routing_key: &'a str, exchange_name: &'a str, sub_tx: Sender<SubChannelCommand>, pub_tx: Sender<PubChannelCommand>, cache_tx: Sender<Command>) -> Self { 
        Self {
            name: queue_name,
            routing_key,
            exchange_name,
            sub_tx,
            pub_tx,
            cache_tx,
        }
    }

    pub fn new_args(name: &str) -> BasicConsumeArguments {
        let args = BasicConsumeArguments::new(
            name, 
            CONSUMER_TAG,
        ).manual_ack(true)
        .finish();

        args
    }
    
    //Start a consumer on channel
    pub async fn process_queue(&self) -> Result<(), Box<dyn std::error::Error>> {
        let args = BasicConsumeArguments::new(
            &self.name,
            "parser",
        ).manual_ack(true)
        .finish();
        //TODO: Refactor:
        //  Declare/grab sub channel here
        //  Declare queue here 
        //Add queue to background thread 
        //  Call consume function here
        //  Determine return type


        match mq::consume_from_queue(args, self.sub_tx.clone(), self).await {
              Ok(()) => (),
              Err(e) => {
                  let msg = format!("parsing_queue::process_queue - error returned from
                  consume_from_queue: {}", e);
                  return Err(Box::from(msg));
              },
        };
        //
        Ok(())


        //consuming behavior defined here
        //let (ctag, mut messages_rx) = channel.basic_consume_rx(args.clone()).await?;
        //while let Some(deliver) = messages_rx.recv().await {
        //   //need to convert to another function
        //   let content = match deliver.content {
        //       Some(x) => x,
        //       None => continue,
        //   };


        //   //unserialize content
        //   let stringed_bytes = match str::from_utf8(&content) {
        //       Ok(stringed) => stringed,
        //       Err(e) => {
        //           let msg = format!("parsing_queue::process_queue - stringing content bytes failed {}", e);
        //           println!("{}", msg);
        //           ""
        //       },
        //   };
        //   let unserialized_content: Value = match serde_json::from_str(&stringed_bytes) {
        //       Ok(unser_con) => unser_con,
        //       Err(e) => {
        //           let msg = format!("parsing_queue::process_queue - unserializing content into json failed {}", e);
        //           println!("{}", msg);
        //           //Panic!
        //           Value::Null

        //       }
        //   };
        //   println!("Unserialized Content: {:?}", unserialized_content);
        //   if unserialized_content.is_null() || unserialized_content["symbol"].is_null() {
        //        let delivery = match deliver.deliver {
        //            Some(x) => x,
        //            None => {
        //                println!("parsing_queue::process_queue - deliver was None");
        //                continue;
        //            },

        //        };
        //       println!("Symbol is null! for the following delivery: {}",unserialized_content);
        //       let args = BasicAckArguments::new(delivery.delivery_tag(), false);

        //       match channel.basic_ack(args).await {
        //           Ok(_) => {}
        //           Err(e) => {
        //               let msg = format!("parsing_queue::process_queue - Error occurred while acking message after null content: {}", e);
        //               println!("{}", msg);
        //           },
        //       }
        //       continue;

        //   } else {
        //        //main consumer logic to be retried later
        //        let res = match self.process_func(pub_channel, unserialized_content).await {
        //            Ok(_) => {},
        //            Err(e) => {
        //                let msg = format!("parsing_queue::process_queue - Error occurred while processing message: {}", e);
        //                println!("{}", msg);
        //            },
        //        };
        //        
        //        let delivery = match deliver.deliver {
        //            Some(x) => x,
        //            None => {
        //                println!("parsing_queue::process_queue - deliver was None");
        //                continue;
        //            },

        //        };

        //        let args = BasicAckArguments::new(delivery.delivery_tag(), false);

        //        match channel.basic_ack(args).await {
        //            Ok(_) => {},
        //            Err(e) => {
        //                let msg = format!("parsing_queue.AsyncConsumer.consume - Error occurred while acking message: {}", e);
        //                println!("{}", msg);
        //            },
        //        };
        //        continue;

        //   }
        //}
        //if !channel.is_open() {
        //    println!("parsing_queue::process_queue - Channel closed");
        //    dbg!(ctag);
        //    return Ok(());
        //    
        //} else if let Err(e) = channel.basic_cancel(BasicCancelArguments::new(&ctag)).await {
        //    let msg = format!("parsing_queue::process_queue - Error occurred while cancelling consumer: {}", e);
        //    println!("{}", msg);
        //    return Err(msg.into());
        //} else {
        //    println!("parsing_queue::process_queue - done");
        //    Ok(())
        //}
        
    }
    
    //async fn process_func(&mut self, pub_channel: &mut Channel, unserialized_content: Value) -> Result<(), Box<dyn std::error::Error>> {

    //    //TODO: Move deserialization of delivery here

    //    let symbol = unserialized_content["symbol"].to_string().replace("\"", "");
    //    let mut url = "".to_string();
    //    if symbol.contains("test") {
    //        url = format!(r#"http://localhost:7878/{}"#, symbol); 
    //    } else {
    //        //Old style url: .neo_opt=0
    //        url = format!(r#"https://finance.yahoo.com/quote/{}/options?.neo_opt=0"#, symbol);
    //        //NOTE: This url needs to be updated for new options
    //        //url = format!(r#"https://finance.yahoo.com/quote/{}/options?p={}"#, symbol, symbol);
    //    }
    //    println!("URL: {:?}", url.clone());

    //    let output_ts = match options_scraper::async_scrape(url.as_str()).await {
    //        Ok(x) => x,
    //        Err(e) => {
    //            let msg = format!("parsing_queue::process_func - Error occurred while scraping: {}", e);
    //            println!("{}", msg);
    //            //Return err here?
    //            options_scraper::TimeSeries {
    //                data: Vec::new(),
    //            }
    //        },
    //    }; 


    //    println!("process_fun: 160 Serialized Object LENGTH: {:?}", output_ts.data.len());
    //    if output_ts.data.len() == 0 {
    //        let msg = "parsing_queue::process_func - 0 objects scraped successfully";
    //        return Err(msg.into());
    //    }

    //    for i in output_ts.data.iter() {

    //        //Parse out into separate function
    //        let (resp_tx, resp_rx) = oneshot::channel();
    //        println!("Contract: {:?}\n", i);
    //        let contract = Contract::new_from_unparsed(i);
    //        let next_key = contract.contract_name.clone();
    //        let command = Command::Set{
    //            key: contract.contract_name.clone(),
    //            value: contract,
    //            resp: resp_tx,
    //        };
    //        match self.tx.send(command).await {
    //            Ok(_) => {

    //            },
    //            Err(e) => {
    //                let msg = format!("parsing_queue::process_func - Error occurred while sending contract to cache: {}", e);
    //                println!("{}", msg);
    //            },
    //        };
    //        let resp = match resp_rx.await {
    //            Ok(x) => x,
    //            Err(e) => {
    //                let msg = format!("parsing_queue::process_func - Error occurred while receiving result of sending contract to cache: {}", e);
    //                println!("{}", msg);
    //                Err(()) 
    //            },
    //        };
    //        /////////


    //        let e_content = String::from(
    //            format!(
    //                r#"
    //                    {{
    //                        "publisher": "parsing",
    //                        "key": {:?}
    //                    }}
    //                "#,
    //                next_key 
    //            )
    //        ).into_bytes();
    //        publish_to_queue(pub_channel, "amq.direct", "writing_queue", e_content).await;
    //        //TODO: remove after verfication on separate queue
    //        dbg!(resp);
    //    }

    //    Ok(())
    //}
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
    
    async fn process_func(&self, deliver: &ConsumerMessage) -> Result<(), Box<dyn std::error::Error + Send>> {
        let content = match &deliver.content {
            Some(x) => x,
            None => {
                let msg = format!("parsing_queue::process_func - content was None!");
                return Err(future_err(msg));
            },
        };
        let stringed_bytes = match str::from_utf8(&content) {
            Ok(stringed) => stringed,
            Err(e) => {
                let msg = format!("parsing_queue::process_queue - stringing content bytes failed {}", e);
                return Err(future_err(msg));
            },
        };
        let unserialized_content: Value = match serde_json::from_str(&stringed_bytes) {
            Ok(unser_con) => unser_con,
            Err(e) => {
                let msg = format!("parsing_queue::process_queue - unserializing content into json failed {}", e);
                return Err(future_err(msg));
            }
        };
        println!("Unserialized Content: {:?}", unserialized_content);

        let symbol = unserialized_content["symbol"].to_string().replace("\"", "");
        let mut url = "".to_string();
        if symbol.contains("test") {
            url = format!(r#"http://localhost:7878/{}"#, symbol); 
        } else {
            //Old style url: .neo_opt=0
            url = format!(r#"https://finance.yahoo.com/quote/{}/options?.neo_opt=0"#, symbol);
            //NOTE: This url needs to be updated for new options
            //url = format!(r#"https://finance.yahoo.com/quote/{}/options?p={}"#, symbol, symbol);
        }

        println!("URL: {:?}", url.clone());

        let output_ts = match options_scraper::async_scrape(url.as_str()).await {
            Ok(x) => x,
            Err(e) => {
                let msg = format!("parsing_queue::process_func - Error occurred while scraping: {}", e);
                return Err(future_err(msg));
            },
        }; 
        if output_ts.data.len() == 0 {
            let msg = String::from("parsing_queue::process_func - 0 objects scraped successfully");
            return Err(future_err(msg));
        }
        for i in output_ts.data.iter() {

            //Parse out into separate function
            let (resp_tx, resp_rx) = oneshot::channel();
            //println!("Contract: {:?}\n", i);
            let contract = Contract::new_from_unparsed(i);
            let next_key = contract.contract_name.clone();
            let command = Command::Set{
                key: contract.contract_name.clone(),
                value: contract,
                resp: resp_tx,
            };
            match self.cache_tx.send(command).await {
                Ok(_) => {

                },
                Err(e) => {
                    let msg = format!("parsing_queue::process_func - Error occurred while sending contract to cache: {}", e);
                    return Err(future_err(msg));
                },
            };
            let resp = match resp_rx.await {
                Ok(x) => x,
                Err(e) => {
                    let msg = format!("parsing_queue::process_func - Error occurred while receiving result of sending contract to cache: {}", e);
                    return Err(future_err(msg));
                },
            };
            let e_content = String::from(
                format!(
                    r#"
                        {{
                            "publisher": "parsing",
                            "key": {:?}
                        }}
                    "#,
                    next_key 
                )
            ).into_bytes();

            let (pub_resp_tx, pub_resp_rx) = oneshot::channel(); 

            let cmd = mq::PubChannelCommand::Publish { 
                queue_name: String::from(self.queue_name()), 
                content: e_content, 
                resp: pub_resp_tx,
            };

            //Publish to next queue
            match self.pub_tx.send(cmd).await {
                Ok(()) => (),
                Err(e) => {
                    let msg = format!("parsing_queue::process_func - Error occurred while sending content to the next queue from queue: {} - {}", self.queue_name(), e);
                    return Err(future_err(msg));
                }
            };
            let resp = match pub_resp_rx.await {
                Ok(x) => x,
                Err(e) => {
                    let msg = format!("parsing_queue::process_func - Error occurred while awaiting result of send item to the next queue: {}", e);
                    return Err(future_err(msg));
                },
            };

        }

        Ok(())
    }
    
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mq::{publish_to_queue, MQConnection};


    use bytes::Bytes;
    use futures_util::TryStreamExt;
    use http_body_util::{combinators::BoxBody, BodyExt, Full, StreamBody};
    use std::{fs,
        //net::{TcpListener, TcpStream}, 
        io::{BufReader, prelude::*},
    };
    use hyper::body::Frame;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response, Result as HyperResult, StatusCode, Method};
    use hyper_util::rt::TokioIo;
    use tokio::{io::{self, BufStream, AsyncReadExt, AsyncWriteExt}, net::{unix::SocketAddr, TcpListener as TokioTcpListener, TcpStream}, signal, sync::{mpsc, oneshot}, fs::File};
    use tokio_util::{codec::Framed, io::ReaderStream, sync::CancellationToken};

    static OPTION_HTML: &str = "./test_data/mock_option.html";
    static NOTFOUND: &[u8] = b"Not Found";

    //Helper functions for spinning up mock http server
    async fn tokio_handle_client(req: Request<hyper::body::Incoming>) -> HyperResult<Response<BoxBody<Bytes, std::io::Error>>> {
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/test_option") => simple_file_send(OPTION_HTML).await,
            _ => Ok(not_found()),
        }
    }
    fn not_found() -> Response<BoxBody<Bytes, std::io::Error>> {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(NOTFOUND.into()).map_err(|e| match e {}).boxed())
            .unwrap()
    }


    async fn simple_file_send(filename: &str) -> HyperResult<Response<BoxBody<Bytes, std::io::Error>>> {
        // Open file for reading
        let file = File::open(filename).await;
        if file.is_err() {
            eprintln!("ERROR: Unable to open file.");
            return Ok(not_found());
        }

        let file: File = file.unwrap();

        // Wrap to a tokio_util::io::ReaderStream
        let reader_stream = ReaderStream::new(file);

        // Convert to http_body_util::BoxBody
        let stream_body = StreamBody::new(reader_stream.map_ok(Frame::data));
        let boxed_body = stream_body.boxed();

        // Send response
        let response = Response::builder()
            .status(StatusCode::OK)
            .body(boxed_body)
            .unwrap();

        Ok(response)
    }


    //#[tokio::test(flavor = "multi_thread", worker_threads=3)]
    //#[ignore = "polls forever on cloud"]
    //async fn test_process() {
    //    // create an mq connection
    //    let mut mq_connection = Arc::new(Mutex::new(MQConnection::new("localhost", 5672, "guest", "guest")));
    //    //Publishing logic
    //    let conn_result = match mq_connection.lock().await.open().await {
    //        Ok(v) => v,
    //        Err(e) => {
    //            println!("parsing_queue::test_process - Error occurred while opening connection: {}", e);
    //            return;
    //        }
    //    };
    //    assert_eq!(conn_result, ());
    //    
    //    // clone a channel to be used for publishing
    //    let mut mq_connection_p = mq_connection.clone();

    //    let mut pub_channel = match mq_connection_p.lock().await.add_channel(Some(2)).await {
    //        Ok(v) => Some(v),
    //        Err(e) => {
    //            println!("parsing_queue::test_process - Error occurred while adding channel: {}", e);
    //            None
    //        }
    //    };
    //    assert!(pub_channel.is_some());

    //    // create a cache channel
    //    let (tx, mut rx) = mpsc::channel::<Command>(32);
    //    //Instantiate cache
    //    let mut scraped_cache = Arc::new(tokio::sync::Mutex::new(ScrapedCache::new(100)));
    //    //Create caching thread
    //    tokio::spawn(async move {
    //       while let Some(cmd) = rx.recv().await {
    //            match cmd {
    //                scraped_cache::Command::Get { key, resp } => {

    //                    let res = scraped_cache.lock().await.get(&key).await.cloned();
    //                    //Switch out later

    //                    let respx = resp.send(Ok(res)); 
    //                    dbg!(respx);
    //                }
    //                scraped_cache::Command::Set { key, value, resp } => {
    //                    let res = scraped_cache.lock().await.set(key, value).await;

    //                    let _ = resp.send(Ok(()));

    //                }
    //            }
    //           
    //       } 
    //    });

    //    
    //    // spin up fake http server to scrape with queue
    //    let mock_server = tokio::spawn(async move {
    //        let listener = TokioTcpListener::bind("127.0.0.1:7878").await.unwrap();

    //        loop {
    //            if let Ok((stream, addr)) = listener.accept().await {
    //                let io = TokioIo::new(stream);
    //                println!("Addr: {:?}", addr);

    //                tokio::task::spawn(async move {
    //                    if let Err(err) = http1::Builder::new()
    //                    .serve_connection(io, service_fn(tokio_handle_client))
    //                    .await
    //                    {
    //                        print!("parsing_queue::test_process - Failed to serve connection: {:?}", err);
    //                    }
    //                });
    //            } else {
    //                eprint!("parsing_queue::test_process - Error reading tcp stream!");
    //            }

    //        }

    //        Ok::<(), std::io::Error>(())
    //        
    //        
    //    });

    //    // create a queue item and publish to the queue
    //    let queue_name = "parsing_queue";
    //    let exchange_name = "amq.direct";


    //    let content = String::from(
    //        r#"
    //            {
    //                "publisher": "parsing_queue::test_process",
    //                "data": "Hello, amqprs!",
    //                "symbol": "test_option"
    //            }
    //        "#,
    //    ).into_bytes();

    //    match publish_to_queue(pub_channel.as_mut().unwrap(), exchange_name, queue_name, content).await {
    //        Ok(_) => {},
    //        Err(e) => {
    //            let msg = format!("parsing_queue::test_process - Error occurred while publishing to queue: {}", e);
    //            println!("{}", msg);
    //        },
    //    };
    //    //futures::join!(mock_server, publish_to_queue(pub_channel.as_mut().unwrap(), exchange_name, queue_name, content));

    //    let mut mq_connection_c = mq_connection.clone();
    //    let mut parsing_queue = Arc::new(Mutex::new(ParsingQueue::new(queue_name, queue_name, exchange_name, tx.clone())));
    //    let parshing_thread = tokio::spawn(async move {
    //        let mut mq_connection_c = mq_connection_c.lock().await;

    //        //declare new channel for background thread
    //        let p_channel_id = Some(49);
    //        let mut sub_channel = match mq_connection_c.add_channel(p_channel_id).await {
    //            Ok(c) => Some(c),
    //            Err(e) => {
    //                panic!("parsing_queue::test_process: Error occurred while adding channel w/ id {} in parsing thread: {}", p_channel_id.unwrap(), e);
    //                None
    //            }
    //        };
    //        println!("Sub Channel Created on Parsing Thread");

    //        //declare new channel for publishing from background thread
    //        let pfs_channel_id = Some(5);
    //        let mut pub_from_sub_channel = match mq_connection_c.add_channel(pfs_channel_id).await {
    //            Ok(c) => Some(c),
    //            Err(e) => {
    //                panic!("parsing_queue::test_process: Error occurred while adding channel w/ id {} in parsing thread: {}", p_channel_id.unwrap(), e);
    //                None
    //            }
    //        };
    //        println!("Pub from Sub Channel Created on Parsing Thread");

    //        let parsing_routing_key = "parsing_queue";

    //        //Add queue to background thread 
    //        let _ = match mq_connection_c.add_queue(sub_channel.as_mut().unwrap(), queue_name, parsing_routing_key, "amq.direct").await {
    //            Ok(_) => {},
    //            Err(e) => {
    //                panic!("parsing_queue::test_process: Error occurred while adding queue w/ name {}: {}", queue_name, e);
    //            }
    //        };
    //        println!("Queue Created on Parsing Thread");
    //        let parsing_queue = parsing_queue.clone();
    //        match parsing_queue.lock().await.process_queue(sub_channel.as_mut().unwrap(), pub_from_sub_channel.as_mut().unwrap()).await {
    //            Ok(_) => {},
    //            Err(e) => {
    //                panic!("parsing_queue::test_process: Error occurred while ParsingQueue::processing queue: {}", e);
    //            }
    //        };

    //    });

    //    dbg!(parshing_thread.await.is_ok());
    //    
    //    // Verify the cache is updated with the data we expected to be scraped
    //    let (resp_tx, resp_rx) = oneshot::channel();
    //    let command = scraped_cache::Command::Get {
    //        key: "testcontract2020".to_string(),
    //        resp: resp_tx,
    //    };
    //    let txc = tx.clone();
    //    match txc.send(command).await {
    //        Ok(_) => {},
    //        Err(e) => {
    //            let msg = format!("parsing_queue::test_process - Error occurred while requesting contract from cache: {}", e);
    //            println!("{}", msg);
    //        },
    //    };
    //    let resp = match resp_rx.await {
    //        Ok(x) => x,
    //        Err(e) => {
    //            let msg = format!("parsing_queue::test_process - Error occurred while receiving contract from cache: {}", e);
    //            println!("{}", msg);
    //            Err(()) 
    //        },
    //    };
    //    //TODO: Fix unwrapping
    //    let contract = match resp {
    //        Ok(x) => {
    //            if x.is_some() {
    //                x.unwrap()
    //            } else {
    //                println!("parsing_queue::test_process - Unwrapped None! from Cache");
    //                Contract::new()
    //            }
    //        },
    //        Err(e) => {
    //            let msg = format!("parsing_queue::test_process - Error occurred while receiving contract from cache: {:?}", e);
    //            panic!("{}", msg);
    //            Contract::new()
    //        },
    //    };

    //    //Shut down mock server running in the background
    //    mock_server.abort();

    //    assert_eq!(contract.strike, 440.0);
    //}
}

//Deprecated Struct used for old consumer pattern
#[derive(Clone)] 
struct ParsingConsumer {
    no_ack: bool,
    tx: Sender<Command>,
    //additional fields as needed
}

//impl ParsingConsumer {
//    pub fn new(no_ack: bool, tx: Sender<Command>) -> Self {
//        Self { 
//            tx, 
//            no_ack 
//        }
//    }
//}
//
//#[async_trait]
//impl AsyncConsumer for ParsingConsumer {
//
//    async fn consume(
//        &mut self,
//        channel: &Channel,
//        deliver: Deliver,
//        _basic_properties: BasicProperties,
//        content: Vec<u8>,
//    ) {
//        //unserialize content
//        let stringed_bytes = match str::from_utf8(&content) {
//            Ok(stringed) => stringed,
//            Err(e) => {
//                let msg = format!("parsing_queue.AsyncConsumer.consume - stringing content bytes failed {}", e);
//                println!("{}", msg);
//                ""
//            },
//        };
//        let unserialized_content: Value = match serde_json::from_str(&stringed_bytes) {
//            Ok(unser_con) => unser_con,
//            Err(e) => {
//                let msg = format!("parsing_queue.AsyncConsumer.consume - unserializing content into json failed {}", e);
//                println!("{}", msg);
//                //Panic!
//                Value::Null
//
//            }
//        };
//        println!("Unserialized Content: {:?}", unserialized_content);
//        if unserialized_content.is_null()|| unserialized_content["symbol"].is_null() {
//            println!("Symbol is null! for the following delivery: {}",unserialized_content);
//            let args = BasicAckArguments::new(deliver.delivery_tag(), false);
//
//            match channel.basic_ack(args).await {
//                Ok(_) => {}
//                Err(e) => {
//                    let msg = format!("parsing_queue.AsyncConsumer.consume - Error occurred while acking message after null content: {}", e);
//                    println!("{}", msg);
//                },
//            }
//
//        } else {
//            let symbol = unserialized_content["symbol"].to_string().replace("\"", "");
//            println!("SYMBOL: {:?}\n", symbol);
//            let url = format!(r#"https://finance.yahoo.com/quote/{}/options?p={}"#, symbol, symbol);
//            println!("URL: {:?}\n", url);
//            let output_ts = match options_scraper::async_scrape(url.as_str()).await {
//                Ok(x) => x,
//                Err(e) => {
//                    let msg = format!("parsing_queue.AsyncConsumer.consume - Error occurred while scraping: {}", e);
//                    println!("{}", msg);
//                    //Panic! here?
//                    options_scraper::TimeSeries {
//                        data: Vec::new(),
//                    }
//                },
//            }; 
//        
//
//            println!("Serialized Object LENGTH: {:?}", output_ts.data.len());
//
//            //Parse out fields of time series objects from string => correct datatype
//            let mut contracts: Vec<Contract> = Vec::new();
//
//            for i in output_ts.data.iter() {
//                let (resp_tx, resp_rx) = oneshot::channel();
//                println!("Contract: {:?}\n", i);
//                let contract = Contract::new_from_unparsed(i);
//                let command = Command::Set{
//                    key: contract.contract_name.clone(),
//                    value: contract,
//                    resp: resp_tx,
//                };
//                match self.tx.send(command).await {
//                    Ok(_) => {
//
//                    },
//                    Err(e) => {
//                        let msg = format!("parsing_queue.AsyncConsumer.consume - Error occurred while sending contract to cache: {}", e);
//                        println!("{}", msg);
//                    },
//                };
//                let resp = match resp_rx.await {
//                    Ok(x) => x,
//                    Err(e) => {
//                        let msg = format!("parsing_queue.AsyncConsumer.consume - Error occurred while receiving result of sending contract to cache: {}", e);
//                        Err(()) 
//                    },
//                };
//                dbg!(resp);
//            }
//
//            //dbg!(contracts);
//
//
//            //Wait until channel logic is fixed to run the commented out code below
//            let e_content = String::from(
//                r#"
//                    {
//                        "publisher": "parsing",
//                        "data": "Hello, from Parsing Queue"
//                    }
//                "#,
//            ).into_bytes();
//            //Insert into next queue (need to find channel based on queue name and send it through that channel)
//            publish_to_queue(channel, "amq.direct", "amqprs.example", e_content).await;
//
//            let args = BasicAckArguments::new(deliver.delivery_tag(), false);
//
//            match channel.basic_ack(args).await {
//                Ok(_) => {}
//                Err(e) => {
//                    let msg = format!("parsing_queue.AsyncConsumer.consume - Error occurred while acking message: {}", e);
//                    println!("{}", msg);
//                },
//            };
//            println!("DELIVERY TAG: {}", deliver.delivery_tag())
//        }
//
//    }
//}

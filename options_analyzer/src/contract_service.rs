use proto::{ContractRequest, ContractResponse, contract_server::{Contract, ContractServer}};
use tonic::transport::Server;
use std::{pin::Pin, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use crate::scraped_cache::Command;

pub mod proto {
    tonic::include_proto!("contracts");
    
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] = 
        tonic::include_file_descriptor_set!("contracts_descriptor");
}


//#[derive(Debug)] //removed Default
//pub struct ContractService {
//    cache_tx: mpsc::Sender<Command>,
//    //rx to receive values from stream queue
//    //probably an atomic hashmap of remote addr => contracts to multiplex # of contracts to send to each
//    //client
//}

//impl ContractService {
//    fn default() -> Self {
//        ContractService {
//        }
//    }
//    pub fn new(cache_tx: mpsc::Sender<Command>) -> ContractService {
//        ContractService {
//            cache_tx,
//        }
//    }
//}

#[derive(Debug, Default)] //removed Default
pub struct ContractService {}

#[tonic::async_trait]
impl Contract for ContractService {


    type ServerStreamContractStream = Pin<Box<dyn Stream<Item = Result<ContractResponse, tonic::Status>> + Send>>;

    async fn server_stream_contract(
        &self,
        request: tonic::Request<ContractRequest>, 
    ) -> Result<tonic::Response<Self::ServerStreamContractStream>, tonic::Status> {
        //boiler plate for now
        println!("Calculator Server::add_stream");
        println!("\t client connected from: {:?}", request.remote_addr());

        let input = request.get_ref();
        
        let return_contract_name: String = input.contract_name.clone();
        
        let mut count = 0;

        let repeat_w = std::iter::repeat_with(move || {
            count += 1;

            let fake_time = format!("{}", count.clone());
            let fake_trade_date = format!("{}", count.clone());
            let fake_strike: i64 = count.clone();
            let fake_last_price: i64 = count.clone();
            let fake_bid: i64 = count.clone();
            let fake_open_int: i64 = count.clone();
            let fake_volume: i64 = count.clone();
            let fake_bid: f32 = count.clone() as f32;
            let fake_ask: f32 = count.clone() as f32;
            let fake_change: f32 = count.clone() as f32;
            let fake_percent_change: f32 = count.clone() as f32;
            let fake_iv: f32 = count.clone() as f32;
            ContractResponse {
                time: fake_time.clone(),
                contract_name: return_contract_name.clone(),
                last_trade_date: fake_trade_date.clone(),
                strike: fake_strike.clone(),
                last_price: fake_last_price.clone(),
                bid: fake_bid.clone(),
                ask: fake_ask.clone(),
                change: fake_change.clone(),
                percent_change: fake_percent_change.clone(), 
                volume: fake_volume.clone(),
                open_interest: fake_open_int.clone(),    
                implied_volatility: fake_iv.clone(),
            }
        });
        let mut stream = Box::pin(tokio_stream::iter(repeat_w).throttle(Duration::from_millis(200)));

        // spawn and channel are required if you want to handle "disconnect" functionality
        // the `out_stream` will not be polled after client disconnect
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match tx.send(Result::<_, tonic::Status>::Ok(item)).await {
                    Ok(_) => {
                        //item (server response) was queued to be sent to the client
                    },
                    Err(_item) => {
                        // output_stream was built from rx and both are dropped
                        break;
                    }
                }
            }
            println!("\t client disconnected");
        });
        
        let output_stream = ReceiverStream::new(rx);
        Ok(tonic::Response::new(
            Box::pin(output_stream) as Self::ServerStreamContractStream
        ))

        //end boilerplate

        //Actual Implentation:
        //for each request 
            //1. Grab request criteria
            //2. Save it to atomic hashmap
        
        //for each contract_name received from stream queue
            //1. Check if in request criteria 
            //2. Possible? or does this require state



    }
}

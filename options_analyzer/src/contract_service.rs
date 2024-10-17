use proto::{ContractRequest, ContractResponse, contract_server::{Contract, ContractServer}}; use tonic::transport::Server;
use std::{pin::Pin, time::Duration};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use crate::scraped_cache::Command;
use crate::stream_queue::StreamCommand;

pub mod proto {
    tonic::include_proto!("contracts");
    
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] = 
        tonic::include_file_descriptor_set!("contracts_descriptor");
}


#[derive(Debug)] //removed Default
pub struct ContractService {
    cache_tx: mpsc::Sender<Command>,
    //stream_rx: mpsc::Sender<StreamCommand>,
    //rx to receive values from stream queue
    //probably an atomic hashmap of remote addr => contracts to multiplex # of contracts to send to each
    //client
}

impl ContractService {
    pub fn new(cache_tx: mpsc::Sender<Command>/*, stream_rx: mpsc::Sender<StreamCommand>*/) -> Self {
        ContractService {
            cache_tx,
            //stream_rx
        }
    }
}

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

        //TODO: Add some sort of identifier here for each connected client

        let (resp_tx, resp_rx) = oneshot::channel();
        let command = Command::Get{
            key: return_contract_name.clone(),
            resp: resp_tx,
        };
        match self.cache_tx.send(command).await {
            Ok(_) => {},
            Err(e) => {
                let msg = format!("contract_service::server_stream_contract - Error occurred while requesting contract from cache: {}", e);
                return Err(tonic::Status::internal(msg));
            },
        };
        let resp = match resp_rx.await {
            Ok(x) => x,
            Err(e) => {
                let msg = format!("contract_service::server_stream_contract - Error occurred while receiving contract from cache: {}", e);
                return Err(tonic::Status::internal(msg));
            },
        };
        let contract = match resp {
            Ok(x) => {
                if x.is_some() {
                    x.unwrap()
                } else {
                    let msg = format!("contract_service::server_stream_contract - Unwrapped None! from Cache for key: {}", return_contract_name.clone());
                    return Err(tonic::Status::internal(msg));
                }
            },
            Err(e) => {
                let msg = format!("contract_service::server_stream_contract - Error occurred while receiving contract from cache: {:?}", e);
                return Err(tonic::Status::internal(msg));
            },
        };

        //Refactor the below to accept responses from stream_queue
        let repeat_w = std::iter::repeat_with(move || {
            ContractResponse { 
                time: contract.timestamp.clone().to_string(), 
                contract_name: contract.contract_name.clone(), 
                last_trade_date: contract.last_trade_date.clone(), 
                strike: contract.strike.clone() as i64, 
                last_price: contract.last_price.clone() as i64, 
                bid: contract.bid.clone() as f32, 
                ask: contract.ask.clone() as f32, 
                change: contract.change.clone() as f32, 
                percent_change: contract.percent_change.clone() as f32, 
                volume: contract.volume.clone(), 
                open_interest: contract.open_interest.clone(), 
                implied_volatility: contract.implied_volatility.clone() as f32,
            }
        });
        let mut stream = Box::pin(tokio_stream::iter(repeat_w).throttle(Duration::from_millis(200)));

        // END Refactor

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

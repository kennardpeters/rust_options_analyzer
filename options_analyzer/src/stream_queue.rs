extern crate tokio;
extern crate async_trait;

use crate::{mpsc::Sender, oneshot};
use crate::mq::{PubChannelCommand, SubChannelCommand, Queue, consume_from_queue, future_err};
use crate::scraped_cache::Command;
use amqprs::channel::{BasicConsumeArguments, ConsumerMessage};
use async_trait::async_trait;


//PubChannelCommand enum is used to open and close publishing channels to the mq server
pub enum StreamCommand {
    StreamContract {
        contract: String,
        //resp: Responder<(), Box<dyn std::error::Error + Send>>,
    },
}

pub struct StreamQueue<'a> {
    pub name: &'a str,
    sub_tx: Sender<SubChannelCommand>,
    pub_tx: Sender<PubChannelCommand>,
    cache_tx: Sender<Command>,
}

static CONSUMER_TAG: &str = "stream";
static STRUCT_NAME: &str = "stream_queue";

//TODO: finish implementation
impl<'a> StreamQueue<'a> {
    pub fn new(queue_name: &'a str, sub_tx: Sender<SubChannelCommand>, pub_tx: Sender<PubChannelCommand>, cache_tx: Sender<Command>) -> Self {
        Self {
            name: queue_name,
            sub_tx,
            pub_tx,
            cache_tx,
        }
    }

    pub fn new_args(&self) -> BasicConsumeArguments {
        let args = BasicConsumeArguments::new(
            &self.name, 
            CONSUMER_TAG,
        ).manual_ack(true)
        .finish();

        args
    }

    pub async fn process_queue(&self) -> Result<(), Box<dyn std::error::Error>> {
        let err_signature = format!("{}::process_queue", STRUCT_NAME);
        //create queue arguments
        let args = self.new_args();
        
        //set up grpc server here and attach it to struct

        match consume_from_queue(args, self.sub_tx.clone(), self).await {
            Ok(()) => (),
            Err(e) => {
                let msg = format!("{} - error returned from consume_from_queue: {}", err_signature, e);
                return Err(Box::from(msg));
            },
        };

        Ok(())

    }
}

#[async_trait]
impl<'a> Queue for StreamQueue<'a> {

    fn queue_name(&self) -> &str {
        self.name
    }

    fn args(&self) -> BasicConsumeArguments {
        BasicConsumeArguments::new(
            self.name, 
            "stream",
        )
    }

    async fn process_func(&self, deliver: &ConsumerMessage) -> Result<(), Box<dyn std::error::Error + Send>> {

        Ok(())
    }

}

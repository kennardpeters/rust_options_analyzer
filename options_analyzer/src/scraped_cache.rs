extern crate lru;
extern crate std;
extern crate tokio;

use lru::LruCache;
use std::num::NonZeroUsize;
use crate::types::{Contract, Responder};
use tokio::sync::mpsc;
//use tokio::sync::Mutex;
//use std::sync::Arc;


#[derive(Debug)]
pub enum Command {
    Get {
        key: String,
        resp: Responder<Option<Contract>, ()>,
    },
    Set {
        key: String,
        value: Contract,
        resp: Responder<(), ()>,
    },
}


pub struct ScrapedCache {
    cache: LruCache<String, Contract>,
}

impl ScrapedCache {
    pub fn new(capacity: usize) -> Self {
        let mut cache = LruCache::new(NonZeroUsize::new(capacity).unwrap());
        Self {
            cache
        }
    }
    //pub async fn process_command(&mut self, command: Command) {
    //    let (tx, mut rx) = mpsc::channel(1);

    //    match command {
    //        Command::Get { key } => {
    //            let _ = tx.send(self.get(&key).await);
    //        }
    //        Command::Set { key, value } => {
    //            self.set(key, value);
    //        }
    //    }

    //    
    //}

    pub async fn get(&mut self, key: &str) -> Option<&Contract> {
        match self.cache.get(key) { //.clone()
            None => None,
            Some(value) => Some(value),
            //{
            //    //Seems sketchy but we'll see
            //    let mut val = Contract::new();
            //    value.clone_into(&mut &val); //..clone()
            //    Some(val)
            //},
        }
    }
    pub async fn set(&mut self, key: String, value: Contract) {
        self.cache.put(key, value); 
    }
}



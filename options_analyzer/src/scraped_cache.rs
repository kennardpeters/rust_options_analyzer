extern crate lru;
extern crate std;
extern crate tokio;

use lru::LruCache;
use std::num::NonZeroUsize;
use crate::types::{Contract, Responder};
use tokio::sync::mpsc;


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
        }
    }
    pub async fn set(&mut self, key: String, value: Contract) {
        self.cache.put(key, value); 
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_get() -> Result<(), Box<dyn std::error::Error>> {
        //Create multiple threads to get multiple keys
        Err("Not implemented")?
    }
    
    #[test]
    fn test_set() -> Result<(), Box<dyn std::error::Error>> {
        //Create multiple threads to set/get multiple keys
        //TODO:
        Err("Not implemented")?
    }

}



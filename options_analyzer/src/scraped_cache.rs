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
    use std::sync::Arc;
    use tokio::sync::oneshot;
    #[tokio::test]
    async fn test_get() -> Result<(), Box<dyn std::error::Error>> {


        //Caching channel (need to clone tx for each additional thread)
        let (tx, mut rx) = mpsc::channel(32);

        //Create multiple threads to get multiple keys
        let mut contract_cache = Arc::new(tokio::sync::Mutex::new(ScrapedCache::new(100)));

        //Set mock contract in cache 
        let mut contract = Contract::new();
        let last_price = -1.0;
        contract.contract_name = "test".to_string();
        contract.last_price = last_price.clone();

        //set the cache in the 
        contract_cache.lock().await.set("test".to_string(), contract.clone()).await;

        //Caching thread
        tokio::spawn(async move {
           while let Some(cmd) = rx.recv().await {
                match cmd {
                    Command::Get { key, resp } => {

                        let res = contract_cache.lock().await.get(&key).await.cloned();
                        //Switch out later

                        let respx = resp.send(Ok(res)); 
                        dbg!(respx);
                    }
                    Command::Set { key, value, resp } => {
                        let res = contract_cache.lock().await.set(key, value).await;

                        let _ = resp.send(Ok(()));

                    }
                }
               
           } 
        });
   
        //Create multiple threads to get example key 
        let tx1 = tx.clone();
        tokio::spawn(async move {
            let (resp_tx, resp_rx) = oneshot::channel();
            let command = Command::Get{
                key: "test".to_string(),
                resp: resp_tx,
            };
            match tx1.send(command).await {
                Ok(_) => {},
                Err(e) => {
                    let msg = format!("scraped_cache::test_get - Error occurred while requesting contract from cache: {}", e);
                    println!("{}", msg);
                },
            };
            let resp = match resp_rx.await {
                Ok(x) => x,
                Err(e) => {
                    let msg = format!("scraped_cache::test_get - Error occurred while receiving contract from cache: {}", e);
                    println!("{}", msg);
                    Err(()) 
                },
            };
            let contract1 = match resp {
                Ok(x) => {
                    x.unwrap()
                },
                Err(e) => {
                    let msg = format!("scraped_cache::test_get - Error occurred while receiving contract from cache: {:?}", e);
                    println!("{}", msg);
                    Contract::new()
                },
            };

            assert_eq!(contract1.last_price, last_price.clone());
            Ok::<Contract, ()>(contract1)
        });

        let tx2 = tx.clone();
        tokio::spawn(async move {
            let (resp_tx, resp_rx) = oneshot::channel();
            let command = Command::Get{
                key: "test".to_string(),
                resp: resp_tx,
            };
            match tx2.send(command).await {
                Ok(_) => {},
                Err(e) => {
                    let msg = format!("scraped_cache::test_get - Error occurred while requesting contract from cache: {}", e);
                    println!("{}", msg);
                },
            };
            let resp = match resp_rx.await {
                Ok(x) => x,
                Err(e) => {
                    let msg = format!("scraped_cache::test_get - Error occurred while receiving contract from cache: {}", e);
                    println!("{}", msg);
                    Err(()) 
                },
            };
            let contract2 = match resp {
                Ok(x) => {
                    x.unwrap()
                },
                Err(e) => {
                    let msg = format!("scraped_cache::test_get - Error occurred while receiving contract from cache: {:?}", e);
                    println!("{}", msg);
                    Contract::new()
                },
            };

            assert_eq!(contract2.last_price, last_price.clone());
            Ok::<Contract, ()>(contract2)
        });

        let tx3 = tx.clone();
        tokio::spawn(async move {
            let (resp_tx, resp_rx) = oneshot::channel();
            let command = Command::Get{
                key: "test".to_string(),
                resp: resp_tx,
            };
            match tx3.send(command).await {
                Ok(_) => {},
                Err(e) => {
                    let msg = format!("scraped_cache::test_get - Error occurred while requesting contract from cache: {}", e);
                    println!("{}", msg);
                },
            };
            let resp = match resp_rx.await {
                Ok(x) => x,
                Err(e) => {
                    let msg = format!("scraped_cache::test_get - Error occurred while receiving contract from cache: {}", e);
                    println!("{}", msg);
                    Err(()) 
                },
            };
            let contract3 = match resp {
                Ok(x) => {
                    x.unwrap()
                },
                Err(e) => {
                    let msg = format!("scraped_cache::test_get - Error occurred while receiving contract from cache: {:?}", e);
                    println!("{}", msg);
                    Contract::new()
                },
            };

            assert_eq!(contract3.last_price, last_price.clone());
            Ok::<Contract, ()>(contract3)
        });


        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        Ok(())
        
    }
    
    #[tokio::test]
    async fn test_set() -> Result<(), Box<dyn std::error::Error>> {
        //Caching channel (need to clone tx for each additional thread)
        let (tx, mut rx) = mpsc::channel(32);

        //Create multiple threads to get multiple keys
        let mut contract_cache = Arc::new(tokio::sync::Mutex::new(ScrapedCache::new(100)));

        //Set mock contract in cache 
        let mut contract = Contract::new();
        let last_price = -1.0;
        contract.contract_name = "test".to_string();
        contract.last_price = last_price.clone();

        //set the cache in the 
        contract_cache.lock().await.set("test".to_string(), contract.clone()).await;

        //Caching thread
        tokio::spawn(async move {
           while let Some(cmd) = rx.recv().await {
                match cmd {
                    Command::Get { key, resp } => {

                        let res = contract_cache.lock().await.get(&key).await.cloned();

                        let respx = resp.send(Ok(res)); 
                        dbg!(respx);
                    }
                    Command::Set { key, value, resp } => {
                        let res = contract_cache.lock().await.set(key, value).await;

                        let _ = resp.send(Ok(()));

                    }
                }
               
           } 
        });
   
        //Create multiple threads to get example key 
        let tx1 = tx.clone();
        tokio::spawn(async move {
            let (resp_tx, resp_rx) = oneshot::channel();
            let command = Command::Get{
                key: "test".to_string(),
                resp: resp_tx,
            };
            match tx1.send(command).await {
                Ok(_) => {},
                Err(e) => {
                    let msg = format!("scraped_cache::test_get - Error occurred while requesting contract from cache: {}", e);
                    println!("{}", msg);
                },
            };
            let resp = match resp_rx.await {
                Ok(x) => x,
                Err(e) => {
                    let msg = format!("scraped_cache::test_get - Error occurred while receiving contract from cache: {}", e);
                    println!("{}", msg);
                    Err(()) 
                },
            };
            let contract1 = match resp {
                Ok(x) => {
                    x.unwrap()
                },
                Err(e) => {
                    let msg = format!("scraped_cache::test_get - Error occurred while receiving contract from cache: {:?}", e);
                    println!("{}", msg);
                    Contract::new()
                },
            };

            assert_eq!(contract1.last_price, last_price.clone());
            Ok::<Contract, ()>(contract1)
        });
        //Create multiple threads to set/get multiple keys
        //TODO:
        Err("Not implemented")?
    }

}



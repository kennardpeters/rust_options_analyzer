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
        resp: Responder<Option<Contract>, Box<dyn std::error::Error + Send>>,
    },
    Set {
        key: String,
        value: Contract,
        resp: Responder<(), Box<dyn std::error::Error + Send>>,
    },
}


pub struct ScrapedCache {
    cache: LruCache<String, Contract>,
}

impl ScrapedCache {
    pub fn new(capacity: usize) -> Self {
        let cap_size = NonZeroUsize::new(capacity);
        if cap_size.is_none() {
            panic!("cap_size was None!");
        }
        let mut cache = LruCache::new(cap_size.unwrap());
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
            Some(value) => Some(value),
            None => None,
        }
    }
    pub async fn set(&mut self, key: String, value: Contract) {
        self.cache.put(key, value); 
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashMap, sync::Arc};
    use serde_json::to_string;
    use tokio::sync::oneshot;
    //#[tokio::test]
    //async fn test_get() -> Result<(), Box<dyn std::error::Error>> {


    //    //Caching channel (need to clone tx for each additional thread)
    //    let (tx, mut rx) = mpsc::channel(32);

    //    //Create multiple threads to get multiple keys
    //    let mut contract_cache = Arc::new(tokio::sync::Mutex::new(ScrapedCache::new(100)));

    //    //Set mock contract in cache 
    //    let mut contract = Contract::new();
    //    let last_price = -1.0;
    //    contract.contract_name = "test".to_string();
    //    contract.last_price = last_price.clone();

    //    //set the cache in the 
    //    contract_cache.lock().await.set("test".to_string(), contract.clone()).await;

    //    //Caching thread
    //    tokio::spawn(async move {
    //       while let Some(cmd) = rx.recv().await {
    //            match cmd {
    //                Command::Get { key, resp } => {

    //                    let res = contract_cache.lock().await.get(&key).await.cloned();
    //                    //Switch out later

    //                    let respx = resp.send(Ok(res)); 
    //                    dbg!(respx);
    //                }
    //                Command::Set { key, value, resp } => {
    //                    let res = contract_cache.lock().await.set(key, value).await;

    //                    let _ = resp.send(Ok(()));

    //                }
    //            }
    //           
    //       } 
    //    });
   
    //    //Create multiple threads to get example key 
    //    let tx1 = tx.clone();
    //    tokio::spawn(async move {
    //        let (resp_tx, resp_rx) = oneshot::channel();
    //        let command = Command::Get{
    //            key: "test".to_string(),
    //            resp: resp_tx,
    //        };
    //        match tx1.send(command).await {
    //            Ok(_) => {},
    //            Err(e) => {
    //                let msg = format!("scraped_cache::test_get - Error occurred while requesting contract from cache: {}", e);
    //                println!("{}", msg);
    //            },
    //        };
    //        let resp = match resp_rx.await {
    //            Ok(x) => x,
    //            Err(e) => {
    //                let msg = format!("scraped_cache::test_get - Error occurred while receiving contract from cache: {}", e);
    //                println!("{}", msg);
    //                Err(()) 
    //            },
    //        };
    //        let contract1 = match resp {
    //            Ok(x) => {
    //                x.unwrap()
    //            },
    //            Err(e) => {
    //                let msg = format!("scraped_cache::test_get - Error occurred while receiving contract from cache: {:?}", e);
    //                println!("{}", msg);
    //                Contract::new()
    //            },
    //        };

    //        assert_eq!(contract1.last_price, last_price.clone());
    //        Ok::<Contract, ()>(contract1)
    //    });

    //    let tx2 = tx.clone();
    //    tokio::spawn(async move {
    //        let (resp_tx, resp_rx) = oneshot::channel();
    //        let command = Command::Get{
    //            key: "test".to_string(),
    //            resp: resp_tx,
    //        };
    //        match tx2.send(command).await {
    //            Ok(_) => {},
    //            Err(e) => {
    //                let msg = format!("scraped_cache::test_get - Error occurred while requesting contract from cache: {}", e);
    //                println!("{}", msg);
    //            },
    //        };
    //        let resp = match resp_rx.await {
    //            Ok(x) => x,
    //            Err(e) => {
    //                let msg = format!("scraped_cache::test_get - Error occurred while receiving contract from cache: {}", e);
    //                println!("{}", msg);
    //                Err(()) 
    //            },
    //        };
    //        let contract2 = match resp {
    //            Ok(x) => {
    //                x.unwrap()
    //            },
    //            Err(e) => {
    //                let msg = format!("scraped_cache::test_get - Error occurred while receiving contract from cache: {:?}", e);
    //                println!("{}", msg);
    //                Contract::new()
    //            },
    //        };

    //        assert_eq!(contract2.last_price, last_price.clone());
    //        Ok::<Contract, ()>(contract2)
    //    });

    //    let tx3 = tx.clone();
    //    tokio::spawn(async move {
    //        let (resp_tx, resp_rx) = oneshot::channel();
    //        let command = Command::Get{
    //            key: "test".to_string(),
    //            resp: resp_tx,
    //        };
    //        match tx3.send(command).await {
    //            Ok(_) => {},
    //            Err(e) => {
    //                let msg = format!("scraped_cache::test_get - Error occurred while requesting contract from cache: {}", e);
    //                println!("{}", msg);
    //            },
    //        };
    //        let resp = match resp_rx.await {
    //            Ok(x) => x,
    //            Err(e) => {
    //                let msg = format!("scraped_cache::test_get - Error occurred while receiving contract from cache: {}", e);
    //                println!("{}", msg);
    //                Err(()) 
    //            },
    //        };
    //        let contract3 = match resp {
    //            Ok(x) => {
    //                x.unwrap()
    //            },
    //            Err(e) => {
    //                let msg = format!("scraped_cache::test_get - Error occurred while receiving contract from cache: {:?}", e);
    //                println!("{}", msg);
    //                Contract::new()
    //            },
    //        };

    //        assert_eq!(contract3.last_price, last_price.clone());
    //        Ok::<Contract, ()>(contract3)
    //    });


    //    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    //    Ok(())
    //    
    //}
    //
    //#[tokio::test]
    //async fn test_set() -> Result<(), Box<dyn std::error::Error>> {
    //    //Caching channel (need to clone tx for each additional thread)
    //    let (tx, mut rx) = mpsc::channel(32);

    //    //Create multiple threads to get multiple keys
    //    let mut contract_cache = Arc::new(tokio::sync::Mutex::new(ScrapedCache::new(100)));


    //    //Caching thread
    //    tokio::spawn(async move {
    //       while let Some(cmd) = rx.recv().await {
    //            match cmd {
    //                Command::Get { key, resp } => {

    //                    let res = contract_cache.lock().await.get(&key).await.cloned();

    //                    let respx = resp.send(Ok(res)); 
    //                    dbg!(respx);
    //                }
    //                Command::Set { key, value, resp } => {
    //                    let res = contract_cache.lock().await.set(key, value).await;

    //                    let _ = resp.send(Ok(()));

    //                }
    //            }
    //           
    //       } 
    //    });
   
    //    //Create multiple threads to get example key 
    //    let tx1 = tx.clone();
    //    let contract_name1 = "test1";
    //    let last_price1 = -1.0;
    //    let thread1 = tokio::spawn(async move {
    //        let (resp_tx, resp_rx) = oneshot::channel();
    //        //Set mock contract in cache 
    //        let mut contract = Contract::new();
    //        contract.contract_name = contract_name1.to_string();
    //        contract.last_price = last_price1.clone();

    //        let command = Command::Set{
    //            key: contract_name1.to_string(),
    //            value: contract.clone(),
    //            resp: resp_tx,
    //        };
    //        match tx1.send(command).await {
    //            Ok(_) => {},
    //            Err(e) => {
    //                let msg = format!("scraped_cache::test_set - Error occurred while sending contract to cache: {}", e);
    //                println!("{}", msg);
    //            },
    //        };
    //        let resp = match resp_rx.await {
    //            Ok(x) => x,
    //            Err(e) => {
    //                let msg = format!("scraped_cache::test_set - Error occurred while receiving result of sending contract from cache: {}", e);
    //                println!("{}", msg);
    //                Err(()) 
    //            },
    //        };

    //    });

    //    let tx2 = tx.clone();
    //    let contract_name2 = "test2";
    //    let last_price2 = -2.0;
    //    let thread2 = tokio::spawn(async move {
    //        let (resp_tx, resp_rx) = oneshot::channel();
    //        //Set mock contract in cache 
    //        let mut contract = Contract::new();
    //        contract.contract_name = contract_name2.to_string();
    //        contract.last_price = last_price2.clone();

    //        let command = Command::Set{
    //            key: contract_name2.to_string(),
    //            value: contract.clone(),
    //            resp: resp_tx,
    //        };
    //        match tx2.send(command).await {
    //            Ok(_) => {},
    //            Err(e) => {
    //                let msg = format!("scraped_cache::test_set - Error occurred while sending contract to cache: {}", e);
    //                println!("{}", msg);
    //            },
    //        };
    //        let resp = match resp_rx.await {
    //            Ok(x) => x,
    //            Err(e) => {
    //                let msg = format!("scraped_cache::test_set - Error occurred while receiving result of sending contract from cache: {}", e);
    //                println!("{}", msg);
    //                Err(()) 
    //            },
    //        };

    //    });


    //    let tx3 = tx.clone();
    //    let contract_name3 = "test3";
    //    let last_price3 = -3.0;
    //    let thread3 = tokio::spawn(async move {
    //        let (resp_tx, resp_rx) = oneshot::channel();
    //        //Set mock contract in cache 
    //        let mut contract = Contract::new();
    //        contract.contract_name = contract_name3.to_string();
    //        contract.last_price = last_price3.clone();

    //        let command = Command::Set{
    //            key: contract_name3.to_string(),
    //            value: contract.clone(),
    //            resp: resp_tx,
    //        };
    //        match tx3.send(command).await {
    //            Ok(_) => {},
    //            Err(e) => {
    //                let msg = format!("scraped_cache::test_set - Error occurred while sending contract to cache: {}", e);
    //                println!("{}", msg);
    //            },
    //        };
    //        let resp = match resp_rx.await {
    //            Ok(x) => x,
    //            Err(e) => {
    //                let msg = format!("scraped_cache::test_set - Error occurred while receiving result of sending contract from cache: {}", e);
    //                println!("{}", msg);
    //                Err(()) 
    //            },
    //        };

    //    });
    //    //Match stmts here?
    //    thread1.await;
    //    thread2.await;
    //    thread3.await;

    //    let contracts = vec![contract_name1, contract_name2, contract_name3];
    //    let mut expected = HashMap::new();
    //    for i in contracts.iter() {
    //        let (resp_tx, resp_rx) = oneshot::channel();
    //        let command = Command::Get{
    //            key: i.to_string(),
    //            resp: resp_tx,
    //        };
    //        match tx.send(command).await {
    //            Ok(_) => {},
    //            Err(e) => {
    //                let msg = format!("scraped_cache::test_set - Error occurred while requesting contract from cache: {}", e);
    //                println!("{}", msg);
    //            },
    //        };
    //        let resp = match resp_rx.await {
    //            Ok(x) => x,
    //            Err(e) => {
    //                let msg = format!("scraped_cache::test_set - Error occurred while receiving result of sending contract from cache: {}", e);
    //                println!("{}", msg);
    //                Err(()) 
    //            },
    //        };
    //        expected.insert(i.to_string(), resp.unwrap().unwrap());
    //    }



    //    //Check the result of setting from multiple threads 
    //    assert_eq!(last_price1, expected.get(contract_name1).unwrap().last_price);
    //    assert_eq!(last_price2, expected.get(contract_name2).unwrap().last_price);
    //    assert_eq!(last_price3, expected.get(contract_name3).unwrap().last_price);
    //    Ok(())
    //}

}



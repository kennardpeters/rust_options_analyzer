extern crate sqlx;
extern crate std;


use futures_util::future::BoxFuture;
use sqlx::error::DatabaseError;
use sqlx::postgres::{PgDatabaseError, PgPool};
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use std::error::Error;
use crate::types::{Contract, Responder};


//DBConnection is a struct to handle storing and opening connections to the database 
pub struct DBConnection<'a> {
    //connection_pool: Option<Arc<Mutex<Pool<Postgres>>>>,
    connection_pool: Option<Pool<Postgres>>,
    pub host: &'a str,
    pub port: u16,
    pub username: &'a str,
    pub password: &'a str,
    pub db_name: &'a str
}

impl<'a> DBConnection<'a> {
    pub fn new(
        host: &'a str,
        port: u16,
        username: &'a str,
        password: &'a str,
        db_name: &'a str
    ) -> DBConnection<'a> {
        Self {
            connection_pool: None,
            host,
            port,
            username,
            password,
            db_name,
        }
    }

    pub async fn open(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let connection_string = format!("postgres://{}:{}@{}:{}/{}", self.username, self.password, self.host, self.port, self.db_name);

        let mut pool = match PgPool::connect(&connection_string).await {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("db::DBConnection::open() - Error while opening to connection: {}", e);
                return Err(msg.into());
            }
        };

        self.connection_pool = Some(pool);

        Ok(())

    }
    pub async fn insert_contract(&mut self, contract: &Contract) -> Result<(), sqlx::Error> {
        let pool = match &self.connection_pool {
            Some(v) => v,
            None => {
                let msg = format!("db::DBConnection::insert_contract() - None unwrapped from connection pool");
                //sqlx::Error::from(j)
                println!("{}", msg);
                return Err(sqlx::Error::PoolClosed);
            },
        };
        let result = sqlx::query(
            r#"
                INSERT INTO contracts
                (time, contract_name, last_trade_date, strike, last_price, bid, ask, change, percent_change, volume, open_interest) Values (NOW(), $1, $2, $3, $4, $5, $6, $7,  $8, $9, $10)
                Returning time
            "#,
        )
            .bind(contract.contract_name.clone())
            .bind(contract.last_trade_date.clone())
            .bind(contract.strike)
            .bind(contract.last_price)
            .bind(contract.bid)
            .bind(contract.ask)
            .bind(contract.change)
            .bind(contract.percent_change)
            .bind(contract.volume)
            .bind(contract.open_interest)
            .fetch_one(pool).await;
        match result {
            Ok(v) => {
                println!("Successfully inserted into postgres!");
                return Ok(());     
            },
            Err(e) => {
                let msg = format!("db::insert_contract - Error occurred while inserting into postgres: {}", e);
                println!("{}", msg);
                return Err(e);
            },
        };
        Ok(())
    }
    pub async fn select_contract(&mut self, contract_name: &str) -> Result<Contract, sqlx::Error> {
        let pool = match &self.connection_pool {
            Some(v) => v,
            None => {
                let msg = format!("db::DBConnection::insert_contract() - None unwrapped from connection pool");
                //sqlx::Error::from(j)
                println!("{}", msg);
                return Err(sqlx::Error::PoolClosed);
            },
        };
        let result = match sqlx::query(
            r#"
                SELECT * FROM contracts WHERE contract_name = $1;
            "#,
        ).bind(contract_name)
        .fetch_one(pool).await {
            Ok(v) => v,
            Err(e) => return Err(e),
        };

        let contract = Contract::from(result);

        //if contract name is empty we know a parsing error occurred 
        if contract.contract_name == "" {
            let msg = format!("db::select_contract - invalid contract returned while parsing pg row: check logs for more information");
            return Err(sqlx::Error::ColumnDecode { index: "0".to_string(), source: Box::from(msg) })
        }

        return Ok(contract);
    }
    pub async fn delete_contract(&mut self, contract_name: &str) -> Result<(), sqlx::Error> {
        let pool = match &self.connection_pool {
            Some(v) => v,
            None => {
                let msg = format!("db::DBConnection::insert_contract() - None unwrapped from connection pool");
                //sqlx::Error::from(j)
                println!("{}", msg);
                return Err(sqlx::Error::PoolClosed);
            },
        };
        let result = match sqlx::query(
            r#"
                DELETE FROM contracts WHERE contract_name = $1;
            "#,
        ).bind(contract_name)
        .execute(pool).await {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        Ok(())
    }
}


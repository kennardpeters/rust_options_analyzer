extern crate sqlx;
extern crate std;


use sqlx::postgres::PgPool;
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use std::error::Error;
use crate::types::{Contract, Responder};

#[derive(Debug)]
pub enum DbCommand {
    Get {
        key: String,
        resp: Responder<Option<Contract>, ()>,
    },
    Post {
        key: String,
        value: String,
        resp: Responder<(), dyn Error>,
    },
}

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
}

//TODO: Utilize channels for this instead
async fn insert_contract(contract: &Contract, db: DBConnection) -> Result<(), sqlx::Error> {
        //let pool = self.pool.lock().await.clone();
        let result = sqlx::query(
            r#"
                INSERT INTO contracts
                (time, contract_name, last_trade_date, strike, last_price, bid, ask, change, percent_change, volume, open_interest) Values (NOW(), $1, $2, $3, $4, $5, $6, $7,  $8, $9, $10)
                Returning time
            "#,
        )
            .bind(contract.contract_name)
            .bind(contract.last_trade_date)
            .bind(contract.strike)
            .bind(contract.last_price)
            .bind(contract.bid)
            .bind(contract.ask)
            .bind(contract.change)
            .bind(contract.percent_change)
            .bind(contract.volume)
            .bind(contract.open_interest)
            .fetch_one(&db.connection_pool).await;
        match result {
            Ok(v) => {
                println!("Successfully inserted into postgres!");
                return Ok(v);     
            },
            Err(e) => {
                let msg = format!("db::insert_contract - Error occurred while inserting into postgres: {}", e);
                println!("{}", msg);
                return Err(e);
            },
        };
}

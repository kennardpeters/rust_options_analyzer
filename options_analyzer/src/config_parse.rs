extern crate std;

use std::{fs, collections::HashMap, str};

use serde::{Serialize, Deserialize};


#[derive(Deserialize)]
pub struct Config {
    reqwest_headers: HashMap<String, String>,
    queues: HashMap<String, String>
}

pub fn get_reqwest_headers() -> Result<HashMap<String, String>, Box<dyn std::error::Error>>{
    let contents = match fs::read_to_string("config.toml") {
        Ok(v) => v,
        Err(e) => {
            let msg = format!("config_parse::get_reqwest_headers() - Failed to read config.toml file - {}", e);
            return Err(msg.into());
        }
    };
    let config: Config = match toml::from_str(&contents) {
        Ok(v) => v,
        Err(e) => {
            let msg = format!("options_scraper::get_reqwest_headers() - Failed to parse TOML from config.toml - {}", e);
            return Err(msg.into());
        }
    };
    Ok(config.reqwest_headers)

}

pub fn get_queue_sequence() -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let contents = match fs::read_to_string("config.toml") {
        Ok(v) => v,
        Err(e) => {
            let msg = format!("config_parse::get_queue_sequence() - Failed to read config.toml file - {}", e);
            return Err(Box::from(msg));
        }
    };
    let config: Config = match toml::from_str(&contents) {
        Ok(v) => v,
        Err(e) => {
            let msg = format!("mq::get_queue_sequence() - Failed to parse TOML from config.toml - {}", e);
            return Err(Box::from(msg));
        }
    };

    Ok(config.queues)
}

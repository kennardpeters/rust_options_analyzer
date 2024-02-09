use tokio::time::{interval, Duration};

mod options_scraper;

#[tokio::main]
async fn main() {
    
    // Define a periodic interval
    let mut interval = interval(Duration::from_secs(15));

    //loop indefinitely
    loop {
        // Wait for the next tick of the interval
        interval.tick().await;
        let url = "";
        let output_object = options_scraper::scrape(url).expect("Scrape Failed!");
        println!("Serialized Object: {:?}", output_object);
    }

}

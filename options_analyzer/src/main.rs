use tokio::time::{interval, Duration};

mod options_scraper;
mod mq;

#[tokio::main]
async fn main() {

    let future = async {
        mq::publish_example().await;
    };
    
    future.await;
    
    // Define a periodic interval
    //let mut interval = interval(Duration::from_secs(15));

        let url = "";
    ////loop indefinitely
    //loop {
    //    // Wait for the next tick of the interval
    //    interval.tick().await;
    //    let output_object = options_scraper::scrape(url).expect("Scrape Failed!");
    //    println!("Serialized Object: {:?}", output_object);
    //}

}

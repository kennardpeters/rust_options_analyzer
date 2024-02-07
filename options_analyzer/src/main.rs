use std::time::Instant;

mod options_scraper;

fn main() {
    let url = "https://finance.yahoo.com/quote/SPY/options?p=SPY";
    let now = Instant::now();

    let output_object = options_scraper::scrape(url).expect("Scrape Failed!");
    println!("Object to serialize: {:?}", output_object);
    
    println!("Running options_scraper::scrape() took {} seconds.", now.elapsed().as_secs());
}

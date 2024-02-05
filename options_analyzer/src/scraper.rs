use std::io::{Write, BufWriter};
use std::fs::File;
use scraper::{Html, Selector};
use std::{str, panic};

use curl::easy::Easy;

fn scrape() -> std::io::Result<()> {
    // Create a new file for writing
    // TODO: Change to json instead of saving to a file once queue is made
    let file = File::create("output.txt")?;

    //Create a buffered writer to write to the file
    let mut writer = BufWriter::new(file);
    
    //Instantiate Easy instance for scraping
    let mut easy = Easy::new();

    
    //Instantiate string to store raw html scraped
    let mut stringed = String::new();

    let url: &str = "";


    //Need to make this more reproducible (also input symbol)
    //TODO: build request url dynamically using format!
    easy.url(url).unwrap();
    //Scope declared here in order to transfer ownership of stringed back to main function
    {
        let mut transfer = easy.transfer();
        transfer.write_function(|data|{
            let stringed_bytes = match str::from_utf8(data) {
                Ok(stringed) => stringed,
                Err(e) => {
                    println!("Stringing bytes failed!");
                    println!("{}", e);
                    //Handle error below
                    panic!("{}", e);
                },
            };

            stringed.push_str(stringed_bytes);

            
            Ok(data.len())
        }).unwrap();
        transfer.perform().unwrap();

    }

    //Instantiate list for storing parsed data
    let mut scraped_elements = Vec::new();

    // parsing block
    let dom = Html::parse_document(&stringed);

    let th_selector = Selector::parse(r#"th > span"#).unwrap();

    for element in dom.select(&th_selector) {
        scraped_elements.push(element.inner_html());
    }

    let td_selector = Selector::parse(r#"table > tbody > tr > td"#).unwrap();

    for element in dom.select(&td_selector) {
            scraped_elements.push(element.inner_html());
    }

    
    //May need a better way here to detect if html is still present in the string
    let mut count = 1; 
    for element in scraped_elements {

        //dynamically parsing html tags still present within the <td> tags
        let fragment = Html::parse_fragment(element.as_str()); 
        let a_selector = Selector::parse("a").unwrap();
        let span_selector = Selector::parse("span").unwrap();
        //Select out span + a elements
        let a = fragment.select(&a_selector).next().ok_or("Nil");
        let span = fragment.select(&span_selector).next().ok_or("Nil");

        //If a exists in inner html add the inner_html of a instead 
        if a.is_ok() {
            let a_element = a.unwrap().inner_html();
            let _err = writer.write(format!("{:?} \n", a_element).as_bytes());
            continue;
        }
        //If span exists in inner html add the inner_html of span instead 
        if span.is_ok() {
            let span_element = span.unwrap().inner_html();
            let _err = writer.write(format!("{:?} \n", span_element).as_bytes());
            continue;
        }

        //Else just write string to file

        let _err = writer.write(format!("{:?} \n", element).as_bytes());

        count += 1;
    }

    Ok(())
    
}

async fn async_scrape() -> Result<(), Box<dyn std::error::Error>> {
    //TODO: Build url dynamically here:
    let url: &str = "";
    let resp = match reqwest::get(url).await {
        Ok(x) => x,
        Err(_) => {
            println!("{}", "error on /random request");
            process::exit(0x0100);
        },
    };


    let text = resp.text().await?;

    //TODO: reuse parsing syntax above
}

use std::time::Instant;


fn main() {
    let now = Instant::now();
    println!("Hello, world!");
    
    println!("Running hello world!() took {} seconds.", now.elapsed().as_secs());
}

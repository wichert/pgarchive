use pgarchive::Archive;
use std::env;
use std::fs::File;

fn main() {
    let args: Vec<String> = env::args().collect();

    for path in args.into_iter().skip(1) {
        println!("Checking {}", path);
        let mut file = File::open(path).unwrap();
        match Archive::parse(&mut file) {
            Ok(hdr) => println!("{:?}", hdr),
            Err(e) => println!("can not read file: {:?}", e),
        };
    }
}

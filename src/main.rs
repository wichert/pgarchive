use pgarchive::header::Header;
use std::env;
use std::fs::File;
use std::io;
use std::io::Write;

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut stdout = io::stdout();
    let mut stderr = io::stderr();

    for path in args.into_iter() {
        let mut file = File::open(path).unwrap();
        match Header::try_from(&mut file) {
            Ok(hdr) => writeln!(stdout, "{:?}", hdr),
            Err(e) => writeln!(stderr, "can not read file: {:?}", e),
        };
    }

    let _cfg = pgarchive::io::ReadConfig::new();

    println!("Hello, world!");
}

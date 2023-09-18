use std::env;
use std::error::Error;
use std::fs::File;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::Path;
use std::process::exit;

use serde_json::Value;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 5 {
        println!("usage: jsonfilter <input-file> <output-file> <key> <value>");
        println!("{:#?}", args);
        exit(1);
    }
    let input_path = &args[1];
    let output_path = &args[2];
    let json_key = &args[3];
    let json_value = &args[4];
    let output_file = File::create(output_path)?;
    let mut file_writer = BufWriter::new(output_file);
    if let Ok(lines) = read_lines(input_path) {
        for line_result in lines {
            let line = line_result?;
            let row: Value = serde_json::from_str(&line)?;
            if row[json_key].as_str().expect("value is not a string") == json_value {
                file_writer.write_all(line.as_bytes())?;
            }
        }
    }
    Ok(())
}

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

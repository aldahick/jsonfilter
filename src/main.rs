use clap::Parser;
use progress::create_progress_bar;
use simd_json::{to_borrowed_value, BorrowedValue, ValueAccess};
use std::{collections::HashSet, error::Error, io::Write};

mod io;
mod progress;

const MEGABYTE: f64 = 1024.0 * 1024.0;
const GIGABYTE: f64 = MEGABYTE * 1024.0;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
  /// Optionally provide a zst archive to unpack.
  /// If archive is provided, it is extracted to the input path.
  #[arg(short, long, default_value = "")]
  archive: String,

  /// Input file - must be newline-separated json.
  #[arg(short, long)]
  input: String,

  /// Output file - newline-separated json will be written to this path.
  #[arg(short, long)]
  output: String,

  /// JSON key to filter on.
  #[arg(short, long)]
  key: String,

  /// JSON value to filter on.
  /// If omitted, the output (file) will instead contain all unique values of the key.
  #[arg(short, long, default_value = "")]
  filter: String,

  /// If set, do not log progress
  #[arg(short, long, default_value_t = false)]
  quiet: bool,
}

fn is_filtered(row: &BorrowedValue, key: &str, filter: &str) -> Option<bool> {
  let value = row.as_object()?.get(key)?;
  Some(value.as_str()? == filter)
}

fn get_json_value<'a>(row: &'a BorrowedValue<'a>, key: &'a str) -> Option<&'a str> {
  let value = row.as_object()?.get(key)?;
  value.as_str()
}

fn extract_archive(args: &Args) -> Result<(), Box<dyn Error>> {
  let total_size = io::get_size(&args.archive)?;
  println!(
    "Extracting {:.2} GB to {}",
    total_size as f64 / GIGABYTE,
    &args.input
  );
  let progress = create_progress_bar(total_size)?;
  let extracted_size = io::extract_archive(&args.archive, &args.input, &progress)?;
  println!(
    "Finished extracting {:.2} GB into {:.2} GB in {} seconds",
    total_size as f64 / GIGABYTE,
    extracted_size as f64 / GIGABYTE,
    progress.elapsed().as_secs()
  );
  Ok(())
}

fn write_unique_values(args: &Args) -> Result<(), Box<dyn Error>> {
  let total_size = io::get_size(&args.input)?;
  let mut total_read: u64 = 0;
  let lines = io::read_lines_buf(&args.input)?;
  let key = args.key.as_str();
  println!(
    "Writing unique values from {:.2} GB to {}",
    total_size as f64 / GIGABYTE,
    &args.output
  );
  let progress = create_progress_bar(total_size)?;
  let mut unique_values: HashSet<String> = HashSet::new();
  for line_result in lines {
    let mut line = line_result?;
    let line_len = line.len() as u64 + 1;
    let row = to_borrowed_value(&mut line)?;
    let value = get_json_value(&row, key);
    match value {
      Some(str) => {
        if unique_values.contains(str) {
          unique_values.insert(str.to_owned());
        }
      }
      None => {}
    }
    total_read += line_len;
    progress.set_position(total_read);
  }
  let json = simd_json::to_string(&unique_values)?;
  io::write_all(&args.output, json.as_bytes())?;
  println!(
    "Finished writing unique values from {:.2} GB into {:.2} MB in {} seconds",
    total_size as f64 / GIGABYTE,
    json.len() as f64 / MEGABYTE,
    progress.elapsed().as_secs()
  );
  Ok(())
}

fn write_filtered_rows(args: &Args) -> Result<(), Box<dyn Error>> {
  let total_size = io::get_size(&args.input)?;
  let mut total_read: u64 = 0;
  let mut total_wrote: u64 = 0;
  let mut writer = io::write_lines(&args.output)?;
  let lines = io::read_lines_buf(&args.input)?;
  let key = args.key.as_str();
  let filter = args.filter.as_str();
  println!(
    "Filtering {:.2} GB to {}",
    total_size as f64 / GIGABYTE,
    &args.output
  );
  let progress = create_progress_bar(total_size)?;
  for line_result in lines {
    let line = line_result?;
    let line_len = line.len() as u64 + 1;
    let mut line_clone = line.clone();
    let row: simd_json::BorrowedValue = to_borrowed_value(line_clone.as_mut_slice())?;
    if is_filtered(&row, key, filter).unwrap_or(false) {
      writer.write_all(&line)?;
      total_wrote += line_len;
    }
    total_read += line_len;
    progress.set_position(total_read);
  }
  println!(
    "Finished filtering {:.2} GB into {:.2} MB in {} seconds",
    total_size as f64 / GIGABYTE,
    total_wrote as f64 / MEGABYTE,
    progress.elapsed().as_secs()
  );
  Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
  let args = Args::parse();
  if !args.archive.is_empty() {
    extract_archive(&args)?;
  }
  if args.filter == "" {
    write_unique_values(&args)
  } else {
    write_filtered_rows(&args)
  }
}

use clap::Parser;
use progress::create_progress_bar;
use simd_json::{to_borrowed_value, BorrowedValue, ValueAccess};
use std::{error::Error, io::Write};

mod io;
mod progress;

const GIGABYTE: u64 = u64::pow(1024, 3);
const MEGABYTE: u64 = u64::pow(1024, 2);

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
  #[arg(short, long)]
  filter: String,

  /// If set, do not log progress
  #[arg(short, long, default_value_t = false)]
  quiet: bool,
}

fn is_filtered(row: &BorrowedValue, key: &str, filter: &str) -> Option<bool> {
  let value = row.as_object()?.get(key)?;
  Some(value.as_str()? == filter)
}

fn extract_archive(args: &Args) -> Result<(), Box<dyn Error>> {
  let total_size = io::get_size(&args.archive)?;
  println!("Extracting {} GB to {}", total_size / GIGABYTE, &args.input);
  let progress = create_progress_bar(total_size)?;
  let extracted_size = io::extract_archive(&args.archive, &args.input, &progress)?;
  println!(
    "Finished extracting {} GB into {} GB in {} seconds",
    total_size / GIGABYTE,
    extracted_size / GIGABYTE,
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
  println!("Filtering {} GB to {}", total_size / GIGABYTE, &args.output);
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
    "Finished filtering {} GB into {} MB in {} seconds",
    total_size / GIGABYTE,
    total_wrote / MEGABYTE,
    progress.elapsed().as_secs()
  );
  Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
  let args = Args::parse();
  if !args.archive.is_empty() {
    extract_archive(&args)?;
  }
  write_filtered_rows(&args)
}

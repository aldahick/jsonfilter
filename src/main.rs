use clap::Parser;
use progress::create_progress_bar;
use simd_json::{to_borrowed_value, BorrowedValue, ValueAccess};
use std::{error::Error, io::Write};

mod io;
mod progress;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
  /// Optionally provide a zst archive to unpack.
  /// If provided, input is the path within the archive.
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

fn main() -> Result<(), Box<dyn Error>> {
  let args = Args::parse();
  let total_size = io::get_size(&args.input)?;
  let progress = create_progress_bar(total_size)?;
  let mut total_read: u64 = 0;
  let mut total_wrote: u64 = 0;
  let mut writer = io::write_lines(&args.output)?;
  let lines = io::read_lines_buf(&args.input)?;
  let key = args.key.as_str();
  let filter = args.filter.as_str();
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
    total_size,
    total_wrote,
    progress.elapsed().as_secs()
  );
  Ok(())
}

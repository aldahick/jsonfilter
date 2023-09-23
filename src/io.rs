use std::{
  fs::File,
  io::{copy, BufRead, BufReader, BufWriter, Result},
  path::Path,
};

use indicatif::ProgressBar;
use zstd::Decoder;

pub fn get_size<P>(path: P) -> Result<u64>
where
  P: AsRef<Path>,
{
  let file = File::open(path)?;
  Ok(file.metadata()?.len())
}

pub fn write_lines<P>(filename: P) -> Result<BufWriter<File>>
where
  P: AsRef<Path>,
{
  let file = File::create(filename)?;
  Ok(BufWriter::new(file))
}

pub struct BufLines<B> {
  buf: B,
  chunk: Vec<u8>,
}

impl<B: BufRead> BufLines<B> {
  fn new(buf: B) -> BufLines<B> {
    BufLines {
      buf,
      chunk: Vec::new(),
    }
  }
}

impl<B: BufRead> Iterator for BufLines<B> {
  type Item = Result<Vec<u8>>;

  fn next(&mut self) -> Option<Self::Item> {
    self.chunk.clear();
    match self.buf.read_until(b'\n', &mut self.chunk) {
      Err(e) => Some(Err(e)),
      Ok(_) => {
        if self.chunk.len() > 0 {
          return Some(Ok(self.chunk.clone()));
        }
        None
      }
    }
  }
}

pub fn read_lines_buf<P>(path: P) -> Result<BufLines<BufReader<File>>>
where
  P: AsRef<Path>,
{
  let file = File::open(path)?;
  Ok(BufLines::new(BufReader::new(file)))
}

pub fn extract_archive<P>(from: P, to: P, progress_bar: &ProgressBar) -> Result<u64>
where
  P: AsRef<Path>,
{
  let archive_file = File::open(from)?;
  let extract_file = File::create(to)?;
  let mut decoder = Decoder::new(archive_file)?;
  decoder.window_log_max(31)?;
  let mut writer = BufWriter::new(&extract_file);
  let mut reader = progress_bar.wrap_read(decoder);
  copy(&mut reader, &mut writer)?;
  Ok(extract_file.metadata()?.len())
}

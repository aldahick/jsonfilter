use std::{
  fs::File,
  io::{copy, BufRead, BufReader, BufWriter, Result, Write},
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

pub fn write_all<P>(filename: P, data: &[u8]) -> Result<()>
where
  P: AsRef<Path>,
{
  let mut file = File::create(filename)?;
  file.write_all(data)
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
  let reader = progress_bar.wrap_read(BufReader::new(archive_file));
  let mut decoder = Decoder::new(reader)?;
  decoder.window_log_max(31)?;
  let mut writer = BufWriter::new(&extract_file);
  copy(&mut decoder, &mut writer)?;
  Ok(extract_file.metadata()?.len())
}

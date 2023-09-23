use std::{
  fs::File,
  io::{BufRead, BufReader, BufWriter, Error},
  path::Path,
};

pub fn get_size<P>(path: P) -> Result<u64, Error>
where
  P: AsRef<Path>,
{
  let file = File::open(path)?;
  Ok(file.metadata()?.len())
}

pub fn write_lines<P>(filename: P) -> Result<BufWriter<File>, Error>
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
  type Item = Result<Vec<u8>, Error>;

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

pub fn read_lines_buf<P>(path: P) -> Result<BufLines<BufReader<File>>, Error>
where
  P: AsRef<Path>,
{
  let file = File::open(path)?;
  Ok(BufLines::new(BufReader::new(file)))
}

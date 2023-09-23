use std::fmt::Write;

use indicatif::{style::TemplateError, ProgressBar, ProgressState, ProgressStyle};

pub fn create_progress_bar(total_size: u64) -> Result<ProgressBar, TemplateError> {
  let progress = ProgressBar::new(total_size);
  progress.set_style(
    ProgressStyle::with_template(
      "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})",
    )?
    .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
      write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
    })
    .progress_chars("#>-"),
  );
  Ok(progress)
}

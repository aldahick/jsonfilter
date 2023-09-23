# jsonfilter

filters a big file with JSON lines by a key/value and writes to another file

usage:

```text
Usage: jsonfilter [OPTIONS] --archive <ARCHIVE> --input <INPUT> --output <OUTPUT> --key <KEY> --filter <FILTER>

Options:
  -a, --archive <ARCHIVE>  Optionally provide a zst archive to unpack. If provided, input is the path within the archive
  -i, --input <INPUT>      Input file - must be newline-separated json
  -o, --output <OUTPUT>    Output file - newline-separated json will be written to this path
  -k, --key <KEY>          JSON key to filter on
  -f, --filter <FILTER>    JSON value to filter on
  -q, --quiet              If set, do not log progress
  -h, --help               Print help
  -V, --version            Print version
```

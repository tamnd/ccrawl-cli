---
title: "Bulk and archives"
description: "List, download, parse, and convert whole WARC, WAT, and WET files for a crawl."
weight: 30
---

Single-record fetching covers most questions. When you want volume, you work
with whole archive files: list their paths, download them, parse them locally,
and convert them into something your pipeline can read.

## Listing paths

`ccrawl paths <kind>` prints the file paths for a crawl, one per line, ready to
pipe into `download`. The kinds are `warc`, `wat`, `wet`, `robotstxt`,
`non200responses`, `cc-index`, `cc-index-table`, and `segment`:

```bash
ccrawl paths warc -c 2024-51            # every WARC path for the crawl
ccrawl paths wet -n 1                   # the first WET path
ccrawl paths robotstxt -o url           # full https URLs, not relative paths
```

## Downloading files

`ccrawl download <kind>` pulls whole files from the crawl manifest,
concurrently and resumably. Pass `-` instead of a kind to read an explicit list
of paths on stdin:

```bash
ccrawl download warc -n 5               # first 5 WARC files of the crawl
ccrawl download robotstxt -n 1          # one robots.txt archive
ccrawl paths wet -n 100 | ccrawl download -   # download a chosen list
```

Files land under `<data-dir>/raw` in the source directory tree by default; use
`--out` to choose a directory and `--flat` to drop the tree. Concurrency is set
by `--workers` (`-j`).

## Parsing a local archive

`ccrawl parse <file>` decodes a WARC, WAT, or WET file into structured records.
The format is detected from the file name and can be forced with `--format`:

```bash
ccrawl parse file.warc.gz -o jsonl
ccrawl parse file.warc.gz --type response --status 200 -o table -n 20
ccrawl parse file.warc.wet.gz --lang eng -o jsonl
```

The same content flags as `get` apply, so you can extract while you parse:

```bash
ccrawl parse file.warc.gz --links -o url       # every outbound link in the file
ccrawl parse file.warc.gz --text -o jsonl      # text of every response
ccrawl parse file.warc.gz --markdown -o jsonl  # Markdown of every response
```

`parse` reads stdin with `-`, so it pairs with `download` for a stream that
never hits disk:

```bash
ccrawl paths wet -n 1 | ccrawl download - --out - | ccrawl parse - --lang eng
```

## Converting to Parquet or JSONL

`ccrawl convert` turns archive files into columnar Parquet or line-delimited
JSON for your own tooling. It defaults to Parquet:

```bash
ccrawl convert file.warc.gz --to parquet -O out.parquet
ccrawl convert file.warc.wet.gz --to jsonl
ccrawl convert ./warc/ --to parquet --out ./parquet   # a whole directory
```

Add `--markdown` to convert HTML response bodies to Markdown as they are
written. Parquet output makes a crawl slice queryable in DuckDB, Athena, or
Spark without any further plumbing.

## Building a dataset library

For volume work you want the files in one tidy, browsable place rather than
scattered across ad-hoc `--out` dirs. The `--library` flag gives the archive
files a home and extends the same four commands to list, download, and process
them in place. The library lives under `~/notes/ccrawl` by default; override it
with `--library-dir` or `CCRAWL_LIBRARY`. It is a separate tree from the data
dir, so clearing scratch state never touches the corpus you keep.

```bash
ccrawl download wet -n 20 --library -c 2024-51    # pull 20 WET files into the library
ccrawl paths    wet --library -c 2024-51          # list the WET files you have locally
ccrawl parse    wet --library --lang eng -o jsonl # decode every local WET file, eng only
ccrawl convert  wet --library --to parquet        # write parquet beside the raw files
```

In library mode the argument is a kind, not a file. Raw archives land under
`<crawl>/<kind>/` and processed output under `<crawl>/<format>/<kind>/`, so a
directory listing is the index:

```
~/notes/ccrawl/CC-MAIN-2024-51/
  wet/                 raw WET archives
  parquet/wet/         the same files as Parquet
  jsonl/wet/           and as JSONL
```

A few things worth knowing:

- `download` skips files already present, so re-running only fetches what is
  missing and the corpus grows incrementally.
- `paths --library` lists what you have on disk for a kind, the local mirror of
  the remote manifest.
- `parse --library` streams every archive of a kind through one output, so a
  global `-n` caps the whole run rather than each file.
- `convert --library` picks the destination subtree from `--to`
  (`parquet`/`jsonl`); pass `-O/--out` to send it elsewhere.

`ccrawl config show` prints the active `library_dir` so you can confirm where a
run reads and writes.

## Counting files

`ccrawl stats` shows the shape of a crawl: how many files of each kind it ships.

```bash
ccrawl stats -c 2024-51
```

For row-level questions over a whole crawl, reach for
[the columnar index](/guides/columnar-index/) rather than downloading and
parsing everything.

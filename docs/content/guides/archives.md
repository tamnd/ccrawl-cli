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

## Counting files

`ccrawl stats` shows the shape of a crawl: how many files of each kind it ships.

```bash
ccrawl stats -c 2024-51
```

For row-level questions over a whole crawl, reach for
[the columnar index](/guides/columnar-index/) rather than downloading and
parsing everything.

# ccrawl

A fast, friendly command line for [Common Crawl](https://commoncrawl.org). One
binary that finds pages in the URL index, fetches the exact bytes Common Crawl
saw, streams WARC/WAT/WET archives, queries the columnar Parquet index, looks up
domain ranks, and builds datasets.

```
ccrawl get example.com --text
```

```
Example Domain
This domain is for use in documentation examples without needing permission.
Learn more
```

## Why

Working with Common Crawl usually means stitching together the CDX API, S3
paths, multi-member gzip WARC files, and a pile of Python. ccrawl puts all of it
behind one tool with sensible defaults, real output formats, and pipelines that
compose. It speaks to the public data on `data.commoncrawl.org` over plain
HTTPS, so there are no credentials to set up and nothing to pay for.

## Install

```sh
go install github.com/tamnd/ccrawl-cli/cmd/ccrawl@latest
```

Or grab a prebuilt binary from the [releases page](https://github.com/tamnd/ccrawl-cli/releases).
The binary is pure Go with no runtime dependencies. DuckDB is optional and only
needed for the columnar index commands (see [Columnar index](#columnar-index)).

Build from source:

```sh
git clone https://github.com/tamnd/ccrawl-cli
cd ccrawl-cli
make build      # produces ./ccrawl
```

## Quick start

```sh
ccrawl crawls latest                  # newest crawl ID, for example CC-MAIN-2026-21
ccrawl search example.com             # captures of a URL in the index
ccrawl get example.com --text         # the readable text of the latest capture
ccrawl get example.com --markdown     # the same page as Markdown
ccrawl search 'example.com/*' -o url  # every captured URL under a path
```

## How it works

Common Crawl publishes a new crawl most months. Each crawl ships:

- a **URL index** (the CDX server and a columnar Parquet copy) that maps a URL to
  the WARC file, byte offset, and length where its capture lives,
- **WARC** files holding the full HTTP request and response,
- **WAT** files with extracted metadata and links,
- **WET** files with plain text.

ccrawl uses the index to find a capture, then fetches just that record with an
HTTP byte-range request. A WARC file is a stream of gzip members, one per record,
so a single record decompresses on its own without downloading the whole file.
That is what makes `ccrawl get` feel instant.

## Commands

| Command | What it does |
| --- | --- |
| `crawls` | List, resolve, and inspect the monthly crawls |
| `search` | Query the URL index (CDX) for captures of a URL or pattern |
| `get` | Fetch what Common Crawl captured for a URL (curl for Common Crawl) |
| `fetch` | Retrieve WARC records by explicit location, or from stdin |
| `download` | Download whole archive files for a crawl |
| `paths` | List the archive file paths for a crawl |
| `parse` | Decode a local WARC/WAT/WET file into records |
| `extract` | Pull text, links, title, or Markdown from a captured page |
| `news` | Work with the continuous CC-NEWS dataset |
| `table` | Query the columnar Parquet index (alias `columnar`, `athena`) |
| `rank` | Look up host and domain ranks from the web graph |
| `db` | Build and query a local DuckDB database |
| `convert` | Convert WARC/WAT/WET archives to Parquet or JSONL |
| `stats` | Show the shape of a crawl: file counts per archive kind |
| `config` | Show resolved configuration and data paths |
| `cache` | Inspect and clear the on-disk cache |

Run `ccrawl <command> --help` for the full flag list on any command.

## Recipes

Find every PDF Common Crawl saw on a domain and download them:

```sh
ccrawl search 'example.com/*' --mime application/pdf -o jsonl \
  | ccrawl fetch - --dir --out-dir pdfs/
```

Get the latest text of a page and pipe it somewhere:

```sh
ccrawl get example.com --text | wc -w
```

Collect outbound links from a page as a clean list:

```sh
ccrawl get example.com --links -o url
```

Search one specific crawl instead of the latest:

```sh
ccrawl search example.com -c 2024-51
```

Stream a local WARC you already downloaded:

```sh
ccrawl parse local.warc.gz --type response -o table -n 20
```

Scan CC-NEWS for a publisher (CC-NEWS has no index, so this streams the month):

```sh
ccrawl news search bbc.co.uk --year 2026 --month 5 -n 50
```

## Output formats

Every list command renders through the same formatter. Pick a format with `-o`,
or let ccrawl choose: a table when writing to a terminal, JSONL when piped.

```sh
ccrawl search example.com -o table   # aligned columns for reading
ccrawl search example.com -o jsonl   # one JSON object per line, for piping
ccrawl search example.com -o json    # a single JSON array
ccrawl search example.com -o csv     # spreadsheet friendly
ccrawl search example.com -o url     # just the URL column
```

Narrow the columns with `--fields`, or template each row:

```sh
ccrawl search example.com --fields url,status
ccrawl search example.com --template '{{.URL}} {{.Status}}'
```

## Columnar index

The columnar (Parquet) index is the fastest way to answer bulk questions across a
whole crawl without touching a single WARC. ccrawl builds the SQL for you.

```sh
ccrawl table urls --tld gov --mime application/pdf -o url
ccrawl table count --domain example.com
ccrawl table langs --tld jp
```

These run against the public Parquet files using a local `duckdb` binary if one
is on your PATH. If DuckDB is not installed, ccrawl prints ready-to-run SQL so you
can paste it into DuckDB, Athena, Spark, or Trino yourself:

```sh
ccrawl table sql --tld gov --mime application/pdf --print
```

Install DuckDB from [duckdb.org](https://duckdb.org/docs/installation) to run the
queries directly. The ccrawl binary never links DuckDB, so installs stay small and
pure Go.

The `locations` subcommand emits exactly the records `ccrawl fetch` reads, so the
columnar index and the byte-range fetcher compose:

```sh
ccrawl table locations --domain example.com -o jsonl | ccrawl fetch - --text
```

## Configuration

ccrawl stores its cache and downloads under XDG directories by default. See the
resolved paths and settings any time:

```sh
ccrawl config show
```

Useful global flags (all have sensible defaults):

| Flag | Meaning |
| --- | --- |
| `-c, --crawl` | Crawl ID, year, or `latest`/`all` (default `latest`) |
| `-o, --output` | Output format (default auto) |
| `-n, --limit` | Maximum results (`0` means unlimited) |
| `-j, --workers` | Concurrency for downloads and scans |
| `--source` | Bulk data source: `https` or `s3` |
| `--rate` | Minimum delay between requests, to stay polite |
| `--no-cache` | Bypass the on-disk cache |

## Development

```sh
make test    # run the test suite
make vet     # go vet
make build   # build ./ccrawl
```

The code is two packages: `ccrawl/` is the library (clients, parsers,
extractors), and `cli/` is the command tree built on Cobra. The library has no
dependency on the CLI, so it is usable on its own.

## License

[Apache 2.0](LICENSE).

Common Crawl data is provided by the [Common Crawl Foundation](https://commoncrawl.org)
under their [terms of use](https://commoncrawl.org/terms-of-use). This project is
an independent client and is not affiliated with the foundation.

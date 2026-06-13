---
title: "CLI"
description: "Every command and subcommand, with the flags that matter."
weight: 10
---

```
ccrawl <command> [subcommand] [flags]
```

Run `ccrawl <command> --help` for the full flag list on any command. This page
is the map.

## Commands

| Command | What it does |
|---|---|
| `crawls` | List, resolve, and inspect the monthly crawls |
| `search` | Query the URL index (CDX) for captures of a URL or pattern |
| `get` | Fetch what Common Crawl captured for a URL (the curl for Common Crawl) |
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
| `version` | Print the version and exit |

## crawls

| Subcommand | Does |
|---|---|
| `crawls list` | List the monthly crawls, newest first |
| `crawls latest` | Print the newest crawl ID |
| `crawls resolve <ref>` | Resolve a year or `latest` to a crawl ID |
| `crawls info <id>` | File counts per archive kind for a crawl |

## search

```
ccrawl search <url|pattern> [flags]
```

A trailing `/*` matches everything under a path. Filters: `--mime`, `--status`.
Shaping: `--fields`, `--template`, `-o`, `-n`. Aliases: `index`, `cdx`.

## get

```
ccrawl get <url> [flags]
```

Content flags (pick one): `--text`, `--markdown`, `--links`, `--headers`. With
none, prints the raw HTTP response body.

## fetch

```
ccrawl fetch [-] [flags]
```

Locate a record with `--file`, `--offset`, `--length`, or stream JSONL
locations on stdin with `-`. Content flags: `--body` (default), `--text`,
`--markdown`, `--links`, `--headers`, `--meta`. Write one file per record with
`--dir` and `--out-dir`.

## download

```
ccrawl download <kind|-> [flags]
```

Kinds: `warc`, `wat`, `wet`, `robotstxt`, `non200responses`, `cc-index`,
`cc-index-table`. Use `-` to read paths on stdin. `--out` sets the directory,
`--flat` drops the source tree, `-j/--workers` sets concurrency. `--library`
files the archives under `<library>/<crawl>/<kind>/` instead of the data dir
(see [bulk and archives](/guides/archives/)).

## paths

```
ccrawl paths <kind> [flags]
```

Kinds: `warc`, `wat`, `wet`, `robotstxt`, `non200responses`, `cc-index`,
`cc-index-table`, `segment`. `--kinds` lists them. `-o url` prints full URLs.
`--library` lists the files of a kind already in the library instead of the
remote manifest.

## parse

```
ccrawl parse <file|-> [flags]
```

Force the format with `--format` (`warc|wat|wet`). Filters: `--type`,
`--status`, `--mime`, `--lang`, `--url`. Content: `--links`, `--text`,
`--markdown`, `--meta`. With `--library` the argument is a kind and `parse`
decodes every local archive of that kind through one output.

## extract

| Subcommand | Does |
|---|---|
| `extract title <url>` | The page title |
| `extract text <url>` | Readable plain text |
| `extract markdown <url>` | HTML converted to Markdown |
| `extract links <url>` | Outbound links |

## news

| Subcommand | Does |
|---|---|
| `news list` | List CC-NEWS files for `--year`/`--month` |
| `news download` | Download CC-NEWS files |
| `news search <host>` | Stream and match a host (no index) |

## table

Aliases: `columnar`, `athena`.

| Subcommand | Does |
|---|---|
| `table urls` | Matching URLs |
| `table locations` | Record locations, ready for `fetch` |
| `table count` | Count of matching captures |
| `table langs` | Breakdown by content language |
| `table mimes` | Breakdown by MIME type |
| `table sql` | Build the SQL from the filter flags and print it |
| `table query <sql>` | Run raw SQL (`ccindex` is the source) |
| `table schema` | The columns of the index |

Filters across the subcommands: `--domain`, `--host`, `--tld`, `--mime`,
`--status`, `--lang`, `--path-prefix`, `--subset`. Engine: `--engine`
(`auto|duckdb|print`), `--print`.

## rank

| Subcommand | Does |
|---|---|
| `rank domain <domain>` | Rank of a registered domain |
| `rank host <host>` | Rank of a host |
| `rank top` | Top-ranked hosts or domains |

Always pass `--table <url>` (the gzipped rank table). `top` takes `--tld`.

## db

| Subcommand | Does |
|---|---|
| `db load` | Load matching index records into local DuckDB |
| `db sql <query>` | Run SQL against the local database |
| `db shell` | Open an interactive DuckDB shell |
| `db path` | Print the database file path |

`db load` takes the same filter flags as `table`.

## convert

```
ccrawl convert <file|dir> [flags]
```

`--to parquet|jsonl` (default `parquet`). `-O/--out` sets the output file or
directory. `--markdown` converts HTML bodies on the way. With `--library` the
argument is a kind: `convert` reads every local archive of that kind and writes
the output under `<crawl>/<format>/<kind>/`.

## Meta

| Command | Does |
|---|---|
| `stats` | File counts per archive kind for a crawl |
| `config show` | Resolved configuration and data paths |
| `cache info` | Cache size and entry count |
| `cache dir` | Print the cache directory |
| `cache clear` | Remove every cached entry |
| `version` | Print the version and exit |

## Global flags

These apply to most commands. See [configuration](/reference/configuration/)
for the full list and their defaults.

| Flag | Meaning |
|---|---|
| `-c, --crawl` | Crawl ID, year, or `latest`/`all` (default `latest`) |
| `-o, --output` | Output format (default auto) |
| `-n, --limit` | Maximum results (`0` means unlimited) |
| `-j, --workers` | Concurrency for downloads and scans |
| `--source` | Bulk data source: `https` or `s3` |
| `--rate` | Minimum delay between requests |
| `--no-cache` | Bypass the on-disk cache |
| `--library` | Read and write under the dataset library |
| `--library-dir` | Library root (default `~/notes/ccrawl`) |
| `--data-dir` | Root data directory |

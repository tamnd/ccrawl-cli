---
title: "CLI"
description: "Every command and subcommand, with the flags that matter."
weight: 10
---

```
ccrawl <command> [subcommand] [flags]
```

Run `ccrawl <command> --help` for the full flag list on any command.

## Commands

| Command | What it does |
|---|---|
| `crawls` | List, resolve, and inspect the monthly crawls |
| `search` | Query the URL index (CDX) for captures of a URL or pattern |
| `get` | Fetch what Common Crawl captured for a URL |
| `fetch` | Retrieve WARC records by explicit location, or from stdin |
| `download` | Download whole archive files for a crawl |
| `paths` | List the archive file paths for a crawl |
| `parse` | Decode a local WARC/WAT/WET file into records |
| `extract` | Pull text, links, title, or Markdown from a captured page |
| `content` | Live-fetch content signals: text, outlinks, quality |
| `news` | Work with the continuous CC-NEWS dataset |
| `table` | Query the columnar Parquet index |
| `rank` | Look up host and domain ranks from the web graph |
| `host` | Enumerate and enrich hosts from the CC web graph |
| `crawl` | Recrawl engine: seed, fetch, and write WARC output |
| `sched` | Recrawl scheduling: tier assignment and differential CDX analysis |
| `index` | Build and query a local BM25 full-text search index |
| `api` | Start the v2 REST API server |
| `db` | Build and query a local DuckDB database |
| `convert` | Convert WARC/WAT/WET archives to Parquet or JSONL |
| `stats` | Show the shape of a crawl: file counts per archive kind |
| `config` | Show resolved configuration and data paths |
| `cache` | Inspect and clear the on-disk cache |
| `version` | Print the version and exit |

---

## crawls

| Subcommand | Does |
|---|---|
| `crawls list` | List the monthly crawls, newest first |
| `crawls latest` | Print the newest crawl ID |
| `crawls resolve <ref>` | Resolve a year or `latest` to a crawl ID |
| `crawls info <id>` | File counts per archive kind for a crawl |

---

## search

```
ccrawl search <url|pattern> [flags]
```

A trailing `/*` matches everything under a path.
Filters: `--mime`, `--status`.
Shaping: `--fields`, `--template`, `-o`, `-n`.
Alias: `cdx`.

---

## get

```
ccrawl get <url> [flags]
```

Content flags (pick one): `--text`, `--markdown`, `--links`, `--headers`.
With none, prints the raw HTTP response body.

---

## fetch

```
ccrawl fetch [-] [flags]
```

Locate a record with `--file`, `--offset`, `--length`, or stream JSONL locations on stdin with `-`.
Content flags: `--body` (default), `--text`, `--markdown`, `--links`, `--headers`, `--meta`.
Write one file per record with `--dir` and `--out-dir`.

---

## download

```
ccrawl download <kind|-> [flags]
```

Kinds: `warc`, `wat`, `wet`, `robotstxt`, `non200responses`, `cc-index`, `cc-index-table`.
Use `-` to read paths on stdin.
`--out` sets the directory, `--flat` drops the source tree, `-j/--workers` sets concurrency.

---

## paths

```
ccrawl paths <kind> [flags]
```

Kinds: `warc`, `wat`, `wet`, `robotstxt`, `non200responses`, `cc-index`, `cc-index-table`, `segment`.
`--kinds` lists them.
`-o url` prints full URLs.

---

## parse

```
ccrawl parse <file|-> [flags]
```

Force the format with `--format` (`warc|wat|wet`).
Filters: `--type`, `--status`, `--mime`, `--lang`, `--url`.
Content flags: `--links`, `--text`, `--markdown`, `--meta`.

---

## extract

| Subcommand | Does |
|---|---|
| `extract title <url>` | The page title |
| `extract text <url>` | Readable plain text |
| `extract markdown <url>` | HTML converted to Markdown |
| `extract links <url>` | Outbound links |

---

## content

Live-fetch a URL and compute content signals.
Unlike `extract`, these commands use the v2 crawler config (10 MB body limit, brotli support, redirect following).

| Subcommand | Does |
|---|---|
| `content extract <url>` | Clean text, title, description, canonical URL, language, word count |
| `content outlinks <url>` | Structured outbound links with anchor text |
| `content quality <url>` | Quality signals: word count, spam score, parked detection, short-content flag |

```sh
ccrawl content extract https://golang.org/
ccrawl content quality https://example.com/ -o json
ccrawl content outlinks https://news.ycombinator.com/ -n 20
```

---

## news

| Subcommand | Does |
|---|---|
| `news list` | List CC-NEWS files for `--year`/`--month` |
| `news download` | Download CC-NEWS files |
| `news search <host>` | Stream and match a host (no index) |

---

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

Filters: `--domain`, `--host`, `--tld`, `--mime`, `--status`, `--lang`, `--path-prefix`, `--subset`.
Engine: `--engine` (`auto|duckdb|print`).

---

## rank

| Subcommand | Does |
|---|---|
| `rank domain <domain>` | Rank of a registered domain |
| `rank host <host>` | Rank of a host |
| `rank top` | Top-ranked hosts or domains (requires `--table <url>`) |

`rank top` takes `--tld` to filter by TLD.

---

## host

Enumerate and enrich hosts from the CC web graph.
All subcommands accept `--graph <release-id>` to pin a specific web-graph release (default: latest).

| Subcommand | Does |
|---|---|
| `host top` | Top hosts by harmonic centrality, streamed from the rank table |
| `host get <hostname>` | Enriched profile for one host |
| `host vertices` | Stream the vertex ID to hostname mapping |
| `host degrees` | Compute in-degree and out-degree from edge files (~7.7 GB) |
| `host cdx` | Aggregate CDX statistics per host via DuckDB |
| `host enrich` | Full enrichment pipeline: rank + degrees + CDX |
| `host dataset` | Build all 262M hosts as partitioned Parquet shards |

### host top

```sh
ccrawl host top -n 20 -o table
ccrawl host top --graph cc-main-2026-mar-apr-may -n 1000 -o jsonl > top1k.jsonl
```

### host get

```sh
ccrawl host get golang.org -o json
```

### host vertices

```sh
ccrawl host vertices --graph cc-main-2026-mar-apr-may -n 5
```

### host degrees

Streams all edge files to compute per-host in/out-degree.
Requires ~7.7 GB of edge data.

```sh
ccrawl host degrees --graph cc-main-2026-mar-apr-may -n 100 -o jsonl
```

### host cdx

Runs a DuckDB `GROUP BY url_host_name` over the columnar Parquet index.
Without `--filter` this scans ~184 GB of Parquet.

```sh
ccrawl host cdx --filter example.com -o json
ccrawl host cdx -n 100 -o jsonl
```

| Flag | Meaning |
|---|---|
| `--filter` | Restrict to one host (`url_host_name`) |

### host enrich

Runs all enrichment phases in sequence.
Phases 3 and 4 are opt-in because they require large data transfers.

```sh
ccrawl host enrich -n 20
ccrawl host enrich --graph cc-main-2026-mar-apr-may -n 100
ccrawl host enrich --degrees --cdx -o jsonl > enriched.jsonl
```

| Flag | Meaning |
|---|---|
| `--graph` | Web-graph release ID (default: latest) |
| `--degrees` | Phase 3: compute in/out-degree from edge files (~7.7 GB) |
| `--cdx` | Phase 4: aggregate CDX statistics via DuckDB (~184 GB) |

### host dataset

Builds a complete host dataset from scratch: ~184 GB of CDX Parquet + the rank table, joined and written as 28 per-prefix Parquet shards.
Uses pure-Go parallel download (no DuckDB dependency) with per-phase resume markers.

```sh
ccrawl host dataset --work-dir /data/cc-work --out-dir /data/cc-shards
ccrawl host dataset --prefix a --work-dir /data/cc-work --out-dir /data/cc-shards
ccrawl host dataset --upload --hf-repo your-org/cc-host-dataset --work-dir /data --out-dir /data/shards
```

| Flag | Meaning |
|---|---|
| `--work-dir` | Directory for intermediate per-prefix files (default: `~/.ccrawl/dataset`) |
| `--out-dir` | Directory for output Parquet shards (default: `.`) |
| `--prefix` | Process only this prefix (a–z, 0, misc); empty = all 28 |
| `--cdx-workers` | Concurrent CDX Parquet download workers (default 8) |
| `--cdx-limit` | Stop after N CDX files; 0 = all (benchmarking only) |
| `--skip-cdx-raw` | Skip CDX extract phase (assume `cdx-raw-*.jsonl.gz` present) |
| `--skip-cdx-agg` | Skip CDX aggregate phase (assume `cdx-agg-*.jsonl.gz` present) |
| `--skip-rank-split` | Skip rank-split phase (assume `rank-*.tsv.gz` present) |
| `--upload` | Upload each shard to HuggingFace after building |
| `--hf-repo` | HuggingFace dataset repository (default: `open-index/cc-host-dataset`) |
| `--graph` | Web-graph release ID (default: latest) |

---

## crawl

Recrawl engine commands for seeding and fetching live URLs.

| Subcommand | Does |
|---|---|
| `crawl seed` | Generate seed URLs from the web-graph rank table |
| `crawl fetch <url>` | Crawl a single URL with robots.txt checking and content digest |
| `crawl status` | Show daily crawl budget allocation across the five recrawl tiers |

### crawl seed

Streams the rank table and emits one seed URL per host.
Use `--max-tier` to restrict to high-priority hosts (tier 1 = top 100 K by harmonic rank, tier 5 = all).

```sh
ccrawl crawl seed -n 100 -o table
ccrawl crawl seed --max-tier 2 -n 1000000 -o jsonl > seeds.jsonl
ccrawl crawl seed --graph cc-main-2026-mar-apr-may --max-tier 3 -n 5000000
```

| Flag | Meaning |
|---|---|
| `--graph` | Web-graph release ID (default: latest) |
| `--max-seeds` | Maximum hosts to emit (default 10 000 000) |
| `--max-tier` | Skip hosts with tier higher than this (1-5, default 5 = all) |

### crawl fetch

Fetches one URL with the v2 crawler config: polite user-agent, brotli support, redirect following (up to 5 hops), 10 MB body limit, SHA-1 digest.

```sh
ccrawl crawl fetch https://golang.org/ -o json
ccrawl crawl fetch https://example.com/ --robots -o json
```

| Flag | Meaning |
|---|---|
| `--robots` | Check robots.txt before fetching |

### crawl status

Prints the daily page budget across the five recrawl tiers assuming 10 000 pages/s sustained throughput.

```sh
ccrawl crawl status -o table
```

---

## sched

Recrawl scheduling commands.
`sched diff` requires DuckDB on PATH.

| Subcommand | Does |
|---|---|
| `sched assign` | Assign crawl tiers to hosts by harmonic rank and change rate |
| `sched diff` | Compare two crawls and compute per-host content change rates |

### sched assign

```sh
ccrawl sched assign -n 20 -o table
ccrawl sched assign --graph cc-main-2026-mar-apr-may --change-rate 0.5 -o jsonl
```

| Flag | Meaning |
|---|---|
| `--graph` | Web-graph release ID (default: latest) |
| `--change-rate` | Assumed change rate for all hosts (0-1, default 0.5) |

Tier assignment:

| Tier | Recrawl interval | Criteria |
|---|---|---|
| 1 | 24 h | harmonic rank <= 100 K and change rate > 0.8 |
| 2 | 3 days | rank <= 1 M and change rate >= 0.5 |
| 3 | 7 days | rank <= 5 M and change rate >= 0.2 |
| 4 | 30 days | rank <= 10 M |
| 5 | on-demand | everything else |

### sched diff

Joins two CDX Parquet indexes on URL and compares `content_digest` to compute per-host change rates.
Requires DuckDB on PATH.
Scans ~368 GB of Parquet (184 GB per crawl).

```sh
ccrawl sched diff --crawl-a CC-MAIN-2026-17 --crawl-b CC-MAIN-2026-21 -n 20
ccrawl sched diff --crawl-a CC-MAIN-2026-12 --crawl-b CC-MAIN-2026-17 -o jsonl > changes.jsonl
```

| Flag | Meaning |
|---|---|
| `--crawl-a` | Older crawl ID |
| `--crawl-b` | Newer crawl ID |

---

## index

Build and query a local BM25 full-text search index over any set of URLs.

| Subcommand | Does |
|---|---|
| `index build` | Fetch URLs, extract text, and build a BM25 inverted index |
| `index search <query>` | Query the index; results ranked by BM25 score |

### index build

Fetches each URL in parallel (8 workers by default), extracts clean text, tokenizes it, and writes a BM25 inverted index with per-document length normalization.
The index directory contains `terms.dat`, `postings.dat`, `forward.jsonl`, and `stats.dat`.

```sh
ccrawl index build --urls https://golang.org/,https://pkg.go.dev/ -o json
ccrawl index build --dir /data/idx --urls https://example.com/ --workers 16
ccrawl index build --dir /data/idx --input docs.jsonl
```

| Flag | Meaning |
|---|---|
| `--dir` | Directory to write the index into (default: `~/data/ccrawl/index`) |
| `--urls` | Comma-separated URLs to fetch and index |
| `--input` | JSONL file of `ForwardDoc` records to index directly |
| `--workers` | Parallel fetch workers (default 8) |

### index search

Queries the local index using BM25 scoring with per-document length normalization and optional link-graph boost.

```sh
ccrawl index search "golang web server"
ccrawl index search "machine learning" --dir /data/idx -n 20 -o json
```

| Flag | Meaning |
|---|---|
| `--dir` | Index directory to search (default: `~/data/ccrawl/index`) |

---

## api

Start the v2 HTTP REST API server.
The host store is loaded from the web-graph rank table on startup (top 1 M hosts).
Full-text search is available when `--index-dir` points to a built index.

```
GET /v2/host/{host}       enriched host profile
GET /v2/hosts?tld=&n=     top N hosts, optional TLD filter
GET /v2/search?q=&k=      BM25 full-text search (requires --index-dir)
GET /v2/health            health check
```

```sh
ccrawl api --addr :8080
ccrawl api --addr :8080 --index-dir /data/idx
```

| Flag | Meaning |
|---|---|
| `--addr` | Listen address (default `:8080`) |
| `--index-dir` | Path to a built inverted index directory |

---

## db

| Subcommand | Does |
|---|---|
| `db load` | Load matching index records into local DuckDB |
| `db sql <query>` | Run SQL against the local database |
| `db shell` | Open an interactive DuckDB shell |
| `db path` | Print the database file path |

`db load` takes the same filter flags as `table`.

---

## convert

```
ccrawl convert <file|dir> [flags]
```

`--to parquet|jsonl` (default `parquet`).
`-O/--out` sets the output file or directory.
`--markdown` converts HTML bodies on the way.

---

## Global flags

These apply to every command.

| Flag | Short | Meaning | Default |
|---|---|---|---|
| `--crawl` | `-c` | Crawl ID, year, or `latest`/`all` | `latest` |
| `--output` | `-o` | Output format: `auto`, `table`, `json`, `jsonl`, `csv`, `tsv`, `url`, `raw` | `auto` |
| `--limit` | `-n` | Maximum records (0 = unlimited) | `0` |
| `--workers` | `-j` | Concurrency for downloads and scans | `8` |
| `--source` | | Bulk data source: `https` or `s3` | `https` |
| `--rate` | | Minimum delay between requests | `0s` |
| `--timeout` | | Per-request timeout | `0s` |
| `--no-cache` | | Bypass the on-disk cache | false |
| `--fields` | | Comma-separated columns to show | |
| `--template` | | Go template applied per record | |
| `--library` | | Read and write under the dataset library | false |
| `--library-dir` | | Library root | `~/notes/ccrawl` |
| `--data-dir` | | Root data directory | |
| `--dry-run` | | Print actions, do not perform them | false |
| `--quiet` | `-q` | Suppress progress output | false |
| `--verbose` | `-v` | Increase verbosity (repeatable) | |
| `--color` | | Color output: `auto`, `always`, `never` | `auto` |
| `--no-header` | | Omit the header row in table output | false |
| `--db` | | Tee every record into a store (e.g. `out.db`, `postgres://...`) | |
| `--profile` | | Named profile to load | |

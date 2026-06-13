---
title: "Configuration"
description: "The data directory, environment variables, and global flags, with their defaults."
weight: 20
---

ccrawl needs almost no configuration. There is no config file; every option is
a flag or an environment variable, and the defaults are chosen so the common
case needs neither.

## The data directory

ccrawl keeps all of its state under one tree, `~/data/ccrawl` by default: the
on-disk cache, downloaded archives, converted Parquet, and the local DuckDB
file. See the resolved paths any time:

```bash
ccrawl config show
```

```
data_dir     ~/data/ccrawl
cache_dir    ~/data/ccrawl/cache
raw_dir      ~/data/ccrawl/raw
parquet_dir  ~/data/ccrawl/parquet
db_path      ~/data/ccrawl/ccrawl.duckdb
```

Point the whole tree somewhere else with `CCRAWL_DATA_DIR`, or per-command with
`--data-dir`.

## Environment variables

| Variable | Used for |
|---|---|
| `CCRAWL_DATA_DIR` | Root data directory (overrides the default `~/data/ccrawl`) |

## Global flags

| Flag | Default | Meaning |
|---|---|---|
| `-c, --crawl` | `latest` | Crawl ID, a year, or `latest`/`all` |
| `-o, --output` | auto | `table`, `json`, `jsonl`, `csv`, `tsv`, `url`, `raw` |
| `-n, --limit` | `0` | Maximum results; `0` is unlimited |
| `-j, --workers` | per command | Concurrency for downloads and scans |
| `--source` | `https` | Bulk data source: `https` or `s3` |
| `--rate` | `200ms` | Minimum delay between requests, to stay polite |
| `--retries` | `5` | Retry attempts on 429 and 5xx |
| `--timeout` | `2m` | Per-request timeout |
| `--no-cache` | off | Bypass the on-disk cache for this run |
| `--data-dir` | `~/data/ccrawl` | Root data directory |
| `--fields` | all | Comma-separated columns to show |
| `--template` | none | Go text/template applied per row |
| `--no-header` | off | Omit the header row in table/csv output |
| `--color` | auto | `auto`, `always`, or `never` |
| `-q, --quiet` | off | Suppress progress output |
| `-v, --verbose` | off | Increase verbosity (repeatable) |
| `--dry-run` | off | Print actions without performing them |

## Output auto-detection

The default output format adapts to where it is going: an aligned table when
the output is a terminal, JSONL when it is piped. That keeps interactive use
readable and scripted use parseable without you setting `-o` either time. See
[output formats](/reference/output/) for the full set.

## Caching and politeness

ccrawl caches small index responses and manifests on disk so repeated commands
do not re-fetch them. `--rate` keeps a minimum gap between requests so a busy
session stays a good citizen against the public data. `cache info`, `cache
dir`, and `cache clear` manage the cache.

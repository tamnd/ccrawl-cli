---
title: "Markdown pipelines"
description: "The markdown export and refetch commands: schemas, HuggingFace layout, resume behaviour, and the three knobs that set throughput."
weight: 15
---

The `markdown` group turns Common Crawl WARC files into Markdown Parquet datasets and commits them to a HuggingFace dataset repo.
There are two pipelines and they answer different questions.

| Command | Source of the HTML | Use it when |
|---|---|---|
| `markdown export` | the HTML Common Crawl already captured | you want the crawl as it was, no live traffic |
| `markdown refetch` | a live fetch of every URL in the shard | you want today's page plus response metadata |

Both walk the same unit of work: one WARC file from a crawl's manifest is one shard, one shard becomes one Parquet file, and one Parquet file lands at a fixed path in the repo.

```sh
ccrawl markdown export --shards 0 --repo open-index/open-markdown-v2
ccrawl markdown refetch --shards 0 --repo open-index/open-markdown-refetch-v1
```

Set `HF_TOKEN` before either command, or pass `--push=false` to write Parquet locally and skip HuggingFace entirely.

## Selecting shards

`--shards` takes a single index, an inclusive range, a comma list, a mix of the two, or `all`.
Indices are 0-based positions in the crawl's `warc.paths.gz` manifest, which has around 90 000 entries per crawl.

```sh
ccrawl markdown export --shards 0
ccrawl markdown export --shards 0-49
ccrawl markdown export --shards 1,3,5
ccrawl markdown export --shards 0-9,20
ccrawl markdown export --shards all --limit 200
```

An out-of-range index is a usage error, not a silent skip.
`--limit` trims the resolved list after expansion, so `--shards all --limit 200` means the first 200 shards of the manifest.

## Output schemas

### open-markdown-v2, written by `markdown export`

| Column | Type | Meaning |
|---|---|---|
| `doc_id` | string | stable SHA-256 of the URL, first 16 bytes as hex |
| `url` | string | the URL Common Crawl captured |
| `host` | string | hostname taken from the URL |
| `crawl_date` | string | `WARC-Date` of the record, `YYYY-MM-DD` |
| `warc_record_id` | string | `WARC-Record-ID` of the source record |
| `html_length` | int | raw HTML body bytes before conversion |
| `markdown_length` | int | converted Markdown bytes |
| `markdown` | string | the converted Markdown |

### open-markdown-refetch-v1, written by `markdown refetch`

Everything the export schema has, plus the live response and its timings.
`doc_id` is computed the same way, so the two datasets join on it directly.

| Column | Type | Meaning |
|---|---|---|
| `doc_id` | string | stable SHA-256 of the URL, first 16 bytes as hex |
| `url` | string | the URL taken from the Common Crawl shard |
| `final_url` | string | URL after redirects |
| `host` | string | hostname |
| `ip_address` | string | server IP the fetch landed on |
| `crawl_id` | string | the CC crawl the URL came from, for example `CC-MAIN-2026-25` |
| `crawl_date` | string | `WARC-Date` of the original record |
| `warc_record_id` | string | `WARC-Record-ID` of the original record |
| `fetched_at` | int64 | Unix milliseconds of the live fetch |
| `status` | int | HTTP status code |
| `content_type` | string | `Content-Type` response header |
| `fetch_duration_ms` | int | whole fetch wall clock |
| `ttfb_ms` | int | time to first byte |
| `etag` | string | `ETag` response header |
| `last_modified` | string | `Last-Modified` response header |
| `resp_headers` | string | full response head, status line and headers |
| `body_length` | int | raw body bytes |
| `digest` | string | SHA-1 hex of the raw body |
| `html_length` | int | HTML body bytes, set only when the response is HTML |
| `markdown_length` | int | converted Markdown bytes |
| `markdown` | string | the converted Markdown |
| `error` | string | fetch error, empty on success |

A failed fetch still produces a row.
That is deliberate: a shard of Common Crawl URLs contains a lot of dead hosts, and knowing a URL is gone is a result worth keeping.
Filter on `error = ''` when you only want live pages.

## HuggingFace path layout

Both pipelines write the same layout, so the two repos load with the same code.

```
data/crawl=CC-MAIN-2026-25/000000.parquet
data/crawl=CC-MAIN-2026-25/000001.parquet
data/crawl=CC-MAIN-2026-25/000042.parquet
```

The number is the shard index, zero padded to six digits, so a file name maps straight back to a manifest position.
The `crawl=` component is a Hive partition, which means DuckDB and the `datasets` library both see `crawl` as a column without any extra configuration.

```sql
SELECT host, count(*)
FROM read_parquet('data/crawl=*/*.parquet', hive_partitioning=1)
WHERE crawl = 'CC-MAIN-2026-25'
GROUP BY host ORDER BY 2 DESC LIMIT 20;
```

Parquet is written with zstd compression.

## Resuming

Every committed shard is appended to a ledger file, `<out>/.committed` unless `--ledger` moves it.
On start the run reads the ledger and skips anything already in it, then reports what it found:

```
markdown: ledger /Users/you/data/ccrawl/markdown/CC-MAIN-2026-25/.committed already records 412 committed shards
```

So a killed run resumes by re-running the exact same command.
Nothing is recomputed and nothing is committed twice.

Local Parquet files are deleted after they commit, which is what keeps a `--shards all` run inside a fixed disk budget.
Pass `--keep-parquet` to hold on to them, and watch the disk if you do.
`--min-free-gb` (default 2) pauses new downloads when free space drops below the threshold rather than failing mid-shard.

A per-shard conversion failure is logged and counted, and the run keeps going.
Only a fatal commit failure or a cancelled context aborts, and the final line always reports the split:

```
markdown: 48 committed, 2 skipped, 0 failed of 50 | 1843229 rows | html=61.2 GiB md=8.4 GiB parquet=2.1 GiB | 41m18s elapsed (72.6 shards/hour)
```

### Exit codes

The markdown pipelines resume from the ledger, so they do not use the supervised-restart exit code.
They exit 0 on success and 1 on a fatal error, and you re-run the same command to continue.

`urls publish` and `domains publish` are the commands that exit 75, because those runs commit into a single growing dataset and have a stall clock.
See [exit codes](/reference/exit-codes/) for the full contract.

## Tuning: parallel, workers, commit-batch

Three flags set throughput, and they control three different resources.

| Flag | Resource | Default (export) | Default (refetch) |
|---|---|---|---|
| `--parallel` | network and disk, shards in flight | 3 | 2 |
| `--workers` | CPU, HTML to Markdown conversion | `NumCPU` | `NumCPU` |
| `--commit-batch` | HuggingFace round trips per commit | 1 | 1 |

`--parallel` is how many shards download and process at once.
Raise it when the download is the bottleneck, which it usually is on a fast machine with a slow link.
Each in-flight shard holds a WARC on disk, so `--parallel 8` on 1 GiB shards means roughly 8 GiB of working set.

`--workers` is a single conversion pool shared across every in-flight shard, not a per-shard pool.
That is the point: raising `--parallel` does not oversubscribe the cores.
Leave it at 0 to get `NumCPU` unless you are sharing the box.

`--commit-batch` is how many finished Parquet files go into one HuggingFace commit.
The commit round trip is slow and the HF API takes one commit at a time per repo, so committing one file at a time throttles a fast run.
A background committer batches finished shards and commits them off the critical path, which means a larger batch trades commit frequency for throughput.
Use 1 for a short run where you want each shard durable immediately, 10 or more for a long `--shards all` run.

A reasonable starting point on a machine with a fast link:

```sh
ccrawl markdown export --shards all --parallel 4 --commit-batch 10
```

### Refetch has a fourth knob

`markdown refetch` also drives a live fetch, so it adds `--fetch-workers`, the number of concurrent requests per shard.
Left at 0 it derives a value from the process file descriptor limit divided across `--parallel`, capped at 3000, and prints what it picked:

```
refetch: fd limit 1048576
refetch: auto fetch-workers=3000 (per shard, 2 shards in parallel)
```

The command raises its own soft `RLIMIT_NOFILE` at startup, so you do not need a shell `ulimit` change.
`--rate` sets a per-host request rate in requests per second, 0 meaning unlimited, and `--max-redirects` caps redirect hops at 5 by default.

`--fetch-only` stores the raw HTML and skips conversion, so the fetch runs at full speed and you convert offline later over the `html` column.
That is the right shape when you are fetch-bound and want the network phase to finish as fast as the hosts allow.

Downloaded WARC shards are cached under `<data-dir>/ami/warc` so a re-run skips the download.
Move it with `--warc-cache-dir` or turn it off with `--no-warc-cache`.

Refetch reports where the time went, which is the fastest way to find out what to tune:

```
phase totals: extract=61s fetch=1840s convert=402s export=88s publish=120s
phase avg/shard: extract=30s fetch=920s convert=201s export=44s | fetch-only 431 pages/s (urls/fetch-sec)
failures: 184203 total | dns=91002 timeout=64881 refused=19422 skip=1102 other=7796
```

## Flags

Shared by `markdown export` and `markdown refetch`:

| Flag | Meaning |
|---|---|
| `--shards` | Shard range: `N`, `N-M`, `N,M`, or `all` |
| `--repo` | HuggingFace dataset repo, `org/name` |
| `--out` | Directory for Parquet files |
| `--push` | Commit each shard to HuggingFace (default true, `--push=false` to stay local) |
| `--limit` | Process at most this many shards (0 = all) |
| `--parallel` | Shards in flight at once |
| `--workers` | Conversion workers shared across shards (0 = `NumCPU`) |
| `--commit-batch` | Parquet files per HuggingFace commit |
| `--keep-parquet` | Keep local Parquet after it commits |
| `--min-free-gb` | Pause new downloads below this much free disk |
| `--ledger` | Resume ledger path (default `<out>/.committed`) |

`markdown export` only:

| Flag | Meaning |
|---|---|
| `--skip-errors` | Continue past per-shard failures instead of aborting |

`markdown refetch` only:

| Flag | Meaning |
|---|---|
| `--fetch-workers` | Concurrent fetches per shard (0 = auto from the fd limit) |
| `--fetch-only` | Store raw HTML and skip conversion |
| `--rate` | Per-host request rate limit in req/s (0 = unlimited) |
| `--max-redirects` | Maximum redirect hops per fetch (default 5) |
| `--warc-cache-dir` | Where to cache downloaded WARC shards |
| `--no-warc-cache` | Do not cache downloaded shards |

For a worked run, see the [Markdown corpus guide](/guides/markdown-corpus/).

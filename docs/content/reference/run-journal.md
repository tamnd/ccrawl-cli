---
title: "Run journal"
linkTitle: "Run journal"
description: "The JSON Lines event log a long run writes, the metrics it exposes, and the queries to ask of both."
weight: 25
---

A publish run takes hours or days. When one of them finishes short, or stalls, or gets killed by a supervisor, the question afterwards is always the same: which shards failed, and when did it slow down. The run journal answers both without a regex over stderr.

Every long running command writes one JSON object per line to a journal file, narrates itself on stderr in whichever shape you asked for, and can serve Prometheus metrics for the duration of the run. Three global flags control all of it.

| Flag | Meaning | Default |
|---|---|---|
| `--progress` | `text`, `json`, or `none` | text on a terminal, json otherwise |
| `--journal` | Where the JSON Lines go | `run.jsonl` beside the ledger |
| `--metrics-addr` | Address to serve `/metrics` on | off |

The default for `--progress` is the useful one: run it in a terminal and you get human lines, redirect it into a log file or run it under a supervisor and you get JSON, without anybody having to remember a flag.

## Which commands report

`markdown export` and `markdown refetch` report per shard, since a shard is the unit of work they resume on. `download`, `export`, `host enrich` and `crawl seed` report per item or per phase. The event shape is the same for all of them, so one query works everywhere.

`markdown export` and `markdown refetch` default their journal to `run.jsonl` beside the ledger, so a resumed run's events land next to the record of what it resumed from. The rest write a journal only when `--journal` asks for one.

## Event schema

Four fields are on every line: `ts`, `event`, `run`, and `pipeline`. The rest depend on the event.

| Field | Meaning |
|---|---|
| `ts` | UTC timestamp, millisecond resolution |
| `event` | `start`, `shard`, `item`, `phase`, `tick`, or `end` |
| `run` | This attempt, timestamp plus pid |
| `pipeline` | `markdown export`, `markdown refetch`, `download`, `export`, `host enrich`, `crawl seed` |
| `crawl` | Crawl ID, where the command has one |
| `phase` | The stage a multi-phase command is in |
| `name` | The file or URL an item event is about |
| `shard` | Shard index, on shard events |
| `status` | `ok`, `failed`, or `skipped` |
| `error` | Why it failed |
| `rows`, `bytes` | Rows written and bytes moved |
| `warc_bytes`, `html_bytes`, `md_bytes`, `parquet_bytes` | Bytes by kind, on the markdown pipelines |
| `extract_s`, `fetch_s`, `convert_s`, `export_s`, `publish_s` | Seconds in each phase of one shard |
| `done`, `total`, `committed`, `skipped`, `failed` | Progress counts |
| `inflight` | Units of work in flight at that moment |
| `fetch_failed` | URLs a refetch shard could not fetch |
| `rate_per_hour`, `eta_s`, `elapsed_s` | Live rates |
| `free_disk_bytes`, `rss_bytes` | Resources at that moment |

`fetch_failed` is separate from `failed` on purpose. A refetch shard that hit a thousand dead hosts still succeeded, and folding the two together would make a healthy run look like a broken one.

A `tick` lands every thirty seconds. That is often enough to see a stall on a three day run and rare enough that the journal stays small.

## Queries worth keeping

Which shards failed, and why:

```bash
jq -r 'select(.event=="shard" and .status=="failed") | "\(.shard) \(.error)"' run.jsonl
```

When the rate dropped:

```bash
jq -r 'select(.event=="tick") | [.ts, .rate_per_hour, .inflight] | @tsv' run.jsonl
```

What the last attempt did, when the journal holds several:

```bash
jq -r --arg run "$(jq -r .run run.jsonl | tail -1)" 'select(.run==$run)' run.jsonl
```

Bytes committed per shard, largest first:

```bash
jq -r 'select(.event=="shard" and .status=="ok") | [.parquet_bytes, .shard] | @tsv' run.jsonl | sort -rn | head
```

Whether the run ran out of disk before it ran out of work:

```bash
jq -r 'select(.event=="tick") | [.ts, .free_disk_bytes, .rss_bytes] | @tsv' run.jsonl
```

## Metrics

`--metrics-addr :9090` serves the same numbers at `/metrics` in the Prometheus text format, for the runs you want on a dashboard rather than in a shell. The address is bound before the command starts working, so a busy port fails immediately instead of an hour in.

| Metric | Type | Labels |
|---|---|---|
| `ccrawl_shards_total` | counter | `pipeline`, `status` |
| `ccrawl_items_total` | counter | `pipeline`, `status` |
| `ccrawl_rows_total` | counter | `pipeline` |
| `ccrawl_bytes_total` | counter | `pipeline`, `kind` |
| `ccrawl_phase_duration_seconds` | histogram | `pipeline`, `phase` |
| `ccrawl_inflight` | gauge | `pipeline` |
| `ccrawl_done` | gauge | `pipeline` |
| `ccrawl_total` | gauge | `pipeline` |
| `ccrawl_rate_per_hour` | gauge | `pipeline` |
| `ccrawl_run_elapsed_seconds` | gauge | `pipeline` |
| `ccrawl_free_disk_bytes` | gauge | `pipeline` |
| `ccrawl_rss_bytes` | gauge | |

The endpoint lives for the length of the run and goes away with it, so scrape it with a short interval and treat a gap as the run having ended.

## Resident set size

`rss_bytes` is read from `/proc/self/status` on Linux and from `ps` on macOS, and is 0 on anything else. It is sampled on the tick, not on a hot path.

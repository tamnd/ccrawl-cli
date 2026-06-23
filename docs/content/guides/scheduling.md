---
title: "Recrawl scheduling"
description: "Score URLs by predicted change rate, diff two CDX snapshots to find what has changed, and assign crawl priorities."
weight: 80
---

`ccrawl sched` decides *which* URLs to recrawl and *when*.
It combines harmonic centrality with observed change rates to produce a priority score, then compares two CDX snapshots to identify pages that have actually changed.

## Assigning crawl priorities

`sched assign` scores every URL in a seed list and emits them in priority order.
Higher-scored URLs should be recrawled first.

```bash
ccrawl sched assign seeds.jsonl -o jsonl > prioritized.jsonl
ccrawl sched assign seeds.jsonl --change-rate 0.3 -o jsonl  # override default change rate
```

The score for each URL is:

```
score = harmonic_rank_value × change_rate_estimate
```

Flags:

| Flag | Default | Purpose |
|---|---|---|
| `--graph` | latest | Web-graph release to use for harmonic centrality |
| `--change-rate` | 0.1 | Default change rate for URLs with no history (0.0–1.0) |

The output is a JSONL stream of the same seed objects with an added `score` field, sorted descending.
Pass this directly into `crawl fetch`:

```bash
ccrawl sched assign seeds.jsonl | ccrawl crawl fetch - -o jsonl > pages.jsonl
```

## Diffing two CDX snapshots

`sched diff` compares the `content_digest` column of two CDX Parquet snapshots and emits URLs whose content has changed between them.
This is the authoritative signal for what actually needs recrawling.

```bash
ccrawl sched diff \
  --crawl-a CC-MAIN-2024-51 \
  --crawl-b CC-MAIN-2025-18 \
  -o jsonl > changed.jsonl
```

Each output row includes the URL, the digest in each crawl, and a `changed: true` flag.
Rows with the same digest in both snapshots are omitted.

Flags:

| Flag | Required | Purpose |
|---|---|---|
| `--crawl-a` | yes | Earlier crawl ID (e.g. `CC-MAIN-2024-51`) |
| `--crawl-b` | yes | Later crawl ID (e.g. `CC-MAIN-2025-18`) |
| `--filter` | no | Restrict to a single host or domain |

**This command requires `duckdb` on your PATH** and scans approximately 184 GB of Parquet per crawl (~368 GB total for two crawls).
Run it on a machine with a fast outbound connection or against locally cached Parquet files.

The key column is `content_digest` (the SHA-1 of the response body as recorded by CC), not the URL digest.
This means a URL that returns 200 with the same body counts as unchanged even if the crawl time differs.

## Combining the two

For a production recrawl schedule, combine both signals:

```bash
# 1. Find what changed between the last two CC snapshots
ccrawl sched diff --crawl-a CC-MAIN-2024-51 --crawl-b CC-MAIN-2025-18 \
  -o jsonl > changed.jsonl

# 2. Score by rank × change rate
ccrawl sched assign changed.jsonl --change-rate 0.5 -o jsonl > prioritized.jsonl

# 3. Fetch top-priority URLs
head -n 10000 prioritized.jsonl | ccrawl crawl fetch - -o jsonl > pages.jsonl
```

This is more efficient than a full reseed: you only fetch the URLs where Common Crawl itself saw a change.

## Estimating change rates per domain

If you have a history of multiple diffs, you can compute a per-domain change rate and pass it back in:

```bash
# compute how often each host changes (fraction of URLs that changed between two crawls)
ccrawl sched diff --crawl-a CC-MAIN-2024-45 --crawl-b CC-MAIN-2024-51 -o jsonl \
  | jq -r '.url_host_name' | sort | uniq -c | awk '{print $2, $1}' > host_changes.txt
```

This is currently a manual step; a future `sched rates` subcommand will automate it.

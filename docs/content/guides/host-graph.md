---
title: "Host graph and enrichment"
description: "Enumerate every host Common Crawl has seen, join in graph topology, and aggregate per-host CDX statistics."
weight: 65
---

Common Crawl publishes a **web graph** alongside its crawl archives: a snapshot of the domain-level link graph distilled into rank tables, vertex maps, and edge files.
`ccrawl host` reads those files and builds enriched per-host records.

## Looking up a single host

```bash
ccrawl host get golang.org -o json
```

This streams the rank table, finds the entry for `golang.org`, and returns its harmonic rank position and value.

## Browsing the top of the graph

```bash
ccrawl host top -n 20 -o table
ccrawl host top -n 1000 -o jsonl > top1k.jsonl
```

Results are streamed from the rank table in rank order, so `-n 20` is fast even though the full table covers 262 million hosts.
Pin a specific web-graph release with `--graph`:

```bash
ccrawl host top --graph cc-main-2026-mar-apr-may -n 100
```

## Vertex map

The vertex file maps each numeric vertex ID to a hostname.
`host vertices` streams it:

```bash
ccrawl host vertices -n 10
ccrawl host vertices --graph cc-main-2026-mar-apr-may -n 5 -o jsonl
```

This is useful when joining edge files (which use vertex IDs) back to human-readable names.

## Degrees: in and out links

The edge files record domain-level links.
`host degrees` streams all edge files (~7.7 GB) and computes in-degree and out-degree for every host:

```bash
ccrawl host degrees -n 100 -o table
ccrawl host degrees -o jsonl > degrees.jsonl
```

This is a large scan.
Run it on a machine with a fast connection, or pipe to a file and query locally.

## CDX statistics per host

`host cdx` queries the columnar Parquet index and returns per-host URL counts, HTTP status breakdown, top MIME type, language, first/last seen crawl, and total bytes:

```bash
ccrawl host cdx --filter golang.org -o json     # one host
ccrawl host cdx -n 100 -o jsonl                 # top 100 hosts by URL count
```

Without `--filter` this scans ~184 GB of Parquet.
It requires `duckdb` on your `PATH`.
The query runs directly against the public S3 Parquet index, so no local download is needed.

## Publishing the domain ranks to HuggingFace

To mirror the whole domain-rank table to a HuggingFace dataset rather than enrich hosts one at a time, use `domains publish`.
It streams the ranks top to bottom, cuts rank-ordered Parquet shards, and commits them to `open-index/ccrawl-domains`, deleting each local shard right after it commits so disk stays flat.

```bash
ccrawl domains publish
ccrawl domains publish --no-push   # scan and report, upload nothing
```

The companion `urls publish` does the same for a crawl's URL index into `open-index/ccrawl-urls`.
See the [CLI reference](/reference/cli/#urls) for the full flag set on both.

## Full enrichment pipeline

`host enrich` runs all four phases in one command and streams enriched `HostRecord` rows:

```bash
ccrawl host enrich -n 20                                 # rank only (fast)
ccrawl host enrich --degrees -n 100                      # rank + degrees
ccrawl host enrich --degrees --cdx -o jsonl > out.jsonl  # full enrichment
```

The phases are:

| Phase | Flag | Data scanned | What it adds |
|---|---|---|---|
| 1+5 | always | rank table (~2.8 GB) | harmonic rank and value |
| 2 | always | vertex file (~1.1 GB) | vertex ID map (used for degree join) |
| 3 | `--degrees` | edge files (~7.7 GB) | in-degree, out-degree |
| 4 | `--cdx` | CDX Parquet (~184 GB) | URL count, status mix, language, bytes |

Phases 3 and 4 are opt-in because they require large data scans.
Phase 4 requires `duckdb` on your `PATH`.

Pipe to a file or a database store with `--db`:

```bash
ccrawl host enrich --degrees --cdx -o jsonl > enriched.jsonl
ccrawl host enrich --degrees --cdx --db hosts.db
```

## Picking a web-graph release

The web graph is published a few times a year, separate from the monthly crawls.
Pass `--graph <release-id>` to pin a specific release.
Without it, ccrawl resolves the latest available release automatically.

```bash
ccrawl host top --graph cc-main-2026-mar-apr-may -n 20
```

Find current and past releases at [commoncrawl.org/web-graphs](https://commoncrawl.org/web-graphs).

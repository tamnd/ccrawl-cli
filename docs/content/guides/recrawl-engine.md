---
title: "Building a recrawl engine"
description: "Seed a crawl from the CC host list, fetch live URLs respecting robots.txt, and store results for later indexing."
weight: 70
---

`ccrawl crawl` is a small but opinionated web crawler built on top of the Common Crawl host and CDX data.
It is designed for one job: re-crawl the URLs Common Crawl already knows about, update the content, and feed the results into the search index.

## How it works

The pipeline has three stages:

1. `crawl seed` — pick which hosts (and their URLs) to crawl based on rank and recency
2. `crawl fetch` — fetch the live pages, respecting `robots.txt`, and write WARC or JSONL
3. `index build` — build a local BM25 inverted index from the fetched content (covered in the [search index guide](/guides/search-index/))

## Seeding from the host list

`crawl seed` queries the CC CDX Parquet index to produce a seed list of `(host, url, score)` tuples.
It ranks seeds by harmonic centrality so the most important pages are crawled first.

```bash
ccrawl crawl seed -o jsonl > seeds.jsonl
ccrawl crawl seed --max-tier 2 -o jsonl > seeds.jsonl    # only hosts in the top two rank tiers
ccrawl crawl seed --max-seeds 10000 -o jsonl             # cap at 10 k URLs
```

Flags:

| Flag | Default | Purpose |
|---|---|---|
| `--max-tier` | 5 | Only include hosts at or below this rank tier (1 = top 1%, 5 = all) |
| `--max-seeds` | unlimited | Hard cap on the number of seed URLs emitted |
| `--graph` | latest | Web-graph release to use for host ranks |

Tier 1 is a few thousand hosts, tier 2 a few hundred thousand, tier 5 is all 262 million.
Start with `--max-tier 2` and grow.

## Fetching pages

`crawl fetch` reads seeds on stdin (or from a file), fetches the live pages, and writes a WARC or JSONL output.

```bash
# quick pipeline: seed → fetch → JSONL
ccrawl crawl seed --max-tier 2 | ccrawl crawl fetch -o jsonl > pages.jsonl

# write WARC (suitable for archiving or feeding back into WET/WAT processing)
ccrawl crawl seed --max-tier 1 | ccrawl crawl fetch -f warc -o out/

# fetch from an existing seed file
ccrawl crawl fetch seeds.jsonl -o jsonl > pages.jsonl
```

Flags:

| Flag | Default | Purpose |
|---|---|---|
| `--robots` | true | Respect `robots.txt` rules |
| `--workers` | 8 | Parallel fetch workers |
| `--delay` | 1s | Politeness delay per host |
| `--timeout` | 30s | Per-request timeout |
| `-f` / `--format` | jsonl | Output format: `jsonl` or `warc` |

The crawler uses a shared connection pool (200 idle connections, 10 per host) and a domain-level frontier with anti-starvation to avoid concentrating on a single host.

## Politeness

By default, `crawl fetch` reads and honors `robots.txt` for every host.
It caches the parsed ruleset per host for the lifetime of the run.
Disable only if you own the target domain:

```bash
ccrawl crawl fetch seeds.jsonl --robots=false
```

The politeness delay (`--delay`) is applied per domain, not per worker, so increasing `--workers` does not increase per-host request rate.

## Output format

**JSONL** (`-f jsonl`): one JSON object per page, with URL, status code, final URL after redirects, content type, raw HTML, extracted text, and outbound links.
Pipe directly into `index build`:

```bash
ccrawl crawl seed --max-tier 2 | ccrawl crawl fetch -o jsonl | ccrawl index build --dir idx/ -
```

**WARC** (`-f warc`): standards-compliant WARC/1.1 files, one per worker, written to the directory given with `-o`.
Use these if you want to replay the crawl later or feed it into other tools.

## Resuming an interrupted crawl

`crawl fetch` writes a `.seen` file alongside the output.
If you re-run the same command, already-seen URLs are skipped automatically.

## End-to-end example

```bash
# Seed tier-2 hosts, fetch live, build a local search index
ccrawl crawl seed --max-tier 2 --max-seeds 50000 -o jsonl > seeds.jsonl
ccrawl crawl fetch seeds.jsonl -o jsonl > pages.jsonl
ccrawl index build --dir idx/ pages.jsonl
ccrawl index search --dir idx/ "golang concurrency"
```

This takes a few hours on a home connection and produces an index you can search in milliseconds.

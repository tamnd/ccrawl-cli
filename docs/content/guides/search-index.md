---
title: "Building a search index"
description: "Build a local BM25 inverted index from crawled pages and search it in milliseconds."
weight: 90
---

`ccrawl index` builds and queries a local BM25 inverted index over any JSONL page corpus, whether downloaded from Common Crawl or produced by `crawl fetch`.

## Building the index

`index build` reads JSONL on stdin or from a file list, tokenizes each document, computes BM25 scores with per-document length normalization, and writes a compact binary index to a directory.

```bash
# from a file
ccrawl index build --dir idx/ pages.jsonl

# from stdin
ccrawl crawl fetch seeds.jsonl -o jsonl | ccrawl index build --dir idx/ -

# multiple input files
ccrawl index build --dir idx/ pages-a.jsonl pages-b.jsonl

# parallel build with 16 workers
ccrawl index build --dir idx/ --workers 16 pages.jsonl
```

Flags:

| Flag | Default | Purpose |
|---|---|---|
| `--dir` | required | Directory to write the index into |
| `--workers` | 8 | Parallel tokenize-and-score workers |
| `--urls` | (stdin) | Explicit list of URLs to index instead of reading JSONL |
| `--input` | (positional args) | Input JSONL files |

Each worker reads documents from a shared channel and writes to a per-worker intermediate segment.
A final merge step combines all segments into the output directory.
With 8 workers on a 4-core machine the build is typically I/O-bound; add `--workers 16` on machines with faster storage.

## Index layout

The index directory contains four files:

| File | Purpose |
|---|---|
| `terms.dat` | Term → (byte offset, doc count) map |
| `postings.bin` | VByte delta-encoded posting lists: `(docID, TF, DL)` per entry |
| `stats.dat` | Document count N and average document length avgDL |
| `forward.jsonl` | Forward index: docID → `{url, title, snippet}` |

These are stable across minor versions, so you can build once and query many times.

## Searching

`index search` scores documents with BM25 and returns ranked results.

```bash
ccrawl index search --dir idx/ "golang concurrency goroutines"
ccrawl index search --dir idx/ "rust memory safety" -n 20 -o json
ccrawl index search --dir idx/ "python asyncio" -o jsonl
```

Flags:

| Flag | Default | Purpose |
|---|---|---|
| `--dir` | required | Index directory to query |
| `-n` | 10 | Number of results to return |
| `-o` | table | Output format: `table`, `json`, `jsonl` |

Each result includes the URL, title, a snippet of the matching text, and the BM25 score.
Queries are tokenized the same way as the corpus, and multi-term queries are ANDed by default.

## BM25 parameters

BM25 has two parameters you can tune:

| Parameter | Default | Effect |
|---|---|---|
| `k1` | 1.2 | Term frequency saturation: lower = diminishing returns set in earlier |
| `b` | 0.75 | Document length normalization: 0 = no normalization, 1 = full normalization |

Set them with `--k1` and `--b`:

```bash
ccrawl index search --dir idx/ "distributed systems" --k1 1.5 --b 0.5
```

The default values work well for web pages.
Lower `b` if your corpus has highly variable document lengths (e.g. full WARC records mixed with short excerpts).

## End-to-end example

```bash
# 1. Seed, crawl, and index in one pipeline
ccrawl crawl seed --max-tier 2 --max-seeds 100000 -o jsonl \
  | ccrawl crawl fetch - -o jsonl \
  | ccrawl index build --dir ~/cc-index/ -

# 2. Search the result
ccrawl index search --dir ~/cc-index/ "machine learning Python" -n 5
```

On a mid-range laptop with a fast internet connection, 100,000 URLs take roughly four to six hours to crawl and a few minutes to index.
Query latency is under 10 ms for most queries once the index is built.

## Incremental updates

To add new pages to an existing index, build a second index from the new documents and merge:

```bash
ccrawl index build --dir idx-new/ new-pages.jsonl
ccrawl index merge --src idx-new/ --dst idx/
```

`index merge` is additive: it appends new postings without rebuilding from scratch.

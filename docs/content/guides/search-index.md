---
title: "Building a search index"
description: "Build a local BM25 inverted index over a page corpus and rank it, and know where the ceiling is."
weight: 90
---

`ccrawl index` builds and queries a local BM25 inverted index over a JSONL page corpus.

It is a reference implementation. It is enough to rank the pages a ccrawl run produced and to see what an inverted index is made of, and it is not a search engine to put behind a service. The whole index is built in memory and the whole term table is read back on every query, which sets a ceiling of a few hundred thousand documents on an ordinary machine. The [measured numbers](#what-it-costs) are below. For search over a real fraction of a crawl, produce the corpus with ccrawl and hand it to something built for the job.

## Building the index

`index build` reads one JSON object per line, tokenizes each document, and writes the index to `--dir`. Every line needs a `url` and the `text` to index, and may carry a `title` and a `language`. That is the shape `ccrawl parse` writes for a WET file, so a Common Crawl text archive can go straight in.

```bash
# from a WET file, English records only
ccrawl parse CC-MAIN-20260710070534-00000.warc.wet.gz --lang eng -o jsonl > eng.jsonl
ccrawl index build --dir idx/ --input eng.jsonl

# the same thing without the intermediate file
ccrawl parse CC-MAIN-20260710070534-00000.warc.wet.gz --lang eng -o jsonl \
  | ccrawl index build --dir idx/ --input -

# fetch a handful of live pages and index those instead
ccrawl index build --dir idx/ --urls https://example.com/,https://go.dev/
```

Flags:

| Flag | Default | Purpose |
|---|---|---|
| `--dir` | `<data-dir>/index` | Directory to write the index into |
| `--input` | none | JSONL file of documents, or `-` for stdin |
| `--urls` | none | Comma-separated page URLs to fetch live and index |
| `-j`, `--workers` | 8 | Fetch concurrency for `--urls`; the JSONL path is single threaded |

One of `--input` or `--urls` is required. A build with neither is a usage error and exits 2, rather than writing an empty index and reporting success.

The command prints one row when it finishes:

```json
{"index_dir":"idx/","docs_added":13053,"docs_skipped":0,"terms":799548}
```

`docs_skipped` counts lines that were not JSON, had no URL, or tokenized to nothing. A skipped line does not fail the build, because a real WET file always has some.

## Index layout

The index directory holds four files:

| File | Purpose |
|---|---|
| `terms.dat` | One line per term: term, byte offset into the postings, document frequency, IDF |
| `postings.dat` | VByte delta-encoded posting lists, `(docID, TF, DL)` per entry |
| `stats.dat` | Document count and average document length, for BM25 |
| `forward.jsonl` | Forward index, one JSON row per document: URL, title, language, snippet |

There is no merge step and no sharding. `index build` writes the whole index in one pass, and running it again over the same directory replaces all four files. To add documents, index the whole corpus again.

## Searching

`index search` scores documents with BM25 and prints them best first.

```bash
ccrawl index search "golang concurrency goroutines" --dir idx/
ccrawl index search "rust memory safety" --dir idx/ -n 20 -o json
```

Flags:

| Flag | Default | Purpose |
|---|---|---|
| `--dir` | `<data-dir>/index` | Index directory to query |
| `-n`, `--limit` | 100 | Number of documents to return |
| `-o` | table | Output format: `table`, `json`, `jsonl`, and the rest |

Each result carries the document ID, the BM25 score, and the URL, title and snippet from the forward index.

Query terms are ORed, not ANDed. A two-word query returns every document holding either word, and the documents holding both score higher and sort to the top. A query that matches nothing exits 3, which is the same "ran fine, found nothing" signal the rest of ccrawl uses.

BM25 runs with k1 1.2 and b 0.75. They are not exposed as flags.

## What it costs

Measured on this laptop against English WET records from CC-MAIN-2026-30, which is real Common Crawl text rather than a synthetic corpus:

| Documents | Text in | Build time | Peak RSS | Index on disk | Query |
|---|---|---|---|---|---|
| 13,053 (1 WET file) | 97 MB | 4.2 s | 458 MB | 85 MB | 0.4 s |
| 52,495 (4 WET files) | 405 MB | 17.4 s | 1.45 GB | 335 MB | 1.05 s |

Two things to read off that table.

The build costs about 26 KB of RAM per document and nothing spills to disk, so the corpus ceiling is the machine: roughly 600,000 documents in 16 GB. A crawl holds 100,000 WET files, so that ceiling is about 45 of them.

The query cost is the index load, not the search. Opening the index reads the whole term table, and the CLI reads the whole forward index to attach titles and snippets, so a query that matches nothing costs the same as one that matches everything, and the cost grows with the corpus. There is no warm process to amortize it against.

## End-to-end example

```bash
# 1. Take the text of one WET file from the newest crawl
ccrawl paths wet -n 1 -o url | xargs curl -sO
ccrawl parse *.warc.wet.gz --lang eng -o jsonl > eng.jsonl

# 2. Index it
ccrawl index build --dir idx/ --input eng.jsonl

# 3. Rank it
ccrawl index search "machine learning" --dir idx/ -n 5 -o jsonl
```

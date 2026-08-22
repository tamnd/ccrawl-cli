---
title: "Recrawl datasets"
description: "The two recrawl repos: the capture schema column by column, how the shards and the ledger are laid out, and DuckDB queries that run against the hub as written."
weight: 14
---

`ccrawl recrawl run` fetches a published work list and writes what came back.
`ccrawl recrawl publish` commits those shards to a dataset repo as they close.
This page is the reader's side of that: what is in the files, how they are arranged, and how to query them.

There are two repos because the two work lists finish on completely different schedules and nobody wants a card that averages them.

| Repo | Work list | Rows in the work list |
|---|---|---|
| [open-index/ccrawl-recrawl-domains](https://huggingface.co/datasets/open-index/ccrawl-recrawl-domains) | [open-index/ccrawl-domains](https://huggingface.co/datasets/open-index/ccrawl-domains), one home page per ranked domain | 121 million |
| [open-index/ccrawl-recrawl-urls](https://huggingface.co/datasets/open-index/ccrawl-recrawl-urls) | [open-index/ccrawl-urls](https://huggingface.co/datasets/open-index/ccrawl-urls), the URL index for one monthly crawl | 2.1 billion |

Both hold the same columns, written by the same code, so a query written for one runs against the other.

## Layout

```
data/
  server1-shard0of3-a1b2c3d4e5f6.parquet
  server2-shard1of3-9f8e7d6c5b4a.parquet
ledger/
  server1-shard0of3.csv
  server2-shard1of3.csv
README.md
```

A shard's name carries the machine that fetched it, the slice of the work list that machine took, and a short hash of the contents.
The hash is what makes republishing a shard a no-op rather than a duplicate, which matters because the fleet runs for months and a machine can be restarted at any point in that.

Shards are independent of each other.
Any subset of them is a valid sample of the whole, so a query over ten files is a real answer about the corpus rather than a partial one, subject to which slices those files came from.

Each machine writes exactly one ledger file and never touches another machine's.
Three machines committing in the same minute therefore cannot lose each other's numbers, which is a real failure mode when several writers share one stats file over a run this long.
The dataset card is regenerated from the union of every ledger on every commit, so it corrects itself from any machine and a machine that was down for a day does not leave a permanently wrong card behind.

## Sharding

The work list is split by registered domain, not by URL.

That is the whole reason the fleet is polite.
If the split were by URL, one host would land on all three machines, each machine would wait its own politeness interval, and the site would see three times the request rate while every machine believed it was behaving.
Splitting on the registered domain keeps a site and its politeness clock on one machine however many machines there are.

It also means a slice is not a uniform sample of hosts.
Shard 0 holds whichever registered domains hash into it, so a query restricted to one shard is a sample of domains rather than a random sample of pages.

## The capture schema

One row is one attempt to fetch one URL.

`From` says where the value came from, and it changes what the column is evidence of.
**served** is the site's own answer, **computed** is this pipeline's opinion over the bytes, **measured** is a number off our clock on our network, and **asked** is what we sent.

| Column | Type | From | Description |
|---|---|---|---|
| `url` | VARCHAR | asked | the URL that was requested, straight off the work list |
| `host` | VARCHAR | computed | host of the requested URL, parsed from it |
| `status` | INTEGER | served | HTTP status, 0 when the fetch never got one |
| `fetched_at` | BIGINT | measured | when the fetch happened, unix milliseconds |
| `content_type` | VARCHAR | served | Content-Type header as served |
| `body_length` | BIGINT | computed | length of the body in bytes |
| `digest` | VARCHAR | computed | SHA-1 of the body |
| `unchanged` | BOOLEAN | served | true when the server answered 304 Not Modified |
| `etag` | VARCHAR | served | ETag header, empty if the server sent none |
| `last_modified` | VARCHAR | served | Last-Modified header, empty if the server sent none |
| `warc_file` | VARCHAR | computed | WARC file holding the response, empty for a Parquet run |
| `warc_offset` | BIGINT | computed | byte offset of the record in that WARC file |
| `warc_length` | BIGINT | computed | byte length of the record |
| `error` | VARCHAR | computed | why the fetch failed, one of `dns`, `timeout`, `refused`, `tls`, `skip`, `other`, empty on success |
| `meta_json` | VARCHAR | computed | extra context as JSON, including `error_detail` on a failed row |
| `markdown` | VARCHAR | computed | the page rendered to Markdown |
| `markdown_length` | BIGINT | computed | length of the Markdown in bytes |
| `ttfb_ms` | BIGINT | measured | time to first byte in milliseconds |
| `fetch_duration_ms` | BIGINT | measured | total fetch time in milliseconds |
| `final_url` | VARCHAR | served | URL after redirects, empty when it did not move |
| `ip_address` | VARCHAR | measured | IP the request went to, which for a CDN is the nearest edge |
| `resp_headers` | VARCHAR | served | response headers as JSON |
| `req_headers` | VARCHAR | asked | request headers as JSON, what we sent |
| `body` | BLOB | served | the response body exactly as served, before any decoding |
| `title` | VARCHAR | computed | the document title |
| `text` | VARCHAR | computed | the page as plain text, boilerplate stripped |
| `text_length` | BIGINT | computed | length of the text in bytes |
| `word_count` | BIGINT | computed | words in the extracted text |
| `language` | VARCHAR | computed | language of the Markdown, ISO 639-3, detected not declared |
| `language_confidence` | DOUBLE | computed | how sure the detector is, 0 to 1 |
| `simhash` | BIGINT | computed | fingerprint of the Markdown, for near duplicates |
| `extractor` | VARCHAR | computed | engine and version that rendered the page, as `name@version` |

The text columns are last in the schema and were appended rather than inserted, so a reader written against the older shape reads a newer file unchanged.
Parquet is read by name, and a reader that never asks for those columns never touches them.

### A row is an attempt, not a page

Failures are kept.
A name that does not resolve, a timeout and a refused connection are all rows with `status` 0 and the reason in `error`.
So `count(*)` counts URLs we tried, and it does not count readable pages.

This is not a small correction on the domain list.
A domain rank computed from a months-old web graph contains a lot of names that have since lapsed, and those are rows here rather than gaps.

`error` is a fixed vocabulary of six words and not the message the network stack produced.
A Go network error carries the host, usually a port and often a resolver address, so a column holding those groups into one row per URL and answers nothing, while six words group into six rows and say what the corpus cost.
The original text is kept in `meta_json` under `error_detail` for the cases where the class is not enough.

A failure row has no body, no digest and no headers, because there was no response to take them from.
A WARC run is the exception: a WARC record is a response, so a run writing WARC counts the failure on its summary and writes no record for it.

### What a conditional refetch stores

A first pass sends no validators, so it gets no 304s and every row carries its body.

A later pass over the same URLs sends back the `etag` and `last_modified` from the earlier row.
When the server answers 304 Not Modified, `unchanged` is true and the body is empty.
That is deliberate: over a corpus where most pages do not move between passes, storing only the ones that did is the difference between a dataset and a copy of one.

To read the body a 304 refers to, join back to the earlier pass on `url` and take the row whose `digest` matches.

### Extraction

Every HTML page is rendered to Markdown and plain text while it is still in memory, during the fetch rather than in a second pass over the corpus.
The run is waiting on the network anyway.

`extractor` names the engine and version that did it.
A row where `extractor` is empty was not rendered at all, which is how a PDF or a JSON API response is told apart from an HTML page that rendered to nothing.
Rows fetched months apart may carry different engine versions, since the fleet runs across releases.

## Reading it with DuckDB

No download step.
These run as written against the hub, given DuckDB 0.10 or newer.

```sql
-- What answered and what did not, which is the first thing to know
SELECT status, count(*) AS rows
FROM read_parquet('hf://datasets/open-index/ccrawl-recrawl-domains/data/*.parquet')
GROUP BY status
ORDER BY rows DESC;
```

```sql
-- Readable pages only, which is what most queries actually want
SELECT count(*) AS pages
FROM read_parquet('hf://datasets/open-index/ccrawl-recrawl-domains/data/*.parquet')
WHERE status = 200 AND error = '';
```

```sql
-- Why the failures failed, grouped by the class rather than the raw message
SELECT error, count(*) AS rows
FROM read_parquet('hf://datasets/open-index/ccrawl-recrawl-domains/data/*.parquet')
WHERE error <> ''
GROUP BY error
ORDER BY rows DESC
LIMIT 20;
```

```sql
-- Read a page as Markdown, no HTML parsing on your side
SELECT url, title, language, word_count, markdown
FROM read_parquet('hf://datasets/open-index/ccrawl-recrawl-domains/data/*.parquet')
WHERE status = 200 AND markdown <> ''
LIMIT 1;
```

```sql
-- When these rows were fetched, which is not when the crawl saw them
SELECT date_trunc('day', to_timestamp(fetched_at / 1000)) AS day, count(*) AS rows
FROM read_parquet('hf://datasets/open-index/ccrawl-recrawl-domains/data/*.parquet')
GROUP BY day
ORDER BY day;
```

```sql
-- Languages, by pages with real text in them
SELECT language, count(*) AS pages, round(avg(word_count)) AS avg_words
FROM read_parquet('hf://datasets/open-index/ccrawl-recrawl-domains/data/*.parquet')
WHERE language <> '' AND language_confidence > 0.8
GROUP BY language
ORDER BY pages DESC
LIMIT 20;
```

To pull one machine's slice rather than the whole corpus, glob on the shard name, since the layout puts that in the file name for exactly this reason:

```sql
SELECT count(*)
FROM read_parquet('hf://datasets/open-index/ccrawl-recrawl-domains/data/server1-shard0of3-*.parquet');
```

If all you want is the text, project `markdown` or `text` and leave `body` alone.
Parquet reads by column, so a query that never names `body` never reads the bodies off disk, and the bodies are almost all of the bytes.

## Running one yourself

See [running the recrawl fleet](../../guides/recrawl-fleet/) for the fleet side, and `ccrawl recrawl run --help` for the flags.

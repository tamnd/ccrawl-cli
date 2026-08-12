---
title: "Troubleshooting"
description: "The handful of things that trip people up, and how to fix each one."
weight: 40
---

Most of these come down to network reality or an optional dependency, not a bug.

## "no duckdb binary found"

The columnar commands (`ccrawl columnar`, `ccrawl db`) run SQL with a local `duckdb`.
When there is none on your `PATH`, ccrawl prints the SQL instead of running it, so you can paste it into DuckDB, Athena, Spark, or Trino.
To run locally, install DuckDB from [duckdb.org](https://duckdb.org/docs/installation).
The ccrawl binary itself never links DuckDB.

## A columnar query is slow

A cold `ccrawl columnar` query with no domain or TLD filter reads across every Parquet shard of the crawl over HTTPS.
That is bandwidth-bound: seconds on a well-connected host, minutes on home broadband.
Narrow it with `--domain` or `--tld` so DuckDB can prune shards, run it from a better-connected machine, or `--print` the SQL and run it in Athena where the compute sits next to the data.

## A columnar glob "cannot be listed"

The Common Crawl S3 bucket denies anonymous listing, and HTTPS cannot list a directory, so a raw `*.parquet` glob has nothing to expand against.
ccrawl handles this for you by reading the crawl's shard manifest and turning the glob into an explicit file list before it runs DuckDB.
If you copied a printed SQL query (which keeps the glob on purpose for Athena and Spark) into a plain local DuckDB, expand the glob yourself or let `ccrawl columnar` run it instead.

## A rank table returns 404

The web-graph rank tables are versioned per release, and old releases are retired.
If `--table <url>` 404s, the release moved.
Check the current one on the [web graph release list](https://commoncrawl.org/web-graphs) and use the `domain` file for `rank domain` and the `host` file for `rank host`.

## A news scan times out

CC-NEWS has no index, so `ccrawl news search` streams whole WARC files looking for matches.
It is inherently slower than an indexed `search`, and a rare host may mean streaming a lot of data before a hit.
Use `-n` to stop once you have enough, raise `--workers` to scan more files at once, and prefer the indexed [search](/guides/finding-pages/) whenever the data exists in a monthly crawl.

## Nothing is found for a URL you expected

Common Crawl is a sample, not a mirror; not every page is captured in every crawl.
Widen the search with a path pattern (`'example.com/*'`), try another crawl with `-c <year>`, or search across all of them with `-c all`.

## Requests fail with 403 or 5xx across the board

ccrawl retries 403, 429, and 5xx responses with exponential backoff and jitter, honoring a `Retry-After` header when the CDN sends one, so a transient throttle on `data.commoncrawl.org` usually recovers on its own.
`--retries` sets how many attempts (default 5) and `--rate` adds delay between requests to stay polite.
If errors persist for everything, the data service may be having an outage: check the [Common Crawl status page](https://status.commoncrawl.org) before digging further.

## A search says the result is incomplete

```
search: CC-MAIN-2026-30: CDX page 252: HTTP 503, skipping the page
search: the result is incomplete, 1 index page could not be read; run it again or pass --strict to fail instead
```

The index refused a page of a wide query after every retry, so those records are not in the output and everything else is.
Running the query again usually gets it, because the failure is load rather than a page that does not exist.
`--strict` turns the skip back into an error for a pipeline that cannot use a partial answer, and `--retries` raises how many attempts each page gets.

A page whose body stops part way through is not this: it is read again automatically and only shows up here if every attempt came back short.

## Checking what ccrawl resolved

When something behaves unexpectedly, `ccrawl config show` prints the crawl, source, data directory, and every resolved path, and `-v` adds per-request detail.
That is usually enough to see what it actually did.

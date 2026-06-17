---
title: "Troubleshooting"
description: "The handful of things that trip people up, and how to fix each one."
weight: 40
---

Most of these come down to network reality or an optional dependency, not a bug.

## "no duckdb binary found"

The columnar commands (`ccrawl table`, `ccrawl db`) run SQL with a local `duckdb`.
When there is none on your `PATH`, ccrawl prints the SQL instead of running it, so you can paste it into DuckDB, Athena, Spark, or Trino.
To run locally, install DuckDB from [duckdb.org](https://duckdb.org/docs/installation).
The ccrawl binary itself never links DuckDB.

## A columnar query is slow

A cold `ccrawl table` query with no domain or TLD filter reads across every Parquet shard of the crawl over HTTPS.
That is bandwidth-bound: seconds on a well-connected host, minutes on home broadband.
Narrow it with `--domain` or `--tld` so DuckDB can prune shards, run it from a better-connected machine, or `--print` the SQL and run it in Athena where the compute sits next to the data.

## A columnar glob "cannot be listed"

The Common Crawl S3 bucket denies anonymous listing, and HTTPS cannot list a directory, so a raw `*.parquet` glob has nothing to expand against.
ccrawl handles this for you by reading the crawl's shard manifest and turning the glob into an explicit file list before it runs DuckDB.
If you copied a printed SQL query (which keeps the glob on purpose for Athena and Spark) into a plain local DuckDB, expand the glob yourself or let `ccrawl table` run it instead.

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

## Checking what ccrawl resolved

When something behaves unexpectedly, `ccrawl config show` prints the crawl, source, data directory, and every resolved path, and `-v` adds per-request detail.
That is usually enough to see what it actually did.

---
title: "The columnar index"
description: "Answer dataset-wide questions over the Parquet copy of the URL index with DuckDB or Athena."
weight: 40
---

Common Crawl publishes the URL index twice: as the CDX server you query a URL at a time, and as a columnar **Parquet** copy of the same data.
The Parquet index is the fastest way to answer questions across a whole crawl: how many `.gov` PDFs are there, what languages appear, which domains have the most captures.
`ccrawl columnar` builds the SQL for you and runs it.
(`table` and `athena` are aliases for the same command, so older scripts keep working.)

## The basics

```bash
ccrawl columnar count --domain example.com    # how many captures of a domain
ccrawl columnar count --tld gov               # how many .gov pages in the crawl
ccrawl columnar urls --tld gov --mime application/pdf -o url
ccrawl columnar langs --tld jp                # breakdown of captures by language
ccrawl columnar mimes --domain example.com    # breakdown by MIME type
ccrawl columnar schema                        # the columns of the index
```

The filter flags map onto the index columns: `--domain`, `--host`, `--tld`, `--mime`, `--status`, `--lang`, and `--path-prefix`.
They combine, and they let DuckDB skip Parquet shards it does not need.
A `--domain` or `--host` filter also adds a bound on `url_surtkey`, the reversed-host key the index is physically sorted by, so the engine prunes whole row groups by their min and max key instead of reading them just to test the equality.
That makes a `*.example.com` query noticeably faster on a cold scan.

## Running it: DuckDB or print

If a `duckdb` binary is on your `PATH`, `ccrawl columnar` runs the query and streams the result.
With no `duckdb` on your `PATH`, ccrawl prints the SQL so you can run it wherever you like.
You can also ask for the SQL explicitly:

```bash
ccrawl columnar sql --tld gov --mime application/pdf --print
```

That SQL is valid in DuckDB, Athena, Spark, and Trino, because the index is plain Parquet on S3.
The printed query keeps the `*.parquet` glob, which those engines expand themselves.

## Raw SQL

For anything the filter flags do not cover, write SQL directly.
The token `ccindex` stands in for the read_parquet source of the current crawl:

```bash
ccrawl columnar query "SELECT url, fetch_status FROM ccindex WHERE url_host_tld = 'gov' LIMIT 10"
```

## Composing with fetch

`columnar locations` emits exactly the record locations `ccrawl fetch` reads, so the columnar index and the byte-range fetcher snap together.
Find captures with SQL, fetch their bytes:

```bash
ccrawl columnar locations --domain example.com -o jsonl | ccrawl fetch - --text
```

## Why ccrawl resolves the file list for you

There is one sharp edge worth understanding.
The Common Crawl S3 bucket denies anonymous listing, and plain HTTPS cannot list a directory either.
So when a local `duckdb` is handed a `*.parquet` glob over HTTPS, it has no way to expand it: there is nothing to list.

ccrawl works around this transparently.
Each crawl publishes a manifest of its index shards (`cc-index-table.paths.gz`), and ccrawl reads that manifest to turn the glob into an explicit `read_parquet([...])` list of real file URLs before handing the query to DuckDB.
You never see this; `ccrawl columnar count` just works.
The only place it surfaces is the printed SQL, which keeps the glob on purpose so Athena and Spark, which can list, expand it the normal way.

## A note on speed

A cold columnar query with no predicate on the partition key has to read footers and column chunks across every shard of the crawl (a few hundred Parquet files), all over HTTPS.
That is bandwidth-bound, not CPU-bound, so it runs in seconds on a well-connected host and minutes on a home connection.

Three ways to keep it fast:

- **Narrow with the filter flags** (`--domain`, `--tld`) so DuckDB can prune shards instead of scanning all of them.
- **Run from a well-connected machine** when you do a full scan.
- **Push it to Athena** with `--print` when the data and the compute should sit together.

To pull a crawl slice local first and query it without the network in the loop, see [building a dataset](/guides/datasets/).

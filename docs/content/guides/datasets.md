---
title: "Building a dataset"
description: "Load a slice of Common Crawl into a local DuckDB database and query it offline."
weight: 50
---

When you are going to ask the same data many questions, pull it local once and query it without the network in the loop.
`ccrawl db` builds a local DuckDB database from the columnar index and lets you run SQL against it.

## Loading

`ccrawl db load` reads matching records from the columnar index and writes them into a table in your local DuckDB file (`<data-dir>/ccrawl.duckdb` by default).
The same filter flags as `ccrawl table` apply:

```bash
ccrawl db load --domain example.com        # load one domain's captures
ccrawl db load --tld gov --mime application/pdf
```

The load reports how many rows it wrote and the table it created.
You now have a normal DuckDB database; nothing else in ccrawl is required to use it.

## Querying

Run SQL against what you loaded:

```bash
ccrawl db sql "SELECT count(*) FROM captures"
ccrawl db sql "SELECT url, fetch_status FROM captures LIMIT 10"
```

Or open an interactive DuckDB shell on the same file:

```bash
ccrawl db shell
```

`ccrawl db path` prints the database file location, so you can point any other DuckDB-aware tool at it:

```bash
duckdb "$(ccrawl db path)"
```

## Local Parquet instead of a database

If you would rather have flat Parquet files than a database, download a crawl slice and convert it, then query the Parquet directly.
This keeps the data in open files that any engine can read:

```bash
ccrawl download warc -n 5
ccrawl convert ./raw --to parquet --out ./parquet
duckdb -c "SELECT count(*) FROM read_parquet('parquet/*.parquet')"
```

See [bulk and archives](/guides/archives/) for the download and convert steps, and [the columnar index](/guides/columnar-index/) for querying the remote index without loading anything.

---
title: "Building a Markdown corpus"
description: "Turn Common Crawl WARC files into a Markdown Parquet dataset on HuggingFace, from a single shard to a whole crawl."
weight: 55
---

Common Crawl ships HTML.
Most of what people want to do with it, training corpora, retrieval, analysis, wants clean Markdown.
`ccrawl markdown` does that conversion at crawl scale and publishes the result as a Parquet dataset on HuggingFace.

There are two ways to get the HTML, and picking one is the first decision:

- `markdown export` reads the HTML Common Crawl already captured. No live traffic, fully reproducible, and the content is as old as the crawl.
- `markdown refetch` takes the URL list from a shard and fetches every page live. Current content plus response metadata, at the cost of hitting real servers and losing the pages that have gone away.

Start with `export`.
It is the cheaper pipeline and it tells you whether your machine and your link can keep up before you point traffic at anyone.

For the full flag list and both schemas, see the [markdown reference](/reference/markdown/).

## One shard first

A crawl's WARC manifest has roughly 90 000 files, and each one is a shard.
Do a single shard locally, with no HuggingFace involved, and look at what comes out:

```bash
ccrawl markdown export --shards 0 --push=false --out ./md --keep-parquet
```

That downloads one WARC, extracts the HTML bodies, converts each to Markdown, and writes one zstd Parquet file.
It takes a few minutes and produces something in the low hundreds of megabytes.

Check the result with DuckDB before you scale anything:

```bash
duckdb -c "SELECT count(*), avg(markdown_length) FROM read_parquet('md/*.parquet')"
duckdb -c "SELECT url, markdown FROM read_parquet('md/*.parquet') LIMIT 1"
```

Read one of those `markdown` values with your own eyes.
Conversion quality is the whole point of the dataset, and a quick look at a real page catches problems that a row count never will.

## Publishing to HuggingFace

Set a token with write access to the target repo, then drop `--push=false`:

```bash
export HF_TOKEN=hf_...
ccrawl markdown export --shards 0-9 --repo your-org/your-markdown-dataset
```

The repo is created if it does not exist.
Each shard lands at `data/crawl=CC-MAIN-2026-25/000000.parquet`, one file per shard, with the crawl as a Hive partition so `datasets` and DuckDB both see `crawl` as a column.

Loading it back is the standard path, nothing custom:

```python
from datasets import load_dataset
ds = load_dataset("your-org/your-markdown-dataset", split="train", streaming=True)
print(next(iter(ds))["markdown"][:500])
```

## Scaling to a whole crawl

Once one shard looks right, the run is the same command with a wider range:

```bash
ccrawl markdown export --shards all --parallel 4 --commit-batch 10 \
  --repo your-org/your-markdown-dataset
```

Three flags matter and they push on three different resources:

- `--parallel` is shards in flight, which is network and disk. Raise it when downloads are the bottleneck. Each in-flight shard holds a WARC on disk.
- `--workers` is the conversion pool, which is CPU. It is shared across every in-flight shard, so raising `--parallel` never oversubscribes the cores. The `NumCPU` default is usually right.
- `--commit-batch` is Parquet files per HuggingFace commit. The commit round trip is slow and the API takes one commit at a time per repo, so a long run wants 10 or more rather than the default 1.

Local Parquet is deleted after it commits, so disk stays flat across a run of any length.
`--min-free-gb` pauses new downloads instead of failing when the disk gets tight.

A whole crawl is days of work.
Run it under a supervisor and let it restart, because that is what resume is for.

## Resuming

Every committed shard is written to a ledger, `<out>/.committed` by default.
Re-running the exact same command reads the ledger, skips what is done, and says so:

```
markdown: ledger /Users/you/data/ccrawl/markdown/CC-MAIN-2026-25/.committed already records 412 committed shards
```

There is nothing else to do.
No flag to pass, no offset to remember, no risk of committing a shard twice.
Kill the run, reboot the machine, come back a week later, same command.

Per-shard failures do not stop the run.
They are logged and counted, and the final line splits committed from skipped from failed:

```
markdown: 48 committed, 2 skipped, 0 failed of 50 | 1843229 rows | html=61.2 GiB md=8.4 GiB parquet=2.1 GiB | 41m18s elapsed (72.6 shards/hour)
```

The markdown pipelines do not use exit 75.
They resume from the ledger, so a supervisor just re-runs the command.
Exit 75 belongs to `urls publish` and `domains publish`, which is covered in [exit codes](/reference/exit-codes/).

## Refetching live

`markdown refetch` starts from the same shard but fetches every URL itself, so you get today's page, the response headers, the IP, the timings, and an `error` column for the ones that failed.

```bash
ccrawl markdown refetch --shards 0 --push=false --out ./refetch --keep-parquet
```

Expect a lot of failures.
An unfiltered Common Crawl shard is full of expired domains and blackholed hosts, and the fetch engine is tuned to prove a host is dead quickly rather than hold a worker on it.
Failed URLs still produce rows, which is the point: knowing a URL is gone is a result.

```bash
duckdb -c "SELECT error != '' AS failed, count(*) FROM read_parquet('refetch/*.parquet') GROUP BY 1"
```

Concurrency comes from `--fetch-workers`, per shard.
Left at 0 it derives a value from the file descriptor limit divided across `--parallel` and prints what it chose.
The command raises its own soft limit at startup, so no `ulimit` change is needed.

Be a good citizen about the traffic.
`--rate` caps requests per second per host, and there is no reason to leave it unlimited if you are crawling anything you do not own.

If the fetch is your bottleneck and conversion can wait, `--fetch-only` stores raw HTML and skips the convert phase so the network runs flat out.
Convert offline later over the `html` column.

Refetch prints where the time actually went, which is the only reliable guide to what to tune next:

```
phase totals: extract=61s fetch=1840s convert=402s export=88s publish=120s
failures: 184203 total | dns=91002 timeout=64881 refused=19422 skip=1102 other=7796
```

Fetch dominating means raise `--fetch-workers` or accept it.
Convert dominating means raise `--workers`.
Publish dominating means raise `--commit-batch`.

## Joining the two datasets

`doc_id` is a stable hash of the URL in both schemas, independent of crawl and shard.
So the archived page and the live one join directly, and you can measure how much of the web has changed since the crawl:

```sql
SELECT count(*) AS both,
       sum(CASE WHEN e.markdown != r.markdown THEN 1 ELSE 0 END) AS changed
FROM read_parquet('md/*.parquet') e
JOIN read_parquet('refetch/*.parquet') r USING (doc_id)
WHERE r.error = '';
```

The same join deduplicates across crawls, since a URL keeps its `doc_id` forever.

## Where to go next

- [Markdown reference](/reference/markdown/) for every flag and both full schemas
- [Bulk and archives](/guides/archives/) for working with WARC files directly
- [Building a dataset](/guides/datasets/) for the local DuckDB path when you do not want to publish anything

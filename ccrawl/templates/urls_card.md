---
configs:
- config_name: default
  data_files:
  - split: train
    path: "data/**/*.parquet"
{{- range .Configs}}
- config_name: "{{.}}"
  data_files:
  - split: train
    path: "data/{{.}}/*.parquet"
{{- end}}
license: odc-by
task_categories:
- text-retrieval
- other
language:
- multilingual
pretty_name: Common Crawl URL Index
size_categories:
- {{.SizeCat}}
tags:
- common-crawl
- web
- url-index
- crawl-frontier
- parquet
- open-data
---

# Common Crawl URL Index

> Every URL Common Crawl has seen, as a slim columnar table, ready to seed a crawler frontier

## Table of Contents

- [What is it?](#what-is-it)
- [What is being released?](#what-is-being-released)
- [Breakdown by crawl](#breakdown-by-crawl)
- [How to download and use this dataset](#how-to-download-and-use-this-dataset)
- [Dataset statistics](#dataset-statistics)
- [Dataset card](#dataset-card-for-common-crawl-url-index)
  - [Dataset summary](#dataset-summary)
  - [Dataset structure](#dataset-structure)
  - [Dataset creation](#dataset-creation)
  - [Considerations for using the data](#considerations-for-using-the-data)
- [Additional information](#additional-information)

## What is it?

This dataset is the URL-level index of [Common Crawl](https://commoncrawl.org), republished as clean Parquet. Common Crawl is a non-profit that crawls the web every month and freely publishes its archives. Each crawl ships a columnar URL index that lists every captured page with its host, fetch status, content type, detected language, and a pointer into the WARC archive that holds the raw response.

We take that index as it is and republish it shard for shard, with no aggregation, deduplication, filtering, or enrichment. The rows and their order match the source columnar parts one to one, so this is a faithful mirror that loads with the standard Hugging Face tools and reads directly from DuckDB.
{{if .HasRows}}
Right now the index holds **{{.TotalCrawls}}** across **{{.TotalURLs}}** in **{{.TotalBytes}}** of compressed Parquet, spanning **{{.TotalShards}}**. New monthly crawls are added as Common Crawl releases them.
{{end}}
The URL index is the map of the crawl. Before you download a single WARC you can already answer questions like which hosts were captured, how many pages returned a 200, what languages and MIME types show up, and which domains dominate a crawl. It is the natural starting point for building a crawl frontier, sampling the web by host or language, or fetching just the pages you care about straight out of the WARC archives.

It is released under the **Open Data Commons Attribution License (ODC-By) v1.0**, the same license Common Crawl uses.

## What is being released?

One source columnar part becomes one Parquet shard, under a directory named for its crawl. Common Crawl splits each crawl's URL index into 300 parts, so each crawl is 300 shards you can load one at a time or stream together.

```
data/
  {{.Latest}}/
    part-00000.parquet
    part-00001.parquet
    ...
    part-00299.parquet
  CC-MAIN-2026-21/
    part-00000.parquet
    ...
stats.csv                     one row per committed crawl
```

Each row is one captured URL. The `warc_filename`, `warc_record_offset`, and `warc_record_length` columns point at the exact bytes of the response in Common Crawl's WARC archives, so you can range-fetch the original page without downloading a whole WARC. `stats.csv` tracks every committed crawl with its shard count, row count, Parquet size, and commit timestamps, which makes it easy to see coverage at a glance and to estimate remaining work.
{{if .HasRows}}
## Breakdown by crawl

URLs per crawl, newest first.

```
{{range .Bars}}{{.}}
{{end}}```
{{end}}
## How to download and use this dataset

Load one crawl, filter by host or status, or stream the whole thing. It is a standard Hugging Face Parquet layout, so it works with DuckDB, `datasets`, `pandas`, and `huggingface_hub` out of the box.

### Using DuckDB

DuckDB reads Parquet directly from Hugging Face, no download step needed.

```sql
-- Top 20 hosts by captured pages in the latest crawl
SELECT url_host_registered_domain AS domain, count(*) AS pages
FROM read_parquet('hf://datasets/{{.Repo}}/data/{{.Latest}}/*.parquet')
GROUP BY domain
ORDER BY pages DESC
LIMIT 20;
```

```sql
-- Fetch-status distribution for one crawl
SELECT fetch_status, count(*) AS pages
FROM read_parquet('hf://datasets/{{.Repo}}/data/{{.Latest}}/*.parquet')
GROUP BY fetch_status
ORDER BY pages DESC;
```

```sql
-- Language mix across the whole index
SELECT content_languages AS lang, count(*) AS pages
FROM read_parquet('hf://datasets/{{.Repo}}/data/**/*.parquet')
WHERE content_languages IS NOT NULL
GROUP BY lang
ORDER BY pages DESC
LIMIT 20;
```

```sql
-- All English HTML pages on a single host, with WARC pointers to fetch them
SELECT url, warc_filename, warc_record_offset, warc_record_length
FROM read_parquet('hf://datasets/{{.Repo}}/data/{{.Latest}}/*.parquet')
WHERE url_host_registered_domain = 'wikipedia.org'
  AND content_languages LIKE '%eng%'
  AND content_mime_type = 'text/html'
  AND fetch_status = 200;
```

### Using `datasets`

```python
from datasets import load_dataset

# Stream every crawl without downloading everything
ds = load_dataset("{{.Repo}}", split="train", streaming=True)
for row in ds:
    print(row["url"], row["fetch_status"])

# Load a single crawl by name
ds = load_dataset("{{.Repo}}", name="{{.Latest}}", split="train", streaming=True)
```

### Using `huggingface_hub`

```python
from huggingface_hub import snapshot_download

# Download one crawl
snapshot_download(
    "{{.Repo}}",
    repo_type="dataset",
    local_dir="./ccrawl-urls/",
    allow_patterns="data/{{.Latest}}/*.parquet",
)
```

For faster downloads, install `pip install huggingface_hub[hf_transfer]` and set `HF_HUB_ENABLE_HF_TRANSFER=1`.

### Using the CLI

```bash
# Download the first shard of the latest crawl
huggingface-cli download {{.Repo}} \
    --include "data/{{.Latest}}/part-00000.parquet" \
    --repo-type dataset --local-dir ./ccrawl-urls/
```

## Dataset statistics
{{if .HasRows}}
| Crawl | Shards | URLs | Parquet Size | State |
|-------|-------:|-----:|-------------:|:------|
{{range .Stats}}| `{{.Crawl}}` | {{.Shards}} | {{.URLs}} | {{.Size}} | {{.State}} |
{{end}}| **Total** | **{{.TotalShardsNum}}** | **{{.TotalRowsNum}}** | **{{.TotalBytes}}** | |
{{else}}
The first crawl is publishing now. This table fills in as crawls commit.
{{end}}
## How this dataset is built

The pipeline is a single Go binary that works one crawl at a time. It enumerates the crawl's columnar parts, projects the published columns out of each part with ranged HTTP reads, writes one Zstandard Parquet shard per part, and commits shards to the hub in batches, deleting each local file right after its commit so disk stays flat. The fetch, project, and commit stages run concurrently across a worker pool rather than as three separate global passes, so the elapsed figure below is end-to-end publish wall-clock for the crawl, not the sum of isolated phase timings.
{{with .Build}}
Live numbers for the newest crawl `{{.Latest}}`:

- Input: {{.InputParts}}, each streamed and projected without downloading the whole part
- Output: {{.Output}} of Zstandard Parquet committed so far, out of {{.TotalOutput}} across the whole dataset
- Coverage: {{.Coverage}}
- URLs: {{.Rows}}
- Elapsed: {{.Elapsed}} of publish wall-clock, from the first shard commit to the latest{{if .Rate}}
- Speed: {{.Rate}}{{end}}{{if .ETA}}
- Estimated completion: {{.ETA}}{{end}}{{if .Complete}}
- Status: complete, every shard is on the hub{{end}}

Each output shard keeps a projected subset of the source columns, so its Parquet size is smaller than the source part by design, and that gap is column projection plus compression rather than compression alone. The WARC pointer columns are kept intact so you can still range-fetch the original response.
{{end}}
# Dataset card for Common Crawl URL Index

## Dataset summary

A faithful Parquet mirror of Common Crawl's columnar URL index (`cc-index-table`, `subset=warc`). Each monthly crawl's index is republished shard for shard, in source order, with no aggregation or filtering. People use it for:

- **Crawl frontiers** - seed a crawler with real, recently seen URLs instead of guessing
- **Web-scale sampling** - draw pages by host, TLD, language, or MIME type
- **Targeted WARC fetches** - use the WARC pointers to pull just the responses you need
- **Web measurement** - study host coverage, status codes, and content types across crawls
- **Retrieval and dedup pipelines** - the SURT sort key and content digest are built in

## Dataset structure

### Data instances

One row is one captured URL:

```json
{
  "url_surtkey": "org,wikipedia)/wiki/common_crawl",
  "url": "https://en.wikipedia.org/wiki/Common_Crawl",
  "url_host_name": "en.wikipedia.org",
  "url_host_registered_domain": "wikipedia.org",
  "url_host_tld": "org",
  "url_protocol": "https",
  "fetch_time": "2026-06-18T04:11:57Z",
  "fetch_status": 200,
  "fetch_redirect": null,
  "content_digest": "3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ",
  "content_mime_type": "text/html",
  "content_mime_detected": "text/html",
  "content_charset": "UTF-8",
  "content_languages": "eng",
  "content_truncated": null,
  "warc_filename": "crawl-data/CC-MAIN-2026-25/segments/.../warc/CC-MAIN-...warc.gz",
  "warc_record_offset": 812634789,
  "warc_record_length": 24518
}
```

### Data fields

Columns are in source order. Types are the Parquet types written by the pipeline.

| Column | Type | Description |
|--------|------|-------------|
{{range .Columns}}| `{{index . 0}}` | {{index . 1}} | {{index . 2}} |
{{end}}
### Data splits

One named config per crawl, plus a `default` config that globs every crawl. Each loads its shards as a single `train` split.

```python
# One crawl by config name
ds = load_dataset("{{.Repo}}", name="{{.Latest}}", split="train")

# A specific crawl by path
ds = load_dataset("{{.Repo}}", data_files="data/{{.Latest}}/*.parquet", split="train")
```

## Dataset creation

### Why we built this

Common Crawl's columnar index is one of the most useful public datasets on the web, but the official copy lives behind a Hive-partitioned S3 layout that is awkward to browse and needs AWS tooling to query. We republish it in a plain, readable Hugging Face layout so you can point DuckDB or `datasets` straight at it, load a single crawl by name, and stream without any special setup.

### Source data

Everything comes from Common Crawl's cc-index columnar table, the `subset=warc` partition of each monthly crawl. Source format is Hive-partitioned Parquet on S3 and its HTTPS mirror, enumerated from each crawl's `cc-index-table.paths.gz` manifest.

### Processing steps

The pipeline is written in Go. For each crawl:

1. **Enumerate** the crawl's columnar parts from `cc-index-table.paths.gz`
2. **Skip** parts already committed, checked against the hub so a restart resumes cleanly
3. **Stream** each source part with ranged HTTP reads, projecting the published columns
4. **Write** one Zstandard-compressed Parquet shard per part, preserving source row order
5. **Commit** finished shards in batches to Hugging Face, with `stats.csv` and this card
6. **Delete** each local shard right after its commit lands, so disk stays flat

The pipeline picks up where it left off: `stats.csv` and the shards already on the hub are the resume signal, and committed parts are skipped on restart. No filtering, deduplication, or content changes. The rows match Common Crawl's index exactly, shard for shard. All Parquet files use Zstandard compression.

### Personal and sensitive information

The index describes public web pages: their URLs, hosts, and capture metadata. It does not contain page bodies. URLs can still carry personal information that site owners put in the open, and no scrubbing has been done. Treat URLs as public but potentially sensitive strings.

## Considerations for using the data

### Social impact

A readable, queryable URL index lowers the bar for web research, retrieval, and crawler building. Work that once needed an AWS account and Athena now runs from a laptop with DuckDB.

### Biases

Common Crawl is a sample of the web, not the whole web. Its seed lists, crawl budget, and politeness rules shape what gets captured, and popular hosts are covered more densely than the long tail. The index inherits every one of those biases. We did not correct for them.

### Known limitations

- **Snapshot, not history.** Each crawl is a point-in-time capture; a URL missing from one crawl may appear in another.
- **Fetch status varies.** Not every row is a 200. Redirects, 404s, and other statuses are all present, as captured.
- **Detected fields are heuristics.** `content_languages` and `content_mime_detected` come from Common Crawl's detectors and can be wrong.
- **WARC pointers are crawl-specific.** A `warc_filename` only resolves within its own crawl's archives.

## Additional information

### Licensing

Released under the [Open Data Commons Attribution License (ODC-By) v1.0](https://opendatacommons.org/licenses/by/1-0/), the same terms Common Crawl publishes under. Please credit [Common Crawl](https://commoncrawl.org) when you use this data.

Not affiliated with or endorsed by Common Crawl.

### Thanks

All the data here comes from [Common Crawl](https://commoncrawl.org), which crawls the web and gives the archives away for free. None of this would exist without their work.

### Contact

Questions, feedback, or issues, open a discussion on the [Community tab](https://huggingface.co/datasets/{{.Repo}}/discussions).

*Last updated: {{.Updated}}*

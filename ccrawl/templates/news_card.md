---
configs:
- config_name: default
  data_files:
  - split: train
    path: "data/**/*.parquet"
{{- range .Configs}}
- config_name: "{{.Name}}"
  data_files:
  - split: train
    path: "data/{{.Path}}/*.parquet"
{{- end}}
license: odc-by
task_categories:
- text-retrieval
- other
language:
- multilingual
pretty_name: Common Crawl News Index
size_categories:
- {{.SizeCat}}
tags:
- common-crawl
- news
- cc-news
- url-index
- parquet
- open-data
---

# Common Crawl News Index

> The index Common Crawl never published: every CC-NEWS article, with the byte offset that fetches it

## Table of Contents

- [What is it?](#what-is-it)
- [What is being released?](#what-is-being-released)
- [Breakdown by month](#breakdown-by-month)
- [Breakdown by language](#breakdown-by-language)
- [How to download and use this dataset](#how-to-download-and-use-this-dataset)
- [Dataset statistics](#dataset-statistics)
- [How this dataset is built](#how-this-dataset-is-built)
- [Dataset card](#dataset-card-for-common-crawl-news-index)
  - [Dataset summary](#dataset-summary)
  - [Dataset structure](#dataset-structure)
  - [Dataset creation](#dataset-creation)
  - [Considerations for using the data](#considerations-for-using-the-data)
- [Additional information](#additional-information)

## What is it?

[Common Crawl](https://commoncrawl.org) runs a continuous news crawl called CC-NEWS. It has been collecting articles from news sites around the world since 2016 and publishing them as WARC archives, month by month, for anyone to use.

Unlike the monthly main crawl, CC-NEWS ships with no index. There is no CDX file and no columnar table. There is a list of WARC file names and nothing else, so the only way to find out whether a story from one publisher is in there is to download and decompress every archive of that month and look. That is a few hundred gigabytes of reading to answer one question.

This dataset is that missing index. Every stored response in CC-NEWS becomes one row with its URL, host, fetch time, status, content type, identified language, and the exact byte span of the record inside its WARC file. Those last three columns are the useful ones: `warc_filename`, `warc_record_offset`, and `warc_record_length` are a ranged HTTP GET, so once you have found a row you can pull the original page out of Common Crawl's archives without downloading the archive.
{{if .HasRows}}
Right now the index covers **{{.TotalMonths}}** across **{{.TotalArticles}}** in **{{.TotalBytes}}** of compressed Parquet, built from **{{.TotalFiles}}**. New months are added as Common Crawl publishes them.
{{end}}
It is released under the **Open Data Commons Attribution License (ODC-By) v1.0**, the same license Common Crawl uses.

## What is being released?

One source WARC file becomes one Parquet shard, in a directory named for its year and month. Shards keep the name of the archive they index, so a file in the tree says which archive it came from without being opened.

```
data/
  {{.LatestPath}}/
    CC-NEWS-20260701022501-08467.parquet
    CC-NEWS-20260701043018-08468.parquet
    ...
stats.csv                     one row per month: files, articles, bytes read, bytes written
languages.csv                 one row per month and language
```

Each row is one captured article. `stats.csv` tracks coverage per month so you can see at a glance which months are whole and which are still building, and `languages.csv` is the language breakdown behind the table below.

This is an index, not a corpus. It does not contain article text. It contains the pointers that let you fetch article text for exactly the articles you want.
{{if .HasRows}}
## Breakdown by month

Articles per month, newest first.

```
{{range .Bars}}{{.}}
{{end}}```
{{end}}{{if .Langs}}
## Breakdown by language

Identified from the extracted article text, not from the page's own label. Shares are of all indexed articles, so they add up to less than 100 percent, and the difference is the rows with too little text to identify.

| Language | Code | Articles | Share | |
|----------|------|---------:|------:|:--|
{{range .Langs}}| {{.Name}} | `{{.Code}}` | {{.Articles}} | {{.Share}} | `{{.Bar}}` |
{{end}}{{end}}
## How to download and use this dataset

It is a standard Hugging Face Parquet layout, so it works with DuckDB, `datasets`, `pandas`, and `huggingface_hub` out of the box.

### Using DuckDB

DuckDB reads Parquet directly from Hugging Face, no download step needed.

```sql
-- Which publishers filed the most stories in one month
SELECT url_host_registered_domain AS publisher, count(*) AS articles
FROM read_parquet('hf://datasets/{{.Repo}}/data/{{.LatestPath}}/*.parquet')
GROUP BY publisher
ORDER BY articles DESC
LIMIT 20;
```

```sql
-- Every Spanish-language article from one publisher, with the pointers to fetch them
SELECT url, fetch_time, warc_filename, warc_record_offset, warc_record_length
FROM read_parquet('hf://datasets/{{.Repo}}/data/{{.LatestPath}}/*.parquet')
WHERE url_host_registered_domain = 'elpais.com'
  AND content_languages = 'spa'
  AND fetch_status = 200
ORDER BY fetch_time;
```

```sql
-- Where a publisher and our identifier disagree about the language of a page
SELECT content_language_declared, content_languages, count(*) AS articles
FROM read_parquet('hf://datasets/{{.Repo}}/data/**/*.parquet')
WHERE content_language_declared != ''
  AND content_languages != ''
  AND left(content_language_declared, 2) != left(content_languages, 2)
GROUP BY 1, 2
ORDER BY articles DESC
LIMIT 20;
```

```sql
-- Publishing rhythm: articles per hour of the day
SELECT hour(fetch_time) AS hour_utc, count(*) AS articles
FROM read_parquet('hf://datasets/{{.Repo}}/data/{{.LatestPath}}/*.parquet')
GROUP BY hour_utc
ORDER BY hour_utc;
```

### Fetching the article behind a row

The three WARC columns are a byte range. Any tool that can send a `Range` header can pull the original response out of Common Crawl's archives.

```bash
# The row said: CC-NEWS-20260701022501-08467.warc.gz, offset 71779, length 20806
curl -s -r 71779-92584 \
  https://data.commoncrawl.org/crawl-data/CC-NEWS/2026/07/CC-NEWS-20260701022501-08467.warc.gz \
  | gunzip
```

The [ccrawl](https://github.com/tamnd/ccrawl-cli) CLI that builds this dataset takes the same three columns directly:

```bash
ccrawl fetch --file crawl-data/CC-NEWS/2026/07/CC-NEWS-20260701022501-08467.warc.gz \
  --offset 71779 --length 20806 --text
```

### Using `datasets`

```python
from datasets import load_dataset

# Stream every month without downloading everything
ds = load_dataset("{{.Repo}}", split="train", streaming=True)
for row in ds:
    print(row["fetch_time"], row["url"])

# Load a single month by name
ds = load_dataset("{{.Repo}}", name="{{.Latest}}", split="train", streaming=True)
```

### Using `huggingface_hub`

```python
from huggingface_hub import snapshot_download

# Download one month
snapshot_download(
    "{{.Repo}}",
    repo_type="dataset",
    local_dir="./ccrawl-news/",
    allow_patterns="data/{{.LatestPath}}/*.parquet",
)
```

For faster downloads, install `pip install huggingface_hub[hf_transfer]` and set `HF_HUB_ENABLE_HF_TRANSFER=1`.

## Dataset statistics
{{if .HasRows}}
| Month | Files | Articles | Parquet Size | WARC Bytes Read | State |
|-------|------:|---------:|-------------:|----------------:|:------|
{{range .Stats}}| `{{.Month}}` | {{.Files}} | {{.Articles}} | {{.Size}} | {{.Source}} | {{.State}} |
{{end}}| **Total** | **{{.TotalFilesNum}}** | **{{.TotalRowsNum}}** | **{{.TotalBytes}}** | **{{.TotalSource}}** | |
{{else}}
The first month is publishing now. This table fills in as shards commit.
{{end}}
## How this dataset is built

The pipeline is a single Go binary that works one month at a time. It reads the month's `warc.paths.gz` manifest, streams each WARC over HTTP, walks the gzip members to record where every response starts and how long it is, turns each one into a row, and commits one Zstandard Parquet shard per source file to the hub in batches. The archives are never written to disk: they are decompressed, indexed, and dropped as they arrive, so a run holds one output shard per worker and nothing else. A stream that dies partway is resumed from the last complete record rather than restarted, which matters when the files are a gigabyte each.
{{with .Build}}
Live numbers for the newest month `{{.Latest}}`:

- Input: {{.InputFile}}, streamed and indexed without being stored
- Read so far: {{.Read}} of compressed WARC{{if .Projected}}, about {{.Projected}} for the whole month at this average{{end}}
- Output: {{.Output}} of Zstandard Parquet committed
- Coverage: {{.Coverage}}
- Articles: {{.Articles}}{{if .OKShare}}, {{.OKShare}} of them a 2xx{{end}}{{if .HTMLShare}}, {{.HTMLShare}} sniffed as HTML{{end}}
- Elapsed: {{.Elapsed}} of publish wall-clock, from the first shard commit to the latest{{if .Rate}}
- Speed: {{.Rate}}{{end}}{{if .ETA}}
- Estimated completion: {{.ETA}}{{end}}{{if .Complete}}
- Status: complete, every file in the month is indexed{{end}}
{{end}}{{if .Savings}}
Across the whole dataset, {{.Savings}}. That ratio is the reason this exists: the reading was done once so that nobody querying CC-NEWS has to do it again.
{{end}}
# Dataset card for Common Crawl News Index

## Dataset summary

A record-level index of Common Crawl's CC-NEWS archives, built by reading the archives themselves, because Common Crawl publishes no index for them. One row per stored HTTP response, with a byte-range pointer back into the original WARC. People use it for:

- **Finding articles without downloading archives** - filter by host, language, date, or status first, fetch second
- **News corpora by publisher or language** - select the rows you want, then pull only those responses
- **Media measurement** - publication volume and rhythm per outlet, per country, per language, over time
- **Deduplication** - `content_digest` is Common Crawl's own payload SHA-1, so identical bodies match across months
- **Seed lists** - real article URLs with the timestamps they were live

## Dataset structure

### Data instances

One row is one captured article:

```json
{
  "url_surtkey": "com,example)/2026/07/01/a-story",
  "url": "https://www.example.com/2026/07/01/a-story",
  "url_host_name": "www.example.com",
  "url_host_registered_domain": "example.com",
  "url_host_tld": "com",
  "url_protocol": "https",
  "fetch_time": "2026-07-01T02:25:01Z",
  "fetch_status": 200,
  "fetch_redirect": "",
  "content_digest": "3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ",
  "content_mime_type": "text/html",
  "content_mime_detected": "text/html",
  "content_charset": "UTF-8",
  "content_languages": "eng",
  "content_truncated": "",
  "warc_filename": "crawl-data/CC-NEWS/2026/07/CC-NEWS-20260701022501-08467.warc.gz",
  "warc_record_offset": 71779,
  "warc_record_length": 20806,
  "content_language_confidence": 0.99,
  "content_language_declared": "en-US",
  "content_length": 148326,
  "warc_record_id": "urn:uuid:f0a1c2d3-4e5f-6789-abcd-ef0123456789"
}
```

The first eighteen columns are the cc-index column names in cc-index order, so a query written against the sibling dataset [open-index/ccrawl-urls](https://huggingface.co/datasets/open-index/ccrawl-urls) runs here unchanged. The four after that have no cc-index counterpart and are kept at the end.

### Data fields

| Column | Type | Description |
|--------|------|-------------|
{{range .Columns}}| `{{index . 0}}` | {{index . 1}} | {{index . 2}} |
{{end}}
Which columns are reported and which are computed matters here more than it does in a mirror of an existing index, so it is worth being plain about it. `fetch_status`, `content_mime_type`, `content_charset`, and `content_truncated` are read off the response the crawler stored. `content_digest` and `warc_record_id` are Common Crawl's own WARC headers. Everything else is computed while reading the archive, including both language columns and both byte-span columns, because none of it exists anywhere in a CC-NEWS file.

### Data splits

One named config per month, plus a `default` config that globs every month. Each loads its shards as a single `train` split.

```python
# One month by config name
ds = load_dataset("{{.Repo}}", name="{{.Latest}}", split="train")

# A specific month by path
ds = load_dataset("{{.Repo}}", data_files="data/{{.LatestPath}}/*.parquet", split="train")
```

## Dataset creation

### Why we built this

Searching CC-NEWS for one publisher meant streaming every WARC in the month, which is hundreds of gigabytes and hours of wall-clock for a query that should take seconds. The archives are a fine format for storage and a poor one for lookup, and the missing piece was never the data, it was the index. So we read the archives once and published the index.

### Source data

Everything comes from Common Crawl's CC-NEWS archives at `crawl-data/CC-NEWS/YYYY/MM/`, enumerated from each month's `warc.paths.gz` manifest and read over HTTPS from `data.commoncrawl.org`.

CC-NEWS WARCs are raw crawler output. Unlike the main crawl's archives they carry no `WARC-Identified-Content-Language` and no `WARC-Identified-Payload-Type` headers, and there are no metadata records, which is why the language and detected MIME columns here are ours rather than Common Crawl's.

### Processing steps

The pipeline is written in Go. For each month:

1. **Enumerate** the month's WARC files from `warc.paths.gz`
2. **Skip** files already committed, checked against the hub so a restart resumes cleanly
3. **Stream** each WARC over HTTP, tracking the position of every gzip member as it goes
4. **Index** each stored response into a row, sniffing its MIME type and identifying its language from the extracted text
5. **Write** one Zstandard-compressed Parquet shard per source file, in record order
6. **Commit** finished shards in batches to Hugging Face, with `stats.csv`, `languages.csv`, and this card
7. **Delete** each local shard right after its commit lands, so disk stays flat

Language identification runs over the article text extracted from the page, not over the raw HTML, and the page is decoded from the charset its server declared before anything looks at it. Both of those matter. A trigram profile of markup and CSS class names is a profile of the boilerplate, and a Windows-1251 Russian page read as UTF-8 does not fail to identify, it identifies as Estonian.

### Personal and sensitive information

The index describes public news pages: their URLs, hosts, and capture metadata. It does not contain page bodies. URLs can still carry personal information that publishers put in the open, and no scrubbing has been done. Treat URLs as public but potentially sensitive strings.

## Considerations for using the data

### Social impact

CC-NEWS is one of the few large, freely licensed, continuously updated multilingual news archives in existence, and until now using it meant having the bandwidth to read all of it. An index turns that into a query. Work that needed a cluster now runs from a laptop.

### Biases

CC-NEWS is a sample of the news web shaped by Common Crawl's seed lists, its crawl budget, and its politeness rules. Large English-language publishers are covered far more densely than the long tail, some outlets block the crawler outright, and a publisher's presence or absence in a month says as much about the crawler as about the publisher. The index inherits every one of those biases and does not correct for them. The language breakdown above is a breakdown of what was crawled, not of what was published.

### Known limitations

- **Detected fields are heuristics.** `content_languages` and `content_mime_detected` are computed by us and can be wrong, particularly on short pages and between closely related languages. `content_language_confidence` is published next to the label precisely so you can filter on it.
- **Not deduplicated.** The same article can appear more than once within a month and across months. `content_digest` is the tool for that: identical bodies share a digest.
- **Responses only.** CC-NEWS stores a request record beside every response. Only responses are indexed.
- **Byte spans are file-specific.** An offset resolves only against the exact `warc_filename` in the same row.
- **A month is whole only when it says so.** The `State` column in the statistics table is the truth about coverage. A month still building is a partial index and will report fewer articles than it eventually holds.

## Additional information

### Licensing

Released under the [Open Data Commons Attribution License (ODC-By) v1.0](https://opendatacommons.org/licenses/by/1-0/), the same terms Common Crawl publishes under. Please credit [Common Crawl](https://commoncrawl.org) when you use this data.

Not affiliated with or endorsed by Common Crawl.

### Thanks

All the data here comes from [Common Crawl](https://commoncrawl.org), which crawls the web and gives the archives away for free. None of this would exist without their work.

### Contact

Questions, feedback, or issues, open a discussion on the [Community tab](https://huggingface.co/datasets/{{.Repo}}/discussions).

*Last updated: {{.Updated}}*

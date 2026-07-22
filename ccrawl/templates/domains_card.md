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
- graph-ml
- other
pretty_name: Common Crawl Domain Ranks
size_categories:
- {{.SizeCat}}
tags:
- common-crawl
- web-graph
- domain-ranks
- harmonic-centrality
- pagerank
- parquet
- open-data
---

# Common Crawl Domain Ranks

> Web domains ranked by harmonic centrality and PageRank, ready to prioritize a crawl

## Table of Contents

- [What is it?](#what-is-it)
- [What is being released?](#what-is-being-released)
- [Breakdown by release](#breakdown-by-release)
- [How to download and use this dataset](#how-to-download-and-use-this-dataset)
- [Dataset statistics](#dataset-statistics)
- [Dataset card](#dataset-card-for-common-crawl-domain-ranks)
  - [Dataset summary](#dataset-summary)
  - [Dataset structure](#dataset-structure)
  - [Dataset creation](#dataset-creation)
  - [Considerations for using the data](#considerations-for-using-the-data)
- [Additional information](#additional-information)

## What is it?

This dataset is the domain-level ranking from [Common Crawl](https://commoncrawl.org)'s hyperlink web graph, republished as clean Parquet. Common Crawl builds a graph of which domains link to which, then scores every domain by harmonic centrality and PageRank. A high rank means many other well-connected domains link to it, which is a solid proxy for importance when you decide what to crawl or trust first.

We take the ranks as they are and republish them with no changes to the numbers. The one edit is convenience: the source keys each row by a reversed host string (`com.example`), and we un-reverse it into a plain domain (`example.com`). The rows stay in the source's rank order, so `part-000` holds the highest-centrality domains and rank falls as the part number rises.
{{if .HasRows}}
Right now it holds **{{.TotalReleases}}** across **{{.TotalDomains}}** in **{{.TotalBytes}}** of compressed Parquet, cut into **{{.TotalShards}}**. New quarterly releases are added as Common Crawl publishes them.
{{end}}
Harmonic centrality and PageRank are two ways to answer the same question: how central is a domain in the web's link graph. Because the file is pre-sorted by harmonic centrality, reading from the top gives you the most important domains first, which is exactly what you want when seeding a crawl, building an allow-list, or picking a high-signal sample of the web.

It is released under the **Open Data Commons Attribution License (ODC-By) v1.0**, the same license Common Crawl uses.

## What is being released?

Each web-graph release is one directory of rank-ordered shards. Each shard holds a fixed number of rows, so a domain's rank position is just shard index times shard size plus its row offset.

```
data/
  {{.Latest}}/
    part-000.parquet          highest-centrality domains
    part-001.parquet
    ...
stats.csv                     one row per committed release
```

Read `part-000` first for the most important domains. `stats.csv` tracks every committed release with its shard count, domain count, Parquet size, source size, and shard-row size, so coverage and remaining work are easy to read off.
{{if .HasRows}}
## Breakdown by release

Domains per release, newest first.

```
{{range .Bars}}{{.}}
{{end}}```
{{end}}
## How to download and use this dataset

Read the top of a release for the most important domains, or stream the whole ranking. It is a standard Hugging Face Parquet layout, so it works with DuckDB, `datasets`, `pandas`, and `huggingface_hub` out of the box.

### Using DuckDB

DuckDB reads Parquet directly from Hugging Face, no download step needed.

```sql
-- Top 50 domains by harmonic centrality
SELECT domain, harmonic_pos, harmonic_val
FROM read_parquet('hf://datasets/{{.Repo}}/data/{{.Latest}}/*.parquet')
ORDER BY harmonic_pos
LIMIT 50;
```

```sql
-- Where does one domain rank?
SELECT domain, harmonic_pos, pagerank_pos
FROM read_parquet('hf://datasets/{{.Repo}}/data/{{.Latest}}/*.parquet')
WHERE domain = 'wikipedia.org';
```

```sql
-- Most central .org domains
SELECT domain, harmonic_pos
FROM read_parquet('hf://datasets/{{.Repo}}/data/{{.Latest}}/*.parquet')
WHERE domain LIKE '%.org'
ORDER BY harmonic_pos
LIMIT 20;
```

```sql
-- Domains where PageRank and harmonic centrality disagree most
SELECT domain, harmonic_pos, pagerank_pos,
       abs(harmonic_pos - pagerank_pos) AS gap
FROM read_parquet('hf://datasets/{{.Repo}}/data/{{.Latest}}/*.parquet')
ORDER BY gap DESC
LIMIT 20;
```

### Using `datasets`

```python
from datasets import load_dataset

# Stream the ranking, most important domains first
ds = load_dataset("{{.Repo}}", split="train", streaming=True)
for row in ds:
    print(row["harmonic_pos"], row["domain"])

# Load one release by name
ds = load_dataset("{{.Repo}}", name="{{.Latest}}", split="train", streaming=True)
```

### Using `huggingface_hub`

```python
from huggingface_hub import snapshot_download

# Download one release
snapshot_download(
    "{{.Repo}}",
    repo_type="dataset",
    local_dir="./ccrawl-domains/",
    allow_patterns="data/{{.Latest}}/*.parquet",
)
```

For faster downloads, install `pip install huggingface_hub[hf_transfer]` and set `HF_HUB_ENABLE_HF_TRANSFER=1`.

### Using the CLI

```bash
# Download just the top shard of the latest release
huggingface-cli download {{.Repo}} \
    --include "data/{{.Latest}}/part-000.parquet" \
    --repo-type dataset --local-dir ./ccrawl-domains/
```

## Dataset statistics
{{if .HasRows}}
| Release | Shards | Domains | Parquet Size | Source Size |
|---------|-------:|--------:|-------------:|------------:|
{{range .Stats}}| `{{.Graph}}` | {{.Shards}} | {{.Domains}} | {{.Size}} | {{.Source}} |
{{end}}| **Total** | **{{.TotalShardsNum}}** | **{{.TotalDomainsNum}}** | **{{.TotalBytes}}** | |
{{else}}
The first release is publishing now. This table fills in as releases commit.
{{end}}
# Dataset card for Common Crawl Domain Ranks

## Dataset summary

A faithful Parquet mirror of Common Crawl's domain-level web-graph ranks. Each quarterly release ranks every domain in the crawl by harmonic centrality and PageRank, and we republish that ranking in source order, shard for shard. People use it for:

- **Crawl prioritization** - start from the most central domains and work down
- **Allow-lists and seed lists** - a ranked, license-clean list of real domains
- **Web-graph research** - study centrality, PageRank, and how the two disagree
- **Sampling** - take a high-signal slice of the web by rank threshold
- **Reputation features** - centrality as a cheap prior for domain trust

## Dataset structure

### Data instances

One row is one domain and its ranks:

```json
{
  "domain": "wikipedia.org",
  "harmonic_pos": 1,
  "harmonic_val": 29491890.0,
  "pagerank_pos": 3,
  "pagerank_val": 0.0024193,
  "n_hosts": 4821
}
```

### Data fields

| Column | Type | Description |
|--------|------|-------------|
{{range .Columns}}| `{{index . 0}}` | {{index . 1}} | {{index . 2}} |
{{end}}
### Data splits

One named config per release, plus a `default` config that globs every release. Each loads its shards as a single `train` split, in rank order.

```python
# One release by config name
ds = load_dataset("{{.Repo}}", name="{{.Latest}}", split="train")

# A specific release by path
ds = load_dataset("{{.Repo}}", data_files="data/{{.Latest}}/*.parquet", split="train")
```

## Dataset creation

### Why we built this

Common Crawl publishes the domain ranks as a single large gzipped TSV per release, keyed by a reversed host string. That is fine for a one-off download but awkward to query and to load a slice of. We republish it as rank-ordered Parquet with un-reversed domains so you can read the top of the ranking directly, query it from DuckDB, or stream it with `datasets`, without downloading the whole file first.

### Source data

Everything comes from Common Crawl's [hyperlink web graph](https://commoncrawl.org/web-graphs), the domain-level rank tables. Source format is a single gzip-compressed, tab-separated file per release, pre-sorted by harmonic centrality, with columns for harmonic position and value, PageRank position and value, the reversed host, and the host count.

### Processing steps

The pipeline is written in Go. For each release:

1. **Stream** the gzipped ranks TSV top to bottom, never buffering the whole file
2. **Parse** each row, un-reversing the host key (`com.example` becomes `example.com`)
3. **Cut** a new Zstandard-compressed Parquet shard every fixed number of rows, keeping rank order exact
4. **Skip** shards already on the hub while still reading through the stream, so ordering never drifts
5. **Commit** finished shards in batches to Hugging Face, with `stats.csv` and this card
6. **Delete** each local shard right after its commit lands, so disk stays flat

The only change to the data is un-reversing the host string into a plain domain. The rank numbers, the row order, and the set of domains match the source release exactly. All Parquet files use Zstandard compression.

### Personal and sensitive information

The data is domain names and their ranks. It contains no personal data beyond what a domain name itself reveals.

## Considerations for using the data

### Social impact

A readable, ranked list of domains makes it easy to prioritize crawling, build seed lists, and study the shape of the web's link graph without heavy tooling.

### Biases

Centrality reflects the link structure Common Crawl observed, which reflects what it crawled. Well-linked, long-established, English-language, and commercial domains tend to rank higher, and the ranking amplifies existing prominence. A high rank means well connected, not trustworthy or high quality. We did not correct for any of this.

### Known limitations

- **Domain level, not host level.** Ranks are aggregated to the registrable domain; `n_hosts` says how many hosts fed into each one.
- **Snapshot per release.** Each quarterly release is a point-in-time view; ranks shift between releases.
- **Two metrics can disagree.** Harmonic centrality and PageRank measure related but different things; a domain can rank very differently under each.
- **Coverage follows the crawl.** Domains Common Crawl did not reach are not in the graph.

## Additional information

### Licensing

Released under the [Open Data Commons Attribution License (ODC-By) v1.0](https://opendatacommons.org/licenses/by/1-0/), the same terms Common Crawl publishes under. Please credit [Common Crawl](https://commoncrawl.org) when you use this data.

Not affiliated with or endorsed by Common Crawl.

### Thanks

All the data here comes from [Common Crawl](https://commoncrawl.org), which builds the web graph and gives it away for free. None of this would exist without their work.

### Contact

Questions, feedback, or issues, open a discussion on the [Community tab](https://huggingface.co/datasets/{{.Repo}}/discussions).

*Last updated: {{.Updated}}*

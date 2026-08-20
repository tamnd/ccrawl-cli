---
configs:
- config_name: default
  data_files:
  - split: train
    path: "data/*.parquet"
license: odc-by
task_categories:
- text-generation
- text-retrieval
- other
pretty_name: {{.PrettyName}}
size_categories:
- {{.SizeCat}}
tags:
{{- range .Tags}}
- {{.}}
{{- end}}
---

# {{.PrettyName}}

> {{.Tagline}}

## Table of Contents

- [What is it?](#what-is-it)
- [What is being released?](#what-is-being-released)
- [Coverage](#coverage)
- [How to download and use this dataset](#how-to-download-and-use-this-dataset)
- [Dataset statistics](#dataset-statistics)
- [Dataset card](#dataset-card-for-this-recrawl)
  - [Dataset summary](#dataset-summary)
  - [Dataset structure](#dataset-structure)
  - [Dataset creation](#dataset-creation)
  - [Considerations for using the data](#considerations-for-using-the-data)
- [Additional information](#additional-information)

## What is it?

{{.What}}
{{if .HasRows}}
Right now it holds **{{.RowsNum}} pages** in **{{.Bytes}}** of compressed Parquet across **{{.Files}}**, fetched by **{{.Servers}}** covering **{{.Slices}}** slices of the work list. {{.Progress}}
{{else}}
The first shards are publishing now. The numbers below fill in as they commit.
{{end}}
It is released under the **Open Data Commons Attribution License (ODC-By) v1.0**, the same license Common Crawl uses for the work list this was built from.

## What is being released?

Every fetch is one row. The body is stored inline, exactly as it came off the wire, so a row is self-contained and a query never has to reach back into a WARC file to see what a page said.

```
data/
  server1-shard0of3-a1b2c3d4e5f6.parquet
  server2-shard1of3-9f8e7d6c5b4a.parquet
  ...
ledger/
  server1-shard0of3.csv     what that machine has published
  server2-shard1of3.csv
  ...
```

A shard's name says which machine fetched it, which slice of the work list that machine took, and a short hash of its contents. The hash is there because the fleet runs for months and a machine can be restarted at any point, and naming a file by what is inside it makes republishing the same shard a no-op rather than a duplicate.

Each machine writes exactly one ledger file and never touches another's. Three machines committing at the same moment therefore cannot lose each other's numbers, which is a real failure mode when several writers share one stats file over a run this long. The totals on this card are the union of those ledger files.
{{if .HasRows}}
## Coverage

Pages per machine, largest first.

```
{{range .Bars}}{{.}}
{{end}}```
{{end}}
## How to download and use this dataset

It is a standard Hugging Face Parquet layout, so DuckDB, `datasets`, `pandas` and `huggingface_hub` all read it without a download step.

### Using DuckDB

```sql
-- What answered, and what did not
SELECT status, count(*) AS pages
FROM read_parquet('hf://datasets/{{.Repo}}/data/*.parquet')
GROUP BY status
ORDER BY pages DESC;
```

```sql
-- Read the text of one page as it was served
SELECT url, status, content_type, decode(body) AS html
FROM read_parquet('hf://datasets/{{.Repo}}/data/*.parquet')
WHERE status = 200
LIMIT 1;
```

```sql
-- Slowest hosts to answer, by time to first byte
SELECT host, count(*) AS pages, median(ttfb_ms) AS ttfb
FROM read_parquet('hf://datasets/{{.Repo}}/data/*.parquet')
WHERE status = 200
GROUP BY host
HAVING pages > 5
ORDER BY ttfb DESC
LIMIT 20;
```

```sql
-- Pages that moved somewhere else
SELECT url, final_url, status
FROM read_parquet('hf://datasets/{{.Repo}}/data/*.parquet')
WHERE final_url <> '' AND final_url <> url
LIMIT 50;
```

### Using `datasets`

```python
from datasets import load_dataset

ds = load_dataset("{{.Repo}}", split="train", streaming=True)
for row in ds:
    if row["status"] == 200:
        print(row["url"], len(row["body"]))
        break
```

### Using `huggingface_hub`

```python
from huggingface_hub import snapshot_download

snapshot_download(
    "{{.Repo}}",
    repo_type="dataset",
    local_dir="./{{.Kind}}-recrawl/",
    allow_patterns="data/*.parquet",
)
```

For faster downloads, install `pip install huggingface_hub[hf_transfer]` and set `HF_HUB_ENABLE_HF_TRANSFER=1`.

Bodies are stored raw and can be large, so streaming a slice beats downloading everything unless you really do want all of it.

## Dataset statistics
{{if .HasRows}}
| Machine | Slice | Shards | Pages | Size | Position in the work list |
|---------|-------|-------:|------:|-----:|---------------------------|
{{range .Stats}}| `{{.Server}}` | {{.Shard}} | {{.Files}} | {{.Rows}} | {{.Size}} | {{.State}} |
{{end}}| **Total** | | **{{.FilesNum}}** | **{{.RowsNum}}** | **{{.Bytes}}** | |

Position is a part number and a row offset into the work list, which is how a crawl this size keeps its place. It is the honest way to state progress against a list whose end is months away.
{{else}}
The first shards are publishing now. This table fills in as they commit.
{{end}}
## How this dataset is built

The pipeline is a single Go binary and two processes per machine.

The first streams the work list straight out of the published Parquet, never building a frontier, fetches every entry with one request per host per politeness interval, honours `robots.txt`, and writes each response as a Parquet row into a local shard. It keeps its place in a checkpoint that is a part number and a row offset and nothing else, so its state is the same few hundred bytes whether the work list has a thousand rows or six billion.

The second watches that directory and commits each shard as it closes, refreshes its ledger row and this card in the same commit, then deletes the local file. A shard being written is named `.parquet.tmp` and is renamed only once its footer is on disk, so the publisher can never pick up a half-written file. Committing as shards close is what keeps disk flat across a run measured in months, and it is why this dataset is usable long before it is finished.

The work list is split across machines by registered domain, not by URL, so every page of a site is fetched by the same machine and that site sees one politeness clock rather than three.

# Dataset card for this recrawl

## Dataset summary

A live refetch of a published Common Crawl work list, stored as Parquet with response bodies inline. People use it for:

- **Freshness** - compare what a page serves today against what the archive recorded
- **Availability** - measure what fraction of an index still answers, and with what
- **Content extraction** - the raw body is right there, no WARC seeking
- **Infrastructure research** - status, timing, headers and IP per fetch
- **Training corpora** - a license-clean, current snapshot of real pages

## Dataset structure

### Data instances

One row is one fetch:

```json
{
  "url": "https://example.com/",
  "host": "example.com",
  "status": 200,
  "fetched_at": 1766000000000,
  "content_type": "text/html; charset=utf-8",
  "body_length": 51234,
  "digest": "3f786850e387550fdab836ed7e6dc881de23001b",
  "ttfb_ms": 142,
  "fetch_duration_ms": 318,
  "final_url": "",
  "body": "<!doctype html>..."
}
```

### Data fields

| Column | Type | Description |
|--------|------|-------------|
{{range .Columns}}| `{{index . 0}}` | {{index . 1}} | {{index . 2}} |
{{end}}
### Data splits

One `train` split covering every shard. The shards are independent of each other, so any subset of them is a valid sample of the whole.

## Dataset creation

### Why we built this

An index tells you an address existed. It does not tell you what is there now. Refetching a published work list and storing the whole response makes the current web queryable the same way the archive is, and keeps both the failures and the successes so the shape of the result is honest.

### Source data

{{.Source}}

### Processing steps

1. **Stream** the work list out of published Parquet, one batch at a time, never into a frontier
2. **Split** it by registered domain so a site stays on one machine with one politeness clock
3. **Ask** `robots.txt` once per host and cache the answer, refusing what it refuses
4. **Fetch** each entry, recording the body, the headers, the timing and the outcome
5. **Cut** a new Zstandard Parquet shard once enough payload has gone into the open one
6. **Commit** each closed shard to the hub with a refreshed ledger and card, then delete it locally

Nothing is rewritten. The body is stored exactly as served, before any decoding, and a failed fetch is kept as a row with its error rather than dropped.

### Personal and sensitive information

These are public web pages fetched as an anonymous client, and they are stored as served. A public page can still contain personal information its author put there, and this dataset does not filter for that. Pages behind a login, a paywall or a `robots.txt` refusal were not fetched.

## Considerations for using the data

### Social impact

A current, queryable snapshot of real pages makes it possible to study the live web without every researcher running their own crawl, which is the polite outcome for the sites involved as well.

### Biases

{{.Bias}}

### Known limitations

- **A point in time.** Each row is what one URL served at one moment, and the web moved on afterwards.
- **Failures are rows.** Timeouts, DNS failures and refusals are recorded, so filter on `status` and `error` before treating a row as content.
- **Politeness shapes coverage.** Hosts that rate-limit or refuse are underrepresented on purpose.
- **Bodies are raw.** No decoding, no charset normalisation, no boilerplate stripping. That is deliberate, but it is work you have to do.
- **Still growing.** Until every slice reports complete, this is a partial view of the work list.

## Additional information

### Licensing

Released under the [Open Data Commons Attribution License (ODC-By) v1.0](https://opendatacommons.org/licenses/by/1-0/), the same terms the source work list is published under. Please credit [Common Crawl](https://commoncrawl.org) when you use this data.

Content in the `body` column belongs to whoever published the page. Treat it the way you would treat any web content you fetched yourself.

Not affiliated with or endorsed by Common Crawl.

### Thanks

The work list comes from [Common Crawl](https://commoncrawl.org), which maps the web and gives the result away for free. None of this would exist without their work.

### Contact

Questions, feedback, or issues, open a discussion on the [Community tab](https://huggingface.co/datasets/{{.Repo}}/discussions).

*Last updated: {{.Updated}}*

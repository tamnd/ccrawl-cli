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
- [What a row actually means](#what-a-row-actually-means)
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
Right now it holds **{{.RowsNum}} rows** in **{{.Bytes}}** of compressed Parquet across **{{.Files}}**, fetched by **{{.Servers}}** covering **{{.Slices}}** slices of the work list. {{.Progress}}
{{else}}
The first shards are publishing now. The numbers below fill in as they commit.
{{end}}
It is released under the **Open Data Commons Attribution License (ODC-By) v1.0**, the same license Common Crawl uses for the work list this was built from.

## What is being released?

Every fetch is one row. The body is stored inline, exactly as it came off the wire, so a row is self-contained and a query never has to reach back into a WARC file to see what a page said. Alongside it every HTML page carries the same page rendered to Markdown and to plain text, with the language it is written in and a fingerprint of its content, extracted at fetch time rather than in a pass somebody has to run afterwards.

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

Rows per machine, largest first.

```
{{range .Bars}}{{.}}
{{end}}```
{{end}}
## What a row actually means

An index tells you an address existed in an archive.
This tells you what happened when we asked for it, which raises questions an index never does.
Read this section before drawing a conclusion from a count of rows.

### A row is an attempt, not a page

Every attempt is stored, including the ones that failed.
A timeout, a name that does not resolve and a refused connection are all rows, with `status` 0 and the reason in `error`.
So `count(*)` is a count of URLs we tried, and it is not a count of readable pages.
Filter on `status = 200 AND error = ''` before treating rows as content, and use the failures deliberately when the failures are what you are studying.

A large share of a domain work list does not resolve at all, because a domain rank computed from a months-old web graph includes names that have since lapsed.
That is a real property of the source and not a fault in the fetch, and keeping those rows is what lets you measure it.

`error` holds one of six words rather than the message the network stack produced: `dns`, `timeout`, `refused`, `tls`, `skip` or `other`.
That is deliberate, because a Go network error carries the host and often a port and a resolver address, so a column holding those groups into one row per URL and answers nothing.
The original text is not thrown away, it is in `meta_json` under `error_detail`, which is where to look when the class is not enough.

### When it was fetched, against when it was crawled

`fetched_at` is when we asked, and it has nothing to do with when Common Crawl saw the page.
The gap between the two is months and it grows for the length of the run, since the fleet walks a work list of billions of rows over a period measured in months.
Two rows in this dataset can be weeks apart even though they came from the same source crawl.

That gap is the point of the dataset and also its main trap.
A difference between what the archive recorded and what a row here holds is a change that happened somewhere in that window, and this data cannot tell you when inside it.
If you need the fetch date, read `fetched_at` per row rather than assuming a release date for the shard.

### What a 404 means, and what it does not

A 404 means the server answered and said it has nothing at that address.
It does not distinguish a page that was deleted from a page that was moved without a redirect, and it does not distinguish either of those from a server that returns 404 to clients it does not like.
A 403 is closer to the last of those, and a 429 is a server asking us to slow down.
None of these are absence of a page in any strong sense, they are one server's answer to one anonymous request on one day.

Sites that block unfamiliar clients are therefore underrepresented as content and overrepresented in the 403s, and that skew is systematic rather than random.

### What a 304 means, and how to get the body

`unchanged` is true when the server answered 304 Not Modified, which happens when a previous pass gave us an `etag` or a `last_modified` for that URL and we sent it back.
The body on such a row is empty, and that is deliberate: over a corpus where most pages do not move between passes, storing only the ones that did is the difference between a dataset and a copy of one.

To get the body a 304 refers to, look up the same `url` in an earlier pass and take the row whose `digest` matches.
A first pass over a work list has no validators to send, so it has no 304s at all, and every row carries its body.

### How robots.txt is handled

`robots.txt` is not consulted on these two recrawls, and the card says so rather than leaving it to be inferred.

The reason is the shape of the work list rather than a view about the standard.
Every row of a domain corpus is a different host, so checking robots first turns one request per page into two, and it was measured at 45 percent of the crawler's time with a third of the hosts never answering the request at all.
The fetch is a single unconditional GET of one address that a public index already lists, at one request per host per politeness interval, from a client that identifies itself in `req_headers`.

Nothing here is behind a login or a paywall, because nothing here was logged in.
`disallowed` counts and a robots-aware mode both still exist in the crawler and are used for link crawls, where the cost is amortised over every page a host gives up.
If you run a site in here and would rather not be, the [Community tab](https://huggingface.co/datasets/{{.Repo}}/discussions) is the place to say so and rows will be removed.

## How to download and use this dataset

It is a standard Hugging Face Parquet layout, so DuckDB, `datasets`, `pandas` and `huggingface_hub` all read it without a download step.

### Using DuckDB

```sql
-- What answered, and what did not. Status 0 is a fetch that never got a response
SELECT status, count(*) AS rows
FROM read_parquet('hf://datasets/{{.Repo}}/data/*.parquet')
GROUP BY status
ORDER BY rows DESC;
```

```sql
-- Why the failures failed, which on a domain list is most of the work list
SELECT error, count(*) AS rows
FROM read_parquet('hf://datasets/{{.Repo}}/data/*.parquet')
WHERE error <> ''
GROUP BY error
ORDER BY rows DESC;
```

```sql
-- Read a page as Markdown, no HTML parsing on your side
SELECT url, title, language, word_count, markdown
FROM read_parquet('hf://datasets/{{.Repo}}/data/*.parquet')
WHERE markdown <> ''
LIMIT 1;
```

```sql
-- What languages the corpus is in, by pages with real text in them
SELECT language, count(*) AS pages, round(avg(word_count)) AS avg_words
FROM read_parquet('hf://datasets/{{.Repo}}/data/*.parquet')
WHERE language <> '' AND language_confidence > 0.8
GROUP BY language
ORDER BY pages DESC
LIMIT 20;
```

```sql
-- Near duplicate pages, which templated sites produce a lot of
SELECT simhash, count(*) AS pages, min(url) AS example
FROM read_parquet('hf://datasets/{{.Repo}}/data/*.parquet')
WHERE simhash <> 0
GROUP BY simhash
HAVING pages > 1
ORDER BY pages DESC
LIMIT 20;
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
    if row["markdown"]:
        print(row["url"], row["language"], row["word_count"])
        print(row["markdown"][:500])
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

Bodies are stored raw and can be large, so streaming a slice beats downloading everything unless you really do want all of it. If all you want is the text, project the `markdown` or `text` column and Parquet will skip the bodies on disk rather than read them and throw them away.

## Dataset statistics
{{if .HasRows}}
| Machine | Slice | Shards | Rows | Size | Position in the work list |
|---------|-------|-------:|------:|-----:|---------------------------|
{{range .Stats}}| `{{.Server}}` | {{.Shard}} | {{.Files}} | {{.Rows}} | {{.Size}} | {{.State}} |
{{end}}| **Total** | | **{{.FilesNum}}** | **{{.RowsNum}}** | **{{.Bytes}}** | |

Position is a part number and a row offset into the work list, which is how a crawl this size keeps its place. It is the honest way to state progress against a list whose end is months away.
{{else}}
The first shards are publishing now. This table fills in as they commit.
{{end}}
## How this dataset is built

The pipeline is a single Go binary and two processes per machine.

The first streams the work list straight out of the published Parquet, never building a frontier, fetches every entry with one request per host per politeness interval, and writes each response as a Parquet row into a local shard. It keeps its place in a checkpoint that is a part number and a row offset and nothing else, so its state is the same few hundred bytes whether the work list has a thousand rows or six billion.

The second watches that directory and commits each shard as it closes, refreshes its ledger row and this card in the same commit, then deletes the local file. A shard being written is named `.parquet.tmp` and is renamed only once its footer is on disk, so the publisher can never pick up a half-written file. Committing as shards close is what keeps disk flat across a run measured in months, and it is why this dataset is usable long before it is finished.

The work list is split across machines by registered domain, not by URL, so every page of a site is fetched by the same machine and that site sees one politeness clock rather than three.

# Dataset card for this recrawl

## Dataset summary

A live refetch of a published Common Crawl work list, stored as Parquet with response bodies inline. People use it for:

- **Freshness** - compare what a page serves today against what the archive recorded
- **Availability** - measure what fraction of an index still answers, and with what
- **Content extraction** - the raw body is right there, no WARC seeking, and the rendered text is beside it
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
  "body": "<!doctype html>...",
  "title": "Example Domain",
  "text": "Example Domain This domain is for use in illustrative examples...",
  "word_count": 28,
  "language": "eng",
  "language_confidence": 1.0,
  "simhash": 13835058055282163712,
  "extractor": "h2m@v0.2.1",
  "markdown": "# Example Domain\n\nThis domain is for use in..."
}
```

### Data fields

`From` says where the value came from, which changes what it is evidence of.
**served** is the site's own answer.
**computed** is this pipeline's opinion, and a different extractor or language detector would produce a different one over the same bytes.
**measured** is a number off our clock on our network, so it says as much about the machine that fetched the page as about the page.
**asked** is what we sent.

| Column | Type | From | Description |
|--------|------|------|-------------|
{{range .Columns}}| `{{index . 0}}` | {{index . 1}} | {{index . 2}} | {{index . 3}} |
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
3. **Fetch** each entry, recording the body, the headers, the timing and the outcome
4. **Render** every HTML page to Markdown and plain text while it is still in memory, and detect its language from the result
5. **Cut** a new Zstandard Parquet shard once enough payload has gone into the open one
6. **Commit** each closed shard to the hub with a refreshed ledger and card, then delete it locally

Nothing is rewritten. The body is stored exactly as served, before any decoding, and a failed fetch is kept as a row with its error rather than dropped. The rendered columns are added beside the body and never in place of it, so anybody who thinks a different extractor would do better can run one over the same bytes.

Rendering happens during the fetch because the fetch is waiting on the network and the machine is not. It costs the crawl nothing measurable, and it saves reading the whole corpus back a second time, which at this size is the difference between a dataset that is usable on the day it publishes and one that is usable months later.

### Personal and sensitive information

These are public web pages fetched as an anonymous client, and they are stored as served. A public page can still contain personal information its author put there, and this dataset does not filter for that. Nothing behind a login or a paywall was fetched, because nothing here was logged in. See [how robots.txt is handled](#how-robotstxt-is-handled) above for what was and was not consulted before asking.

## Considerations for using the data

### Social impact

A current, queryable snapshot of real pages makes it possible to study the live web without every researcher running their own crawl, which is the polite outcome for the sites involved as well.

### Biases

{{.Bias}}

### Known limitations

- **A point in time.** Each row is what one URL served at one moment, and the web moved on afterwards.
- **Failures are rows.** Timeouts, DNS failures and refusals are recorded, so filter on `status` and `error` before treating a row as content.
- **Blocking shapes coverage.** Sites that refuse unfamiliar clients answer with a 403 rather than with a page, so they are underrepresented as content and overrepresented in the error statuses, systematically rather than at random.
- **Bodies are raw.** The `body` column is the bytes as served, with no decoding and no charset normalisation. The rendered columns beside it are where the boilerplate has been stripped.
- **Extraction is one engine's opinion.** `extractor` names the engine and version that produced the text, and a different engine would disagree about where an article starts. Rows fetched months apart may carry different versions.
- **Only HTML is rendered.** A PDF, an image or a JSON API response keeps its body and leaves the text columns empty, which `extractor` being empty tells you apart from a page that rendered to nothing.
- **Still growing.** Until every slice reports complete, this is a partial view of the work list.

## Additional information

### Licensing

Released under the [Open Data Commons Attribution License (ODC-By) v1.0](https://opendatacommons.org/licenses/by/1-0/), the same terms the source work list is published under. Please credit [Common Crawl](https://commoncrawl.org) when you use this data.

Content in the `body`, `text` and `markdown` columns belongs to whoever published the page. Treat it the way you would treat any web content you fetched yourself.

Not affiliated with or endorsed by Common Crawl.

### Thanks

The work list comes from [Common Crawl](https://commoncrawl.org), which maps the web and gives the result away for free. None of this would exist without their work.

### Contact

Questions, feedback, or issues, open a discussion on the [Community tab](https://huggingface.co/datasets/{{.Repo}}/discussions).

*Last updated: {{.Updated}}*

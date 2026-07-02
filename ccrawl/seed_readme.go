package ccrawl

import (
	"fmt"
	"strings"
)

// SeedDatasetStats holds cumulative stats for the URL-seed dataset card. The
// row and byte totals cover the shards published so far; when fewer shards are
// published than the seed holds, GenerateSeedREADME scales them to a full-seed
// estimate the way the open-markdown card does.
type SeedDatasetStats struct {
	Repo            string
	CrawlID         string
	PublishedShards int
	TotalShards     int
	Rows            int64
	URLBytes        int64 // uncompressed URL bytes across published shards
	ParquetBytes    int64 // compressed Parquet bytes on HF
	Subset          string
}

// GenerateSeedREADME produces the HuggingFace dataset card for a published URL
// seed. The dataset is deliberately small and generic: one url column plus a
// derived host column, Hive-partitioned by crawl so multiple snapshots coexist.
// The card explains the provenance (a Common Crawl columnar URL index), the
// layout, how to load it, and how to turn it back into a meguri crawl frontier
// so the corpus never has to be pulled from Common Crawl twice.
func GenerateSeedREADME(s SeedDatasetStats) string {
	shards := max(s.PublishedShards, s.TotalShards)
	scaled := shards > s.PublishedShards && s.PublishedShards > 0

	rows, urlb, pq := s.Rows, s.URLBytes, s.ParquetBytes
	if scaled {
		rows = scaleEst(rows, s.PublishedShards, shards)
		urlb = scaleEst(urlb, s.PublishedShards, shards)
		pq = scaleEst(pq, s.PublishedShards, shards)
	}
	approx := ""
	if scaled {
		approx = "~"
	}

	subset := s.Subset
	if subset == "" {
		subset = "warc"
	}

	var b strings.Builder

	fmt.Fprintf(&b, `---
configs:
- config_name: default
  data_files:
  - split: train
    path: "data/crawl=%s/**/*.parquet"
- config_name: %s
  data_files:
  - split: train
    path: "data/crawl=%s/**/*.parquet"
license: odc-by
task_categories:
- text-retrieval
language:
- multilingual
pretty_name: Common Crawl URL Seed
size_categories:
- %s
tags:
- common-crawl
- urls
- crawl-frontier
- parquet
- open-data
---

`, s.CrawlID, s.CrawlID, s.CrawlID, sizeCategory(rows))

	fmt.Fprintf(&b, `# **Common Crawl URL Seed**

> Every URL in a Common Crawl snapshot, as partitioned Parquet, ready to reseed a crawl

## What is it?

This dataset is the full URL list of a [Common Crawl](https://commoncrawl.org) snapshot, pulled straight from the crawl's columnar URL index and written as partitioned Parquet. Common Crawl is a non-profit that crawls the web and freely publishes its archives. Each snapshot ships a columnar index whose `+"`url`"+` column names every page the crawl captured. This dataset is that column, deduplicated and sharded, so a downstream crawler can reseed from a known frontier without pulling the 150 GB+ index again.

It currently holds crawl **%s** (%s subset) with **%s URLs across %s shards**.%s

The dataset is released under the **Open Data Commons Attribution License (ODC-By) v1.0**, the same license Common Crawl uses.

## What is being released?

The URLs are sharded by host. Each shard is the set of URLs whose host hashes into one contiguous slice of the 64-bit hostkey space, so every URL of a host lands in exactly one shard and the shards tile the whole space with no gap and no overlap. That is the same partitioning [meguri](https://github.com/tamnd/meguri) assigns when it ingests a seed, so a shard maps one to one onto a crawl-frontier partition. The shards live under a crawl-partitioned directory:

`+"```"+`
data/
  crawl=%s/
    shard-00000.parquet
    shard-00001.parquet
    shard-00002.parquet
    ...
manifest.json
`+"```"+`

`+"`manifest.json`"+` is the shard map: it records each shard's hostkey range and row count, so a puller can rebuild the exact same frontier partitions the crawl used.

## Schema

| Column | Type | Description |
|---|---|---|
| `+"`url`"+` | string | A URL captured by the crawl |
| `+"`host`"+` | string | Host parsed from the URL, the key the shard is partitioned on |

## How to load it

### Using `+"`datasets`"+`

`+"```python"+`
from datasets import load_dataset

# stream every URL in the crawl
ds = load_dataset("%s", name="%s", split="train", streaming=True)
for row in ds:
    print(row["url"])

# load one shard into memory
ds = load_dataset(
    "%s",
    data_files="data/crawl=%s/shard-00000.parquet",
    split="train",
)
`+"```"+`

### Using DuckDB

`+"```sql"+`
SELECT url
FROM read_parquet('hf://datasets/%s/data/crawl=%s/**/*.parquet')
WHERE host = 'en.wikipedia.org';
`+"```"+`

### Reseed a crawl frontier

Download the shards and hand them to meguri to rebuild the crawl frontier without touching Common Crawl again:

`+"```bash"+`
huggingface-cli download %s --repo-type dataset \
  --include "data/crawl=%s/**" --local-dir ./cc-urls

# the parquet url column is the seed; feed it straight into a meguri store
meguri shard build --urls ./cc-urls/data/crawl=%s --out ./frontier
`+"```"+`

## Provenance

The URLs come from the Common Crawl columnar URL index (`+"`cc-index/table/cc-main/warc`"+`), subset `+"`%s`"+`, for crawl `+"`%s`"+`. Only the `+"`url`"+` column of the index is read; nothing about page content is included here. The pull, the host sharding, and this dataset are produced by [ccrawl-cli](https://github.com/tamnd/ccrawl-cli).

`, s.CrawlID, subset, docStr(approx, rows), fmtInt(int64(shards)), scaledNote(scaled), // intro
		s.CrawlID,         // file tree
		s.Repo, s.CrawlID, // datasets name
		s.Repo, s.CrawlID, // datasets data_files
		s.Repo, s.CrawlID, // duckdb
		s.Repo, s.CrawlID, s.CrawlID, // reseed download + build
		subset, s.CrawlID, // provenance
	)

	// Sizes table.
	b.WriteString("## Sizes\n\n")
	if scaled {
		fmt.Fprintf(&b, "Measured across %s of %s and projected to the full seed of %s shards.\n\n",
			plural(s.PublishedShards, "shard"), s.CrawlID, fmtInt(int64(shards)))
		b.WriteString("| Stage | Measured | Projected (" + fmtInt(int64(shards)) + " shards) |\n")
		b.WriteString("|---|---|---|\n")
		fmt.Fprintf(&b, "| URLs | %s | ~%s |\n", fmtInt(s.Rows), fmtInt(rows))
		fmt.Fprintf(&b, "| URL text (uncompressed) | %s | ~%s |\n", fmtBytes(s.URLBytes), fmtBytes(urlb))
		fmt.Fprintf(&b, "| Parquet (Zstd) | %s | ~%s |\n", fmtBytes(s.ParquetBytes), fmtBytes(pq))
	} else {
		fmt.Fprintf(&b, "Measured across %s of %s.\n\n",
			plural(s.PublishedShards, "shard"), s.CrawlID)
		b.WriteString("| Stage | Size |\n")
		b.WriteString("|---|---|\n")
		fmt.Fprintf(&b, "| URLs | %s |\n", fmtInt(rows))
		fmt.Fprintf(&b, "| URL text (uncompressed) | %s |\n", fmtBytes(urlb))
		fmt.Fprintf(&b, "| Parquet (Zstd) | %s |\n", fmtBytes(pq))
	}

	b.WriteString(`
## Licensing

Released under the **Open Data Commons Attribution License (ODC-By) v1.0**. Use is also subject to [Common Crawl's Terms of Use](https://commoncrawl.org/terms-of-use). The URLs point at content that remains subject to its publishers' rights.
`)

	return b.String()
}

// docStr renders the URL count for the intro line, prefixing the approx marker
// when the totals were scaled from a partial run.
func docStr(approx string, rows int64) string { return approx + fmtInt(rows) }

// scaledNote adds a sentence noting the totals are a projection when a run has
// published only part of the seed.
func scaledNote(scaled bool) string {
	if scaled {
		return " Totals are projected from the shards published so far and will settle as the rest land."
	}
	return ""
}

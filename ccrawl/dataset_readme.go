package ccrawl

import (
	"fmt"
	"strings"
)

// CorpusCardStats holds the counts for the document-corpus dataset card. The
// totals cover the files published so far; when a run publishes only part of a
// crawl, GenerateCorpusREADME marks the totals as partial rather than scaling,
// since the file sizes here are already exact per file.
type CorpusCardStats struct {
	Repo            string
	CrawlID         string
	Subset          string // wet, warc, or wat
	PublishedFiles  int
	TotalFiles      int
	Rows            int64 // document rows across published files
	ParquetBytes    int64 // compressed Parquet bytes on HF
	PartialProgress bool
}

// GenerateCorpusREADME produces the HuggingFace dataset card for a published
// document corpus: the plain-text records of a Common Crawl snapshot, converted
// to Parquet and Hive-partitioned by crawl. The card explains the provenance,
// the layout, how to load it, and how to point ccrawl back at it so a rebuild
// pulls the cached Parquet instead of redownloading and reconverting the crawl.
func GenerateCorpusREADME(s CorpusCardStats) string {
	subset := s.Subset
	if subset == "" {
		subset = "wet"
	}
	kind := subsetKind(subset)

	partial := ""
	if s.PartialProgress {
		partial = " Publishing is in progress, so the file count grows as the rest land."
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
- text-generation
- text-retrieval
language:
- multilingual
pretty_name: Common Crawl %s Corpus
size_categories:
- %s
tags:
- common-crawl
- %s
- text
- parquet
- open-data
---

`, s.CrawlID, s.CrawlID, s.CrawlID, kind.pretty, sizeCategory(s.Rows), subset)

	fmt.Fprintf(&b, `# **Common Crawl %s Corpus**

> The %s of a Common Crawl snapshot, converted to partitioned Parquet, ready to index

## What is it?

This dataset is the %s of a [Common Crawl](https://commoncrawl.org) snapshot, converted from the crawl's %s archives into partitioned Parquet. Common Crawl is a non-profit that crawls the web and freely publishes its archives. Each snapshot ships %s files that carry %s; this dataset is those records in a columnar form that a search index or a language pipeline can read without touching the multi-terabyte raw archives again.

It currently holds crawl **%s** with **%s documents across %s Parquet files**.%s%s

The dataset is released under the **Open Data Commons Attribution License (ODC-By) v1.0**, the same license Common Crawl uses.

## Layout

Files are Hive-partitioned by crawl, so several snapshots can share one repo and a reader can filter to one crawl without a full scan:

`+"```"+`
data/
  crawl=%s/
    %s
    ...
`+"```"+`

## Schema

%s

## How to load it

### Using `+"`datasets`"+`

`+"```python"+`
from datasets import load_dataset

# stream the whole corpus
ds = load_dataset("%s", name="%s", split="train", streaming=True)
for row in ds:
    print(row["url"], row.get("text", "")[:200])
`+"```"+`

### Using DuckDB

`+"```sql"+`
SELECT url, text
FROM read_parquet('hf://datasets/%s/data/crawl=%s/**/*.parquet')
LIMIT 10;
`+"```"+`

### Restore the local cache with ccrawl

`+"```bash"+`
# pull every Parquet file for the crawl back into a local directory
ccrawl dataset pull --repo %s --crawl %s --out ./corpus
`+"```"+`

Once the files are local, an indexer reads the directory straight off; the crawl never has to be downloaded from Common Crawl or reconverted.

## Provenance

The text comes from the Common Crawl %s archives for crawl `+"`%s`"+`. The download and the Parquet conversion are produced by [ccrawl-cli](https://github.com/tamnd/ccrawl-cli). Nothing here is added or rewritten beyond parsing the archive records into columns.

`, kind.pretty, kind.contains, // title, blockquote
		kind.contains, subset, // what is it (1)
		strings.ToUpper(subset), kind.contains, // what is it (2)
		s.CrawlID, fmtInt(s.Rows), fmtInt(int64(s.TotalFiles)), partial, "", // counts
		s.CrawlID, exampleFileName(subset), // layout tree
		schemaTable(subset),  // schema
		s.Repo, subset,       // datasets load
		s.Repo, s.CrawlID,    // duckdb
		s.Repo, s.CrawlID,    // pull
		strings.ToUpper(subset), s.CrawlID, // provenance
	)

	// Sizes.
	b.WriteString("## Sizes\n\n")
	b.WriteString("| Column set | Size |\n")
	b.WriteString("|---|---|\n")
	fmt.Fprintf(&b, "| Documents | %s |\n", fmtInt(s.Rows))
	fmt.Fprintf(&b, "| Parquet (Zstd) | %s |\n", fmtBytes(s.ParquetBytes))
	fmt.Fprintf(&b, "| Files | %s |\n", fmtInt(int64(s.TotalFiles)))

	b.WriteString(`
## Licensing

Released under the **Open Data Commons Attribution License (ODC-By) v1.0**. Use is also subject to [Common Crawl's Terms of Use](https://commoncrawl.org/terms-of-use). The text points at content that remains subject to its publishers' rights.
`)

	return b.String()
}

// subsetKindInfo describes one archive kind for the card prose.
type subsetKindInfo struct {
	pretty   string // WET -> "Text", WARC -> "WARC", WAT -> "Metadata"
	contains string // human phrase for what the records hold
}

func subsetKind(subset string) subsetKindInfo {
	switch strings.ToLower(subset) {
	case "warc":
		return subsetKindInfo{pretty: "WARC", contains: "raw HTTP responses and page HTML"}
	case "wat":
		return subsetKindInfo{pretty: "Metadata", contains: "page metadata and outbound links"}
	default:
		return subsetKindInfo{pretty: "Text", contains: "the extracted plain text of every crawled page"}
	}
}

// exampleFileName is a representative Parquet file name for the layout tree.
func exampleFileName(subset string) string {
	switch strings.ToLower(subset) {
	case "warc":
		return "CC-MAIN-...-00000.warc.parquet"
	case "wat":
		return "CC-MAIN-...-00000.wat.parquet"
	default:
		return "CC-MAIN-...-00000.wet.parquet"
	}
}

// schemaTable renders the columnar schema for the subset the corpus came from.
func schemaTable(subset string) string {
	switch strings.ToLower(subset) {
	case "warc":
		return `| Column | Type | Description |
|---|---|---|
| ` + "`record_id`" + ` | string | WARC record ID |
| ` + "`crawl_id`" + ` | string | Crawl the record belongs to |
| ` + "`target_uri`" + ` | string | URL of the captured page |
| ` + "`date`" + ` | timestamp | Capture time |
| ` + "`http_status`" + ` | int | HTTP status of the fetch |
| ` + "`content_type`" + ` | string | Declared content type |
| ` + "`title`" + ` | string | Parsed page title |
| ` + "`language`" + ` | string | Detected language |
| ` + "`markdown`" + ` | string | Page rendered to Markdown |
| ` + "`text`" + ` | string | Extracted plain text |`
	case "wat":
		return `| Column | Type | Description |
|---|---|---|
| ` + "`record_id`" + ` | string | WAT record ID |
| ` + "`crawl_id`" + ` | string | Crawl the record belongs to |
| ` + "`url`" + ` | string | URL of the page |
| ` + "`date`" + ` | timestamp | Capture time |
| ` + "`http_status`" + ` | int | HTTP status of the fetch |
| ` + "`title`" + ` | string | Parsed page title |
| ` + "`links_count`" + ` | int | Number of outbound links |
| ` + "`links`" + ` | string | Outbound links, JSON |
| ` + "`metas`" + ` | string | Page meta tags, JSON |`
	default:
		return `| Column | Type | Description |
|---|---|---|
| ` + "`record_id`" + ` | string | WET record ID |
| ` + "`crawl_id`" + ` | string | Crawl the record belongs to |
| ` + "`url`" + ` | string | URL of the crawled page |
| ` + "`date`" + ` | timestamp | Capture time |
| ` + "`content_language`" + ` | string | Detected language |
| ` + "`text_length`" + ` | int | Length of the plain text in bytes |
| ` + "`text`" + ` | string | Extracted plain text of the page |`
	}
}

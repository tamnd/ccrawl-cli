package ccrawl

import (
	"fmt"
	"strconv"
	"strings"
)

// MarkdownDatasetStats holds cumulative stats for the open-markdown README.
// The byte and row totals cover the shards committed so far; when fewer shards
// are committed than the crawl holds, GenerateMarkdownREADME scales them to a
// full-crawl estimate the same way the original open-markdown card did.
type MarkdownDatasetStats struct {
	CrawlID         string
	CommittedShards int
	TotalShards     int
	Rows            int64
	WARCBytes       int64 // compressed .warc.gz bytes downloaded
	HTMLBytes       int64
	MDBytes         int64
	ParquetBytes    int64
	// Cumulative pipeline timings across committed shards, in seconds. Zero
	// values drop the Processing Times chart from the card.
	DownloadS int64
	ConvertS  int64
	ExportS   int64
	PublishS  int64
}

// GenerateMarkdownREADME produces the HuggingFace dataset card for
// open-markdown-v2. It mirrors the original open-index/open-markdown card
// section for section: the intro, the released-files description, the download
// snippets, the full dataset card, the schema, the compression-ratio table, and
// the processing-time chart. The only deltas are this dataset's Hive partition
// layout (data/crawl=.../NNNNNN.parquet) and its actual column set, which drops
// warc_refers_to and uses a SHA-256 doc_id.
func GenerateMarkdownREADME(s MarkdownDatasetStats) string {
	shards := s.CommittedShards
	if s.TotalShards > shards {
		shards = s.TotalShards
	}
	scaled := shards > s.CommittedShards && s.CommittedShards > 0

	rows, warc, html, md, pq := s.Rows, s.WARCBytes, s.HTMLBytes, s.MDBytes, s.ParquetBytes
	if scaled {
		rows = scaleEst(rows, s.CommittedShards, shards)
		warc = scaleEst(warc, s.CommittedShards, shards)
		html = scaleEst(html, s.CommittedShards, shards)
		md = scaleEst(md, s.CommittedShards, shards)
		pq = scaleEst(pq, s.CommittedShards, shards)
	}

	approx := ""
	if scaled {
		approx = "~"
	}

	docsStr := approx + fmtInt(rows)
	committedShards := plural(s.CommittedShards, "shard")

	reduction := ""
	if html > 0 && md > 0 {
		pct := float64(html-md) / float64(html) * 100
		reduction = fmt.Sprintf(" Processed %s%s of raw HTML into %s%s of clean Markdown, a **%.1f%% reduction**.",
			approx, fmtBytes(html), approx, fmtBytes(md), pct)
	}

	var b strings.Builder

	// Frontmatter. The default config reads every crawl; a per-crawl config
	// lets callers load a single snapshot by name.
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
- feature-extraction
language:
- multilingual
pretty_name: Open Markdown
size_categories:
- %s
tags:
- common-crawl
- web
- markdown
- html-to-markdown
- parquet
- open-data
---

`, s.CrawlID, s.CrawlID, s.CrawlID, sizeCategory(rows))

	fmt.Fprintf(&b, `# **Open Markdown**

> Clean markdown from the web, ready for training and retrieval

## What is it?

**Open Markdown** is a large-scale web text dataset built from [Common Crawl](https://commoncrawl.org). Common Crawl is a non-profit that crawls the web and freely provides its archives to the public. Every page goes through a pipeline that extracts the main content from raw HTML, converts it to clean Markdown, and packages the result into Parquet files with WARC metadata for traceability.

The dataset currently includes crawl **%s** with **%s documents across %s shards**.%s We plan to add more snapshots over time.

**Open Markdown** is released under the **Open Data Commons Attribution License (ODC-By) v1.0**, the same license used by Common Crawl.

## What is being released?

Each Common Crawl WARC file (about 1 GB of compressed HTML) becomes one Parquet shard. The shards live under a crawl-specific, Hive-partitioned directory so multiple snapshots can coexist and partition-aware tools can filter by crawl without a full scan:

`+"```"+`
data/
  crawl=%s/
    000000.parquet
    000001.parquet
    000002.parquet
    ...
`+"```"+`

Every row in a Parquet file is one web page. Each row keeps the `+"`warc_record_id`"+` parsed from the original WARC header so you can trace a document back to its source record, plus `+"`html_length`"+` and `+"`markdown_length`"+` to measure the compression from raw HTML to clean Markdown.

## How to download and use Open Markdown

### Using `+"`datasets`"+`

`+"```python"+`
from datasets import load_dataset

# stream the entire dataset
ds = load_dataset("open-index/open-markdown-v2", name="%s", split="train", streaming=True)
for doc in ds:
    print(doc["url"], doc["markdown_length"])

# load a single shard into memory
ds = load_dataset(
    "open-index/open-markdown-v2",
    data_files="data/crawl=%s/000000.parquet",
    split="train",
)
`+"```"+`

### Using `+"`huggingface_hub`"+`

`+"```python"+`
from huggingface_hub import snapshot_download

folder = snapshot_download(
    "open-index/open-markdown-v2",
    repo_type="dataset",
    local_dir="./open-markdown-v2/",
    allow_patterns="data/crawl=%s/**/*.parquet",
)
`+"```"+`

For faster downloads, install `+"`pip install huggingface_hub[hf_transfer]`"+` and set `+"`HF_HUB_ENABLE_HF_TRANSFER=1`"+`.

### Using DuckDB

`+"```sql"+`
SELECT url, host, markdown_length
FROM read_parquet('hf://datasets/open-index/open-markdown-v2/data/crawl=%s/**/*.parquet')
WHERE host = 'en.wikipedia.org'
LIMIT 10;
`+"```"+`

# Dataset card for Open Markdown

## Dataset Description

- **Homepage and Repository:** [https://huggingface.co/datasets/open-index/open-markdown-v2](https://huggingface.co/datasets/open-index/open-markdown-v2)
- **Point of Contact:** please create a discussion on the Community tab
- **License:** Open Data Commons Attribution License (ODC-By) v1.0

## Dataset Structure

### Data Instance

The following is an example row from the dataset:

`+"```json"+`
{
  "doc_id": "6aaa5be7a9175105aa60e39ea1d087fc",
  "url": "https://example.com/article/interesting-topic",
  "host": "example.com",
  "crawl_date": "2026-06-12",
  "warc_record_id": "<urn:uuid:a1b2c3d4-e5f6-7890-abcd-ef1234567890>",
  "html_length": 48210,
  "markdown_length": 3847,
  "markdown": "# Interesting Topic\n\nThis is the main content of the page..."
}
`+"```"+`

### Data Fields

| Column | Type | Description |
|---|---|---|
| `+"`doc_id`"+` | string | Deterministic SHA-256 hash of the URL, first 16 bytes in hex. Identical URLs always produce the same `+"`doc_id`"+` across crawls, so cross-crawl dedup is an equi-join on this column |
| `+"`url`"+` | string | Original URL of the crawled page |
| `+"`host`"+` | string | Hostname extracted from the URL |
| `+"`crawl_date`"+` | string | Date of the WARC record (YYYY-MM-DD) |
| `+"`warc_record_id`"+` | string | WARC-Record-ID of the original HTTP response (`+"`<urn:uuid:...>`"+`) |
| `+"`html_length`"+` | int64 | Byte length of the original HTML body before conversion |
| `+"`markdown_length`"+` | int64 | Byte length of the converted Markdown body |
| `+"`markdown`"+` | string | Clean Markdown content extracted from the page |

### Data Splits

The default subset includes all available data across all crawl snapshots. You can also load a specific crawl by using its ID as the config name (for example `+"`%s`"+`).

## Dataset Creation

### Curation Rationale

Most open web datasets either release raw text without structure or keep the HTML and leave parsing to the user. **Open Markdown** sits in between: it converts every page to Markdown so the content is immediately usable for training and retrieval, while preserving the `+"`warc_record_id`"+` so you can always trace back to the source record.

### Source Data

The source data consists of web pages crawled by the [Common Crawl](https://commoncrawl.org) foundation. Common Crawl archives billions of pages across the public web and makes the raw WARC files freely available on Amazon S3.

### Data Processing Steps

The processing pipeline runs as a single streaming pass with no intermediate files:

1. **Download** raw .warc.gz files from Common Crawl S3 (each file is roughly 1 GB compressed)
2. **Filter** to keep only HTTP 200 responses with a `+"`text/html`"+` content type, discarding images, scripts, redirects, and error pages
3. **Convert** HTML to clean Markdown. [go-trafilatura](https://github.com/markusmobius/go-trafilatura) tuned for recall (`+"`FavorRecall`"+`, with Readability and DomDistiller fallbacks) isolates the main content node and strips navigation, ads, and boilerplate, then a direct node-tree walk renders GitHub-flavored Markdown with links resolved to absolute URLs
4. **Export** straight to Apache Parquet with Zstd compression

The pipeline streams from the compressed WARC through conversion directly into Parquet. Pages that produce empty conversions are dropped.

`,
		s.CrawlID, docsStr, fmtInt(int64(shards)), reduction, // intro line
		s.CrawlID, // file tree
		s.CrawlID, // datasets name=
		s.CrawlID, // datasets data_files=
		s.CrawlID, // huggingface_hub allow_patterns=
		s.CrawlID, // duckdb path
		s.CrawlID, // data splits example
	)

	// Compression Ratios table. Rows are rendered only when we have data for
	// them so a fresh single-shard run still produces a clean table.
	b.WriteString("### Compression Ratios\n\n")
	if scaled {
		fmt.Fprintf(&b, "Numbers below are measured across %s of %s (%s pages) and projected to the full crawl of %s WARC files.\n\n",
			committedShards, s.CrawlID, fmtInt(s.Rows), fmtInt(int64(shards)))
		b.WriteString("| Stage | Measured | Projected (" + fmtInt(int64(shards)) + " files) | Reduction |\n")
		b.WriteString("|---|---|---|---|\n")
		writeRatioRow(&b, "Raw WARC (.warc.gz, downloaded)", s.WARCBytes, warc, "")
		writeRatioRow(&b, "HTML extracted (uncompressed)", s.HTMLBytes, html, "")
		writeRatioRow(&b, "Markdown (clean text)", s.MDBytes, md, pctVs(html, md)+" vs HTML")
		writeRatioRow(&b, "Final Parquet (Zstd)", s.ParquetBytes, pq, pctVs(md, pq)+" vs Markdown")
	} else {
		fmt.Fprintf(&b, "Numbers below are measured across %s of %s (%s pages).\n\n",
			committedShards, s.CrawlID, fmtInt(s.Rows))
		b.WriteString("| Stage | Size | Reduction |\n")
		b.WriteString("|---|---|---|\n")
		fmt.Fprintf(&b, "| Raw WARC (.warc.gz, downloaded) | %s | — |\n", fmtBytes(warc))
		fmt.Fprintf(&b, "| HTML extracted (uncompressed) | %s | — |\n", fmtBytes(html))
		fmt.Fprintf(&b, "| Markdown (clean text) | %s | **%s vs HTML** |\n", fmtBytes(md), pctVs(html, md))
		fmt.Fprintf(&b, "| Final Parquet (Zstd) | %s | **%s vs Markdown** |\n", fmtBytes(pq), pctVs(md, pq))
	}
	b.WriteString("\nThe big win is the HTML to Markdown conversion: trafilatura keeps only the main content and the renderer drops every tag, script, style, and navigation block.")
	if html > 0 && md > 0 {
		drop := float64(html-md) / float64(html) * 100
		fmt.Fprintf(&b, " This cuts %s%s of uncompressed HTML down to %s%s of Markdown, a **%.1f%% reduction**.",
			approx, fmtBytes(html), approx, fmtBytes(md), drop)
	}
	b.WriteString(" Parquet with Zstd then compresses the Markdown further.\n\n")

	// Processing Times. The pipeline downloads, converts, and writes Parquet in
	// one streaming pass, so those three are reported as a single Convert bar;
	// publishing to HuggingFace is the second bar.
	convertS := s.DownloadS + s.ConvertS + s.ExportS
	if convertS > 0 || s.PublishS > 0 {
		maxS := convertS
		if s.PublishS > maxS {
			maxS = s.PublishS
		}
		b.WriteString("### Processing Times\n\n")
		fmt.Fprintf(&b, "Pipeline timings across %s of %s:\n\n",
			committedShards, s.CrawlID)
		b.WriteString("```\n")
		if convertS > 0 {
			b.WriteString(timingBar("Convert  (stream WARC to Markdown to Parquet)  ", convertS, maxS))
		}
		if s.PublishS > 0 {
			b.WriteString(timingBar("Publish  (HuggingFace upload)                  ", s.PublishS, maxS))
		}
		b.WriteString("```\n\n")
	}

	b.WriteString(`### Personal and Sensitive Information

No additional PII filtering is applied beyond what Common Crawl provides. As the dataset is sourced from the public web, it is likely that some personally identifiable information is present. If you find your own PII in the dataset and would like it removed, please open an issue on the repository.

## Considerations for Using the Data

### Social Impact

By releasing both the dataset and the full processing pipeline, we aim to lower the barrier to training and evaluating language models on high quality web data. Researchers and practitioners who cannot afford to run their own Common Crawl processing pipelines can use **Open Markdown** directly.

### Discussion of Biases

**Open Markdown** inherits the biases present in Common Crawl and the public web at large. The trafilatura extraction step favors article-like pages and may underrepresent content from forums, social media, and non-standard page layouts. We have not applied any machine-learning-based quality or toxicity filters, as such filters have been shown to disproportionately remove content from certain dialects and communities.

### Known Limitations

Code-heavy pages may not convert well to Markdown. If you are training a model that needs strong code performance, consider supplementing **Open Markdown** with a dedicated code dataset such as [The Stack v2](https://huggingface.co/datasets/bigcode/the-stack-v2). Similarly, highly structured pages like Wikipedia may have better formatting in dedicated Wikipedia dumps than in their Common Crawl versions.

## Additional Information

### Licensing

The dataset is released under the **Open Data Commons Attribution License (ODC-By) v1.0**. The use of this dataset is also subject to [Common Crawl's Terms of Use](https://commoncrawl.org/terms-of-use). The original content remains subject to the rights and terms of its respective publishers.

### Contact

Please open a discussion on the [Community tab](https://huggingface.co/datasets/open-index/open-markdown-v2/discussions) for questions, feedback, or issues.
`)

	return b.String()
}

// writeRatioRow renders one measured-plus-projected line of the Compression
// Ratios table. The reduction column is left blank for raw-size rows.
func writeRatioRow(b *strings.Builder, stage string, measured, projected int64, reduction string) {
	red := "—"
	if reduction != "" {
		red = "**" + reduction + "**"
	}
	fmt.Fprintf(b, "| %s | %s | ~%s | %s |\n", stage, fmtBytes(measured), fmtBytes(projected), red)
}

// pctVs returns the percent reduction from before to after as a signed string
// (for example "-94.2%").
func pctVs(before, after int64) string {
	if before <= 0 {
		return "—"
	}
	pct := float64(after-before) / float64(before) * 100
	return fmt.Sprintf("%.1f%%", pct)
}

// plural renders a count with its noun, adding an "s" when the count is not 1
// (1 shard, 2 shards). The count keeps thousands separators.
func plural(n int, noun string) string {
	word := noun
	if n != 1 {
		word += "s"
	}
	return fmtInt(int64(n)) + " " + word
}

// sizeCategory maps a row count to the HuggingFace size_categories bucket.
func sizeCategory(rows int64) string {
	switch {
	case rows >= 10_000_000_000:
		return "n>10B"
	case rows >= 1_000_000_000:
		return "1B<n<10B"
	case rows >= 100_000_000:
		return "100M<n<1B"
	case rows >= 10_000_000:
		return "10M<n<100M"
	case rows >= 1_000_000:
		return "1M<n<10M"
	case rows >= 100_000:
		return "100K<n<1M"
	default:
		return "n<100K"
	}
}

// timingBar renders one row of the ASCII bar chart in the Processing Times card.
func timingBar(label string, totalS, maxS int64) string {
	const barWidth = 24
	filled := 0
	if maxS > 0 && totalS > 0 {
		filled = int(float64(totalS) / float64(maxS) * barWidth)
		if filled < 1 {
			filled = 1
		}
		if filled > barWidth {
			filled = barWidth
		}
	}
	var bar strings.Builder
	for i := 0; i < barWidth; i++ {
		if i < filled {
			bar.WriteRune('█')
		} else {
			bar.WriteRune('░')
		}
	}
	return fmt.Sprintf("%s  %s  %s\n", label, bar.String(), fmtDuration(totalS))
}

func fmtDuration(secs int64) string {
	if secs <= 0 {
		return "0s"
	}
	h := secs / 3600
	m := (secs % 3600) / 60
	s := secs % 60
	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

// fmtInt formats an integer with thousands separators (1234567 -> 1,234,567).
func fmtInt(n int64) string {
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return s
	}
	var parts []string
	for len(s) > 3 {
		parts = append([]string{s[len(s)-3:]}, parts...)
		s = s[:len(s)-3]
	}
	parts = append([]string{s}, parts...)
	return strings.Join(parts, ",")
}

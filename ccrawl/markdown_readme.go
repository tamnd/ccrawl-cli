package ccrawl

import (
	"fmt"
	"strings"
)

// MarkdownDatasetStats holds cumulative stats for the open-markdown README.
type MarkdownDatasetStats struct {
	CrawlID         string
	CommittedShards int
	TotalShards     int
	Rows            int64
	HTMLBytes       int64
	MDBytes         int64
	ParquetBytes    int64
}

// GenerateMarkdownREADME produces a HuggingFace dataset card for open-markdown-v2.
// It is committed to the repo as README.md alongside each shard.
func GenerateMarkdownREADME(s MarkdownDatasetStats) string {
	var b strings.Builder
	w := func(f string, a ...any) { fmt.Fprintf(&b, f, a...) }

	// YAML frontmatter — matches open-index/open-markdown layout.
	w("---\n")
	w("configs:\n")
	w("- config_name: default\n")
	w("  data_files:\n")
	w("  - split: train\n")
	w("    path: \"data/crawl=%s/**/*.parquet\"\n", s.CrawlID)
	w("license: odc-by\n")
	w("task_categories:\n")
	w("  - text-generation\n")
	w("  - feature-extraction\n")
	w("language:\n")
	w("  - multilingual\n")
	w("tags:\n")
	for _, t := range []string{
		"common-crawl", "web", "markdown", "html-to-markdown", "parquet", "open-data",
	} {
		w("  - %s\n", t)
	}
	w("size_categories:\n")
	w("  - 100M<n<1B\n")
	w("pretty_name: Open Markdown\n")
	w("---\n\n")

	// Header
	w("# Open Markdown — %s\n\n", s.CrawlID)
	w("The full Common Crawl converted to clean, structured Markdown. ")
	w("Every HTML page is stripped of boilerplate, navigation, and ads, ")
	w("then converted to Markdown with links resolved to absolute URLs.\n\n")

	// Progress line
	if s.CommittedShards >= s.TotalShards && s.TotalShards > 0 {
		w("All **%d** shards committed for crawl `%s` — dataset complete.\n\n",
			s.TotalShards, s.CrawlID)
	} else {
		w("**%d / %d** shards committed for crawl `%s` — new shards added as processing continues.\n\n",
			s.CommittedShards, s.TotalShards, s.CrawlID)
	}

	// Stats
	docsStr := fmtCount(s.Rows)
	if s.CommittedShards < s.TotalShards && s.CommittedShards > 0 {
		docsStr = "~" + fmtCount(scaleEst(s.Rows, s.CommittedShards, s.TotalShards))
	}

	w("## Stats\n\n")
	w("| Metric | Value |\n|---|---|\n")
	w("| Crawl | `%s` |\n", s.CrawlID)
	shardsVal := fmt.Sprintf("%d / %d", s.CommittedShards, s.TotalShards)
	if s.CommittedShards >= s.TotalShards && s.TotalShards > 0 {
		shardsVal = fmt.Sprintf("%d (complete)", s.TotalShards)
	}
	w("| Shards | %s |\n", shardsVal)
	w("| Documents | %s |\n", docsStr)
	if s.HTMLBytes > 0 {
		prefix := ""
		if s.CommittedShards < s.TotalShards {
			prefix = "~"
		}
		w("| Raw HTML | %s%s |\n", prefix, fmtBytes(s.HTMLBytes))
		w("| Clean Markdown | %s%s |\n", prefix, fmtBytes(s.MDBytes))
		w("| Parquet (zstd) | %s%s |\n", prefix, fmtBytes(s.ParquetBytes))
		if s.MDBytes > 0 {
			ratio := float64(s.MDBytes) / float64(s.HTMLBytes) * 100
			w("| Markdown / HTML | %.1f%% |\n", ratio)
		}
	}
	w("\n")

	// Schema
	w("## Schema\n\n")
	w("Each row is one web page converted to Markdown.\n\n")
	w("| Column | Type | Description |\n|---|---|---|\n")
	for _, col := range [][]string{
		{"doc_id", "string", "Stable SHA-256 URL hash (32 hex chars) — enables cross-crawl dedup by equi-join"},
		{"url", "string", "Original page URL"},
		{"host", "string", "Hostname (e.g. `en.wikipedia.org`)"},
		{"crawl_date", "string", "ISO date from WARC-Date header (`YYYY-MM-DD`)"},
		{"warc_record_id", "string", "WARC record identifier"},
		{"html_length", "int64", "Raw HTML body bytes before conversion"},
		{"markdown_length", "int64", "Converted Markdown bytes"},
		{"markdown", "string", "Clean Markdown text (GFM: tables, absolute links, cleaned prose)"},
	} {
		w("| `%s` | %s | %s |\n", col[0], col[1], col[2])
	}
	w("\n")

	// Usage
	w("## Usage\n\n")
	w("### Python (streaming)\n\n")
	w("```python\n")
	w("from datasets import load_dataset\n\n")
	w("ds = load_dataset(\"open-index/open-markdown-v2\", split=\"train\", streaming=True)\n")
	w("for doc in ds:\n")
	w("    print(doc[\"url\"], doc[\"markdown_length\"])\n")
	w("    break\n")
	w("```\n\n")

	w("### DuckDB (no download required)\n\n")
	w("```sql\n")
	w("SELECT url, host, markdown_length\n")
	w("FROM read_parquet(\n")
	w("  'hf://datasets/open-index/open-markdown-v2/data/crawl=%s/**/*.parquet'\n", s.CrawlID)
	w(")\n")
	w("WHERE host = 'en.wikipedia.org'\n")
	w("LIMIT 10;\n")
	w("```\n\n")

	w("### Cross-crawl dedup via doc_id\n\n")
	w("```sql\n")
	w("-- Pages in 2026-25 not seen in 2026-08 (new or changed content)\n")
	w("SELECT a.doc_id, a.url, a.crawl_date\n")
	w("FROM read_parquet('hf://datasets/open-index/open-markdown-v2/data/crawl=CC-MAIN-2026-25/**/*.parquet') a\n")
	w("LEFT JOIN read_parquet('hf://datasets/open-index/open-markdown-v2/data/crawl=CC-MAIN-2026-08/**/*.parquet') b\n")
	w("  ON a.doc_id = b.doc_id\n")
	w("WHERE b.doc_id IS NULL\n")
	w("LIMIT 100;\n")
	w("```\n\n")

	// How it was built
	w("## How it was built\n\n")
	w("1. **Download** — CC WARC files are streamed over HTTP directly from `data.commoncrawl.org`.\n")
	w("   No WARC is written to disk — the HTTP response body streams straight into the WARC iterator.\n\n")
	w("2. **Extract** — each WARC response record is filtered (status 200, HTML MIME).\n")
	w("   The HTML body is charset-transcoded to UTF-8, then run through go-readability and kage sanitize\n")
	w("   to strip ads, navigation, and boilerplate.\n\n")
	w("3. **Convert** — the cleaned DOM tree is rendered as GFM Markdown with `mdconv`:\n")
	w("   tables, strikethrough, absolute links, entity decoding, cleaned prose.\n\n")
	w("4. **Export** — rows are written to a zstd-compressed Parquet file (one file per WARC shard).\n")
	w("   N workers parallelise extraction and conversion while the HTTP download streams concurrently.\n\n")
	w("5. **Publish** — each shard is committed to HuggingFace immediately after writing.\n")
	w("   Path: `data/crawl={crawl}/{shard:06d}.parquet`. The dataset is queryable before all shards complete.\n\n")
	w("Built with [`ccrawl`](https://github.com/tamnd/ccrawl-cli).\n\n")

	// License
	w("## License\n\n")
	w("Data derived from [Common Crawl](https://commoncrawl.org/), ")
	w("released under the [Open Data Commons Attribution License (ODC-By)](https://opendatacommons.org/licenses/by/1-0/).\n")
	w("You must attribute Common Crawl when using or redistributing this dataset.\n")

	return b.String()
}

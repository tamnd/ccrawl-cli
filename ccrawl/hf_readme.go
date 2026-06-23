package ccrawl

import (
	"fmt"
	"strings"
)

// DatasetStats accumulates per-shard statistics for the README.
type DatasetStats struct {
	CrawlID         string
	CommittedShards int
	TotalShards     int
	TotalURLs       int64
	TotalBytes      int64 // sum of WARC record bytes across all URL rows
	// Batched-pipeline fields (non-zero activates batch-aware progress line).
	TotalBatches     int
	CommittedBatches int
}

// GenerateDatasetREADME produces a HuggingFace dataset card for cc-host-dataset.
// It is committed to the repo as README.md and updated on each shard commit.
func GenerateDatasetREADME(s DatasetStats) string {
	if s.TotalShards == 0 {
		s.TotalShards = 28
	}

	var progressLine string
	if s.CommittedShards >= s.TotalShards && s.TotalShards > 0 {
		progressLine = fmt.Sprintf("All **%d** shards committed for crawl `%s` — dataset complete.",
			s.TotalShards, s.CrawlID)
	} else if s.TotalBatches > 0 {
		progressLine = fmt.Sprintf("**%d / %d** shards committed for crawl `%s` — new shards added every ~4 minutes.",
			s.CommittedShards, s.TotalShards, s.CrawlID)
	} else {
		progressLine = fmt.Sprintf("**%d / %d** prefix shards committed for crawl `%s`.",
			s.CommittedShards, s.TotalShards, s.CrawlID)
	}

	urlsStr := fmtCount(s.TotalURLs)
	if s.CommittedShards < s.TotalShards && s.CommittedShards > 0 {
		urlsStr = "~" + fmtCount(scaleEst(s.TotalURLs, s.CommittedShards, s.TotalShards))
	}
	bytesStr := ""
	if s.TotalBytes > 0 {
		bytesStr = fmtBytes(s.TotalBytes)
		if s.CommittedShards < s.TotalShards {
			bytesStr = "~" + fmtBytes(scaleEst(s.TotalBytes, s.CommittedShards, s.TotalShards))
		}
	}

	var b strings.Builder

	// ── YAML frontmatter ────────────────────────────────────────────────────
	w := func(f string, a ...any) { fmt.Fprintf(&b, f, a...) }
	w("---\n")
	w("configs:\n")
	w("- config_name: default\n")
	w("  data_files:\n")
	w("  - split: train\n")
	if s.TotalBatches > 0 {
		w("    path: \"data/crawl=%s/subset=urls/**/*.parquet\"\n", s.CrawlID)
	} else {
		w("    path: \"data/crawl=%s/subset=urls/*.parquet\"\n", s.CrawlID)
	}
	w("license: odc-by\n")
	w("task_categories:\n  - feature-extraction\n  - text-classification\n")
	w("language:\n  - multilingual\n")
	w("tags:\n")
	for _, t := range []string{
		"common-crawl", "web-crawl", "url-index", "parquet",
		"warc", "seo", "web-graph", "open-data",
	} {
		w("  - %s\n", t)
	}
	w("size_categories:\n  - 1B<n<10B\n")
	w("pretty_name: CC Host Dataset\n")
	w("---\n\n")

	// ── Header ───────────────────────────────────────────────────────────────
	w("# CC Host Dataset — %s\n\n", s.CrawlID)
	w("A per-URL index of the entire [Common Crawl](https://commoncrawl.org/) " +
		"with host-level rank signals.\n")
	w("Every URL captured by the crawler becomes one row, enriched with 20 raw CDX fields " +
		"and harmonic centrality rank from the CC web graph.\n")
	w("The dataset covers roughly **1.9 billion URLs** across ~262 million hosts.\n\n")

	// ── Progress / Stats ─────────────────────────────────────────────────────
	w("%s\n\n", progressLine)
	w("## Stats\n\n")
	w("| Metric | Value |\n|---|---|\n")
	w("| Crawl | `%s` |\n", s.CrawlID)
	shardsVal := fmt.Sprintf("%d / %d", s.CommittedShards, s.TotalShards)
	if s.CommittedShards >= s.TotalShards {
		shardsVal = fmt.Sprintf("%d (complete)", s.TotalShards)
	}
	w("| Shards committed | %s |\n", shardsVal)
	if urlsStr != "" {
		w("| URLs indexed | %s |\n", urlsStr)
	}
	if bytesStr != "" {
		w("| WARC record bytes (raw) | %s |\n", bytesStr)
	}
	w("\n")

	// ── Schema ───────────────────────────────────────────────────────────────
	w("## Schema\n\n")
	w("Each row is one URL capture. " +
		"Twenty fields come from CC CDX Parquet; six come from the CC web-graph rank table.\n\n")

	schemaSection := func(title string, rows [][]string) {
		w("### %s\n\n", title)
		w("| Field | Type | Description |\n|---|---|---|\n")
		for _, r := range rows {
			w("| `%s` | %s | %s |\n", r[0], r[1], r[2])
		}
		w("\n")
	}

	schemaSection("Identity", [][]string{
		{"url", "string", "Full crawled URL"},
		{"surt", "string", "SURT canonical form — sort key for CDX range lookups"},
		{"host", "string", "Forward hostname (`www.example.com`)"},
		{"rd", "string", "Registered domain / eTLD+1 (`example.com`)"},
		{"tld", "string", "Effective TLD (`com`, `co.uk`)"},
		{"proto", "string", "`http` or `https`"},
	})
	schemaSection("Fetch result", [][]string{
		{"st", "int32", "HTTP status code"},
		{"redir", "string", "Final redirect target URL (empty if none)"},
		{"ts", "string", "Fetch timestamp — ISO-8601"},
		{"bytes", "int64", "WARC record length in bytes"},
	})
	schemaSection("Content", [][]string{
		{"mime", "string", "Detected MIME type (`text/html`, `application/pdf`, …)"},
		{"mime_d", "string", "Declared MIME from `Content-Type` header"},
		{"charset", "string", "Character set from `Content-Type`"},
		{"lang", "string", "Content language(s), comma-separated BCP-47"},
		{"trunc", "string", "Truncation reason (`bytes`, `disconnect`, …) or empty"},
		{"digest", "string", "SHA-1 content hash — use for dedup and change detection"},
	})
	schemaSection("WARC pointer", [][]string{
		{"warc_f", "string", "Relative WARC file path on `data.commoncrawl.org`"},
		{"warc_o", "int64", "Byte offset into the WARC file"},
		{"robots_ok", "bool", "`robotstxt_forceget` — robots.txt allowed the crawl"},
		{"crawl", "string", "CC crawl ID (`CC-MAIN-2026-21`)"},
	})
	schemaSection("Rank signals (from CC web graph)", [][]string{
		{"harmonic_pos", "int64", "Position in harmonic centrality ranking (1 = most central)"},
		{"harmonic_val", "float64", "Raw harmonic centrality score"},
		{"pagerank_pos", "int64", "PageRank position"},
		{"pagerank_val", "float64", "Raw PageRank score"},
		{"graph_id", "string", "Web-graph release ID"},
	})

	// ── Usage ────────────────────────────────────────────────────────────────
	w("## Usage\n\n")

	w("### DuckDB — no download required\n\n")
	w("```sql\n")
	w("-- Install httpfs extension once\n")
	w("INSTALL httpfs; LOAD httpfs;\n\n")
	w("-- Top English hosts by rank\n")
	w("SELECT host, rd, harmonic_pos, count(*) AS urls\n")
	w("FROM read_parquet(\n")
	w("  'hf://datasets/open-index/cc-host-dataset/data/crawl=%s/subset=urls/*.parquet'\n", s.CrawlID)
	w(")\n")
	w("WHERE st = 200 AND lang LIKE '%%en%%'\n")
	w("GROUP BY host, rd, harmonic_pos\n")
	w("ORDER BY harmonic_pos\n")
	w("LIMIT 20;\n")
	w("```\n\n")

	w("### Fetch raw HTML for any URL (WARC byte-range)\n\n")
	w("```python\n")
	w("import requests, gzip, duckdb\n\n")
	w("row = duckdb.sql(\"\"\"\n")
	w("  SELECT url, warc_f, warc_o, bytes\n")
	w("  FROM read_parquet('hf://datasets/open-index/cc-host-dataset/data/crawl=%s/subset=urls/hosts-a.parquet')\n", s.CrawlID)
	w("  WHERE url = 'https://www.example.com/'\n")
	w("\"\"\").fetchone()\n\n")
	w("url, warc_f, warc_o, length = row\n")
	w("resp = requests.get(\n")
	w("  f\"https://data.commoncrawl.org/{warc_f}\",\n")
	w("  headers={\"Range\": f\"bytes={warc_o}-{warc_o + length - 1}\"}\n")
	w(")\n")
	w("html = gzip.decompress(resp.content)\n")
	w("print(html[:500].decode(errors=\"replace\"))\n")
	w("```\n\n")

	w("### Detect content changes across crawls\n\n")
	w("```python\n")
	w("import duckdb\n\n")
	w("result = duckdb.sql(\"\"\"\n")
	w("  WITH new AS (\n")
	w("    SELECT url, digest\n")
	w("    FROM read_parquet('hf://datasets/open-index/cc-host-dataset/data/crawl=CC-MAIN-2026-21/subset=urls/hosts-a.parquet')\n")
	w("    WHERE st = 200\n")
	w("  ),\n")
	w("  old AS (\n")
	w("    SELECT url, digest\n")
	w("    FROM read_parquet('hf://datasets/open-index/cc-host-dataset/data/crawl=CC-MAIN-2026-17/subset=urls/hosts-a.parquet')\n")
	w("    WHERE st = 200\n")
	w("  )\n")
	w("  SELECT count(*) AS changed_urls,\n")
	w("         round(count(*) * 100.0 / (SELECT count(*) FROM new), 2) AS pct_changed\n")
	w("  FROM new JOIN old USING (url)\n")
	w("  WHERE new.digest != old.digest\n")
	w("\"\"\").fetchone()\n")
	w("print(f\"{result[0]:,} URLs changed ({result[1]}%%)\")\n")
	w("```\n\n")

	w("### Multi-crawl comparison with hive partitioning\n\n")
	w("```sql\n")
	w("-- DuckDB extracts 'crawl' and 'subset' as columns automatically\n")
	w("SELECT crawl, count(*) AS urls, count(DISTINCT host) AS hosts\n")
	w("FROM read_parquet(\n")
	w("  'hf://datasets/open-index/cc-host-dataset/data/**/*.parquet',\n")
	w("  hive_partitioning = true\n")
	w(")\n")
	w("GROUP BY crawl\n")
	w("ORDER BY crawl DESC;\n")
	w("```\n\n")

	w("### Python / HuggingFace datasets\n\n")
	w("```python\n")
	w("from datasets import load_dataset\n\n")
	w("ds = load_dataset(\n")
	w("  \"open-index/cc-host-dataset\",\n")
	w("  split=\"train\",\n")
	w("  streaming=True\n")
	w(")\n")
	w("for row in ds:\n")
	w("    print(row[\"url\"], row[\"host\"], row[\"harmonic_pos\"])\n")
	w("    break\n")
	w("```\n\n")

	// ── How it was built ─────────────────────────────────────────────────────
	w("## How it was built\n\n")
	w("1. **CDX extract** — all 302 CC CDX Parquet files (~570 MB each, ~184 GB total) are " +
		"downloaded in parallel with pure-Go workers.\n" +
		"   `parquet-go` column projection reads only the 20 needed columns; the rest are skipped.\n" +
		"   Each row is fanned to one of 28 per-prefix gzip-JSONL writers in a single pass.\n\n")
	w("2. **Rank split** — the CC web-graph host rank table (~5 GB gzipped TSV) is downloaded " +
		"once and split into 28 per-prefix files.\n\n")
	w("3. **Shard build** — for each prefix, the rank map is loaded into memory (~300 MB), " +
		"the JSONL stream is iterated, and each row is joined with the rank entry for its host.\n" +
		"   Output is one Parquet file per prefix, compressed with ZSTD level 3.\n\n")
	w("4. **Publish** — each shard is committed to HuggingFace immediately after it is built.\n" +
		"   Commit path: `data/crawl={crawl}/subset=urls/hosts-{prefix}.parquet`.\n" +
		"   The dataset is usable before all shards complete.\n\n")
	w("Built with [`ccrawl`](https://github.com/tamnd/ccrawl-cli) v0.2.4.\n\n")

	// ── License ──────────────────────────────────────────────────────────────
	w("## License\n\n")
	w("Data is derived from [Common Crawl](https://commoncrawl.org/), " +
		"released under the [Open Data Commons Attribution License (ODC-By)]" +
		"(https://opendatacommons.org/licenses/by/1-0/).\n")
	w("You must attribute Common Crawl when using or redistributing this dataset.\n")

	return b.String()
}

func fmtCount(n int64) string {
	switch {
	case n <= 0:
		return "—"
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.2fB", float64(n)/1e9)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%.1fK", float64(n)/1e3)
	default:
		return fmt.Sprintf("%d", n)
	}
}

func fmtBytes(n int64) string {
	const (
		kb = int64(1024)
		mb = kb * 1024
		gb = mb * 1024
		tb = gb * 1024
	)
	switch {
	case n <= 0:
		return "—"
	case n >= tb:
		return fmt.Sprintf("%.1f TB", float64(n)/float64(tb))
	case n >= gb:
		return fmt.Sprintf("%.1f GB", float64(n)/float64(gb))
	case n >= mb:
		return fmt.Sprintf("%.0f MB", float64(n)/float64(mb))
	default:
		return fmt.Sprintf("%.0f KB", float64(n)/float64(kb))
	}
}

func scaleEst(total int64, committed, totalShards int) int64 {
	if committed <= 0 {
		return 0
	}
	return total * int64(totalShards) / int64(committed)
}

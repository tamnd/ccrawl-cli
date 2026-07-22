package ccrawl

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// GenerateURLsREADME renders the dataset card for open-index/ccrawl-urls. stats
// is the full ledger, one row per crawl. The layout is plain readable
// directories (data/<crawl>/part-NNNNN.parquet), so the default config globs
// every shard and a named config per crawl loads one snapshot. The card mirrors
// the depth of the open-index/arctic card: table of contents, layout tree,
// per-crawl breakdown bars, worked query examples, and a full dataset card.
func GenerateURLsREADME(repo string, stats []URLCrawlStat) string {
	rows := append([]URLCrawlStat(nil), stats...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].Crawl > rows[j].Crawl })

	var totalRows, totalBytes int64
	var totalShards int
	for _, r := range rows {
		totalRows += r.Rows
		totalBytes += r.ParquetBytes
		totalShards += r.Shards
	}
	latest := "CC-MAIN-2026-25"
	if len(rows) > 0 {
		latest = rows[0].Crawl
	}

	var b strings.Builder

	// Frontmatter. The default config reads every crawl; a named config per
	// crawl loads a single snapshot by id.
	b.WriteString("---\n")
	b.WriteString("configs:\n")
	b.WriteString("- config_name: default\n")
	b.WriteString("  data_files:\n")
	b.WriteString("  - split: train\n")
	b.WriteString("    path: \"data/**/*.parquet\"\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "- config_name: %q\n", r.Crawl)
		b.WriteString("  data_files:\n")
		b.WriteString("  - split: train\n")
		fmt.Fprintf(&b, "    path: \"data/%s/*.parquet\"\n", r.Crawl)
	}
	b.WriteString("license: odc-by\n")
	b.WriteString("task_categories:\n")
	b.WriteString("- text-retrieval\n")
	b.WriteString("- other\n")
	b.WriteString("language:\n")
	b.WriteString("- multilingual\n")
	b.WriteString("pretty_name: Common Crawl URL Index\n")
	b.WriteString("size_categories:\n")
	fmt.Fprintf(&b, "- %s\n", sizeCategory(totalRows))
	b.WriteString("tags:\n")
	b.WriteString("- common-crawl\n")
	b.WriteString("- web\n")
	b.WriteString("- url-index\n")
	b.WriteString("- crawl-frontier\n")
	b.WriteString("- parquet\n")
	b.WriteString("- open-data\n")
	b.WriteString("---\n\n")

	b.WriteString("# Common Crawl URL Index\n\n")
	b.WriteString("> Every URL Common Crawl has seen, as a slim columnar table, ready to seed a crawler frontier\n\n")

	// Table of contents.
	b.WriteString("## Table of Contents\n\n")
	b.WriteString(tocEntry(0, "What is it?", "what-is-it") + "\n")
	b.WriteString(tocEntry(0, "What is being released?", "what-is-being-released") + "\n")
	b.WriteString(tocEntry(0, "Breakdown by crawl", "breakdown-by-crawl") + "\n")
	b.WriteString(tocEntry(0, "How to download and use this dataset", "how-to-download-and-use-this-dataset") + "\n")
	b.WriteString(tocEntry(0, "Dataset statistics", "dataset-statistics") + "\n")
	b.WriteString(tocEntry(0, "Dataset card", "dataset-card-for-common-crawl-url-index") + "\n")
	b.WriteString(tocEntry(1, "Dataset summary", "dataset-summary") + "\n")
	b.WriteString(tocEntry(1, "Dataset structure", "dataset-structure") + "\n")
	b.WriteString(tocEntry(1, "Dataset creation", "dataset-creation") + "\n")
	b.WriteString(tocEntry(1, "Considerations for using the data", "considerations-for-using-the-data") + "\n")
	b.WriteString(tocEntry(0, "Additional information", "additional-information") + "\n\n")

	// What is it.
	b.WriteString("## What is it?\n\n")
	b.WriteString("This dataset is the URL-level index of [Common Crawl](https://commoncrawl.org), republished as clean Parquet. ")
	b.WriteString("Common Crawl is a non-profit that crawls the web every month and freely publishes its archives. ")
	b.WriteString("Each crawl ships a columnar URL index that lists every captured page with its host, fetch status, content type, detected language, and a pointer into the WARC archive that holds the raw response.\n\n")
	b.WriteString("We take that index as it is and republish it shard for shard, with no aggregation, deduplication, filtering, or enrichment. ")
	b.WriteString("The rows and their order match the source columnar parts one to one, so this is a faithful mirror that loads with the standard Hugging Face tools and reads directly from DuckDB.\n\n")
	if len(rows) > 0 {
		fmt.Fprintf(&b, "Right now the index holds **%s** across **%s** in **%s** of compressed Parquet, spanning **%s**. ",
			plural(len(rows), "crawl"), fmtIntRows(totalRows), humanBytes(totalBytes), fmtInt(int64(totalShards))+" shards")
		b.WriteString("New monthly crawls are added as Common Crawl releases them.\n\n")
	}
	b.WriteString("The URL index is the map of the crawl. Before you download a single WARC you can already answer questions like which hosts were captured, how many pages returned a 200, what languages and MIME types show up, and which domains dominate a crawl. ")
	b.WriteString("It is the natural starting point for building a crawl frontier, sampling the web by host or language, or fetching just the pages you care about straight out of the WARC archives.\n\n")
	b.WriteString("It is released under the **Open Data Commons Attribution License (ODC-By) v1.0**, the same license Common Crawl uses.\n\n")

	// What is being released.
	b.WriteString("## What is being released?\n\n")
	b.WriteString("One source columnar part becomes one Parquet shard, under a directory named for its crawl. ")
	b.WriteString("Common Crawl splits each crawl's URL index into 300 parts, so each crawl is 300 shards you can load one at a time or stream together.\n\n")
	b.WriteString("```\ndata/\n")
	fmt.Fprintf(&b, "  %s/\n", latest)
	b.WriteString("    part-00000.parquet\n")
	b.WriteString("    part-00001.parquet\n")
	b.WriteString("    ...\n")
	b.WriteString("    part-00299.parquet\n")
	b.WriteString("  CC-MAIN-2026-21/\n")
	b.WriteString("    part-00000.parquet\n")
	b.WriteString("    ...\n")
	b.WriteString("stats.csv                     one row per committed crawl\n")
	b.WriteString("```\n\n")
	b.WriteString("Each row is one captured URL. The `warc_filename`, `warc_record_offset`, and `warc_record_length` columns point at the exact bytes of the response in Common Crawl's WARC archives, so you can range-fetch the original page without downloading a whole WARC. ")
	b.WriteString("`stats.csv` tracks every committed crawl with its shard count, row count, Parquet size, and commit timestamps, which makes it easy to see coverage at a glance and to estimate remaining work.\n\n")

	// Breakdown bars.
	if len(rows) > 0 {
		b.WriteString("## Breakdown by crawl\n\n")
		b.WriteString("URLs per crawl, newest first.\n\n")
		b.WriteString("```\n")
		var maxRows int64
		for _, r := range rows {
			maxRows = max(maxRows, r.Rows)
		}
		for _, r := range rows {
			frac := 0.0
			if maxRows > 0 {
				frac = float64(r.Rows) / float64(maxRows)
			}
			b.WriteString(barRow(r.Crawl, frac, humanCountShort(r.Rows)) + "\n")
		}
		b.WriteString("```\n\n")
	}

	// How to download and use.
	b.WriteString("## How to download and use this dataset\n\n")
	b.WriteString("Load one crawl, filter by host or status, or stream the whole thing. ")
	b.WriteString("It is a standard Hugging Face Parquet layout, so it works with DuckDB, `datasets`, `pandas`, and `huggingface_hub` out of the box.\n\n")

	b.WriteString("### Using DuckDB\n\n")
	b.WriteString("DuckDB reads Parquet directly from Hugging Face, no download step needed.\n\n")
	fmt.Fprintf(&b, "```sql\n-- Top 20 hosts by captured pages in the latest crawl\nSELECT url_host_registered_domain AS domain, count(*) AS pages\nFROM read_parquet('hf://datasets/%s/data/%s/*.parquet')\nGROUP BY domain\nORDER BY pages DESC\nLIMIT 20;\n```\n\n", repo, latest)
	fmt.Fprintf(&b, "```sql\n-- Fetch-status distribution for one crawl\nSELECT fetch_status, count(*) AS pages\nFROM read_parquet('hf://datasets/%s/data/%s/*.parquet')\nGROUP BY fetch_status\nORDER BY pages DESC;\n```\n\n", repo, latest)
	fmt.Fprintf(&b, "```sql\n-- Language mix across the whole index\nSELECT content_languages AS lang, count(*) AS pages\nFROM read_parquet('hf://datasets/%s/data/**/*.parquet')\nWHERE content_languages IS NOT NULL\nGROUP BY lang\nORDER BY pages DESC\nLIMIT 20;\n```\n\n", repo)
	fmt.Fprintf(&b, "```sql\n-- All English HTML pages on a single host, with WARC pointers to fetch them\nSELECT url, warc_filename, warc_record_offset, warc_record_length\nFROM read_parquet('hf://datasets/%s/data/%s/*.parquet')\nWHERE url_host_registered_domain = 'wikipedia.org'\n  AND content_languages LIKE '%%eng%%'\n  AND content_mime_type = 'text/html'\n  AND fetch_status = 200;\n```\n\n", repo, latest)

	b.WriteString("### Using `datasets`\n\n")
	b.WriteString("```python\nfrom datasets import load_dataset\n\n")
	fmt.Fprintf(&b, "# Stream every crawl without downloading everything\nds = load_dataset(%q, split=\"train\", streaming=True)\n", repo)
	b.WriteString("for row in ds:\n    print(row[\"url\"], row[\"fetch_status\"])\n\n")
	fmt.Fprintf(&b, "# Load a single crawl by name\nds = load_dataset(%q, name=%q, split=\"train\", streaming=True)\n", repo, latest)
	b.WriteString("```\n\n")

	b.WriteString("### Using `huggingface_hub`\n\n")
	b.WriteString("```python\nfrom huggingface_hub import snapshot_download\n\n")
	fmt.Fprintf(&b, "# Download one crawl\nsnapshot_download(\n    %q,\n    repo_type=\"dataset\",\n    local_dir=\"./ccrawl-urls/\",\n    allow_patterns=\"data/%s/*.parquet\",\n)\n", repo, latest)
	b.WriteString("```\n\n")
	b.WriteString("For faster downloads, install `pip install huggingface_hub[hf_transfer]` and set `HF_HUB_ENABLE_HF_TRANSFER=1`.\n\n")

	b.WriteString("### Using the CLI\n\n")
	b.WriteString("```bash\n")
	fmt.Fprintf(&b, "# Download the first shard of the latest crawl\nhuggingface-cli download %s \\\n    --include \"data/%s/part-00000.parquet\" \\\n    --repo-type dataset --local-dir ./ccrawl-urls/\n", repo, latest)
	b.WriteString("```\n\n")

	// Dataset statistics.
	b.WriteString("## Dataset statistics\n\n")
	if len(rows) > 0 {
		b.WriteString("| Crawl | Shards | URLs | Parquet Size | State |\n")
		b.WriteString("|-------|-------:|-----:|-------------:|:------|\n")
		for _, r := range rows {
			state := fmt.Sprintf("%d/%d", r.Shards, r.TotalShards)
			if r.Complete {
				state = "complete"
			}
			fmt.Fprintf(&b, "| `%s` | %d | %s | %s | %s |\n",
				r.Crawl, r.Shards, fmtInt(r.Rows), humanBytes(r.ParquetBytes), state)
		}
		fmt.Fprintf(&b, "| **Total** | **%d** | **%s** | **%s** | |\n\n",
			totalShards, fmtInt(totalRows), humanBytes(totalBytes))
	} else {
		b.WriteString("The first crawl is publishing now. This table fills in as crawls commit.\n\n")
	}

	// Dataset card.
	b.WriteString("# Dataset card for Common Crawl URL Index\n\n")

	b.WriteString("## Dataset summary\n\n")
	b.WriteString("A faithful Parquet mirror of Common Crawl's columnar URL index (`cc-index-table`, `subset=warc`). ")
	b.WriteString("Each monthly crawl's index is republished shard for shard, in source order, with no aggregation or filtering. ")
	b.WriteString("People use it for:\n\n")
	b.WriteString("- **Crawl frontiers** - seed a crawler with real, recently seen URLs instead of guessing\n")
	b.WriteString("- **Web-scale sampling** - draw pages by host, TLD, language, or MIME type\n")
	b.WriteString("- **Targeted WARC fetches** - use the WARC pointers to pull just the responses you need\n")
	b.WriteString("- **Web measurement** - study host coverage, status codes, and content types across crawls\n")
	b.WriteString("- **Retrieval and dedup pipelines** - the SURT sort key and content digest are built in\n\n")

	b.WriteString("## Dataset structure\n\n")
	b.WriteString("### Data instances\n\n")
	b.WriteString("One row is one captured URL:\n\n")
	b.WriteString("```json\n{\n")
	b.WriteString("  \"url_surtkey\": \"org,wikipedia)/wiki/common_crawl\",\n")
	b.WriteString("  \"url\": \"https://en.wikipedia.org/wiki/Common_Crawl\",\n")
	b.WriteString("  \"url_host_name\": \"en.wikipedia.org\",\n")
	b.WriteString("  \"url_host_registered_domain\": \"wikipedia.org\",\n")
	b.WriteString("  \"url_host_tld\": \"org\",\n")
	b.WriteString("  \"url_protocol\": \"https\",\n")
	b.WriteString("  \"fetch_time\": \"2026-06-18T04:11:57Z\",\n")
	b.WriteString("  \"fetch_status\": 200,\n")
	b.WriteString("  \"fetch_redirect\": null,\n")
	b.WriteString("  \"content_digest\": \"3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ\",\n")
	b.WriteString("  \"content_mime_type\": \"text/html\",\n")
	b.WriteString("  \"content_mime_detected\": \"text/html\",\n")
	b.WriteString("  \"content_charset\": \"UTF-8\",\n")
	b.WriteString("  \"content_languages\": \"eng\",\n")
	b.WriteString("  \"content_truncated\": null,\n")
	b.WriteString("  \"warc_filename\": \"crawl-data/CC-MAIN-2026-25/segments/.../warc/CC-MAIN-...warc.gz\",\n")
	b.WriteString("  \"warc_record_offset\": 812634789,\n")
	b.WriteString("  \"warc_record_length\": 24518\n")
	b.WriteString("}\n```\n\n")

	b.WriteString("### Data fields\n\n")
	b.WriteString("Columns are in source order. Types are the Parquet types written by the pipeline.\n\n")
	b.WriteString("| Column | Type | Description |\n")
	b.WriteString("|--------|------|-------------|\n")
	for _, c := range urlColumnDocs {
		fmt.Fprintf(&b, "| `%s` | %s | %s |\n", c[0], c[1], c[2])
	}
	b.WriteString("\n")

	b.WriteString("### Data splits\n\n")
	b.WriteString("One named config per crawl, plus a `default` config that globs every crawl. Each loads its shards as a single `train` split.\n\n")
	b.WriteString("```python\n")
	fmt.Fprintf(&b, "# One crawl by config name\nds = load_dataset(%q, name=%q, split=\"train\")\n\n", repo, latest)
	fmt.Fprintf(&b, "# A specific crawl by path\nds = load_dataset(%q, data_files=\"data/%s/*.parquet\", split=\"train\")\n", repo, latest)
	b.WriteString("```\n\n")

	b.WriteString("## Dataset creation\n\n")
	b.WriteString("### Why we built this\n\n")
	b.WriteString("Common Crawl's columnar index is one of the most useful public datasets on the web, but the official copy lives behind a Hive-partitioned S3 layout that is awkward to browse and needs AWS tooling to query. ")
	b.WriteString("We republish it in a plain, readable Hugging Face layout so you can point DuckDB or `datasets` straight at it, load a single crawl by name, and stream without any special setup.\n\n")
	b.WriteString("### Source data\n\n")
	b.WriteString("Everything comes from Common Crawl's cc-index columnar table, the `subset=warc` partition of each monthly crawl. ")
	b.WriteString("Source format is Hive-partitioned Parquet on S3 and its HTTPS mirror, enumerated from each crawl's `cc-index-table.paths.gz` manifest.\n\n")
	b.WriteString("### Processing steps\n\n")
	b.WriteString("The pipeline is written in Go. For each crawl:\n\n")
	b.WriteString("1. **Enumerate** the crawl's columnar parts from `cc-index-table.paths.gz`\n")
	b.WriteString("2. **Skip** parts already committed, checked against the hub so a restart resumes cleanly\n")
	b.WriteString("3. **Stream** each source part with ranged HTTP reads, projecting the published columns\n")
	b.WriteString("4. **Write** one Zstandard-compressed Parquet shard per part, preserving source row order\n")
	b.WriteString("5. **Commit** finished shards in batches to Hugging Face, with `stats.csv` and this card\n")
	b.WriteString("6. **Delete** each local shard right after its commit lands, so disk stays flat\n\n")
	b.WriteString("The pipeline picks up where it left off: `stats.csv` and the shards already on the hub are the resume signal, and committed parts are skipped on restart. ")
	b.WriteString("No filtering, deduplication, or content changes. The rows match Common Crawl's index exactly, shard for shard. All Parquet files use Zstandard compression.\n\n")
	b.WriteString("### Personal and sensitive information\n\n")
	b.WriteString("The index describes public web pages: their URLs, hosts, and capture metadata. It does not contain page bodies. ")
	b.WriteString("URLs can still carry personal information that site owners put in the open, and no scrubbing has been done. Treat URLs as public but potentially sensitive strings.\n\n")

	b.WriteString("## Considerations for using the data\n\n")
	b.WriteString("### Social impact\n\n")
	b.WriteString("A readable, queryable URL index lowers the bar for web research, retrieval, and crawler building. ")
	b.WriteString("Work that once needed an AWS account and Athena now runs from a laptop with DuckDB.\n\n")
	b.WriteString("### Biases\n\n")
	b.WriteString("Common Crawl is a sample of the web, not the whole web. Its seed lists, crawl budget, and politeness rules shape what gets captured, and popular hosts are covered more densely than the long tail. ")
	b.WriteString("The index inherits every one of those biases. We did not correct for them.\n\n")
	b.WriteString("### Known limitations\n\n")
	b.WriteString("- **Snapshot, not history.** Each crawl is a point-in-time capture; a URL missing from one crawl may appear in another.\n")
	b.WriteString("- **Fetch status varies.** Not every row is a 200. Redirects, 404s, and other statuses are all present, as captured.\n")
	b.WriteString("- **Detected fields are heuristics.** `content_languages` and `content_mime_detected` come from Common Crawl's detectors and can be wrong.\n")
	b.WriteString("- **WARC pointers are crawl-specific.** A `warc_filename` only resolves within its own crawl's archives.\n\n")

	b.WriteString("## Additional information\n\n")
	b.WriteString("### Licensing\n\n")
	b.WriteString("Released under the [Open Data Commons Attribution License (ODC-By) v1.0](https://opendatacommons.org/licenses/by/1-0/), the same terms Common Crawl publishes under. ")
	b.WriteString("Please credit [Common Crawl](https://commoncrawl.org) when you use this data.\n\n")
	b.WriteString("Not affiliated with or endorsed by Common Crawl.\n\n")
	b.WriteString("### Thanks\n\n")
	b.WriteString("All the data here comes from [Common Crawl](https://commoncrawl.org), which crawls the web and gives the archives away for free. None of this would exist without their work.\n\n")
	b.WriteString("### Contact\n\n")
	fmt.Fprintf(&b, "Questions, feedback, or issues, open a discussion on the [Community tab](https://huggingface.co/datasets/%s/discussions).\n\n", repo)

	fmt.Fprintf(&b, "*Last updated: %s*\n", time.Now().UTC().Format("2006-01-02 15:04 UTC"))

	return b.String()
}

// fmtIntRows renders a row count as "N URLs" with grouped digits.
func fmtIntRows(n int64) string {
	return fmtInt(n) + " URLs"
}

// urlColumnDocs documents the output schema in source order.
var urlColumnDocs = [][3]string{
	{"url_surtkey", "VARCHAR", "SURT-canonical sort key for the URL, host reversed and path normalized"},
	{"url", "VARCHAR", "the captured URL"},
	{"url_host_name", "VARCHAR", "full host name"},
	{"url_host_registered_domain", "VARCHAR", "registrable domain, one level below the public suffix"},
	{"url_host_tld", "VARCHAR", "top-level domain"},
	{"url_protocol", "VARCHAR", "scheme, http or https"},
	{"fetch_time", "TIMESTAMP", "when the page was fetched, UTC"},
	{"fetch_status", "INTEGER", "HTTP status code of the capture"},
	{"fetch_redirect", "VARCHAR", "redirect target when the capture was a redirect, else null"},
	{"content_digest", "VARCHAR", "content hash of the response body, for dedup"},
	{"content_mime_type", "VARCHAR", "MIME type reported by the server"},
	{"content_mime_detected", "VARCHAR", "MIME type detected by Common Crawl"},
	{"content_charset", "VARCHAR", "character set of the response"},
	{"content_languages", "VARCHAR", "detected language codes, comma separated"},
	{"content_truncated", "VARCHAR", "reason the capture was truncated, if any"},
	{"warc_filename", "VARCHAR", "path of the WARC file holding the response"},
	{"warc_record_offset", "INTEGER", "byte offset of the record in the WARC file"},
	{"warc_record_length", "INTEGER", "byte length of the record"},
}

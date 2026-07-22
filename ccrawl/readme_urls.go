package ccrawl

import (
	"fmt"
	"sort"
	"strings"
)

// GenerateURLsREADME renders the dataset card for open-index/ccrawl-urls. stats
// is the full ledger, one row per crawl. The layout is plain readable
// directories (data/<crawl>/part-NNNNN.parquet), so the default config globs
// every shard and a named config per crawl loads one snapshot.
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

	b.WriteString("# **Common Crawl URL Index**\n\n")
	b.WriteString("> Every URL Common Crawl has seen, as a slim columnar table, ready to seed a crawler frontier\n\n")

	b.WriteString("## What is it?\n\n")
	b.WriteString("This dataset is the URL-level index of [Common Crawl](https://commoncrawl.org), republished as clean Parquet. ")
	b.WriteString("Common Crawl is a non-profit that crawls the web and freely publishes its archives. ")
	b.WriteString("Each crawl ships a columnar URL index that lists every captured page with its host, fetch status, content type, language, and a pointer into the WARC archive that holds the response.\n\n")
	b.WriteString("We take that index as it is and republish it shard for shard, with no aggregation, dedication, or filtering. ")
	b.WriteString("The rows and their order match the source, so this is a faithful mirror that is easy to load with the Hugging Face tools.\n\n")

	if len(rows) > 0 {
		fmt.Fprintf(&b, "It currently holds **%s crawls**, **%s URLs** across **%s shards** (%s of Parquet).\n\n",
			fmtInt(int64(len(rows))), fmtInt(totalRows), fmtInt(int64(totalShards)), humanBytes(totalBytes))
	}
	b.WriteString("It is released under the **Open Data Commons Attribution License (ODC-By) v1.0**, the same license Common Crawl uses.\n\n")

	b.WriteString("## Layout\n\n")
	b.WriteString("One source columnar part becomes one Parquet shard, under a directory named for its crawl:\n\n")
	b.WriteString("```\ndata/\n  CC-MAIN-2026-25/\n    part-00000.parquet\n    part-00001.parquet\n    ...\n```\n\n")
	b.WriteString("Each row is one captured URL. The `warc_filename`, `warc_record_offset`, and `warc_record_length` columns point at the exact bytes of the response in Common Crawl's WARC archives, so you can fetch the original page.\n\n")

	b.WriteString("## Columns\n\n")
	b.WriteString("| column | type | meaning |\n")
	b.WriteString("|---|---|---|\n")
	for _, c := range urlColumnDocs {
		fmt.Fprintf(&b, "| `%s` | %s | %s |\n", c[0], c[1], c[2])
	}
	b.WriteString("\n")

	b.WriteString("## How to use it\n\n")
	b.WriteString("### Using `datasets`\n\n")
	b.WriteString("```python\nfrom datasets import load_dataset\n\n")
	fmt.Fprintf(&b, "# stream every crawl\nds = load_dataset(%q, split=\"train\", streaming=True)\n", repo)
	b.WriteString("for row in ds:\n    print(row[\"url\"], row[\"fetch_status\"])\n\n")
	if len(rows) > 0 {
		fmt.Fprintf(&b, "# load a single crawl by name\nds = load_dataset(%q, name=%q, split=\"train\", streaming=True)\n", repo, rows[0].Crawl)
	}
	b.WriteString("```\n\n")
	b.WriteString("### Using `huggingface_hub`\n\n")
	b.WriteString("```python\nfrom huggingface_hub import snapshot_download\n\n")
	fmt.Fprintf(&b, "snapshot_download(%q, repo_type=\"dataset\", allow_patterns=\"data/CC-MAIN-2026-25/*.parquet\")\n", repo)
	b.WriteString("```\n\n")

	if len(rows) > 0 {
		b.WriteString("## Coverage\n\n")
		b.WriteString("| crawl | shards | URLs | size | state |\n")
		b.WriteString("|---|---|---|---|---|\n")
		for _, r := range rows {
			state := fmt.Sprintf("%d/%d", r.Shards, r.TotalShards)
			if r.Complete {
				state = "complete"
			}
			fmt.Fprintf(&b, "| `%s` | %d | %s | %s | %s |\n",
				r.Crawl, r.Shards, fmtInt(r.Rows), humanBytes(r.ParquetBytes), state)
		}
		b.WriteString("\n")
	}

	b.WriteString("## Source and attribution\n\n")
	b.WriteString("Built from Common Crawl's columnar URL index (`cc-index-table`, `subset=warc`). ")
	b.WriteString("Please credit [Common Crawl](https://commoncrawl.org) when you use this data.\n")

	return b.String()
}

// urlColumnDocs documents the output schema in source order.
var urlColumnDocs = [][3]string{
	{"url_surtkey", "string", "SURT-canonical sort key for the URL"},
	{"url", "string", "the captured URL"},
	{"url_host_name", "string", "host name"},
	{"url_host_registered_domain", "string", "registrable domain"},
	{"url_host_tld", "string", "top-level domain"},
	{"url_protocol", "string", "scheme, http or https"},
	{"fetch_time", "timestamp(us)", "when the page was fetched, UTC"},
	{"fetch_status", "int32", "HTTP status code of the capture"},
	{"fetch_redirect", "string", "redirect target when the capture was a redirect"},
	{"content_digest", "string", "content hash of the response body"},
	{"content_mime_type", "string", "MIME type from the server"},
	{"content_mime_detected", "string", "MIME type detected by Common Crawl"},
	{"content_charset", "string", "character set"},
	{"content_languages", "string", "detected language codes"},
	{"content_truncated", "string", "reason the capture was truncated, if any"},
	{"warc_filename", "string", "path of the WARC file holding the response"},
	{"warc_record_offset", "int32", "byte offset of the record in the WARC file"},
	{"warc_record_length", "int32", "byte length of the record"},
}

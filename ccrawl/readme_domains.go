package ccrawl

import (
	"fmt"
	"sort"
	"strings"
)

// GenerateDomainsREADME renders the dataset card for open-index/ccrawl-domains.
// stats is the full ledger, one row per web-graph release. Shards keep the
// source's harmonic-centrality order, so part-000 holds the top-ranked domains.
func GenerateDomainsREADME(repo string, stats []DomainGraphStat) string {
	rows := append([]DomainGraphStat(nil), stats...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].Graph > rows[j].Graph })

	var totalDomains, totalBytes int64
	var totalShards int
	for _, r := range rows {
		totalDomains += r.Domains
		totalBytes += r.ParquetBytes
		totalShards += r.Shards
	}

	var b strings.Builder

	b.WriteString("---\n")
	b.WriteString("configs:\n")
	b.WriteString("- config_name: default\n")
	b.WriteString("  data_files:\n")
	b.WriteString("  - split: train\n")
	b.WriteString("    path: \"data/**/*.parquet\"\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "- config_name: %q\n", r.Graph)
		b.WriteString("  data_files:\n")
		b.WriteString("  - split: train\n")
		fmt.Fprintf(&b, "    path: \"data/%s/*.parquet\"\n", r.Graph)
	}
	b.WriteString("license: odc-by\n")
	b.WriteString("task_categories:\n")
	b.WriteString("- graph-ml\n")
	b.WriteString("- other\n")
	b.WriteString("pretty_name: Common Crawl Domain Ranks\n")
	b.WriteString("size_categories:\n")
	fmt.Fprintf(&b, "- %s\n", sizeCategory(totalDomains))
	b.WriteString("tags:\n")
	b.WriteString("- common-crawl\n")
	b.WriteString("- web-graph\n")
	b.WriteString("- domain-ranks\n")
	b.WriteString("- harmonic-centrality\n")
	b.WriteString("- pagerank\n")
	b.WriteString("- parquet\n")
	b.WriteString("- open-data\n")
	b.WriteString("---\n\n")

	b.WriteString("# **Common Crawl Domain Ranks**\n\n")
	b.WriteString("> Web domains ranked by harmonic centrality and PageRank, ready to prioritize a crawl\n\n")

	b.WriteString("## What is it?\n\n")
	b.WriteString("This dataset is the domain-level ranking from [Common Crawl](https://commoncrawl.org)'s hyperlink web graph, republished as clean Parquet. ")
	b.WriteString("Common Crawl builds a graph of which domains link to which, then scores every domain by harmonic centrality and PageRank. ")
	b.WriteString("A high rank means many other well-connected domains link to it, which is a good proxy for importance when you decide what to crawl first.\n\n")
	b.WriteString("We take the ranks as they are and republish them with no changes to the numbers. ")
	b.WriteString("The one edit is convenience: the source keys each row by a reversed host string (`com.example`), and we un-reverse it into a plain domain (`example.com`). ")
	b.WriteString("The rows stay in the source's rank order, so `part-000` holds the highest-centrality domains.\n\n")

	if len(rows) > 0 {
		fmt.Fprintf(&b, "It currently holds **%s releases**, **%s domains** across **%s shards** (%s of Parquet).\n\n",
			fmtInt(int64(len(rows))), fmtInt(totalDomains), fmtInt(int64(totalShards)), humanBytes(totalBytes))
	}
	b.WriteString("It is released under the **Open Data Commons Attribution License (ODC-By) v1.0**, the same license Common Crawl uses.\n\n")

	b.WriteString("## Layout\n\n")
	b.WriteString("Each web-graph release is one directory of rank-ordered shards:\n\n")
	b.WriteString("```\ndata/\n  cc-main-2026-mar-apr-may/\n    part-000.parquet\n    part-001.parquet\n    ...\n```\n\n")
	b.WriteString("Read `part-000` first for the most important domains. Each shard holds a fixed number of rows, so rank position is just shard index times shard size plus the row offset.\n\n")

	b.WriteString("## Columns\n\n")
	b.WriteString("| column | type | meaning |\n")
	b.WriteString("|---|---|---|\n")
	for _, c := range domainColumnDocs {
		fmt.Fprintf(&b, "| `%s` | %s | %s |\n", c[0], c[1], c[2])
	}
	b.WriteString("\n")

	b.WriteString("## How to use it\n\n")
	b.WriteString("### Using `datasets`\n\n")
	b.WriteString("```python\nfrom datasets import load_dataset\n\n")
	fmt.Fprintf(&b, "ds = load_dataset(%q, split=\"train\", streaming=True)\n", repo)
	b.WriteString("for row in ds:\n    print(row[\"domain\"], row[\"harmonic_pos\"])\n```\n\n")
	b.WriteString("### Using `huggingface_hub`\n\n")
	b.WriteString("```python\nfrom huggingface_hub import snapshot_download\n\n")
	fmt.Fprintf(&b, "snapshot_download(%q, repo_type=\"dataset\")\n", repo)
	b.WriteString("```\n\n")

	if len(rows) > 0 {
		b.WriteString("## Coverage\n\n")
		b.WriteString("| release | shards | domains | size |\n")
		b.WriteString("|---|---|---|---|\n")
		for _, r := range rows {
			fmt.Fprintf(&b, "| `%s` | %d | %s | %s |\n",
				r.Graph, r.Shards, fmtInt(r.Domains), humanBytes(r.ParquetBytes))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Source and attribution\n\n")
	b.WriteString("Built from Common Crawl's [hyperlink web graph](https://commoncrawl.org/web-graphs), domain-level rank tables. ")
	b.WriteString("Please credit [Common Crawl](https://commoncrawl.org) when you use this data.\n")

	return b.String()
}

// domainColumnDocs documents the output schema in source order.
var domainColumnDocs = [][3]string{
	{"domain", "string", "registrable domain, un-reversed from the source key"},
	{"harmonic_pos", "int64", "rank position by harmonic centrality, 1 is highest"},
	{"harmonic_val", "float64", "harmonic centrality score"},
	{"pagerank_pos", "int64", "rank position by PageRank, 1 is highest"},
	{"pagerank_val", "float64", "PageRank score"},
	{"n_hosts", "int64", "number of hosts aggregated into this domain"},
}

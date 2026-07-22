package ccrawl

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// GenerateDomainsREADME renders the dataset card for open-index/ccrawl-domains.
// stats is the full ledger, one row per web-graph release. Shards keep the
// source's harmonic-centrality order, so part-000 holds the top-ranked domains.
// The card mirrors the depth of the open-index/arctic card.
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
	latest := "cc-main-2026-mar-apr-may"
	if len(rows) > 0 {
		latest = rows[0].Graph
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

	b.WriteString("# Common Crawl Domain Ranks\n\n")
	b.WriteString("> Web domains ranked by harmonic centrality and PageRank, ready to prioritize a crawl\n\n")

	// Table of contents.
	b.WriteString("## Table of Contents\n\n")
	b.WriteString(tocEntry(0, "What is it?", "what-is-it") + "\n")
	b.WriteString(tocEntry(0, "What is being released?", "what-is-being-released") + "\n")
	b.WriteString(tocEntry(0, "Breakdown by release", "breakdown-by-release") + "\n")
	b.WriteString(tocEntry(0, "How to download and use this dataset", "how-to-download-and-use-this-dataset") + "\n")
	b.WriteString(tocEntry(0, "Dataset statistics", "dataset-statistics") + "\n")
	b.WriteString(tocEntry(0, "Dataset card", "dataset-card-for-common-crawl-domain-ranks") + "\n")
	b.WriteString(tocEntry(1, "Dataset summary", "dataset-summary") + "\n")
	b.WriteString(tocEntry(1, "Dataset structure", "dataset-structure") + "\n")
	b.WriteString(tocEntry(1, "Dataset creation", "dataset-creation") + "\n")
	b.WriteString(tocEntry(1, "Considerations for using the data", "considerations-for-using-the-data") + "\n")
	b.WriteString(tocEntry(0, "Additional information", "additional-information") + "\n\n")

	// What is it.
	b.WriteString("## What is it?\n\n")
	b.WriteString("This dataset is the domain-level ranking from [Common Crawl](https://commoncrawl.org)'s hyperlink web graph, republished as clean Parquet. ")
	b.WriteString("Common Crawl builds a graph of which domains link to which, then scores every domain by harmonic centrality and PageRank. ")
	b.WriteString("A high rank means many other well-connected domains link to it, which is a solid proxy for importance when you decide what to crawl or trust first.\n\n")
	b.WriteString("We take the ranks as they are and republish them with no changes to the numbers. ")
	b.WriteString("The one edit is convenience: the source keys each row by a reversed host string (`com.example`), and we un-reverse it into a plain domain (`example.com`). ")
	b.WriteString("The rows stay in the source's rank order, so `part-000` holds the highest-centrality domains and rank falls as the part number rises.\n\n")
	if len(rows) > 0 {
		fmt.Fprintf(&b, "Right now it holds **%s** across **%s** in **%s** of compressed Parquet, cut into **%s**. ",
			plural(len(rows), "release"), fmtInt(totalDomains)+" domains", humanBytes(totalBytes), fmtInt(int64(totalShards))+" shards")
		b.WriteString("New quarterly releases are added as Common Crawl publishes them.\n\n")
	}
	b.WriteString("Harmonic centrality and PageRank are two ways to answer the same question: how central is a domain in the web's link graph. ")
	b.WriteString("Because the file is pre-sorted by harmonic centrality, reading from the top gives you the most important domains first, which is exactly what you want when seeding a crawl, building an allow-list, or picking a high-signal sample of the web.\n\n")
	b.WriteString("It is released under the **Open Data Commons Attribution License (ODC-By) v1.0**, the same license Common Crawl uses.\n\n")

	// What is being released.
	b.WriteString("## What is being released?\n\n")
	b.WriteString("Each web-graph release is one directory of rank-ordered shards. Each shard holds a fixed number of rows, so a domain's rank position is just shard index times shard size plus its row offset.\n\n")
	b.WriteString("```\ndata/\n")
	fmt.Fprintf(&b, "  %s/\n", latest)
	b.WriteString("    part-000.parquet          highest-centrality domains\n")
	b.WriteString("    part-001.parquet\n")
	b.WriteString("    ...\n")
	b.WriteString("stats.csv                     one row per committed release\n")
	b.WriteString("```\n\n")
	b.WriteString("Read `part-000` first for the most important domains. ")
	b.WriteString("`stats.csv` tracks every committed release with its shard count, domain count, Parquet size, source size, and shard-row size, so coverage and remaining work are easy to read off.\n\n")

	// Breakdown bars.
	if len(rows) > 0 {
		b.WriteString("## Breakdown by release\n\n")
		b.WriteString("Domains per release, newest first.\n\n")
		b.WriteString("```\n")
		var maxDomains int64
		for _, r := range rows {
			maxDomains = max(maxDomains, r.Domains)
		}
		for _, r := range rows {
			frac := 0.0
			if maxDomains > 0 {
				frac = float64(r.Domains) / float64(maxDomains)
			}
			b.WriteString(barRow(r.Graph, frac, humanCountShort(r.Domains)) + "\n")
		}
		b.WriteString("```\n\n")
	}

	// How to download and use.
	b.WriteString("## How to download and use this dataset\n\n")
	b.WriteString("Read the top of a release for the most important domains, or stream the whole ranking. ")
	b.WriteString("It is a standard Hugging Face Parquet layout, so it works with DuckDB, `datasets`, `pandas`, and `huggingface_hub` out of the box.\n\n")

	b.WriteString("### Using DuckDB\n\n")
	b.WriteString("DuckDB reads Parquet directly from Hugging Face, no download step needed.\n\n")
	fmt.Fprintf(&b, "```sql\n-- Top 50 domains by harmonic centrality\nSELECT domain, harmonic_pos, harmonic_val\nFROM read_parquet('hf://datasets/%s/data/%s/*.parquet')\nORDER BY harmonic_pos\nLIMIT 50;\n```\n\n", repo, latest)
	fmt.Fprintf(&b, "```sql\n-- Where does one domain rank?\nSELECT domain, harmonic_pos, pagerank_pos\nFROM read_parquet('hf://datasets/%s/data/%s/*.parquet')\nWHERE domain = 'wikipedia.org';\n```\n\n", repo, latest)
	fmt.Fprintf(&b, "```sql\n-- Most central .org domains\nSELECT domain, harmonic_pos\nFROM read_parquet('hf://datasets/%s/data/%s/*.parquet')\nWHERE domain LIKE '%%.org'\nORDER BY harmonic_pos\nLIMIT 20;\n```\n\n", repo, latest)
	fmt.Fprintf(&b, "```sql\n-- Domains where PageRank and harmonic centrality disagree most\nSELECT domain, harmonic_pos, pagerank_pos,\n       abs(harmonic_pos - pagerank_pos) AS gap\nFROM read_parquet('hf://datasets/%s/data/%s/*.parquet')\nORDER BY gap DESC\nLIMIT 20;\n```\n\n", repo, latest)

	b.WriteString("### Using `datasets`\n\n")
	b.WriteString("```python\nfrom datasets import load_dataset\n\n")
	fmt.Fprintf(&b, "# Stream the ranking, most important domains first\nds = load_dataset(%q, split=\"train\", streaming=True)\n", repo)
	b.WriteString("for row in ds:\n    print(row[\"harmonic_pos\"], row[\"domain\"])\n\n")
	fmt.Fprintf(&b, "# Load one release by name\nds = load_dataset(%q, name=%q, split=\"train\", streaming=True)\n", repo, latest)
	b.WriteString("```\n\n")

	b.WriteString("### Using `huggingface_hub`\n\n")
	b.WriteString("```python\nfrom huggingface_hub import snapshot_download\n\n")
	fmt.Fprintf(&b, "# Download one release\nsnapshot_download(\n    %q,\n    repo_type=\"dataset\",\n    local_dir=\"./ccrawl-domains/\",\n    allow_patterns=\"data/%s/*.parquet\",\n)\n", repo, latest)
	b.WriteString("```\n\n")
	b.WriteString("For faster downloads, install `pip install huggingface_hub[hf_transfer]` and set `HF_HUB_ENABLE_HF_TRANSFER=1`.\n\n")

	b.WriteString("### Using the CLI\n\n")
	b.WriteString("```bash\n")
	fmt.Fprintf(&b, "# Download just the top shard of the latest release\nhuggingface-cli download %s \\\n    --include \"data/%s/part-000.parquet\" \\\n    --repo-type dataset --local-dir ./ccrawl-domains/\n", repo, latest)
	b.WriteString("```\n\n")

	// Dataset statistics.
	b.WriteString("## Dataset statistics\n\n")
	if len(rows) > 0 {
		b.WriteString("| Release | Shards | Domains | Parquet Size | Source Size |\n")
		b.WriteString("|---------|-------:|--------:|-------------:|------------:|\n")
		for _, r := range rows {
			fmt.Fprintf(&b, "| `%s` | %d | %s | %s | %s |\n",
				r.Graph, r.Shards, fmtInt(r.Domains), humanBytes(r.ParquetBytes), humanBytes(r.SourceBytes))
		}
		fmt.Fprintf(&b, "| **Total** | **%d** | **%s** | **%s** | |\n\n",
			totalShards, fmtInt(totalDomains), humanBytes(totalBytes))
	} else {
		b.WriteString("The first release is publishing now. This table fills in as releases commit.\n\n")
	}

	// Dataset card.
	b.WriteString("# Dataset card for Common Crawl Domain Ranks\n\n")

	b.WriteString("## Dataset summary\n\n")
	b.WriteString("A faithful Parquet mirror of Common Crawl's domain-level web-graph ranks. ")
	b.WriteString("Each quarterly release ranks every domain in the crawl by harmonic centrality and PageRank, and we republish that ranking in source order, shard for shard. ")
	b.WriteString("People use it for:\n\n")
	b.WriteString("- **Crawl prioritization** - start from the most central domains and work down\n")
	b.WriteString("- **Allow-lists and seed lists** - a ranked, license-clean list of real domains\n")
	b.WriteString("- **Web-graph research** - study centrality, PageRank, and how the two disagree\n")
	b.WriteString("- **Sampling** - take a high-signal slice of the web by rank threshold\n")
	b.WriteString("- **Reputation features** - centrality as a cheap prior for domain trust\n\n")

	b.WriteString("## Dataset structure\n\n")
	b.WriteString("### Data instances\n\n")
	b.WriteString("One row is one domain and its ranks:\n\n")
	b.WriteString("```json\n{\n")
	b.WriteString("  \"domain\": \"wikipedia.org\",\n")
	b.WriteString("  \"harmonic_pos\": 1,\n")
	b.WriteString("  \"harmonic_val\": 29491890.0,\n")
	b.WriteString("  \"pagerank_pos\": 3,\n")
	b.WriteString("  \"pagerank_val\": 0.0024193,\n")
	b.WriteString("  \"n_hosts\": 4821\n")
	b.WriteString("}\n```\n\n")

	b.WriteString("### Data fields\n\n")
	b.WriteString("| Column | Type | Description |\n")
	b.WriteString("|--------|------|-------------|\n")
	for _, c := range domainColumnDocs {
		fmt.Fprintf(&b, "| `%s` | %s | %s |\n", c[0], c[1], c[2])
	}
	b.WriteString("\n")

	b.WriteString("### Data splits\n\n")
	b.WriteString("One named config per release, plus a `default` config that globs every release. Each loads its shards as a single `train` split, in rank order.\n\n")
	b.WriteString("```python\n")
	fmt.Fprintf(&b, "# One release by config name\nds = load_dataset(%q, name=%q, split=\"train\")\n\n", repo, latest)
	fmt.Fprintf(&b, "# A specific release by path\nds = load_dataset(%q, data_files=\"data/%s/*.parquet\", split=\"train\")\n", repo, latest)
	b.WriteString("```\n\n")

	b.WriteString("## Dataset creation\n\n")
	b.WriteString("### Why we built this\n\n")
	b.WriteString("Common Crawl publishes the domain ranks as a single large gzipped TSV per release, keyed by a reversed host string. ")
	b.WriteString("That is fine for a one-off download but awkward to query and to load a slice of. ")
	b.WriteString("We republish it as rank-ordered Parquet with un-reversed domains so you can read the top of the ranking directly, query it from DuckDB, or stream it with `datasets`, without downloading the whole file first.\n\n")
	b.WriteString("### Source data\n\n")
	b.WriteString("Everything comes from Common Crawl's [hyperlink web graph](https://commoncrawl.org/web-graphs), the domain-level rank tables. ")
	b.WriteString("Source format is a single gzip-compressed, tab-separated file per release, pre-sorted by harmonic centrality, with columns for harmonic position and value, PageRank position and value, the reversed host, and the host count.\n\n")
	b.WriteString("### Processing steps\n\n")
	b.WriteString("The pipeline is written in Go. For each release:\n\n")
	b.WriteString("1. **Stream** the gzipped ranks TSV top to bottom, never buffering the whole file\n")
	b.WriteString("2. **Parse** each row, un-reversing the host key (`com.example` becomes `example.com`)\n")
	b.WriteString("3. **Cut** a new Zstandard-compressed Parquet shard every fixed number of rows, keeping rank order exact\n")
	b.WriteString("4. **Skip** shards already on the hub while still reading through the stream, so ordering never drifts\n")
	b.WriteString("5. **Commit** finished shards in batches to Hugging Face, with `stats.csv` and this card\n")
	b.WriteString("6. **Delete** each local shard right after its commit lands, so disk stays flat\n\n")
	b.WriteString("The only change to the data is un-reversing the host string into a plain domain. ")
	b.WriteString("The rank numbers, the row order, and the set of domains match the source release exactly. All Parquet files use Zstandard compression.\n\n")
	b.WriteString("### Personal and sensitive information\n\n")
	b.WriteString("The data is domain names and their ranks. It contains no personal data beyond what a domain name itself reveals.\n\n")

	b.WriteString("## Considerations for using the data\n\n")
	b.WriteString("### Social impact\n\n")
	b.WriteString("A readable, ranked list of domains makes it easy to prioritize crawling, build seed lists, and study the shape of the web's link graph without heavy tooling.\n\n")
	b.WriteString("### Biases\n\n")
	b.WriteString("Centrality reflects the link structure Common Crawl observed, which reflects what it crawled. ")
	b.WriteString("Well-linked, long-established, English-language, and commercial domains tend to rank higher, and the ranking amplifies existing prominence. ")
	b.WriteString("A high rank means well connected, not trustworthy or high quality. We did not correct for any of this.\n\n")
	b.WriteString("### Known limitations\n\n")
	b.WriteString("- **Domain level, not host level.** Ranks are aggregated to the registrable domain; `n_hosts` says how many hosts fed into each one.\n")
	b.WriteString("- **Snapshot per release.** Each quarterly release is a point-in-time view; ranks shift between releases.\n")
	b.WriteString("- **Two metrics can disagree.** Harmonic centrality and PageRank measure related but different things; a domain can rank very differently under each.\n")
	b.WriteString("- **Coverage follows the crawl.** Domains Common Crawl did not reach are not in the graph.\n\n")

	b.WriteString("## Additional information\n\n")
	b.WriteString("### Licensing\n\n")
	b.WriteString("Released under the [Open Data Commons Attribution License (ODC-By) v1.0](https://opendatacommons.org/licenses/by/1-0/), the same terms Common Crawl publishes under. ")
	b.WriteString("Please credit [Common Crawl](https://commoncrawl.org) when you use this data.\n\n")
	b.WriteString("Not affiliated with or endorsed by Common Crawl.\n\n")
	b.WriteString("### Thanks\n\n")
	b.WriteString("All the data here comes from [Common Crawl](https://commoncrawl.org), which builds the web graph and gives it away for free. None of this would exist without their work.\n\n")
	b.WriteString("### Contact\n\n")
	fmt.Fprintf(&b, "Questions, feedback, or issues, open a discussion on the [Community tab](https://huggingface.co/datasets/%s/discussions).\n\n", repo)

	fmt.Fprintf(&b, "*Last updated: %s*\n", time.Now().UTC().Format("2006-01-02 15:04 UTC"))

	return b.String()
}

// domainColumnDocs documents the output schema in source order.
var domainColumnDocs = [][3]string{
	{"domain", "VARCHAR", "registrable domain, un-reversed from the source host key"},
	{"harmonic_pos", "BIGINT", "rank position by harmonic centrality, 1 is highest"},
	{"harmonic_val", "DOUBLE", "harmonic centrality score"},
	{"pagerank_pos", "BIGINT", "rank position by PageRank, 1 is highest"},
	{"pagerank_val", "DOUBLE", "PageRank score"},
	{"n_hosts", "BIGINT", "number of hosts aggregated into this domain"},
}

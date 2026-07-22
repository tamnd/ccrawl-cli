package ccrawl

import (
	"embed"
	"sort"
	"strings"
	"text/template"
	"time"
)

// cardTemplates holds the two dataset cards as Markdown templates. They live in
// their own files so the prose reads and edits like a normal Markdown document,
// and the pipeline fills in the dynamic parts (totals, breakdown bars, coverage
// table, timestamps) at publish time.
//
//go:embed templates/urls_card.md templates/domains_card.md
var cardTemplates embed.FS

var cards = template.Must(template.ParseFS(cardTemplates, "templates/*.md"))

// barWidth is the fixed width of the breakdown bars in both cards.
const barWidth = 20

// GenerateURLsREADME renders the dataset card for open-index/ccrawl-urls from
// templates/urls_card.md. stats is the full ledger, one row per crawl, newest
// first. The layout is plain readable directories (data/<crawl>/part-NNNNN),
// so the default config globs every shard and a named config per crawl loads
// one snapshot.
func GenerateURLsREADME(repo string, stats []URLCrawlStat) string {
	rows := append([]URLCrawlStat(nil), stats...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].Crawl > rows[j].Crawl })

	var totalRows, totalBytes int64
	var totalShards int
	var maxRows int64
	for _, r := range rows {
		totalRows += r.Rows
		totalBytes += r.ParquetBytes
		totalShards += r.Shards
		maxRows = max(maxRows, r.Rows)
	}

	latest := "CC-MAIN-2026-25"
	configs := make([]string, len(rows))
	bars := make([]string, len(rows))
	table := make([]urlTableRow, len(rows))
	for i, r := range rows {
		configs[i] = r.Crawl
		frac := 0.0
		if maxRows > 0 {
			frac = float64(r.Rows) / float64(maxRows)
		}
		bars[i] = barRow(r.Crawl, frac, humanCountShort(r.Rows))
		state := fmtInt(int64(r.Shards)) + "/" + fmtInt(int64(r.TotalShards))
		if r.Complete {
			state = "complete"
		}
		table[i] = urlTableRow{
			Crawl:  r.Crawl,
			Shards: fmtInt(int64(r.Shards)),
			URLs:   fmtInt(r.Rows),
			Size:   humanBytes(r.ParquetBytes),
			State:  state,
		}
	}
	if len(rows) > 0 {
		latest = rows[0].Crawl
	}

	data := urlsCard{
		Repo:           repo,
		Latest:         latest,
		SizeCat:        sizeCategory(totalRows),
		HasRows:        len(rows) > 0,
		Configs:        configs,
		TotalCrawls:    plural(len(rows), "crawl"),
		TotalURLs:      fmtInt(totalRows) + " URLs",
		TotalBytes:     humanBytes(totalBytes),
		TotalShards:    fmtInt(int64(totalShards)) + " shards",
		TotalShardsNum: fmtInt(int64(totalShards)),
		TotalRowsNum:   fmtInt(totalRows),
		Bars:           bars,
		Stats:          table,
		Columns:        urlColumnDocs,
		Updated:        updatedStamp(),
	}
	return render("urls_card.md", data)
}

// GenerateDomainsREADME renders the dataset card for open-index/ccrawl-domains
// from templates/domains_card.md. stats is the full ledger, one row per
// web-graph release. Shards keep the source's harmonic-centrality order, so
// part-000 holds the top-ranked domains.
func GenerateDomainsREADME(repo string, stats []DomainGraphStat) string {
	rows := append([]DomainGraphStat(nil), stats...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].Graph > rows[j].Graph })

	var totalDomains, totalBytes int64
	var totalShards int
	var maxDomains int64
	for _, r := range rows {
		totalDomains += r.Domains
		totalBytes += r.ParquetBytes
		totalShards += r.Shards
		maxDomains = max(maxDomains, r.Domains)
	}

	latest := "cc-main-2026-mar-apr-may"
	configs := make([]string, len(rows))
	bars := make([]string, len(rows))
	table := make([]domainTableRow, len(rows))
	for i, r := range rows {
		configs[i] = r.Graph
		frac := 0.0
		if maxDomains > 0 {
			frac = float64(r.Domains) / float64(maxDomains)
		}
		bars[i] = barRow(r.Graph, frac, humanCountShort(r.Domains))
		table[i] = domainTableRow{
			Graph:   r.Graph,
			Shards:  fmtInt(int64(r.Shards)),
			Domains: fmtInt(r.Domains),
			Size:    humanBytes(r.ParquetBytes),
			Source:  humanBytes(r.SourceBytes),
		}
	}
	if len(rows) > 0 {
		latest = rows[0].Graph
	}

	data := domainsCard{
		Repo:            repo,
		Latest:          latest,
		SizeCat:         sizeCategory(totalDomains),
		HasRows:         len(rows) > 0,
		Configs:         configs,
		TotalReleases:   plural(len(rows), "release"),
		TotalDomains:    fmtInt(totalDomains) + " domains",
		TotalBytes:      humanBytes(totalBytes),
		TotalShards:     fmtInt(int64(totalShards)) + " shards",
		TotalShardsNum:  fmtInt(int64(totalShards)),
		TotalDomainsNum: fmtInt(totalDomains),
		Bars:            bars,
		Stats:           table,
		Columns:         domainColumnDocs,
		Updated:         updatedStamp(),
	}
	return render("domains_card.md", data)
}

// render executes one embedded card template into a string.
func render(name string, data any) string {
	var b strings.Builder
	// The templates are compiled in at build time, so an execution error here
	// would be a programming bug, not a runtime condition. Fall back to the
	// name so a bad template never publishes silent garbage.
	if err := cards.ExecuteTemplate(&b, name, data); err != nil {
		return "# " + name + "\n\ntemplate error: " + err.Error() + "\n"
	}
	return b.String()
}

// updatedStamp is the "Last updated" line both cards carry.
func updatedStamp() string {
	return time.Now().UTC().Format("2006-01-02 15:04 UTC")
}

// rankBar renders a fixed-width filled/empty bar for a fraction in [0,1], the
// same style the arctic card uses for its by-year breakdown.
func rankBar(frac float64, width int) string {
	if frac < 0 {
		frac = 0
	}
	if frac > 1 {
		frac = 1
	}
	filled := min(int(frac*float64(width)+0.5), width)
	return strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
}

// barRow renders one labelled bar line: "  LABEL  ████░░░░  VALUE".
func barRow(label string, frac float64, value string) string {
	return "  " + padRight(label, 26) + "  " + rankBar(frac, barWidth) + "  " + value
}

// padRight left-justifies s in a field of at least n runes.
func padRight(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return s + strings.Repeat(" ", n-len(s))
}

// urlsCard is the template data for the URL-index card.
type urlsCard struct {
	Repo, Latest, SizeCat, Updated                  string
	HasRows                                         bool
	Configs                                         []string
	TotalCrawls, TotalURLs, TotalBytes, TotalShards string
	TotalShardsNum, TotalRowsNum                    string
	Bars                                            []string
	Stats                                           []urlTableRow
	Columns                                         [][3]string
}

type urlTableRow struct {
	Crawl, Shards, URLs, Size, State string
}

// domainsCard is the template data for the domain-ranks card.
type domainsCard struct {
	Repo, Latest, SizeCat, Updated                       string
	HasRows                                              bool
	Configs                                              []string
	TotalReleases, TotalDomains, TotalBytes, TotalShards string
	TotalShardsNum, TotalDomainsNum                      string
	Bars                                                 []string
	Stats                                                []domainTableRow
	Columns                                              [][3]string
}

type domainTableRow struct {
	Graph, Shards, Domains, Size, Source string
}

// urlColumnDocs documents the URL-index output schema in source order.
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

// domainColumnDocs documents the domain-ranks output schema in source order.
var domainColumnDocs = [][3]string{
	{"domain", "VARCHAR", "registrable domain, un-reversed from the source host key"},
	{"harmonic_pos", "BIGINT", "rank position by harmonic centrality, 1 is highest"},
	{"harmonic_val", "DOUBLE", "harmonic centrality score"},
	{"pagerank_pos", "BIGINT", "rank position by PageRank, 1 is highest"},
	{"pagerank_val", "DOUBLE", "PageRank score"},
	{"n_hosts", "BIGINT", "number of hosts aggregated into this domain"},
}

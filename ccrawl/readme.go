package ccrawl

import (
	"embed"
	"fmt"
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
		Build:          urlBuild(rows, totalBytes),
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
		Build:           domainBuild(rows),
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

// parseStamp parses an RFC3339 ledger timestamp. An empty or malformed stamp is
// reported as not-ok so the caller can drop the build block rather than render a
// bogus duration.
func parseStamp(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// humanDuration renders a duration as a short "2d 3h" / "3h 42m" / "8m" string,
// rounded to the minute above a minute and to the second below it.
func humanDuration(d time.Duration) string {
	if d < time.Minute {
		s := max(int(d.Seconds()), 1)
		return fmt.Sprintf("%ds", s)
	}
	d = d.Round(time.Minute)
	days := d / (24 * time.Hour)
	d -= days * 24 * time.Hour
	hours := d / time.Hour
	d -= hours * time.Hour
	mins := d / time.Minute
	switch {
	case days > 0:
		return fmt.Sprintf("%dd %dh", days, hours)
	case hours > 0:
		return fmt.Sprintf("%dh %dm", hours, mins)
	default:
		return fmt.Sprintf("%dm", mins)
	}
}

// perHour renders n items per hour over elapsed as a short count like "5.2M".
func perHour(n int64, elapsed time.Duration) string {
	h := elapsed.Hours()
	if h <= 0 {
		return "0"
	}
	return humanCountShort(int64(float64(n) / h))
}

// shardsPerHour renders the shard rate, with one decimal below ten so a slow run
// does not round to zero.
func shardsPerHour(shards int, elapsed time.Duration) string {
	h := elapsed.Hours()
	if h <= 0 {
		return "0"
	}
	r := float64(shards) / h
	if r >= 10 {
		return fmt.Sprintf("%.0f", r)
	}
	return fmt.Sprintf("%.1f", r)
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
	Build                                           *urlBuildStats
}

type urlTableRow struct {
	Crawl, Shards, URLs, Size, State string
}

// urlBuildStats are the live build metrics for the newest crawl: what it read,
// what it wrote, how long it has taken, how fast it is going, and when it should
// finish at the current rate. It is nil until the first shard commits, since the
// numbers come from the ledger timestamps that only exist after a commit.
type urlBuildStats struct {
	Latest      string // crawl id
	InputParts  string // "300 columnar source parts"
	Output      string // Parquet bytes committed for this crawl so far
	TotalOutput string // Parquet bytes across the whole dataset
	Coverage    string // "144 / 300 shards"
	Complete    bool
	Rows        string // "12,345,678 URLs"
	Elapsed     string // publish wall-clock, first commit to latest
	Rate        string // "39 shards/hour, 5.2M URLs/hour", empty when too early
	ETA         string // remaining-time estimate, empty when complete or too early
}

// urlBuild computes the live build metrics for the newest crawl from the ledger.
func urlBuild(rows []URLCrawlStat, totalBytes int64) *urlBuildStats {
	if len(rows) == 0 {
		return nil
	}
	r := rows[0]
	first, ok := parseStamp(r.FirstCommitted)
	if !ok {
		return nil
	}
	last, ok := parseStamp(r.LastCommitted)
	if !ok {
		last = first
	}
	elapsed := last.Sub(first)
	b := &urlBuildStats{
		Latest:      r.Crawl,
		InputParts:  fmtInt(int64(r.TotalShards)) + " columnar source parts",
		Output:      humanBytes(r.ParquetBytes),
		TotalOutput: humanBytes(totalBytes),
		Coverage:    fmtInt(int64(r.Shards)) + " / " + fmtInt(int64(r.TotalShards)) + " shards",
		Complete:    r.Complete,
		Rows:        fmtInt(r.Rows) + " URLs",
		Elapsed:     humanDuration(elapsed),
	}
	if elapsed >= time.Minute && r.Shards > 0 {
		b.Rate = shardsPerHour(r.Shards, elapsed) + " shards/hour, " + perHour(r.Rows, elapsed) + " URLs/hour"
	}
	if !r.Complete && r.Shards > 0 && r.TotalShards > r.Shards && elapsed >= time.Minute {
		perShard := elapsed / time.Duration(r.Shards)
		eta := perShard * time.Duration(r.TotalShards-r.Shards)
		finish := last.Add(eta).UTC().Format("2006-01-02 15:04 UTC")
		b.ETA = "about " + humanDuration(eta) + " remaining at the current rate, finishing around " + finish
	}
	return b
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
	Build                                                *domainBuildStats
}

type domainTableRow struct {
	Graph, Shards, Domains, Size, Source string
}

// domainBuildStats are the live build metrics for the newest release. Unlike the
// url side there is no known shard total mid-stream, so it reports throughput and
// elapsed instead of a finish estimate, and says so in Note.
type domainBuildStats struct {
	Latest   string // graph id
	Input    string // gzipped source size
	Output   string // Parquet bytes written
	Shards   string // "7 shards"
	Ratio    string // compression story, source vs Parquet
	Domains  string // "34,000,000 domains"
	Elapsed  string // publish wall-clock, first commit to latest
	Rate     string // "12 shards/hour, 61M domains/hour", empty when too early
	Complete bool
	Note     string // why no finish estimate is given mid-stream
}

// domainBuild computes the live build metrics for the newest release.
func domainBuild(rows []DomainGraphStat) *domainBuildStats {
	if len(rows) == 0 {
		return nil
	}
	r := rows[0]
	first, ok := parseStamp(r.FirstCommitted)
	if !ok {
		return nil
	}
	last, ok := parseStamp(r.CommittedAt)
	if !ok {
		last = first
	}
	elapsed := last.Sub(first)
	b := &domainBuildStats{
		Latest:   r.Graph,
		Input:    humanBytes(r.SourceBytes),
		Output:   humanBytes(r.ParquetBytes),
		Shards:   plural(r.Shards, "shard"),
		Domains:  fmtInt(r.Domains) + " domains",
		Elapsed:  humanDuration(elapsed),
		Complete: r.Complete,
		Note:     "the source is a single pre-sorted stream whose total length is not known until it ends, so a finish time is not projected mid-run",
	}
	if r.SourceBytes > 0 && r.ParquetBytes > 0 {
		factor := float64(r.SourceBytes) / float64(r.ParquetBytes)
		pct := float64(r.ParquetBytes) / float64(r.SourceBytes) * 100
		if factor >= 1 {
			b.Ratio = fmt.Sprintf("the Parquet is about %.1fx smaller than the gzipped source, %.0f%% of its size", factor, pct)
		} else {
			b.Ratio = fmt.Sprintf("the Parquet is about %.0f%% of the gzipped source size", pct)
		}
	}
	if elapsed >= time.Minute && r.Shards > 0 {
		b.Rate = shardsPerHour(r.Shards, elapsed) + " shards/hour, " + perHour(r.Domains, elapsed) + " domains/hour"
	}
	return b
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

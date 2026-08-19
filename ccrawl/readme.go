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
//go:embed templates/urls_card.md templates/domains_card.md templates/news_card.md
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

// GenerateNewsREADME renders the dataset card for open-index/ccrawl-news from
// templates/news_card.md. stats is the ledger, one row per CC-NEWS month, and
// langs is the per-month language ledger the breakdown table is built from.
//
// The layout mirrors the source: data/YYYY/MM holds one Parquet shard per source
// WARC, so the default config globs everything and a named config per month
// loads that month alone.
func GenerateNewsREADME(repo string, stats []NewsMonthStat, langs []NewsLangStat) string {
	rows := append([]NewsMonthStat(nil), stats...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].Month > rows[j].Month })

	var totalRows, totalBytes, totalSource int64
	var totalFiles int
	var maxRows int64
	for _, r := range rows {
		totalRows += r.Rows
		totalBytes += r.ParquetBytes
		totalSource += r.SourceBytes
		totalFiles += r.Files
		maxRows = max(maxRows, r.Rows)
	}

	latest := "2026-07"
	configs := make([]newsConfig, len(rows))
	bars := make([]string, len(rows))
	table := make([]newsTableRow, len(rows))
	for i, r := range rows {
		configs[i] = newsConfig{Name: r.Month, Path: strings.ReplaceAll(r.Month, "-", "/")}
		frac := 0.0
		if maxRows > 0 {
			frac = float64(r.Rows) / float64(maxRows)
		}
		bars[i] = barRow(r.Month, frac, humanCountShort(r.Rows))
		state := fmtInt(int64(r.Files)) + "/" + fmtInt(int64(r.TotalFiles))
		if r.Complete {
			state = "complete"
		}
		table[i] = newsTableRow{
			Month:    r.Month,
			Files:    fmtInt(int64(r.Files)),
			Articles: fmtInt(r.Rows),
			Size:     humanBytes(r.ParquetBytes),
			Source:   humanBytes(r.SourceBytes),
			State:    state,
		}
	}
	if len(rows) > 0 {
		latest = rows[0].Month
	}

	data := newsCard{
		Repo:          repo,
		Latest:        latest,
		LatestPath:    strings.ReplaceAll(latest, "-", "/"),
		SizeCat:       sizeCategory(totalRows),
		HasRows:       len(rows) > 0,
		Configs:       configs,
		TotalMonths:   plural(len(rows), "month"),
		TotalArticles: fmtInt(totalRows) + " articles",
		TotalBytes:    humanBytes(totalBytes),
		TotalSource:   humanBytes(totalSource),
		TotalFiles:    plural(totalFiles, "WARC file"),
		TotalFilesNum: fmtInt(int64(totalFiles)),
		TotalRowsNum:  fmtInt(totalRows),
		Bars:          bars,
		Langs:         newsLangBars(langs, totalRows),
		Stats:         table,
		Columns:       newsColumnDocs,
		Build:         newsBuild(rows),
		Savings:       newsSavings(totalSource, totalBytes),
		Updated:       updatedStamp(),
	}
	return render("news_card.md", data)
}

// newsLangTop is how many languages the card's breakdown lists before folding
// the rest into an "other" line.
const newsLangTop = 15

// newsLangBars builds the language breakdown across every month, biggest first.
// The share is of all indexed articles rather than of the identified ones, so
// the rows add up to less than 100 percent and the gap is exactly the articles
// nothing could be said about. That gap is worth showing rather than hiding in a
// renormalized denominator.
func newsLangBars(langs []NewsLangStat, totalRows int64) []newsLangRow {
	if len(langs) == 0 || totalRows == 0 {
		return nil
	}
	sum := map[string]int64{}
	for _, l := range langs {
		sum[l.Lang] += l.Rows
	}
	type kv struct {
		code string
		n    int64
	}
	all := make([]kv, 0, len(sum))
	var identified, maxN int64
	for c, n := range sum {
		all = append(all, kv{c, n})
		identified += n
		maxN = max(maxN, n)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].n != all[j].n {
			return all[i].n > all[j].n
		}
		return all[i].code < all[j].code
	})

	out := make([]newsLangRow, 0, newsLangTop+2)
	var shown int64
	for i, e := range all {
		if i >= newsLangTop {
			break
		}
		shown += e.n
		out = append(out, newsLangRow{
			Code:     e.code,
			Name:     LanguageName(e.code),
			Articles: fmtInt(e.n),
			Share:    fmt.Sprintf("%.1f%%", float64(e.n)/float64(totalRows)*100),
			Bar:      rankBar(float64(e.n)/float64(maxN), barWidth),
		})
	}
	if rest := identified - shown; rest > 0 {
		out = append(out, newsLangRow{
			Code:     "other",
			Name:     plural(len(all)-newsLangTop, "further language"),
			Articles: fmtInt(rest),
			Share:    fmt.Sprintf("%.1f%%", float64(rest)/float64(totalRows)*100),
			Bar:      rankBar(float64(rest)/float64(maxN), barWidth),
		})
	}
	if un := totalRows - identified; un > 0 {
		out = append(out, newsLangRow{
			Code:     "none",
			Name:     "too little text to identify",
			Articles: fmtInt(un),
			Share:    fmt.Sprintf("%.1f%%", float64(un)/float64(totalRows)*100),
			Bar:      rankBar(float64(un)/float64(maxN), barWidth),
		})
	}
	return out
}

// newsSavings phrases what the index cost against what reading it costs, which
// is the argument for the dataset existing. It is empty until both numbers are
// known.
func newsSavings(source, parquet int64) string {
	if source <= 0 || parquet <= 0 {
		return ""
	}
	return fmt.Sprintf("%s of WARC archives were read to produce %s of Parquet, about %.0fx smaller",
		humanBytes(source), humanBytes(parquet), float64(source)/float64(parquet))
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

// newsCard is the template data for the CC-NEWS index card.
type newsCard struct {
	Repo, Latest, LatestPath, SizeCat, Updated string
	HasRows                                    bool
	Configs                                    []newsConfig
	TotalMonths, TotalArticles, TotalBytes     string
	TotalSource, TotalFiles                    string
	TotalFilesNum, TotalRowsNum                string
	Savings                                    string
	Bars                                       []string
	Langs                                      []newsLangRow
	Stats                                      []newsTableRow
	Columns                                    [][3]string
	Build                                      *newsBuildStats
}

// newsConfig is one named config: the month as the dataset spells it and the
// month as the directory tree spells it.
type newsConfig struct {
	Name string // 2026-07
	Path string // 2026/07
}

type newsTableRow struct {
	Month, Files, Articles, Size, Source, State string
}

type newsLangRow struct {
	Code, Name, Articles, Share, Bar string
}

// newsBuildStats are the live build metrics for the newest month. Unlike the URL
// index, the interesting cost here is the archive bytes read rather than the
// Parquet bytes written, because reading them is the work the dataset exists to
// save everyone else.
type newsBuildStats struct {
	Latest    string // month
	InputFile string // "353 source WARC files"
	Read      string // compressed WARC bytes streamed so far
	Projected string // what the whole month will cost at the current average
	Output    string // Parquet bytes committed for this month
	Coverage  string // "144 / 353 files"
	Complete  bool
	Articles  string // "12,345,678 articles"
	HTMLShare string // share of rows that sniffed as HTML
	OKShare   string // share of rows that were a 2xx
	Elapsed   string
	Rate      string
	ETA       string
}

// newsBuild computes the live build metrics for the newest month from the ledger.
func newsBuild(rows []NewsMonthStat) *newsBuildStats {
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
	b := &newsBuildStats{
		Latest:    r.Month,
		InputFile: fmtInt(int64(r.TotalFiles)) + " source WARC files",
		Read:      humanBytes(r.SourceBytes),
		Output:    humanBytes(r.ParquetBytes),
		Coverage:  fmtInt(int64(r.Files)) + " / " + fmtInt(int64(r.TotalFiles)) + " files",
		Complete:  r.Complete,
		Articles:  fmtInt(r.Rows) + " articles",
		Elapsed:   humanDuration(elapsed),
	}
	if r.Files > 0 && r.TotalFiles > r.Files && r.SourceBytes > 0 {
		perFile := float64(r.SourceBytes) / float64(r.Files)
		b.Projected = humanBytes(int64(perFile * float64(r.TotalFiles)))
	}
	if r.Rows > 0 {
		b.HTMLShare = fmt.Sprintf("%.1f%%", float64(r.RowsHTML)/float64(r.Rows)*100)
		b.OKShare = fmt.Sprintf("%.1f%%", float64(r.Rows2xx)/float64(r.Rows)*100)
	}
	if elapsed >= time.Minute && r.Files > 0 {
		b.Rate = shardsPerHour(r.Files, elapsed) + " files/hour, " + perHour(r.Rows, elapsed) + " articles/hour"
	}
	if !r.Complete && r.Files > 0 && r.TotalFiles > r.Files && elapsed >= time.Minute {
		perFile := elapsed / time.Duration(r.Files)
		eta := perFile * time.Duration(r.TotalFiles-r.Files)
		finish := last.Add(eta).UTC().Format("2006-01-02 15:04 UTC")
		b.ETA = "about " + humanDuration(eta) + " remaining at the current rate, finishing around " + finish
	}
	return b
}

// newsColumnDocs documents the CC-NEWS index output schema in source order. The
// third column says where a value came from, because half of these are read off
// the capture and half are computed here, and a reader deciding whether to trust
// one needs to know which.
var newsColumnDocs = [][3]string{
	{"url_surtkey", "VARCHAR", "SURT-canonical sort key for the URL, computed here"},
	{"url", "VARCHAR", "the captured URL, from WARC-Target-URI"},
	{"url_host_name", "VARCHAR", "full host name, parsed from the URL"},
	{"url_host_registered_domain", "VARCHAR", "registrable domain, one level below the public suffix"},
	{"url_host_tld", "VARCHAR", "top-level domain"},
	{"url_protocol", "VARCHAR", "scheme, http or https"},
	{"fetch_time", "TIMESTAMP", "when the crawler fetched the page, from WARC-Date, UTC"},
	{"fetch_status", "INTEGER", "HTTP status code, from the stored response"},
	{"fetch_redirect", "VARCHAR", "Location header when the capture was a redirect, else empty"},
	{"content_digest", "VARCHAR", "SHA-1 of the response body, from WARC-Payload-Digest"},
	{"content_mime_type", "VARCHAR", "MIME type the server reported in Content-Type"},
	{"content_mime_detected", "VARCHAR", "MIME type sniffed from the first bytes of the body, computed here"},
	{"content_charset", "VARCHAR", "character set the server reported"},
	{"content_languages", "VARCHAR", "ISO 639-3 language identified from the extracted text, computed here"},
	{"content_truncated", "VARCHAR", "reason the capture was truncated, if any"},
	{"warc_filename", "VARCHAR", "path of the CC-NEWS WARC holding the response"},
	{"warc_record_offset", "BIGINT", "byte offset of the record's gzip member in that file, computed here"},
	{"warc_record_length", "BIGINT", "compressed byte length of the record, computed here"},
	{"content_language_confidence", "DOUBLE", "how sure the identifier was, 0 to 1"},
	{"content_language_declared", "VARCHAR", "the page's own html lang attribute, BCP-47, as written"},
	{"content_length", "BIGINT", "size of the stored response body in bytes, decoded"},
	{"warc_record_id", "VARCHAR", "the record's WARC-Record-ID urn:uuid"},
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

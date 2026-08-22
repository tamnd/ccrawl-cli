package ccrawl

import "fmt"

// recrawlCard is the template data for the two recrawl dataset cards.
//
// One template serves both repos because the layout, the ledger, the columns and
// the fleet story are identical, and only the prose about what was fetched
// differs. The differing sentences are passed in rather than branched on inside
// the template, so reading the template tells you what the card says without
// having to hold two versions of it in your head.
type recrawlCard struct {
	Repo, Kind, SizeCat, Updated string
	PrettyName, Tagline          string
	What, Source, Bias           string
	Tags                         []string
	HasRows                      bool
	Servers, Files, Rows, Bytes  string
	FilesNum, RowsNum            string
	Slices                       string
	Progress                     string
	Bars                         []string
	Stats                        []recrawlTableRow
	Columns                      [][4]string
}

type recrawlTableRow struct {
	Server, Shard, Files, Rows, Size, State string
}

// GenerateRecrawlREADME renders the card for one of the two recrawl repos from
// the merged fleet ledger. kind is "domains" or "urls".
//
// Everything on the card is computed from rows that are on the hub, never from
// what a run intends to fetch. A card that promised six billion pages while the
// repo held four million would be worse than no card at all, so the only totals
// here are committed ones and progress is stated as a position in the work list
// rather than as a percentage of a finish nobody can date.
func GenerateRecrawlREADME(repo, kind string, stats []RecrawlStat) string {
	rows := append([]RecrawlStat(nil), stats...)
	SortRecrawlStats(rows)
	t := TotalRecrawlStats(rows)

	var maxRows int64
	for _, r := range rows {
		maxRows = max(maxRows, r.Rows)
	}
	bars := make([]string, len(rows))
	table := make([]recrawlTableRow, len(rows))
	for i, r := range rows {
		frac := 0.0
		if maxRows > 0 {
			frac = float64(r.Rows) / float64(maxRows)
		}
		label := fmt.Sprintf("%s shard %d", r.Server, r.Shard)
		bars[i] = barRow(label, frac, humanCountShort(r.Rows))
		state := fmt.Sprintf("part %d row %s", r.Part, fmtInt(r.Row))
		if r.Done {
			state = "complete"
		}
		table[i] = recrawlTableRow{
			Server: r.Server,
			Shard:  fmt.Sprintf("%d/%d", r.Shard, r.Shards),
			Files:  fmtInt(int64(r.Files)),
			Rows:   fmtInt(r.Rows),
			Size:   humanBytes(r.Bytes),
			State:  state,
		}
	}

	c := recrawlCard{
		Repo:     repo,
		Kind:     kind,
		SizeCat:  sizeCategory(t.Rows),
		HasRows:  len(rows) > 0,
		Servers:  plural(t.Servers, "server"),
		Files:    plural(t.Files, "shard"),
		Rows:     fmtInt(t.Rows) + " rows",
		Bytes:    humanBytes(t.Bytes),
		FilesNum: fmtInt(int64(t.Files)),
		RowsNum:  fmtInt(t.Rows),
		Slices:   fmt.Sprintf("%d of %d", t.Shards, max(t.Slices, t.Shards)),
		Progress: recrawlProgress(t),
		Bars:     bars,
		Stats:    table,
		Columns:  captureColumnDocs,
		Updated:  updatedStamp(),
	}
	applyRecrawlKind(&c, kind)
	return render("recrawl_card.md", c)
}

// recrawlProgress states coverage the only way it can be stated honestly while a
// fleet is still running: how many slices of the work list have finished.
func recrawlProgress(t RecrawlTotals) string {
	switch {
	case t.Servers == 0:
		return "The first shards are publishing now."
	case t.Done == t.Servers:
		return "Every slice of the work list has been walked out, so this dataset is complete."
	case t.Done > 0:
		return fmt.Sprintf("%d of the %s covering the work list have finished their slice, the rest are still fetching.", t.Done, plural(t.Servers, "server"))
	default:
		return "The fleet is still fetching, and new shards are committed as they close."
	}
}

// applyRecrawlKind fills in the sentences that differ between the two repos.
func applyRecrawlKind(c *recrawlCard, kind string) {
	common := []string{"common-crawl", "web-crawl", "recrawl", "html", "markdown", "text", "parquet", "open-data"}
	switch kind {
	case "urls":
		c.PrettyName = "Common Crawl URL Recrawl"
		c.Tagline = "Live refetches of pages from Common Crawl's URL index, with the body inline and the text already extracted"
		c.What = "Common Crawl publishes which URLs it saw and when, but the page bodies live in WARC archives that are awkward to query and are as old as the crawl that made them. This dataset takes the URL index for a single monthly crawl and fetches the pages again now, storing each response as one Parquet row with the body, the headers, the timing and the outcome all in the same place, and the page rendered to Markdown and plain text beside them. It is the same set of addresses, refetched, so you can compare what a page says today against what the archive says it said, and read it without parsing any HTML."
		c.Source = "The work list is [open-index/ccrawl-urls](https://huggingface.co/datasets/open-index/ccrawl-urls), the URL index for one monthly Common Crawl. Every row in this dataset came from fetching one of those URLs."
		c.Bias = "The work list is whatever Common Crawl indexed, so pages it never reached are not here either. Pages that have moved, expired or gone behind a login since the index was built show up as redirects, 404s and 403s rather than as content, and that is data too, but it means a naive count of rows is not a count of readable pages."
		c.Tags = append(common, "url-index")
	default:
		c.PrettyName = "Common Crawl Domain Recrawl"
		c.Tagline = "Live fetches of the home page of every ranked domain in Common Crawl's web graph, rendered to Markdown as they are fetched"
		c.What = "Common Crawl's web graph ranks domains by how central they are, but it does not tell you what those domains actually serve today. This dataset walks that ranking from the top and fetches each domain's home page now, storing the response as one Parquet row with the body, the headers, the timing and the outcome all in the same place, and the page rendered to Markdown and plain text beside them. Because the work list is in rank order, the early shards hold the most central domains on the web."
		c.Source = "The work list is [open-index/ccrawl-domains](https://huggingface.co/datasets/open-index/ccrawl-domains), the domain-level ranks from Common Crawl's hyperlink web graph, read in rank order. Every row in this dataset came from fetching one of those domains."
		c.Bias = "The ranking the work list comes from reflects the link structure Common Crawl observed, which favours well-linked, long-established, commercial and English-language domains. Fetching a home page also says nothing about the rest of the site. A large share of the work list no longer resolves at all, since a domain rank computed from a months-old web graph includes names that have since lapsed, and those are rows with an error rather than missing rows."
		c.Tags = append(common, "domain-ranks")
	}
}

// captureColumnDocs documents the capture schema, which is what a recrawl writes
// and therefore what both recrawl repos publish.
//
// The third field is where the value came from, and it is on the card because it
// changes what a column means. A served column is the site's own answer and is
// evidence about the site. A computed column is this pipeline's opinion, and a
// different extractor or a different language detector would produce a different
// one over the same bytes. A measured column is a number off our clock on our
// network and says as much about the machine that fetched the page as about the
// page. Reading all three as though they were the same kind of fact is the
// mistake this column exists to stop.
var captureColumnDocs = [][4]string{
	{"url", "VARCHAR", "asked", "the URL that was requested, straight off the work list"},
	{"host", "VARCHAR", "computed", "host of the requested URL, parsed from it"},
	{"status", "INTEGER", "served", "HTTP status of the response, 0 if the fetch never got one"},
	{"fetched_at", "BIGINT", "measured", "when the fetch happened, unix milliseconds"},
	{"content_type", "VARCHAR", "served", "Content-Type header as served"},
	{"body_length", "BIGINT", "computed", "length of the body in bytes"},
	{"digest", "VARCHAR", "computed", "SHA-1 of the body, for spotting a page that has not changed"},
	{"unchanged", "BOOLEAN", "served", "true when the server answered 304 Not Modified"},
	{"etag", "VARCHAR", "served", "ETag header, empty if the server sent none"},
	{"last_modified", "VARCHAR", "served", "Last-Modified header, empty if the server sent none"},
	{"warc_file", "VARCHAR", "computed", "WARC file holding this response, empty for a Parquet-only run"},
	{"warc_offset", "BIGINT", "computed", "byte offset of the record in that WARC file"},
	{"warc_length", "BIGINT", "computed", "byte length of the record"},
	{"error", "VARCHAR", "computed", "why the fetch failed, one of dns, timeout, refused, tls, skip, other, empty when it succeeded"},
	{"meta_json", "VARCHAR", "computed", "extra context as JSON, including error_detail for a failed row"},
	{"markdown", "VARCHAR", "computed", "the page rendered to Markdown, empty when it was not rendered"},
	{"markdown_length", "BIGINT", "computed", "length of the Markdown in bytes"},
	{"ttfb_ms", "BIGINT", "measured", "time to first byte in milliseconds, our clock and our network"},
	{"fetch_duration_ms", "BIGINT", "measured", "total fetch time in milliseconds"},
	{"final_url", "VARCHAR", "served", "URL after redirects, empty when the request did not move"},
	{"ip_address", "VARCHAR", "measured", "IP the request went to, which for a CDN is the nearest edge"},
	{"resp_headers", "VARCHAR", "served", "response headers as JSON"},
	{"req_headers", "VARCHAR", "asked", "request headers as JSON, what we sent"},
	{"body", "BLOB", "served", "the response body exactly as served, before any decoding"},
	{"title", "VARCHAR", "computed", "the document title, empty when the page has none"},
	{"text", "VARCHAR", "computed", "the page as plain text, boilerplate stripped"},
	{"text_length", "BIGINT", "computed", "length of the text in bytes"},
	{"word_count", "BIGINT", "computed", "words in the extracted text"},
	{"language", "VARCHAR", "computed", "language of the Markdown, ISO 639-3, detected not declared"},
	{"language_confidence", "DOUBLE", "computed", "how sure the detector is, 0 to 1"},
	{"simhash", "BIGINT", "computed", "fingerprint of the Markdown, for finding near duplicates"},
	{"extractor", "VARCHAR", "computed", "engine and version that rendered the page, as name@version"},
}

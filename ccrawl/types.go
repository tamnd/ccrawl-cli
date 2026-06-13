// Package ccrawl is a Go library for working with Common Crawl data: the
// collection list, the CDX URL index, WARC/WAT/WET archive files, the columnar
// Parquet index, CC-NEWS, and the host/domain ranks. It is the engine behind the
// ccrawl command line tool but is usable on its own.
package ccrawl

import "time"

// Crawl is one Common Crawl collection as published in collinfo.json.
type Crawl struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	CDXAPI string `json:"cdx-api"`
	From   string `json:"from,omitempty"`
	To     string `json:"to,omitempty"`
}

// CDXRecord is one capture from the URL index. Numeric fields stay as strings
// because that is how the CDX server returns them; helpers convert on demand.
type CDXRecord struct {
	CrawlID      string `json:"crawl,omitempty"`
	URLKey       string `json:"urlkey"`
	Timestamp    string `json:"timestamp"` // 14-digit YYYYMMDDHHmmss
	URL          string `json:"url"`
	MIME         string `json:"mime"`
	MIMEDetected string `json:"mime-detected"`
	Status       string `json:"status"`
	Digest       string `json:"digest"`
	Length       string `json:"length"`
	Offset       string `json:"offset"`
	Filename     string `json:"filename"`
	Charset      string `json:"charset,omitempty"`
	Languages    string `json:"languages,omitempty"`
	Truncated    string `json:"truncated,omitempty"`
	Redirect     string `json:"redirect,omitempty"`
}

// Time parses the 14-digit timestamp. The zero time is returned on failure.
func (r CDXRecord) Time() time.Time {
	t, err := time.Parse("20060102150405", r.Timestamp)
	if err != nil {
		return time.Time{}
	}
	return t
}

// Location is the WARC file plus byte span needed to range-fetch this capture.
type Location struct {
	Filename string `json:"filename"`
	Offset   int64  `json:"offset"`
	Length   int64  `json:"length"`
	URL      string `json:"url,omitempty"`
}

// Location returns the byte span of this capture within its WARC file.
func (r CDXRecord) Location() Location {
	off := atoi64(r.Offset)
	length := atoi64(r.Length)
	return Location{Filename: r.Filename, Offset: off, Length: length, URL: r.URL}
}

// WARCHeader holds parsed WARC record headers.
type WARCHeader struct {
	Type          string // warcinfo|request|response|metadata|revisit|conversion|resource
	Date          time.Time
	RecordID      string
	TargetURI     string
	IPAddress     string
	ConcurrentTo  string
	WarcinfoID    string
	BlockDigest   string
	PayloadDigest string
	RefersTo      string
	Truncated     string
	ContentType   string
	ContentLength int64
	Language      string // WARC-Identified-Content-Language (WET records)
	// Response records only: extracted HTTP fields.
	HTTPStatus int
	HTTPMIME   string
	// Source location for range-request retrieval.
	WARCFilename string
	WARCOffset   int64
	WARCLength   int64
}

// WARCRecord is a parsed WARC record: its header and the raw block bytes. For a
// response record the block is the full HTTP message (status line, headers, body).
type WARCRecord struct {
	Header WARCHeader
	Block  []byte
}

// WATRecord is metadata extracted by Common Crawl from a single page.
type WATRecord struct {
	RecordID    string
	CrawlID     string
	URL         string
	Date        time.Time
	HTTPStatus  int
	ContentType string
	Title       string
	Links       []WATLink
	LinksCount  int
	Metas       []WATMeta
	WARCFile    string
	WARCOffset  int64
	WARCLength  int64
}

// WATLink is a hyperlink extracted from page HTML.
type WATLink struct {
	Path  string `json:"path"`
	URL   string `json:"url"`
	Text  string `json:"text,omitempty"`
	Title string `json:"title,omitempty"`
	Alt   string `json:"alt,omitempty"`
}

// WATMeta is a <meta> tag extracted from page HTML.
type WATMeta struct {
	Name     string `json:"name,omitempty"`
	Property string `json:"property,omitempty"`
	Content  string `json:"content,omitempty"`
	Charset  string `json:"charset,omitempty"`
}

// WETRecord is extracted plain text for one page.
type WETRecord struct {
	RecordID        string
	CrawlID         string
	URL             string
	Date            time.Time
	ContentLanguage string
	Text            string
}

// NewsFile describes one CC-NEWS WARC file.
type NewsFile struct {
	Path string
	Year int
	Mon  int
}

// Rank is a host/domain entry from the web-graph rank tables.
type Rank struct {
	Key         string  `json:"key"` // host or domain (forward form)
	HarmonicPos int64   `json:"harmonic_pos"`
	HarmonicVal float64 `json:"harmonic_val"`
	PageRankPos int64   `json:"pagerank_pos"`
	PageRankVal float64 `json:"pagerank_val"`
}

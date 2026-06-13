// Package ccrawl is a Go library for working with Common Crawl data: the
// collection list, the CDX URL index, WARC/WAT/WET archive files, the columnar
// Parquet index, CC-NEWS, and the host/domain ranks. It is the engine behind the
// ccrawl command line tool but is usable on its own.
package ccrawl

import (
	"time"

	"github.com/tamnd/ccrawl-cli/pkg/warc"
	"github.com/tamnd/ccrawl-cli/pkg/wat"
	"github.com/tamnd/ccrawl-cli/pkg/wet"
)

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

// The WARC, WAT, and WET record types live in their own importable packages
// under pkg/. These aliases keep the long-standing ccrawl.* names working for
// existing callers while the canonical definitions sit with their parsers.

// WARCHeader holds parsed WARC record headers.
type WARCHeader = warc.Header

// WARCRecord is a parsed WARC record: its header and the raw block bytes. For a
// response record the block is the full HTTP message (status line, headers, body).
type WARCRecord = warc.Record

// WATRecord is metadata extracted by Common Crawl from a single page.
type WATRecord = wat.Record

// WATLink is a hyperlink extracted from page HTML.
type WATLink = wat.Link

// WATMeta is a <meta> tag extracted from page HTML.
type WATMeta = wat.Meta

// WETRecord is extracted plain text for one page.
type WETRecord = wet.Record

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

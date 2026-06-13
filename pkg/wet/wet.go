// Package wet reads WET files, the Common Crawl archive of extracted plain
// text. A WET file is a WARC whose conversion records hold the readable text of
// each page, so this package decodes those records on top of pkg/warc.
package wet

import (
	"io"
	"strings"
	"time"

	"github.com/tamnd/ccrawl-cli/pkg/warc"
)

// Record is extracted plain text for one page.
type Record struct {
	RecordID        string
	CrawlID         string
	URL             string
	Date            time.Time
	ContentLanguage string
	Text            string
}

// Iterate reads a WET file (WARC conversion records holding plain text) and
// calls fn for each record.
func Iterate(r io.Reader, crawlID string, fn func(Record) error) error {
	return warc.Iterate(r, func(rec warc.Record) error {
		if rec.Header.Type != "conversion" {
			return nil
		}
		text := strings.TrimSpace(string(rec.Block))
		if rec.Header.TargetURI == "" || text == "" {
			return nil
		}
		return fn(Record{
			RecordID:        rec.Header.RecordID,
			CrawlID:         crawlID,
			URL:             rec.Header.TargetURI,
			Date:            rec.Header.Date,
			ContentLanguage: rec.Header.Language,
			Text:            text,
		})
	})
}

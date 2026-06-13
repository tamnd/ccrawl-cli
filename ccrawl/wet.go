package ccrawl

import (
	"io"
	"strings"
)

// IterateWET reads a WET file (WARC conversion records holding plain text) and
// calls fn for each record.
func IterateWET(r io.Reader, crawlID string, fn func(WETRecord) error) error {
	return IterateWARC(r, func(rec WARCRecord) error {
		if rec.Header.Type != "conversion" {
			return nil
		}
		text := strings.TrimSpace(string(rec.Block))
		if rec.Header.TargetURI == "" || text == "" {
			return nil
		}
		return fn(WETRecord{
			RecordID:        rec.Header.RecordID,
			CrawlID:         crawlID,
			URL:             rec.Header.TargetURI,
			Date:            rec.Header.Date,
			ContentLanguage: rec.Header.Language,
			Text:            text,
		})
	})
}

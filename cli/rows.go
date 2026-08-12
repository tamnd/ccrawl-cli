package cli

import (
	"strconv"

	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// cdxRow turns a CDX record into an output Row.
func cdxRow(r ccrawl.CDXRecord) Row {
	return Row{
		Cols:  []string{"timestamp", "url", "status", "mime", "languages", "digest", "filename", "offset", "length", "crawl"},
		Vals:  []string{r.Timestamp, r.URL, r.Status, r.MIMEDetected, r.Languages, r.Digest, r.Filename, r.Offset, r.Length, r.CrawlID},
		Value: r,
	}
}

func wetRow(r ccrawl.WETRecord) Row {
	return Row{
		Cols:  []string{"url", "language", "length", "text"},
		Vals:  []string{r.URL, r.ContentLanguage, strconv.Itoa(len(r.Text)), r.Text},
		Value: r,
	}
}

func watRow(r ccrawl.WATRecord) Row {
	return Row{
		Cols:  []string{"url", "status", "title", "links", "content_type"},
		Vals:  []string{r.URL, strconv.Itoa(r.HTTPStatus), r.Title, strconv.Itoa(r.LinksCount), r.ContentType},
		Value: r,
	}
}

func warcRow(r ccrawl.WARCRecord) Row {
	h := r.Header
	return Row{
		Cols:  []string{"type", "url", "status", "mime", "length", "date"},
		Vals:  []string{h.Type, h.TargetURI, strconv.Itoa(h.HTTPStatus), h.HTTPMIME, strconv.FormatInt(h.ContentLength, 10), h.Date.Format("2006-01-02T15:04:05Z")},
		Value: warcJSON(r),
	}
}

// warcJSON is a WARC record as a JSON object. A WARC header has more fields
// than anyone wants in a line of JSONL, so unlike the WAT and WET records this
// one is picked by hand, and it is picked in one place so that parse and
// convert write the same object.
func warcJSON(r ccrawl.WARCRecord) map[string]any {
	h := r.Header
	return map[string]any{
		"type": h.Type, "url": h.TargetURI, "status": h.HTTPStatus,
		"mime": h.HTTPMIME, "date": h.Date, "record_id": h.RecordID,
		"payload_digest": h.PayloadDigest, "content_length": h.ContentLength,
	}
}

func linkRow(l ccrawl.WATLink) Row {
	return Row{
		Cols:  []string{"url", "text", "title"},
		Vals:  []string{l.URL, l.Text, l.Title},
		Value: l,
	}
}

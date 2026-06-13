// Package wat reads WAT files, the Common Crawl archive of per-page metadata:
// the response status and content type, the HTML title and meta tags, and the
// outbound links. A WAT file is a WARC whose metadata records carry a JSON
// envelope, so this package decodes that envelope on top of pkg/warc.
package wat

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/ccrawl-cli/pkg/warc"
)

// Record is metadata extracted by Common Crawl from a single page.
type Record struct {
	RecordID    string
	CrawlID     string
	URL         string
	Date        time.Time
	HTTPStatus  int
	ContentType string
	Title       string
	Links       []Link
	LinksCount  int
	Metas       []Meta
	WARCFile    string
	WARCOffset  int64
	WARCLength  int64
}

// Link is a hyperlink extracted from page HTML.
type Link struct {
	Path  string `json:"path"`
	URL   string `json:"url"`
	Text  string `json:"text,omitempty"`
	Title string `json:"title,omitempty"`
	Alt   string `json:"alt,omitempty"`
}

// Meta is a <meta> tag extracted from page HTML.
type Meta struct {
	Name     string `json:"name,omitempty"`
	Property string `json:"property,omitempty"`
	Content  string `json:"content,omitempty"`
	Charset  string `json:"charset,omitempty"`
}

// envelope is the JSON document inside a WAT metadata block. Common Crawl
// encodes the container Offset and length fields as strings, hence the string
// types below.
type envelope struct {
	Container struct {
		Filename string `json:"Filename"`
		Offset   string `json:"Offset"`
		GzipMeta struct {
			DeflateLength  string `json:"Deflate-Length"`
			InflatedLength string `json:"Inflated-Length"`
		} `json:"Gzip-Metadata"`
	} `json:"Container"`
	Envelope struct {
		WARCHeaderMeta map[string]string `json:"WARC-Header-Metadata"`
		PayloadMeta    struct {
			HTTPResponseMeta *struct {
				ResponseMessage struct {
					Status string `json:"Status"`
				} `json:"Response-Message"`
				Headers  map[string]string `json:"Headers"`
				HTMLMeta *struct {
					Head *struct {
						Title string `json:"Title"`
						Metas []Meta `json:"Metas"`
					} `json:"Head"`
					Links []Link `json:"Links"`
				} `json:"HTML-Metadata"`
			} `json:"HTTP-Response-Metadata"`
		} `json:"Payload-Metadata"`
	} `json:"Envelope"`
}

// Iterate reads a WAT file and calls fn for each parsed record.
func Iterate(r io.Reader, crawlID string, fn func(Record) error) error {
	return warc.Iterate(r, func(rec warc.Record) error {
		if rec.Header.Type != "metadata" || len(rec.Block) == 0 {
			return nil
		}
		w, err := parseBlock(rec, crawlID)
		if err != nil || w.URL == "" {
			return nil
		}
		return fn(w)
	})
}

func parseBlock(rec warc.Record, crawlID string) (Record, error) {
	var env envelope
	if err := json.Unmarshal(rec.Block, &env); err != nil {
		return Record{}, fmt.Errorf("parse WAT JSON: %w", err)
	}

	w := Record{
		RecordID: rec.Header.RecordID,
		CrawlID:  crawlID,
		Date:     rec.Header.Date,
		WARCFile: env.Container.Filename,
	}
	w.URL = warc.TrimURI(env.Envelope.WARCHeaderMeta["WARC-Target-URI"])
	if w.URL == "" {
		w.URL = rec.Header.TargetURI
	}
	w.WARCOffset, _ = strconv.ParseInt(env.Container.Offset, 10, 64)
	w.WARCLength, _ = strconv.ParseInt(env.Container.GzipMeta.DeflateLength, 10, 64)

	http := env.Envelope.PayloadMeta.HTTPResponseMeta
	if http == nil {
		return w, nil
	}
	if s := http.ResponseMessage.Status; s != "" {
		w.HTTPStatus, _ = strconv.Atoi(strings.Fields(s)[0])
	}
	for k, v := range http.Headers {
		if strings.EqualFold(k, "content-type") {
			ct := v
			if i := strings.Index(ct, ";"); i >= 0 {
				ct = strings.TrimSpace(ct[:i])
			}
			w.ContentType = ct
			break
		}
	}
	if hm := http.HTMLMeta; hm != nil {
		if hm.Head != nil {
			w.Title = hm.Head.Title
			w.Metas = hm.Head.Metas
		}
		w.Links = hm.Links
	}
	w.LinksCount = len(w.Links)

	if w.Date.IsZero() {
		if ds := env.Envelope.WARCHeaderMeta["WARC-Date"]; ds != "" {
			if t, err := time.Parse(time.RFC3339, ds); err == nil {
				w.Date = t
			}
		}
	}
	return w, nil
}

package ccrawl

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// watEnvelope is the JSON document inside a WAT metadata block. Common Crawl
// encodes the container Offset and length fields as strings, hence the string
// types below.
type watEnvelope struct {
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
						Title string    `json:"Title"`
						Metas []WATMeta `json:"Metas"`
					} `json:"Head"`
					Links []WATLink `json:"Links"`
				} `json:"HTML-Metadata"`
			} `json:"HTTP-Response-Metadata"`
		} `json:"Payload-Metadata"`
	} `json:"Envelope"`
}

// IterateWAT reads a WAT file and calls fn for each parsed record.
func IterateWAT(r io.Reader, crawlID string, fn func(WATRecord) error) error {
	return IterateWARC(r, func(rec WARCRecord) error {
		if rec.Header.Type != "metadata" || len(rec.Block) == 0 {
			return nil
		}
		w, err := parseWATBlock(rec, crawlID)
		if err != nil || w.URL == "" {
			return nil
		}
		return fn(w)
	})
}

func parseWATBlock(rec WARCRecord, crawlID string) (WATRecord, error) {
	var env watEnvelope
	if err := json.Unmarshal(rec.Block, &env); err != nil {
		return WATRecord{}, fmt.Errorf("parse WAT JSON: %w", err)
	}

	w := WATRecord{
		RecordID: rec.Header.RecordID,
		CrawlID:  crawlID,
		Date:     rec.Header.Date,
		WARCFile: env.Container.Filename,
	}
	w.URL = trimURI(env.Envelope.WARCHeaderMeta["WARC-Target-URI"])
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
		w.HTTPStatus = atoi(strings.Fields(s)[0])
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

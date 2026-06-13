package wat

import (
	"bytes"
	"fmt"
	"testing"

	kgzip "github.com/klauspost/compress/gzip"
)

// member frames one WARC record as its own gzip member.
func member(t *testing.T, recType, uri, block string) []byte {
	t.Helper()
	rec := fmt.Sprintf("WARC/1.0\r\n"+
		"WARC-Type: %s\r\n"+
		"WARC-Target-URI: %s\r\n"+
		"Content-Length: %d\r\n"+
		"\r\n%s\r\n\r\n", recType, uri, len(block), block)

	var buf bytes.Buffer
	zw := kgzip.NewWriter(&buf)
	if _, err := zw.Write([]byte(rec)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

const watEnvelope = `{
  "Container": {"Filename": "crawl.warc.gz", "Offset": "1234",
    "Gzip-Metadata": {"Deflate-Length": "567"}},
  "Envelope": {
    "WARC-Header-Metadata": {"WARC-Target-URI": "https://page.example/"},
    "Payload-Metadata": {"HTTP-Response-Metadata": {
      "Response-Message": {"Status": "200 OK"},
      "Headers": {"Content-Type": "text/html; charset=utf-8"},
      "HTML-Metadata": {
        "Head": {"Title": "The Title",
          "Metas": [{"name": "description", "content": "hi"}]},
        "Links": [{"path": "A@/href", "url": "https://other.example/"}]
      }}}}}`

func TestIterate(t *testing.T) {
	file := member(t, "metadata", "https://page.example/", watEnvelope)

	var got []Record
	if err := Iterate(bytes.NewReader(file), "CC-MAIN-2026-21", func(w Record) error {
		got = append(got, w)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	w := got[0]
	if w.URL != "https://page.example/" {
		t.Errorf("URL = %q", w.URL)
	}
	if w.CrawlID != "CC-MAIN-2026-21" {
		t.Errorf("CrawlID = %q", w.CrawlID)
	}
	if w.HTTPStatus != 200 {
		t.Errorf("HTTPStatus = %d", w.HTTPStatus)
	}
	if w.ContentType != "text/html" {
		t.Errorf("ContentType = %q", w.ContentType)
	}
	if w.Title != "The Title" {
		t.Errorf("Title = %q", w.Title)
	}
	if w.WARCFile != "crawl.warc.gz" || w.WARCOffset != 1234 || w.WARCLength != 567 {
		t.Errorf("container fields = %q %d %d", w.WARCFile, w.WARCOffset, w.WARCLength)
	}
	if w.LinksCount != 1 || len(w.Links) != 1 || w.Links[0].URL != "https://other.example/" {
		t.Errorf("links = %+v", w.Links)
	}
	if len(w.Metas) != 1 || w.Metas[0].Content != "hi" {
		t.Errorf("metas = %+v", w.Metas)
	}
}

func TestIterateSkipsNonMetadata(t *testing.T) {
	file := member(t, "response", "https://page.example/", "HTTP/1.1 200 OK\r\n\r\nbody")
	count := 0
	if err := Iterate(bytes.NewReader(file), "id", func(Record) error {
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("got %d records from a non-metadata file, want 0", count)
	}
}

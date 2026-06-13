package wet

import (
	"bytes"
	"fmt"
	"testing"

	kgzip "github.com/klauspost/compress/gzip"
)

// member frames one WARC record as its own gzip member.
func member(t *testing.T, recType, uri, lang, block string) []byte {
	t.Helper()
	rec := fmt.Sprintf("WARC/1.0\r\n"+
		"WARC-Type: %s\r\n"+
		"WARC-Target-URI: %s\r\n"+
		"WARC-Identified-Content-Language: %s\r\n"+
		"Content-Length: %d\r\n"+
		"\r\n%s\r\n\r\n", recType, uri, lang, len(block), block)

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

func TestIterate(t *testing.T) {
	var file bytes.Buffer
	file.Write(member(t, "conversion", "https://page.example/", "eng", "  Readable text of the page.  "))
	file.Write(member(t, "conversion", "https://empty.example/", "eng", "   ")) // empty text: skipped
	file.Write(member(t, "warcinfo", "", "", "isPartOf: CC-MAIN"))              // not a conversion: skipped

	var got []Record
	if err := Iterate(bytes.NewReader(file.Bytes()), "CC-MAIN-2026-21", func(w Record) error {
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
	if w.ContentLanguage != "eng" {
		t.Errorf("ContentLanguage = %q", w.ContentLanguage)
	}
	if w.Text != "Readable text of the page." {
		t.Errorf("Text = %q", w.Text)
	}
}

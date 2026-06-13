package ccrawl

import (
	"bytes"
	"fmt"
	"testing"

	kgzip "github.com/klauspost/compress/gzip"
)

// warcMember frames one WARC record and returns it as its own gzip member, the
// way Common Crawl stores records (one gzip member per record).
func warcMember(t *testing.T, recType, uri, payload string) []byte {
	t.Helper()
	block := payload
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

func TestIterateWARCMultiMember(t *testing.T) {
	var file bytes.Buffer
	file.Write(warcMember(t, "response", "https://a.example/", "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<title>A</title>"))
	file.Write(warcMember(t, "response", "https://b.example/", "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\n\r\nmissing"))
	file.Write(warcMember(t, "metadata", "https://c.example/", "k: v"))

	var got []WARCRecord
	if err := IterateWARC(bytes.NewReader(file.Bytes()), func(r WARCRecord) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d records, want 3", len(got))
	}
	if got[0].Header.TargetURI != "https://a.example/" || got[0].Header.HTTPStatus != 200 {
		t.Errorf("record 0 = %+v", got[0].Header)
	}
	if got[0].Header.HTTPMIME != "text/html" {
		t.Errorf("record 0 mime = %q", got[0].Header.HTTPMIME)
	}
	if got[1].Header.HTTPStatus != 404 {
		t.Errorf("record 1 status = %d", got[1].Header.HTTPStatus)
	}
	if string(HTTPBody(got[1].Block)) != "missing" {
		t.Errorf("record 1 body = %q", HTTPBody(got[1].Block))
	}
}

func TestHTTPBodyAndHeaders(t *testing.T) {
	block := []byte("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html>hi</html>")
	if got := string(HTTPBody(block)); got != "<html>hi</html>" {
		t.Errorf("HTTPBody = %q", got)
	}
	if !bytes.Contains(HTTPHeaders(block), []byte("Content-Type")) {
		t.Errorf("HTTPHeaders missing content type")
	}
}

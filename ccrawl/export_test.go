package ccrawl

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// rawRecord builds one gzip-member WARC response record, the same shape a byte
// range fetch returns, so the exporter test does not need the network. It is
// written out longhand rather than through the crawl writer because the
// exporter's job is to pass bytes through untouched, and a fixture it does not
// share with the writer is the only way that stays true.
func rawRecord(t *testing.T, uri string) []byte {
	t.Helper()
	block := "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html>" + uri + "</html>"
	var rec bytes.Buffer
	rec.WriteString("WARC/1.0\r\n")
	rec.WriteString("WARC-Type: response\r\n")
	fmt.Fprintf(&rec, "WARC-Target-URI: %s\r\n", uri)
	rec.WriteString("WARC-Date: 2024-01-02T03:04:05Z\r\n")
	rec.WriteString("WARC-Record-ID: <urn:uuid:00000000-0000-4000-8000-000000000000>\r\n")
	rec.WriteString("Content-Type: application/http; msgtype=response\r\n")
	fmt.Fprintf(&rec, "Content-Length: %d\r\n\r\n", len(block))
	rec.WriteString(block)
	rec.WriteString("\r\n\r\n")
	member, err := gzipMember(rec.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return member
}

func TestWARCExporterRotatesAndCarriesProvenance(t *testing.T) {
	dir := t.TempDir()
	info := WARCInfo{
		Software:    "ccrawl/0.4.0",
		IsPartOf:    "example",
		Description: "warc extraction generated with: ccrawl export example.com/*",
		Format:      "WARC file version 1.0",
		Creator:     "tester <t@example.com>",
	}
	exp := NewWARCExporter(dir, "example", "", 1, info) // tiny size forces rotation
	exp.now = func() time.Time { return time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC) }

	uris := []string{"https://a.example.com/", "https://b.example.com/", "https://c.example.com/"}
	for _, u := range uris {
		if err := exp.Write(rawRecord(t, u)); err != nil {
			t.Fatalf("Write %s: %v", u, err)
		}
	}
	if err := exp.Close(); err != nil {
		t.Fatal(err)
	}

	if exp.Records() != len(uris) {
		t.Fatalf("Records() = %d, want %d", exp.Records(), len(uris))
	}
	if len(exp.Files) != len(uris) {
		t.Fatalf("rotated into %d files, want %d", len(exp.Files), len(uris))
	}

	for i, path := range exp.Files {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		var types []string
		var sawInfo bool
		err = IterateWARC(f, func(r WARCRecord) error {
			types = append(types, r.Header.Type)
			if r.Header.Type == "warcinfo" {
				sawInfo = true
				body := string(r.Block)
				for _, want := range []string{"software: ccrawl/0.4.0", "isPartOf: example", "creator: tester"} {
					if !strings.Contains(body, want) {
						t.Errorf("file %d warcinfo missing %q in:\n%s", i, want, body)
					}
				}
			}
			return nil
		})
		_ = f.Close()
		if err != nil {
			t.Fatalf("iterate %s: %v", path, err)
		}
		if !sawInfo {
			t.Errorf("file %d has no warcinfo record", i)
		}
		if len(types) != 2 || types[0] != "warcinfo" || types[1] != "response" {
			t.Errorf("file %d record types = %v, want [warcinfo response]", i, types)
		}
	}
}

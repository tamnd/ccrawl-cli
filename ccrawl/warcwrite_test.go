package ccrawl

import (
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// readWARCDir reads every file a writer produced, in order, and returns the
// records. Going back through IterateWARC rather than checking the bytes we
// just wrote is the point: it proves the framing is real and not just a string
// that looks like a WARC record.
func readWARCDir(t *testing.T, w *WARCWriter) []WARCRecord {
	t.Helper()
	var out []WARCRecord
	for _, path := range w.Files() {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		err = IterateWARC(f, func(r WARCRecord) error {
			out = append(out, r)
			return nil
		})
		_ = f.Close()
		if err != nil {
			t.Fatalf("iterate %s: %v", path, err)
		}
	}
	return out
}

func crawlOne(t *testing.T, url string, cfg CrawlConfig) *CrawlResult {
	t.Helper()
	res, err := CrawlURL(context.Background(), url, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestWARCWriterWritesAPairedRecordSet(t *testing.T) {
	const body = "<html><body>hello warc</body></html>"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	res := crawlOne(t, srv.URL+"/page", DefaultCrawlConfig)

	dir := t.TempDir()
	w := NewWARCWriter(dir, "test", 0, WARCInfo{Software: "ccrawl/test"})
	if err := w.Write(NewWARCCapture(res)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if w.Records() != 1 {
		t.Fatalf("Records() = %d, want 1", w.Records())
	}

	recs := readWARCDir(t, w)
	if len(recs) != 3 {
		t.Fatalf("got %d records, want warcinfo plus a request and a response", len(recs))
	}
	var info, req, resp WARCRecord
	for _, r := range recs {
		switch r.Header.Type {
		case "warcinfo":
			info = r
		case "request":
			req = r
		case "response":
			resp = r
		default:
			t.Fatalf("unexpected record type %q", r.Header.Type)
		}
	}
	if info.Header.RecordID == "" {
		t.Fatal("warcinfo has no record ID")
	}
	if !strings.Contains(string(info.Block), "software: ccrawl/test") {
		t.Errorf("warcinfo missing the software field: %q", info.Block)
	}
	for _, r := range []WARCRecord{req, resp} {
		if r.Header.WarcinfoID != info.Header.RecordID {
			t.Errorf("%s WARC-Warcinfo-ID = %q, want %q", r.Header.Type, r.Header.WarcinfoID, info.Header.RecordID)
		}
	}
	if req.Header.ConcurrentTo != resp.Header.RecordID {
		t.Errorf("request concurrent-to %q, want the response %q", req.Header.ConcurrentTo, resp.Header.RecordID)
	}
	if resp.Header.ConcurrentTo != req.Header.RecordID {
		t.Errorf("response concurrent-to %q, want the request %q", resp.Header.ConcurrentTo, req.Header.RecordID)
	}
	if resp.Header.TargetURI != res.FinalURL {
		t.Errorf("target URI = %q, want %q", resp.Header.TargetURI, res.FinalURL)
	}
	if resp.Header.IPAddress != "127.0.0.1" && resp.Header.IPAddress != "::1" {
		t.Errorf("WARC-IP-Address = %q, want the loopback address", resp.Header.IPAddress)
	}
	if got := string(HTTPBody(resp.Block)); got != body {
		t.Errorf("stored body = %q, want %q", got, body)
	}
	if got := string(HTTPHeaders(req.Block)); !strings.HasPrefix(got, "GET /page HTTP/1.1\r\n") {
		t.Errorf("request block does not start with the request line: %q", got)
	}
	if !strings.Contains(string(req.Block), "User-Agent: "+DefaultCrawlConfig.UserAgent) {
		t.Errorf("request block is missing the user agent: %q", req.Block)
	}
}

func TestWARCWriterDigestsVerifyAgainstTheBlockAndPayload(t *testing.T) {
	const body = "digest me"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	dir := t.TempDir()
	w := NewWARCWriter(dir, "test", 0, WARCInfo{})
	if err := w.Write(NewWARCCapture(crawlOne(t, srv.URL, DefaultCrawlConfig))); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var checked int
	for _, r := range readWARCDir(t, w) {
		if r.Header.Type == "warcinfo" {
			continue
		}
		if got := WARCDigest(r.Block); got != r.Header.BlockDigest {
			t.Errorf("%s block digest = %q, recomputes to %q", r.Header.Type, r.Header.BlockDigest, got)
		}
		if !strings.HasPrefix(r.Header.BlockDigest, "sha1:") || len(r.Header.BlockDigest) != len("sha1:")+32 {
			t.Errorf("%s block digest %q is not sha1 base32", r.Header.Type, r.Header.BlockDigest)
		}
		if r.Header.Type == "response" {
			if got := WARCDigest(HTTPBody(r.Block)); got != r.Header.PayloadDigest {
				t.Errorf("payload digest = %q, recomputes to %q", r.Header.PayloadDigest, got)
			}
			if r.Header.PayloadDigest != WARCDigest([]byte(body)) {
				t.Errorf("payload digest is not the digest of the body we served")
			}
		}
		checked++
	}
	if checked != 2 {
		t.Fatalf("checked %d records, want 2", checked)
	}
}

// A gzipped and chunked response is the case the old writer could not have got
// right: Go dechunks and decodes on the way in, so the Content-Length and the
// Content-Encoding the server sent describe bytes that are no longer there.
func TestWARCWriterRewritesLengthForDecodedBodies(t *testing.T) {
	body := strings.Repeat("compress me. ", 500)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "text/plain")
		// No Content-Length, so net/http chunks the response.
		gz := gzip.NewWriter(w)
		_, _ = gz.Write([]byte(body))
		_ = gz.Close()
	}))
	defer srv.Close()

	dir := t.TempDir()
	w := NewWARCWriter(dir, "test", 0, WARCInfo{})
	if err := w.Write(NewWARCCapture(crawlOne(t, srv.URL, DefaultCrawlConfig))); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	for _, r := range readWARCDir(t, w) {
		if r.Header.Type != "response" {
			continue
		}
		headers := string(HTTPHeaders(r.Block))
		if strings.Contains(headers, "Content-Encoding") {
			t.Errorf("stored headers still claim a Content-Encoding we decoded away: %q", headers)
		}
		if strings.Contains(headers, "Transfer-Encoding") {
			t.Errorf("stored headers still claim a Transfer-Encoding: %q", headers)
		}
		payload := HTTPBody(r.Block)
		if string(payload) != body {
			t.Fatalf("stored body is %d bytes, want %d", len(payload), len(body))
		}
		want := strconv.Itoa(len(body))
		if !strings.Contains(headers, "Content-Length: "+want) {
			t.Errorf("stored Content-Length does not match the %s byte body: %q", want, headers)
		}
		if strings.Count(headers, "Content-Length:") != 1 {
			t.Errorf("stored headers have more than one Content-Length: %q", headers)
		}
	}
}

func TestWARCWriterFlagsTruncatedBodies(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 4096)))
	}))
	defer srv.Close()

	cfg := DefaultCrawlConfig
	cfg.MaxBody = 100
	res := crawlOne(t, srv.URL, cfg)
	if !res.Truncated {
		t.Fatal("a 4096 byte body under a 100 byte cap should be truncated")
	}
	if len(res.Body) != 100 {
		t.Fatalf("kept %d bytes, want the 100 byte cap", len(res.Body))
	}

	dir := t.TempDir()
	w := NewWARCWriter(dir, "test", 0, WARCInfo{})
	if err := w.Write(NewWARCCapture(res)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	for _, r := range readWARCDir(t, w) {
		if r.Header.Type != "response" {
			continue
		}
		if r.Header.Truncated != "length" {
			t.Errorf("WARC-Truncated = %q, want \"length\"", r.Header.Truncated)
		}
		if n := len(HTTPBody(r.Block)); n != 100 {
			t.Errorf("stored body is %d bytes, want 100", n)
		}
	}
}

func TestWARCWriterRotatesWithoutSplittingAPair(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, "page %s", r.URL.Path)
	}))
	defer srv.Close()

	dir := t.TempDir()
	w := NewWARCWriter(dir, "rotate", 1, WARCInfo{}) // one byte forces a file per capture
	for i := range 3 {
		res := crawlOne(t, fmt.Sprintf("%s/p%d", srv.URL, i), DefaultCrawlConfig)
		if err := w.Write(NewWARCCapture(res)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	files := w.Files()
	if len(files) != 3 {
		t.Fatalf("rotated into %d files, want 3", len(files))
	}
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		var types []string
		err = IterateWARC(f, func(r WARCRecord) error {
			types = append(types, r.Header.Type)
			return nil
		})
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"warcinfo", "request", "response"}
		if strings.Join(types, ",") != strings.Join(want, ",") {
			t.Errorf("%s holds %v, want %v", path, types, want)
		}
	}
}

func TestWARCWriterStampsTheDateItWasGiven(t *testing.T) {
	when := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	dir := t.TempDir()
	w := NewWARCWriter(dir, "test", 0, WARCInfo{})
	w.now = func() time.Time { return when }
	err := w.Write(WARCCapture{
		URL:       "https://example.com/",
		FetchedAt: when,
		Request:   []byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"),
		Response:  []byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n"),
		Body:      []byte("hi"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for _, r := range readWARCDir(t, w) {
		if !r.Header.Date.Equal(when) {
			t.Errorf("%s WARC-Date = %s, want %s", r.Header.Type, r.Header.Date, when)
		}
	}
}

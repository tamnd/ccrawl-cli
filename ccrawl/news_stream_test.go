package ccrawl

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	kgzip "github.com/klauspost/compress/gzip"
	"github.com/parquet-go/parquet-go"
)

// newsMember builds one gzip member holding one stored response, which is what a
// CC-NEWS WARC is a concatenation of.
func newsMember(t *testing.T, uri, body string) []byte {
	t.Helper()
	payload := "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n" + body
	rec := fmt.Sprintf("WARC/1.0\r\n"+
		"WARC-Type: response\r\n"+
		"WARC-Target-URI: %s\r\n"+
		"WARC-Date: 2026-07-01T02:25:01Z\r\n"+
		"Content-Type: application/http; msgtype=response\r\n"+
		"Content-Length: %d\r\n"+
		"\r\n%s\r\n\r\n", uri, len(payload), payload)

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

// TestIndexNewsFileResumeKeepsEveryRow is the regression test for a published
// month that reported more 2xx rows than rows, which cannot happen because the
// 2xx rows are a subset of the rows.
//
// Rows are buffered and handed to the Parquet writer 512 at a time, so when a
// stream dies there are rows counted but not yet written. The resume point used
// to move on every record parsed, so the retry started after those rows and the
// shard never got them: the tallies were right and the data was short. The count
// mismatch was the visible half of silent row loss.
//
// The WARC here has more records than fit in one buffer and the first read dies
// after the buffer has flushed once, so the drop lands mid-buffer with a flush
// already behind it, which is the case that lost rows.
func TestIndexNewsFileResumeKeepsEveryRow(t *testing.T) {
	const records = 700

	var file bytes.Buffer
	ends := make([]int, 0, records)
	for i := range records {
		file.Write(newsMember(t, fmt.Sprintf("https://news.example/a/%03d", i), fmt.Sprintf("<title>story %d</title>", i)))
		ends = append(ends, file.Len())
	}
	raw := file.Bytes()

	// Cut inside record 595: past the flush at 512 and well short of the end.
	cut := ends[594] - 20

	var reqs int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs++
		rng := r.Header.Get("Range")
		if rng == "" {
			// Promise the whole file, deliver part of it, then drop the
			// connection, which is how a long download actually fails.
			w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(raw[:cut])
			panic(http.ErrAbortHandler)
		}
		start, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(rng, "bytes="), "-"))
		if err != nil {
			t.Errorf("bad range header %q: %v", rng, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(raw)-1, len(raw)))
		w.Header().Set("Content-Length", strconv.Itoa(len(raw)-start))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(raw[start:])
	}))
	defer srv.Close()

	dir := t.TempDir()
	j := newsFileJob{
		sourceURL: srv.URL + "/CC-NEWS-20260701022501-08467.warc.gz",
		warcPath:  "crawl-data/CC-NEWS/2026/07/CC-NEWS-20260701022501-08467.warc.gz",
		tmpPath:   filepath.Join(dir, "shard.parquet.tmp"),
		outPath:   filepath.Join(dir, "shard.parquet"),
	}
	h := NewHTTPClient(Config{Retries: 2, Backoff: time.Millisecond, BackoffMax: 5 * time.Millisecond, DataDir: dir})

	rows, size, st, err := indexNewsFile(context.Background(), h, j)
	if err != nil {
		t.Fatalf("indexNewsFile: %v", err)
	}
	if reqs < 2 {
		t.Fatalf("server saw %d requests, so the stream never dropped and the test proves nothing", reqs)
	}
	if rows != records {
		t.Errorf("wrote %d rows, want %d", rows, records)
	}
	if size <= 0 {
		t.Errorf("shard is %d bytes", size)
	}
	// The tallies count a subset of the rows, so they can never exceed them.
	if st.Rows2xx > rows {
		t.Errorf("rows_2xx = %d exceeds rows = %d", st.Rows2xx, rows)
	}
	if st.Rows2xx != records || st.RowsHTML != records {
		t.Errorf("rows_2xx = %d, rows_html = %d, want %d each", st.Rows2xx, st.RowsHTML, records)
	}
	if st.SourceBytes != int64(len(raw)) {
		t.Errorf("source_bytes = %d, want %d", st.SourceBytes, len(raw))
	}

	// Every record has to be in the shard exactly once. A resume that overshoots
	// drops rows and one that undershoots duplicates them, and both leave the row
	// count looking plausible until you read the URLs back.
	got, err := parquet.ReadFile[NewsRow](j.outPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != records {
		t.Fatalf("read back %d rows, want %d", len(got), records)
	}
	seen := make(map[string]int, records)
	for _, r := range got {
		seen[r.URL]++
	}
	for i := range records {
		u := fmt.Sprintf("https://news.example/a/%03d", i)
		if seen[u] != 1 {
			t.Errorf("%s appears %d times, want 1", u, seen[u])
		}
	}
}

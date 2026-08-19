package ccrawl

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestSearchNewsIndexOnlyOpensPublishedShards pins how a search finds its
// shards. A month names 353 WARCs and a month that is part way published has a
// shard for some of them, so letting each worker discover a 404 costs a round
// trip per shard that is not there, and the ones that are there each pay another
// round trip to ask how big they are. One batched question to the hub answers
// both, and this checks that nothing goes looking for the rest.
func TestSearchNewsIndexOnlyOpensPublishedShards(t *testing.T) {
	// Three WARCs in the month, one of them indexed.
	warcs := []string{
		"crawl-data/CC-NEWS/2026/07/CC-NEWS-20260701022501-08467.warc.gz",
		"crawl-data/CC-NEWS/2026/07/CC-NEWS-20260701052811-08468.warc.gz",
		"crawl-data/CC-NEWS/2026/07/CC-NEWS-20260701073816-08469.warc.gz",
	}
	published := newsShardPath(warcs[1])

	dir := t.TempDir()
	local := filepath.Join(dir, "shard.parquet")
	w, err := NewParquetWriter[NewsRow](local)
	if err != nil {
		t.Fatal(err)
	}
	rows := []NewsRow{
		{URL: "https://www.mehrnews.com/a", URLHostName: "www.mehrnews.com", FetchStatus: 200},
		{URL: "https://www.bbc.co.uk/b", URLHostName: "www.bbc.co.uk", FetchStatus: 200},
		{URL: "https://www.mehrnews.com/c", URLHostName: "www.mehrnews.com", FetchStatus: 200},
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	shard, err := os.ReadFile(local)
	if err != nil {
		t.Fatal(err)
	}

	var manifest bytes.Buffer
	zw := gzip.NewWriter(&manifest)
	if _, err := zw.Write([]byte(strings.Join(warcs, "\n") + "\n")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var pathsInfo int
	var asked []string
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "warc.paths.gz"):
			_, _ = rw.Write(manifest.Bytes())
		case strings.Contains(r.URL.Path, "/paths-info/"):
			mu.Lock()
			pathsInfo++
			mu.Unlock()
			var body struct {
				Paths []string `json:"paths"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			out := []map[string]any{}
			for _, p := range body.Paths {
				if p == published {
					out = append(out, map[string]any{"type": "file", "path": p, "size": len(shard)})
				}
			}
			rw.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(rw).Encode(out)
		case strings.Contains(r.URL.Path, "/resolve/main/"):
			p := r.URL.Path[strings.Index(r.URL.Path, "/resolve/main/")+len("/resolve/main/"):]
			mu.Lock()
			asked = append(asked, p)
			mu.Unlock()
			if p != published {
				rw.WriteHeader(http.StatusNotFound)
				return
			}
			http.ServeContent(rw, r, "shard.parquet", time.Time{}, bytes.NewReader(shard))
		default:
			rw.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	prevHF, prevData := hfEndpoint, Endpoints.Data
	hfEndpoint = srv.URL
	Endpoints.Data = srv.URL + "/"
	t.Cleanup(func() { hfEndpoint, Endpoints.Data = prevHF, prevData })

	h := NewHTTPClient(Config{Retries: 1, Backoff: time.Millisecond, BackoffMax: 5 * time.Millisecond, DataDir: dir})
	var got []string
	hits, err := SearchNewsIndex(context.Background(), h, NewsSearchOptions{
		Repo: "open-index/ccrawl-news", Year: 2026, Month: 7, Host: "mehrnews.com", Workers: 2,
	}, func(r NewsRow) error {
		got = append(got, r.URL)
		return nil
	})
	if err != nil {
		t.Fatalf("SearchNewsIndex: %v", err)
	}
	if hits != 2 || len(got) != 2 {
		t.Errorf("got %d hits %v, want 2", hits, got)
	}
	if pathsInfo != 1 {
		t.Errorf("asked paths-info %d times, want 1 for a month of 3 candidates", pathsInfo)
	}
	// The two unpublished shards must never be requested, and the published one
	// must not be asked for its size, since paths-info already reported it.
	for _, p := range asked {
		if p != published {
			t.Errorf("requested %s, which is not published", p)
		}
	}
	if len(asked) == 0 {
		t.Error("the published shard was never read")
	}
}

// TestSearchNewsIndexEmptyMonth checks that a month with nothing published is an
// empty answer rather than an error, so the caller can fall back to the scan.
func TestSearchNewsIndexEmptyMonth(t *testing.T) {
	warcs := []string{"crawl-data/CC-NEWS/2026/07/CC-NEWS-20260701022501-08467.warc.gz"}
	var manifest bytes.Buffer
	zw := gzip.NewWriter(&manifest)
	if _, err := zw.Write([]byte(strings.Join(warcs, "\n") + "\n")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "warc.paths.gz"):
			_, _ = rw.Write(manifest.Bytes())
		case strings.Contains(r.URL.Path, "/paths-info/"):
			_, _ = fmt.Fprint(rw, "[]")
		default:
			t.Errorf("unexpected request for %s", r.URL.Path)
			rw.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	prevHF, prevData := hfEndpoint, Endpoints.Data
	hfEndpoint = srv.URL
	Endpoints.Data = srv.URL + "/"
	t.Cleanup(func() { hfEndpoint, Endpoints.Data = prevHF, prevData })

	h := NewHTTPClient(Config{Retries: 1, Backoff: time.Millisecond, BackoffMax: 5 * time.Millisecond, DataDir: t.TempDir()})
	hits, err := SearchNewsIndex(context.Background(), h, NewsSearchOptions{
		Repo: "open-index/ccrawl-news", Year: 2026, Month: 7, Host: "mehrnews.com",
	}, func(NewsRow) error { return nil })
	if err != nil {
		t.Fatalf("SearchNewsIndex on an unpublished month: %v", err)
	}
	if hits != 0 {
		t.Errorf("got %d hits, want 0", hits)
	}
}

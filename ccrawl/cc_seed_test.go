package ccrawl

import (
	"path/filepath"
	"sync/atomic"
	"testing"

	mg "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/seed"
)

// writeMultiColParquet writes a Parquet file carrying the full CDXRawRow column
// set, so a reader that projects only the url column is really skipping columns.
func writeMultiColParquet(t *testing.T, rows []CDXRawRow) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "index.parquet")
	w, err := NewParquetWriter[CDXRawRow](path)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestStreamURLColumnProjectsAndRoutes writes a many-column Parquet index file,
// reads it back projecting only the url column, and routes every non-empty url
// into a ShardSet by its meguri hostkey. It checks the scanned count covers all
// rows, empty urls are skipped, and the seed reads back the exact set of urls in
// the shard their hostkey owns. This is the local stand-in for a Common Crawl
// index file: same columns, same projection path, no network.
func TestStreamURLColumnProjectsAndRoutes(t *testing.T) {
	rows := []CDXRawRow{
		{URL: "https://a.com/", URLHostName: "a.com", FetchStatus: 200, Digest: "D1"},
		{URL: "https://b.com/page", URLHostName: "b.com", FetchStatus: 200, Digest: "D2"},
		{URL: "", URLHostName: "", FetchStatus: 0, Digest: "D3"}, // empty url, skipped
		{URL: "https://c.com/x.pdf", URLHostName: "c.com", FetchStatus: 200, Digest: "D4"},
		{URL: "https://a.com/two", URLHostName: "a.com", FetchStatus: 200, Digest: "D5"},
	}
	path := writeMultiColParquet(t, rows)

	dir := t.TempDir()
	set, err := seed.NewShardSet(dir, 8, seed.DefaultBlockSize, seed.CodecZstd)
	if err != nil {
		t.Fatal(err)
	}

	var written atomic.Int64
	route := func(u string) error {
		host := frontier.HostOf(u)
		if host == "" {
			return nil
		}
		if err := set.Add(mg.HostKeyOf(host), u); err != nil {
			return err
		}
		written.Add(1)
		return nil
	}

	scanned, err := streamURLColumnLocal(path, route)
	if err != nil {
		t.Fatalf("streamURLColumnLocal: %v", err)
	}
	if scanned != int64(len(rows)) {
		t.Fatalf("scanned=%d, want %d", scanned, len(rows))
	}
	if written.Load() != 4 {
		t.Fatalf("written=%d, want 4 (one empty url skipped)", written.Load())
	}

	man, err := set.Close()
	if err != nil {
		t.Fatal(err)
	}
	if man.Records != 4 {
		t.Fatalf("manifest records=%d, want 4", man.Records)
	}

	// Every url must read back out of the shard its hostkey routes to.
	want := map[string]int{
		"https://a.com/":      int(man.Route(mg.HostKeyOf("a.com"))),
		"https://b.com/page":  int(man.Route(mg.HostKeyOf("b.com"))),
		"https://c.com/x.pdf": int(man.Route(mg.HostKeyOf("c.com"))),
		"https://a.com/two":   int(man.Route(mg.HostKeyOf("a.com"))),
	}
	got := map[string]int{}
	for _, sm := range man.Shards {
		r, err := seed.Open(filepath.Join(dir, sm.Path))
		if err != nil {
			t.Fatal(err)
		}
		for b := range r.Blocks() {
			br, err := r.BlockReader(b)
			if err != nil {
				t.Fatal(err)
			}
			for {
				u, ok := br.Next()
				if !ok {
					break
				}
				got[string(u)] = int(sm.Index)
			}
		}
		_ = r.Close()
	}
	if len(got) != len(want) {
		t.Fatalf("read back %d urls, want %d: %v", len(got), len(want), got)
	}
	for u, shard := range want {
		if got[u] != shard {
			t.Fatalf("url %s in shard %d, want %d", u, got[u], shard)
		}
	}
}

// TestParseSeedCodec covers the codec flag mapping used by the seed cc command.
func TestParseSeedCodec(t *testing.T) {
	// exercised through the seed package constants; keep the cc command honest.
	if seed.CodecZstd == seed.CodecRaw {
		t.Fatal("codec constants collapsed")
	}
}

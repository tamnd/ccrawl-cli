package ccrawl

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
	mg "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/seed"
)

// buildTestSeed writes a small sharded .seed from a list of URLs, the same way
// `ccrawl seed cc` does, so the publish path can be exercised without a network
// pull.
func buildTestSeed(t *testing.T, dir string, urls []string) {
	t.Helper()
	set, err := seed.NewShardSet(dir, 4, 0, seed.CodecZstd)
	if err != nil {
		t.Fatalf("new shard set: %v", err)
	}
	for _, u := range urls {
		host := frontier.HostOf(u)
		if host == "" {
			continue
		}
		if err := set.Add(mg.HostKeyOf(host), u); err != nil {
			t.Fatalf("add %q: %v", u, err)
		}
	}
	if _, err := set.Close(); err != nil {
		t.Fatalf("close shard set: %v", err)
	}
}

// readParquetURLs reads back every url from a transcoded shard.
func readParquetURLs(t *testing.T, path string) []seedURLRow {
	t.Helper()
	rows, err := parquet.ReadFile[seedURLRow](path)
	if err != nil {
		t.Fatalf("read parquet %s: %v", path, err)
	}
	return rows
}

func TestRunSeedPublishNoPush(t *testing.T) {
	seedDir := t.TempDir()
	urls := []string{
		"https://en.wikipedia.org/wiki/Go",
		"https://en.wikipedia.org/wiki/Rust",
		"https://example.com/a",
		"https://example.com/b",
		"https://news.ycombinator.com/item?id=1",
		"http://go.dev/doc",
	}
	buildTestSeed(t, seedDir, urls)

	out := filepath.Join(t.TempDir(), "pq")
	ledger, err := OpenLedger(filepath.Join(t.TempDir(), "led"))
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	defer func() { _ = ledger.Close() }()

	man, err := seed.ReadManifest(seedDir)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	st, err := RunSeedPublish(context.Background(), NewHFClient("no-token"), SeedPublishConfig{
		SeedDir:     seedDir,
		CrawlID:     "CC-MAIN-TEST",
		OutDir:      out,
		Repo:        "open-index/test",
		Push:        false,
		KeepParquet: true,
		Ledger:      ledger,
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Every URL with a host must survive the round trip, exactly once.
	if st.Failed != 0 {
		t.Fatalf("got %d failed shards", st.Failed)
	}
	if st.Published != len(man.Shards) {
		t.Fatalf("published %d shards, want %d", st.Published, len(man.Shards))
	}
	if st.Rows != int64(len(urls)) {
		t.Fatalf("published %d rows, want %d", st.Rows, len(urls))
	}

	// Collect the URLs from every shard Parquet and check the set matches, and
	// that host was derived correctly and matches the shard the URL routed into.
	seen := map[string]bool{}
	for _, sm := range man.Shards {
		pqPath := filepath.Join(out, "shard-"+pad5(sm.Index)+".parquet")
		for _, row := range readParquetURLs(t, pqPath) {
			if row.Host != frontier.HostOf(row.URL) {
				t.Fatalf("row %q host %q != derived %q", row.URL, row.Host, frontier.HostOf(row.URL))
			}
			if man.Route(mg.HostKeyOf(row.Host)) != sm.Index {
				t.Fatalf("url %q landed in shard %d, routes to %d", row.URL, sm.Index, man.Route(mg.HostKeyOf(row.Host)))
			}
			seen[row.URL] = true
		}
	}
	for _, u := range urls {
		if !seen[u] {
			t.Fatalf("url %q missing from published shards", u)
		}
	}

	// A second run with the same ledger must skip every shard.
	st2, err := RunSeedPublish(context.Background(), NewHFClient("no-token"), SeedPublishConfig{
		SeedDir: seedDir, CrawlID: "CC-MAIN-TEST", OutDir: out,
		Repo: "open-index/test", Push: false, KeepParquet: true, Ledger: ledger,
	})
	if err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if st2.Skipped != len(man.Shards) || st2.Published != 0 {
		t.Fatalf("resume: skipped %d published %d, want skipped %d published 0",
			st2.Skipped, st2.Published, len(man.Shards))
	}
}

func pad5(n int) string {
	s := "00000" + itoa(n)
	return s[len(s)-5:]
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

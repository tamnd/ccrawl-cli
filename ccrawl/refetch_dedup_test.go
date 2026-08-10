package ccrawl

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/tamnd/ami/config"
)

// TestRefetchDedupDigest runs the refetch pipeline end to end against a local
// server: the WARC shard, the URLs inside it, and the pages they point at all
// come from one httptest server, so the fetch is real without leaving the box.
// Three of the six pages serve byte identical HTML, so --dedup-digest must write
// four rows and report two drops.
func TestRefetchDedupDigest(t *testing.T) {
	same := articlePage("Common Crawl publishes a web archive every month.")
	pages := []string{
		same,
		articlePage("A different page about bread and flour."),
		same,
		articlePage("A third page about mountains and rivers."),
		same,
		articlePage("A fourth page about trains and timetables."),
	}

	mux := http.NewServeMux()
	for i, body := range pages {
		mux.HandleFunc(fmt.Sprintf("/page/%d", i), func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(body))
		})
	}
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// The shard's records only have to carry the target URIs; the bodies here are
	// never used, because refetch throws the archived copy away and fetches live.
	var shard bytes.Buffer
	for i := range pages {
		shard.Write(warcMember(t, fmt.Sprintf("%s/page/%d", srv.URL, i), "<html><body>archived</body></html>"))
	}
	mux.HandleFunc("/shard.warc.gz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(shard.Bytes())
	})

	fetchCfg := config.Default()
	fetchCfg.Workers = 4
	fetchCfg.StartInflight = 4
	fetchCfg.MinInflight = 1
	fetchCfg.Timeout = 5 * time.Second
	fetchCfg.HeaderTimeout = 3 * time.Second
	fetchCfg.MaxRetries = 0

	run := func(dedup bool, name string) RefetchStats {
		out := filepath.Join(t.TempDir(), name)
		stats, err := PackRefetchShard(context.Background(), NewHTTPClient(Config{}), RefetchPackConfig{
			CrawlID:  "CC-MAIN-TEST",
			WARCPath: srv.URL + "/shard.warc.gz",
			OutPath:  out,
			FetchCfg: fetchCfg,

			DedupDigest: dedup,
		})
		if err != nil {
			t.Fatalf("PackRefetchShard(dedup=%v): %v", dedup, err)
		}
		rows, rerr := parquet.ReadFile[RefetchRow](out)
		if rerr != nil {
			t.Fatalf("read parquet: %v", rerr)
		}
		var content int64
		for _, r := range rows {
			if r.Markdown != "" || r.HTML != "" {
				content++
			}
			if r.Error == "" && r.Markdown != "" && r.Simhash == 0 {
				t.Fatalf("row %s converted but has no simhash", r.URL)
			}
		}
		// Rows is what the dataset card publishes, so it has to be the number of
		// content rows actually in the file, not the number that were fetched.
		if content != stats.Rows {
			t.Fatalf("stats say %d rows, parquet holds %d content rows out of %d", stats.Rows, content, len(rows))
		}
		return stats
	}

	base := run(false, "plain.parquet")
	if base.URLsFound != 6 {
		t.Fatalf("URLsFound = %d, want 6", base.URLsFound)
	}
	if base.Rows != 6 || base.DigestDropped != 0 {
		t.Fatalf("without dedup: Rows = %d, DigestDropped = %d, want 6 and 0", base.Rows, base.DigestDropped)
	}

	got := run(true, "dedup.parquet")
	if got.DigestDropped != 2 {
		t.Fatalf("DigestDropped = %d, want 2", got.DigestDropped)
	}
	if got.Rows != 4 {
		t.Fatalf("Rows = %d, want 4", got.Rows)
	}
}

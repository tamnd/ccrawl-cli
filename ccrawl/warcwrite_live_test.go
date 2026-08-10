package ccrawl

import (
	"context"
	"os"
	"strings"
	"testing"
)

// Writing a WARC from live sites, for checking against an outside tool.
//
// The unit tests prove the writer agrees with itself. They cannot prove it
// agrees with warcio, and a WARC file only earns the name if something that did
// not write it can read it. This test fetches real pages into one rotating
// writer, so the output has several pairs per file and more than one file, and
// leaves the result on disk:
//
//	CCRAWL_WARC_OUT=/tmp/warcout go test ./ccrawl/ -run TestWARCWriterAgainstRealSites -v
//	warcio check -v /tmp/warcout/*.warc.gz
//
// It is skipped without the variable because it needs the network and because a
// test that leaves files behind should be asked for.
func TestWARCWriterAgainstRealSites(t *testing.T) {
	dir := os.Getenv("CCRAWL_WARC_OUT")
	if dir == "" {
		t.Skip("set CCRAWL_WARC_OUT to a directory to write a WARC from live sites")
	}
	urls := []string{
		"https://example.com/",
		"https://go.dev/",
		"https://en.wikipedia.org/wiki/Main_Page",
		"https://news.ycombinator.com/",
		"https://www.rfc-editor.org/rfc/rfc9309.html",
		"https://commoncrawl.org/",
		"https://httpbin.org/gzip",
		"https://httpbin.org/stream-bytes/2048",
		"https://golang.org/doc/",
		"https://www.iana.org/domains/reserved",
	}
	if list := os.Getenv("CCRAWL_WARC_URLS"); list != "" {
		urls = strings.Split(list, ",")
	}

	// Small enough that ten captures land in several files, which is the only way
	// to see rotation and per-file warcinfo on real output.
	w := NewWARCWriter(dir, "ccrawl-live", 64<<10, WARCInfo{
		Software:    "ccrawl/test",
		IsPartOf:    "ccrawl-live",
		Description: "warc written by TestWARCWriterAgainstRealSites",
	})
	var fetched, failed int
	for _, u := range urls {
		res, err := CrawlURL(context.Background(), u, DefaultCrawlConfig)
		if err != nil {
			t.Logf("fetch %s: %v", u, err)
			failed++
			continue
		}
		if err := w.Write(NewWARCCapture(res)); err != nil {
			t.Fatalf("write %s: %v", u, err)
		}
		fetched++
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if fetched == 0 {
		t.Fatal("no URL could be fetched, nothing to check")
	}
	t.Logf("%d captures, %d fetch failures, %d files", fetched, failed, len(w.Files()))
	for _, f := range w.Files() {
		t.Log(f)
	}
}

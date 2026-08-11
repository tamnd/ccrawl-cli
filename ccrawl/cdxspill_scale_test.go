package ccrawl

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// The scale harness behind the memory numbers for E8. It is off by default
// because it runs for minutes and the map half of it is meant to be allowed to
// get large.
//
//	CCRAWL_SCALE_CDX=20000000 go test ./ccrawl -run TestCDX.*Scale -v -timeout 1h
//
// The records are shaped like a domain wildcard across every crawl: one urlkey
// per URL, a few captures each, and the same URLs turning up crawl after crawl.
// Each test reports peak heap for the map it replaced and for the replacement,
// which is the comparison the issue is about.

const scaleTarget = "20240101000000"

// scaleSize reads the harness size, or skips the test when it is not set.
func scaleSize(t *testing.T) (records, crawls, urls int) {
	t.Helper()
	raw := os.Getenv("CCRAWL_SCALE_CDX")
	if raw == "" {
		t.Skip("set CCRAWL_SCALE_CDX to run the scale harness")
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		t.Fatalf("CCRAWL_SCALE_CDX=%q is not a positive count", raw)
	}
	crawls = 10
	if v := os.Getenv("CCRAWL_SCALE_CDX_CRAWLS"); v != "" {
		if c, err := strconv.Atoi(v); err == nil && c > 0 {
			crawls = c
		}
	}
	return n, crawls, (n / crawls) / 3 // three captures per URL inside one crawl
}

// scaleStream calls fn with one crawl's records, in urlkey order, without ever
// holding them, so the harness measures the implementations rather than the
// fixture.
func scaleStream(crawl, urls int, fn func(CDXRecord) error) error {
	for u := 0; u < urls; u++ {
		key := fmt.Sprintf("com,example)/p/%09d", u)
		full := "https://example.com/p/" + strconv.Itoa(u)
		for c := 0; c < 3; c++ {
			if err := fn(CDXRecord{
				CrawlID:   fmt.Sprintf("CC-MAIN-20%02d-01", 15+crawl),
				URLKey:    key,
				URL:       full,
				Timestamp: fmt.Sprintf("20%02d%02d01000000", 15+crawl, 1+c),
				Digest:    fmt.Sprintf("DIGEST%09d%02d", u, c),
				Status:    "200",
				Filename:  "crawl-data/CC-MAIN/segments/1234567890/warc/CC-MAIN-00000.warc.gz",
				Offset:    "123456789",
				Length:    "12345",
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// scaleMeasure runs one implementation and reports how long it took, how many
// URLs it kept, and the highest heap it was holding while it ran.
func scaleMeasure(t *testing.T, name string, records int, run func() int) {
	t.Helper()
	runtime.GC()
	stop, out := make(chan struct{}), make(chan uint64, 1)
	go func() {
		var peak uint64
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				out <- peak
				return
			case <-tick.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.HeapAlloc > peak {
					peak = m.HeapAlloc
				}
			}
		}
	}()
	start := time.Now()
	kept := run()
	elapsed := time.Since(start)
	close(stop)
	t.Logf("%-8s %9d records in %6.1fs, kept %8d URLs, peak heap %6.0f MB",
		name, records, elapsed.Seconds(), kept, float64(<-out)/(1<<20))
}

// TestCDXPickerScale is the --at half: one nearest capture per URL across every
// crawl in the query.
func TestCDXPickerScale(t *testing.T) {
	n, crawls, urls := scaleSize(t)

	// The implementation E8 is filed against: one map over every crawl.
	scaleMeasure(t, "map", n, func() int {
		nearest := map[string]CDXRecord{}
		for c := 0; c < crawls; c++ {
			if err := scaleStream(c, urls, func(r CDXRecord) error {
				cur, ok := nearest[r.URL]
				if !ok || tsDiff(r.Timestamp, scaleTarget) < tsDiff(cur.Timestamp, scaleTarget) {
					nearest[r.URL] = r
				}
				return nil
			}); err != nil {
				t.Fatalf("stream: %v", err)
			}
		}
		kept := len(nearest)
		clear(nearest)
		return kept
	})

	// The picker, held to a budget far under the result size so it spills.
	scaleMeasure(t, "picker", n, func() int {
		p := NewCDXPicker(func(cand, cur CDXRecord) bool {
			return tsDiff(cand.Timestamp, scaleTarget) < tsDiff(cur.Timestamp, scaleTarget)
		}, 100_000)
		defer func() { _ = p.Close() }()
		for c := 0; c < crawls; c++ {
			if err := scaleStream(c, urls, p.Add); err != nil {
				t.Fatalf("stream: %v", err)
			}
			if err := p.EndStream(); err != nil {
				t.Fatalf("EndStream: %v", err)
			}
		}
		if !p.Spilled() {
			t.Error("a 100k budget should have spilled at this size")
		}
		kept := 0
		if err := p.Each(func(CDXRecord) error { kept++; return nil }); err != nil {
			t.Fatalf("Each: %v", err)
		}
		if kept != urls {
			t.Errorf("picker kept %d URLs, want %d", kept, urls)
		}
		return kept
	})
}

// TestCDXURLLogScale is the --latest-only half: the first capture of each URL,
// with every later crawl checked against what already went out.
func TestCDXURLLogScale(t *testing.T) {
	n, crawls, urls := scaleSize(t)

	// The implementation E8 is filed against: one set of every URL emitted.
	scaleMeasure(t, "map", n, func() int {
		seen := map[string]bool{}
		kept := 0
		for c := 0; c < crawls; c++ {
			if err := scaleStream(c, urls, func(r CDXRecord) error {
				if seen[r.URL] {
					return nil
				}
				seen[r.URL] = true
				kept++
				return nil
			}); err != nil {
				t.Fatalf("stream: %v", err)
			}
		}
		clear(seen)
		return kept
	})

	// The log, held to a budget far under the result size so it spills.
	scaleMeasure(t, "urllog", n, func() int {
		l := NewCDXURLLog(100_000)
		defer func() { _ = l.Close() }()
		kept := 0
		for c := 0; c < crawls; c++ {
			if err := scaleStream(c, urls, func(r CDXRecord) error {
				seen, err := l.Seen(r)
				if err != nil || seen {
					return err
				}
				kept++
				return l.Emitted(r)
			}); err != nil {
				t.Fatalf("stream: %v", err)
			}
			if c < crawls-1 {
				if err := l.EndCrawl(); err != nil {
					t.Fatalf("EndCrawl: %v", err)
				}
			}
		}
		if !l.Spilled() {
			t.Error("a 100k budget should have spilled at this size")
		}
		if kept != urls {
			t.Errorf("url log kept %d URLs, want %d", kept, urls)
		}
		return kept
	})
}

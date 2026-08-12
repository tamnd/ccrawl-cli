package ccrawl

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeCDX is a CDX server holding pages of records, enough of one to exercise
// the paging loop and the error paths through it.
type fakeCDX struct {
	pages [][]string
	// fail maps a page number to the status the server answers with instead of
	// serving it.
	fail map[int]int
	hits map[string]int
}

func (f *fakeCDX) start(t *testing.T) {
	t.Helper()
	f.hits = map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("showNumPages") == "true" {
			f.hits["numpages"]++
			_, _ = fmt.Fprintf(w, `{"pages": %d}`, len(f.pages))
			return
		}
		page := 0
		_, _ = fmt.Sscanf(q.Get("page"), "%d", &page)
		f.hits[q.Get("page")]++
		if code, ok := f.fail[page]; ok {
			w.WriteHeader(code)
			return
		}
		if page < len(f.pages) {
			for _, line := range f.pages[page] {
				_, _ = fmt.Fprintln(w, line)
			}
		}
	}))
	t.Cleanup(srv.Close)

	old := cdxBase
	cdxBase = srv.URL + "/"
	t.Cleanup(func() { cdxBase = old })
}

// cdxLines builds n record lines whose URLs carry the page and index, so a test
// can tell exactly how far the stream got.
func cdxLines(page, n int) []string {
	out := make([]string, n)
	for i := range n {
		out[i] = fmt.Sprintf(`{"urlkey":"com,example)/%d/%d","timestamp":"2026010%d000000","url":"https://example.com/%d/%d","status":"200","filename":"f.warc.gz","offset":"0","length":"1"}`,
			page, i, page, page, i)
	}
	return out
}

func testCDXClient() *HTTPClient {
	cfg := DefaultConfig()
	cfg.Delay = 0
	cfg.GlobalRate = 0
	cfg.Retries = 0
	return NewHTTPClient(cfg)
}

// TestCDXStreamReturnsTheCallbackErrorUnwrapped is the one that matters. Callers
// stop a stream by returning a sentinel and comparing it on the way back, and
// some of them, kit's row limit among them, compare it by identity. Wrapping it
// with the page it came from turned "stop here" into a failed search, which is
// what ccrawl search --limit N used to print.
func TestCDXStreamReturnsTheCallbackErrorUnwrapped(t *testing.T) {
	f := &fakeCDX{pages: [][]string{cdxLines(0, 5), cdxLines(1, 5)}}
	f.start(t)

	sentinel := errors.New("stop")
	seen := 0
	err := CDXStream(context.Background(), testCDXClient(), "CC-MAIN-2026-30", CDXQuery{URL: "example.com/*"}, func(CDXRecord) error {
		seen++
		if seen == 3 {
			return sentinel
		}
		return nil
	})
	// Identity, not errors.Is. errors.Is would pass through the wrap and hide
	// exactly the bug this is here for.
	if err != sentinel {
		t.Fatalf("CDXStream returned %v, want the callback's own error back", err)
	}
	if seen != 3 {
		t.Fatalf("the callback ran %d times, want it to stop the stream at 3", seen)
	}
	if f.hits["1"] != 0 {
		t.Fatal("the stream carried on to page 1 after the callback asked it to stop")
	}
}

func TestCDXStreamStopsAtTheLimit(t *testing.T) {
	f := &fakeCDX{pages: [][]string{cdxLines(0, 5), cdxLines(1, 5), cdxLines(2, 5)}}
	f.start(t)

	var got []string
	err := CDXStream(context.Background(), testCDXClient(), "CC-MAIN-2026-30",
		CDXQuery{URL: "example.com/*", Limit: 7}, func(r CDXRecord) error {
			got = append(got, r.URL)
			return nil
		})
	if err != nil {
		t.Fatalf("hitting the limit is not a failure, got %v", err)
	}
	if len(got) != 7 {
		t.Fatalf("got %d records, want 7", len(got))
	}
	if f.hits["2"] != 0 {
		t.Fatal("the stream fetched page 2 after the limit was already reached on page 1")
	}
}

func TestCDXStreamTagsATransportErrorWithThePage(t *testing.T) {
	f := &fakeCDX{pages: [][]string{cdxLines(0, 2), cdxLines(1, 2)}, fail: map[int]int{1: 500}}
	f.start(t)

	seen := 0
	err := CDXStream(context.Background(), testCDXClient(), "CC-MAIN-2026-30", CDXQuery{URL: "example.com/*"}, func(CDXRecord) error {
		seen++
		return nil
	})
	if err == nil {
		t.Fatal("a page the server would not serve came back as success")
	}
	if !strings.Contains(err.Error(), "CDX page 1") {
		t.Fatalf("the error is %q, want it to say which page failed", err)
	}
	if seen != 2 {
		t.Fatalf("the callback ran %d times, want the 2 records page 0 did serve", seen)
	}
}

// TestCDXStreamTreatsA404AsAnEmptyPage pins behaviour that is easy to mistake
// for an oversight. index.commoncrawl.org answers a query it has nothing for
// with 404 and a "No Captures found" body, so a 404 has to read as no records
// rather than as a failure, which is what lets search exit 3 instead of 1.
func TestCDXStreamTreatsA404AsAnEmptyPage(t *testing.T) {
	f := &fakeCDX{pages: [][]string{{}}, fail: map[int]int{0: 404}}
	f.start(t)

	seen := 0
	err := CDXStream(context.Background(), testCDXClient(), "CC-MAIN-2026-30", CDXQuery{URL: "example.com/*"}, func(CDXRecord) error {
		seen++
		return nil
	})
	if err != nil {
		t.Fatalf("a query with no captures returned %v, want no records and no error", err)
	}
	if seen != 0 {
		t.Fatalf("the callback ran %d times against a 404", seen)
	}
}

func TestCDXStreamStampsTheCrawlID(t *testing.T) {
	f := &fakeCDX{pages: [][]string{cdxLines(0, 3)}}
	f.start(t)

	err := CDXStream(context.Background(), testCDXClient(), "CC-MAIN-2026-30", CDXQuery{URL: "example.com/*"}, func(r CDXRecord) error {
		if r.CrawlID != "CC-MAIN-2026-30" {
			t.Fatalf("record carries crawl %q, want the one it was streamed from", r.CrawlID)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCDXStreamReadsAtLeastOnePage covers the server answering zero pages, which
// it does for some queries that nonetheless have results.
func TestCDXStreamReadsAtLeastOnePage(t *testing.T) {
	f := &fakeCDX{}
	f.start(t)
	f.pages = [][]string{cdxLines(0, 2)}
	// Report zero pages while still serving page 0.
	srvPages := f.pages
	f.pages = nil
	defer func() { f.pages = srvPages }()

	seen := 0
	err := CDXStream(context.Background(), testCDXClient(), "CC-MAIN-2026-30", CDXQuery{URL: "example.com/*"}, func(CDXRecord) error {
		seen++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if f.hits["0"] != 1 {
		t.Fatalf("page 0 was fetched %d times, want 1 even though the server said zero pages", f.hits["0"])
	}
}

func TestCDXSearchCollectsRecords(t *testing.T) {
	f := &fakeCDX{pages: [][]string{cdxLines(0, 4)}}
	f.start(t)

	recs, err := CDXSearch(context.Background(), testCDXClient(), "CC-MAIN-2026-30", CDXQuery{URL: "example.com/*", Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("got %d records, want 3", len(recs))
	}
	if recs[0].URL != "https://example.com/0/0" {
		t.Fatalf("first record is %q", recs[0].URL)
	}
}

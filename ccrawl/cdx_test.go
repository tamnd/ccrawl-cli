package ccrawl

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeCDX is a CDX server holding pages of records, enough of one to exercise
// the paging loop and the error paths through it.
type fakeCDX struct {
	pages [][]string
	// fail maps a page number to the status the server answers with instead of
	// serving it.
	fail map[int]int
	// cut maps a page number to how many times the server promises a full body
	// and then drops the connection part way through it, which is what the real
	// index does under load.
	cut map[int]int
	// numPagesFail, when set, is the status the page count request gets.
	numPagesFail int
	hits         map[string]int
}

func (f *fakeCDX) start(t *testing.T) {
	t.Helper()
	f.hits = map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("showNumPages") == "true" {
			f.hits["numpages"]++
			if f.numPagesFail != 0 {
				w.WriteHeader(f.numPagesFail)
				return
			}
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
		if left, ok := f.cut[page]; ok && left > 0 {
			f.cut[page] = left - 1
			body := strings.Join(f.pages[page], "\n") + "\n"
			// The header promises the whole page and the connection dies half way
			// through it, so the client sees a truncation rather than a short page.
			w.Header().Set("Content-Length", fmt.Sprint(len(body)))
			_, _ = w.Write([]byte(body[:len(body)/2]))
			if fl, ok := w.(http.Flusher); ok {
				fl.Flush()
			}
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					_ = conn.Close()
				}
			}
			return
		}
		if page < len(f.pages) {
			for _, line := range f.pages[page] {
				_, _ = fmt.Fprintln(w, line)
			}
		}
	}))
	t.Cleanup(srv.Close)

	old := Endpoints.CDX
	Endpoints.CDX = srv.URL + "/"
	t.Cleanup(func() { Endpoints.CDX = old })
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

// retryingCDXClient is testCDXClient with retries on and a backoff short enough
// that a test can afford them.
func retryingCDXClient(retries int) *HTTPClient {
	cfg := DefaultConfig()
	cfg.Delay = 0
	cfg.GlobalRate = 0
	cfg.Retries = retries
	cfg.Backoff = time.Millisecond
	cfg.BackoffMax = 2 * time.Millisecond
	return NewHTTPClient(cfg)
}

// TestCDXStreamRetriesAPageThatStopsShort is the whole reason for #102. The
// index promises a page and then drops the connection in the middle of it often
// enough that the same wide query run twice returns a different number of
// records, and nothing in the client noticed: the request succeeded, the status
// was 200, and the read failed a megabyte later.
func TestCDXStreamRetriesAPageThatStopsShort(t *testing.T) {
	f := &fakeCDX{pages: [][]string{cdxLines(0, 4), cdxLines(1, 4)}, cut: map[int]int{1: 1}}
	f.start(t)

	var got []string
	err := CDXStream(context.Background(), retryingCDXClient(2), "CC-MAIN-2026-30", CDXQuery{URL: "example.com/*"}, func(r CDXRecord) error {
		got = append(got, r.URL)
		return nil
	})
	if err != nil {
		t.Fatalf("a page that came back short on the first read failed the stream: %v", err)
	}
	if len(got) != 8 {
		t.Fatalf("got %d records, want all 8: a truncated page has to be read again, not skipped", len(got))
	}
	// The retry re-reads the whole page, so the half that did arrive must not be
	// emitted twice.
	seen := map[string]bool{}
	for _, u := range got {
		if seen[u] {
			t.Fatalf("%s came out twice, so the retry replayed records the first read already emitted", u)
		}
		seen[u] = true
	}
	if f.hits["1"] != 2 {
		t.Fatalf("page 1 was fetched %d times, want 2: one short read and one retry", f.hits["1"])
	}
}

// TestCDXStreamKeepsGoingPastAPageItCannotRead covers the other half of #102: a
// page that fails every retry costs that page, not the thousand after it.
func TestCDXStreamKeepsGoingPastAPageItCannotRead(t *testing.T) {
	f := &fakeCDX{pages: [][]string{cdxLines(0, 2), cdxLines(1, 2), cdxLines(2, 2)}, fail: map[int]int{1: 503}}
	f.start(t)

	var lostPage int
	var lostCrawl string
	var got []string
	q := CDXQuery{URL: "example.com/*", OnPageError: func(crawlID string, page int, err error) error {
		lostCrawl, lostPage = crawlID, page
		return nil
	}}
	err := CDXStream(context.Background(), testCDXClient(), "CC-MAIN-2026-30", q, func(r CDXRecord) error {
		got = append(got, r.URL)
		return nil
	})
	if err != nil {
		t.Fatalf("a handled page failure still ended the stream: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("got %d records, want the 4 from pages 0 and 2", len(got))
	}
	if lostCrawl != "CC-MAIN-2026-30" || lostPage != 1 {
		t.Fatalf("the handler was told crawl %q page %d, want CC-MAIN-2026-30 page 1", lostCrawl, lostPage)
	}
}

// TestCDXStreamHandsTheHandlerAWholeLostCrawl covers the page count failing,
// where there is no page number to report and the whole crawl goes.
func TestCDXStreamHandsTheHandlerAWholeLostCrawl(t *testing.T) {
	f := &fakeCDX{pages: [][]string{cdxLines(0, 2)}, numPagesFail: 503}
	f.start(t)

	page := 0
	q := CDXQuery{URL: "example.com/*", OnPageError: func(_ string, p int, _ error) error {
		page = p
		return nil
	}}
	seen := 0
	err := CDXStream(context.Background(), testCDXClient(), "CC-MAIN-2026-30", q, func(CDXRecord) error {
		seen++
		return nil
	})
	if err != nil {
		t.Fatalf("the crawl was dropped, which the handler allowed, but the stream failed anyway: %v", err)
	}
	if page != -1 {
		t.Fatalf("the handler was told page %d, want -1 for a crawl that could not be counted", page)
	}
	if seen != 0 {
		t.Fatalf("%d records came out of a crawl whose page count failed", seen)
	}
}

// TestCDXStreamStillFailsWithoutAHandler pins the default. A library caller that
// has not thought about partial results gets an error, not a quiet hole.
func TestCDXStreamStillFailsWithoutAHandler(t *testing.T) {
	f := &fakeCDX{pages: [][]string{cdxLines(0, 2), cdxLines(1, 2)}, fail: map[int]int{1: 503}}
	f.start(t)

	err := CDXStream(context.Background(), testCDXClient(), "CC-MAIN-2026-30", CDXQuery{URL: "example.com/*"}, func(CDXRecord) error {
		return nil
	})
	if err == nil {
		t.Fatal("a page the server would not serve came back as success with no handler set")
	}
	if !strings.Contains(err.Error(), "CDX page 1") {
		t.Fatalf("the error is %q, want it to name the page", err)
	}
}

// TestCDXStreamGivesUpOnAPageThatIsAlwaysShort makes sure the truncation retry
// terminates rather than reading a hopeless page forever.
func TestCDXStreamGivesUpOnAPageThatIsAlwaysShort(t *testing.T) {
	f := &fakeCDX{pages: [][]string{cdxLines(0, 4)}, cut: map[int]int{0: 99}}
	f.start(t)

	err := CDXStream(context.Background(), retryingCDXClient(2), "CC-MAIN-2026-30", CDXQuery{URL: "example.com/*"}, func(CDXRecord) error {
		return nil
	})
	if err == nil {
		t.Fatal("a page that never came back whole reported success")
	}
	if !strings.Contains(err.Error(), "short") {
		t.Fatalf("the error is %q, want it to say the body kept coming back short", err)
	}
	if f.hits["0"] != 3 {
		t.Fatalf("page 0 was read %d times, want 3: the first go and two retries", f.hits["0"])
	}
}

// The URL substring filters are the whole point of pushing filters: they go on
// the wire as regexes so the server drops the rows, rather than arriving here to
// be counted and thrown away.
//
// Every character of the two strings below was paid for once. The first version
// of this sent "url:.*budget.*" and the server answered 404 No Captures for a
// page with six of them, because without the ~ the value is compared as a
// literal string rather than a regex. The whole feature read 1978 bytes and
// returned nothing, and looked like a spectacular saving until the results were
// compared against the client side path. Measured on CC-MAIN-2026-30 over
// abag.ca.gov, 788 rows of which 2 hold "budget": "~url:.*budget.*" returns the
// 2, "!~url:.*budget.*" returns the other 786, and "url:.*budget.*",
// "~url:budget" and "~!url:.*budget.*" each return nothing at all.
func TestQueryPushesTheURLSubstringFilters(t *testing.T) {
	q := CDXQuery{URL: "*.gov", URLContains: "/budget/", URLNotContains: "?print=1"}
	got := q.cdxValues(0)["filter"]
	want := []string{`~url:.*/budget/.*`, `!~url:.*\?print=1.*`}
	if len(got) != len(want) {
		t.Fatalf("got filters %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("filter %d is %q, want %q", i, got[i], want[i])
		}
	}
}

// The index server anchors a filter regex at the start of the value, so the
// leading .* is what makes a substring filter match a URL at all. Losing it
// would leave a query that runs, transfers nothing, and returns nothing.
func TestContainsRegexMatchesAnywhereInTheValue(t *testing.T) {
	re := containsRegex("/budget/")
	if !strings.HasPrefix(re, ".*") || !strings.HasSuffix(re, ".*") {
		t.Fatalf("containsRegex(%q) = %q, want it unanchored at both ends", "/budget/", re)
	}
	if got := containsRegex("a.b*c"); got != `.*a\.b\*c.*` {
		t.Fatalf("containsRegex escaped %q as %q", "a.b*c", got)
	}
}

func TestNoPushFiltersKeepsTheURLFiltersOffTheWire(t *testing.T) {
	q := CDXQuery{URL: "*.gov", URLContains: "/budget/", NoPushFilters: true, Status: "200"}
	got := q.cdxValues(0)["filter"]
	if len(got) != 1 || got[0] != "=status:200" {
		t.Fatalf("got filters %q, want only the status filter", got)
	}
}

// --explain prints this, and a URL that is not the one the command fetches is
// worse than no URL at all.
func TestCDXRequestURLIsTheRequestMinusThePage(t *testing.T) {
	q := CDXQuery{URL: "*.gov", URLContains: "/budget/"}
	got := CDXRequestURL("CC-MAIN-2026-30", q)
	if !strings.HasPrefix(got, Endpoints.CDX+"CC-MAIN-2026-30-index?") {
		t.Fatalf("request URL %q does not address the crawl's index", got)
	}
	if strings.Contains(got, "page=") {
		t.Fatalf("request URL %q carries a page parameter", got)
	}
	for _, want := range []string{"url=gov", "matchType=domain", "output=json", "filter=~url%3A.%2A%2Fbudget%2F.%2A"} {
		if !strings.Contains(got, want) {
			t.Errorf("request URL %q is missing %q", got, want)
		}
	}
}

// The byte total is what --explain reports, and what makes a pushed filter
// visible as a saving rather than a claim.
func TestCDXBytesReadCountsTheIndexPages(t *testing.T) {
	f := &fakeCDX{pages: [][]string{cdxLines(0, 4)}}
	f.start(t)

	h := testCDXClient()
	if n := h.CDXBytesRead(); n != 0 {
		t.Fatalf("a fresh client has read %d index bytes", n)
	}
	if _, err := CDXSearch(context.Background(), h, "CC-MAIN-2026-30", CDXQuery{URL: "example.com/*"}); err != nil {
		t.Fatal(err)
	}
	var want int64
	for _, line := range f.pages[0] {
		want += int64(len(line)) + 1 // the newline the server writes
	}
	if got := h.CDXBytesRead(); got < want {
		t.Fatalf("counted %d index bytes, want at least the %d bytes of records", got, want)
	}
}

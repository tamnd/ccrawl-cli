package ccrawl

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kgzip "github.com/klauspost/compress/gzip"
)

// warcMember frames one response record as its own gzip member, which is how
// Common Crawl stores them and why a coalesced range decodes at all.
func warcMember(t *testing.T, uri, body string) []byte {
	t.Helper()
	payload := "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n" + body
	rec := fmt.Sprintf("WARC/1.0\r\nWARC-Type: response\r\nWARC-Target-URI: %s\r\nContent-Length: %d\r\n\r\n%s\r\n\r\n",
		uri, len(payload), payload)
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

// warcFile builds a file of n records, optionally with filler between them, and
// returns the bytes plus the location of each record. The filler stands in for
// the records a real run did not ask for, which is exactly what a coalesced
// range has to read over. name is the location filename, which is an absolute
// URL here so the fetch path reaches the test server.
func warcFile(t *testing.T, name, host string, n int, filler int) ([]byte, []Location) {
	t.Helper()
	var buf bytes.Buffer
	var locs []Location
	for i := 0; i < n; i++ {
		off := int64(buf.Len())
		m := warcMember(t, fmt.Sprintf("https://%s/%d", host, i), strings.Repeat("x", 64)+strconv.Itoa(i))
		buf.Write(m)
		locs = append(locs, Location{
			Filename: name, Offset: off, Length: int64(len(m)),
			URL: fmt.Sprintf("https://%s/%d", host, i),
		})
		if filler > 0 && i < n-1 {
			buf.Write(warcMember(t, "https://filler.example/", strings.Repeat("f", filler)))
		}
	}
	return buf.Bytes(), locs
}

// serveWARCs puts a set of files behind a range-honouring server and counts the
// requests, which is the number the whole feature is judged on.
//
// It also records what each request asked for, as path and Range header. A test
// that reads the count across two runs cannot use the raw total: a cancelled
// fetch is abandoned by the client and the handler it already reached goes on
// running, and nothing connects the server's goroutine to the client's, so a
// request belonging to a run that has already returned can land on the counter
// after the next run has reset it. Counting distinct spans instead is immune to
// that, because a straggler asks for a span the next run asks for anyway, and a
// run that really did refetch finished work asks for spans nobody else does.
func serveWARCs(t *testing.T, files map[string][]byte) (*HTTPClient, string, *int64, func() []string) {
	t.Helper()
	var reqs int64
	var mu sync.Mutex
	var spans []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&reqs, 1)
		mu.Lock()
		spans = append(spans, r.URL.Path+" "+r.Header.Get("Range"))
		mu.Unlock()
		name := strings.TrimPrefix(r.URL.Path, "/")
		data, ok := files[name]
		if !ok {
			http.NotFound(w, r)
			return
		}
		http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(data))
	}))
	t.Cleanup(srv.Close)
	// takeSpans returns the distinct spans asked for since the last call and
	// starts a fresh window.
	takeSpans := func() []string {
		mu.Lock()
		defer mu.Unlock()
		seen := make(map[string]bool, len(spans))
		out := make([]string, 0, len(spans))
		for _, s := range spans {
			if !seen[s] {
				seen[s] = true
				out = append(out, s)
			}
		}
		spans = nil
		return out
	}
	return NewHTTPClient(Config{}), srv.URL + "/", &reqs, takeSpans
}

// rebase points a fixture's locations at the test server. The filename is a URL
// rather than a Common Crawl path, which FileURL passes through untouched.
func rebase(locs []Location, base string) []Location {
	out := make([]Location, len(locs))
	for i, l := range locs {
		l.Filename = base + l.Filename
		out[i] = l
	}
	return out
}

func TestGroupLocations(t *testing.T) {
	loc := func(file string, off, length int64) Location {
		return Location{Filename: file, Offset: off, Length: length}
	}
	cases := []struct {
		name  string
		locs  []Location
		gap   int64
		max   int64
		want  int
		spans []int64
	}{
		{
			name:  "adjacent records coalesce",
			locs:  []Location{loc("a", 0, 100), loc("a", 100, 100), loc("a", 200, 100)},
			gap:   0,
			want:  1,
			spans: []int64{300},
		},
		{
			// The point of the gap: a hole smaller than a request is cheaper to
			// read than to skip.
			name:  "a hole inside the gap coalesces",
			locs:  []Location{loc("a", 0, 100), loc("a", 600, 100)},
			gap:   1000,
			want:  1,
			spans: []int64{700},
		},
		{
			name:  "a hole past the gap splits",
			locs:  []Location{loc("a", 0, 100), loc("a", 600, 100)},
			gap:   100,
			want:  2,
			spans: []int64{100, 100},
		},
		{
			name:  "different files never share a request",
			locs:  []Location{loc("a", 0, 100), loc("b", 100, 100)},
			gap:   1 << 20,
			want:  2,
			spans: []int64{100, 100},
		},
		{
			name:  "the span cap closes a group early",
			locs:  []Location{loc("a", 0, 100), loc("a", 100, 100), loc("a", 200, 100)},
			gap:   1 << 20,
			max:   250,
			want:  2,
			spans: []int64{200, 100},
		},
		{
			// Input order is not sorted order, and the grouping has to sort
			// first or a shuffled input coalesces into nothing.
			name:  "unsorted input still coalesces",
			locs:  []Location{loc("b", 0, 100), loc("a", 200, 100), loc("a", 0, 100), loc("a", 100, 100)},
			gap:   0,
			want:  2,
			spans: []int64{300, 100},
		},
		{
			name:  "the same record twice is one range",
			locs:  []Location{loc("a", 0, 100), loc("a", 0, 100)},
			gap:   0,
			want:  1,
			spans: []int64{100},
		},
		{"nothing", nil, 0, 0, 0, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := GroupLocations(tc.locs, tc.gap, tc.max)
			if len(got) != tc.want {
				t.Fatalf("got %d groups, want %d: %+v", len(got), tc.want, got)
			}
			for i, g := range got {
				if g.Span() != tc.spans[i] {
					t.Errorf("group %d span = %d, want %d", i, g.Span(), tc.spans[i])
				}
			}
			// Whatever the grouping did, no location may go missing.
			n := 0
			for _, g := range got {
				n += len(g.Locs)
			}
			if n != len(tc.locs) {
				t.Errorf("groups hold %d locations, want the %d given", n, len(tc.locs))
			}
		})
	}
}

// The claim the mode rests on is fewer requests for the same records, so both
// halves are checked at once: the batch path against the one at a time path,
// record for record, and the request count against the location count.
func TestBatchFetchMatchesOneAtATimeWithFewerRequests(t *testing.T) {
	data, locs := warcFile(t, "a.example.warc.gz", "a.example", 12, 200)
	h, base, reqs, _ := serveWARCs(t, map[string][]byte{"a.example.warc.gz": data})
	locs = rebase(locs, base)

	var want []string
	for _, l := range locs {
		rec, err := FetchWARCRecord(context.Background(), h, l.Filename, l.Offset, l.Length)
		if err != nil {
			t.Fatalf("one at a time: %v", err)
		}
		want = append(want, string(rec.Block))
	}
	oneAtATime := atomic.LoadInt64(reqs)
	if oneAtATime != int64(len(locs)) {
		t.Fatalf("the one at a time path made %d requests for %d records", oneAtATime, len(locs))
	}

	atomic.StoreInt64(reqs, 0)
	var got []string
	stats, err := RunBatchFetch(context.Background(), h, BatchFetchConfig{
		Locations: locs,
		Gap:       1 << 20,
		Workers:   4,
		Window:    4,
		OnRecord: func(_ Location, rec WARCRecord) error {
			got = append(got, string(rec.Block))
			return nil
		},
	})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if stats.Records != len(locs) {
		t.Errorf("fetched %d records, want %d", stats.Records, len(locs))
	}
	sort.Strings(want)
	sort.Strings(got)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("the batch path returned different records than the one at a time path")
	}
	// Everything is in one file within the gap, so it is a single request.
	if stats.Requests != 1 {
		t.Errorf("batch made %d requests, want 1", stats.Requests)
	}
	if got := float64(len(locs)) / float64(stats.Requests); got < 3 {
		t.Errorf("only %.1fx fewer requests, want at least 3x", got)
	}
}

// The output order is a promise, so both settings are pinned. File order is the
// default because it is what the sweep produces; input order costs a reorder
// buffer and is only worth paying for when the caller asked.
func TestBatchFetchOrder(t *testing.T) {
	a, alocs := warcFile(t, "a.example.warc.gz", "a.example", 3, 0)
	b, blocs := warcFile(t, "b.example.warc.gz", "b.example", 3, 0)
	h, base, _, _ := serveWARCs(t, map[string][]byte{"a.example.warc.gz": a, "b.example.warc.gz": b})
	alocs, blocs = rebase(alocs, base), rebase(blocs, base)

	// Interleaved and backwards, so neither order is the order given.
	input := []Location{blocs[2], alocs[1], blocs[0], alocs[2], alocs[0], blocs[1]}

	collect := func(inOrder bool) []string {
		var urls []string
		_, err := RunBatchFetch(context.Background(), h, BatchFetchConfig{
			Locations: input, Gap: 1 << 20, Workers: 4, Window: 2, InOrder: inOrder,
			OnRecord: func(loc Location, _ WARCRecord) error {
				urls = append(urls, loc.URL)
				return nil
			},
		})
		if err != nil {
			t.Fatalf("batch: %v", err)
		}
		return urls
	}

	wantInput := []string{
		"https://b.example/2", "https://a.example/1", "https://b.example/0",
		"https://a.example/2", "https://a.example/0", "https://b.example/1",
	}
	if got := collect(true); fmt.Sprint(got) != fmt.Sprint(wantInput) {
		t.Errorf("input order = %v, want %v", got, wantInput)
	}
	wantFile := []string{
		"https://a.example/0", "https://a.example/1", "https://a.example/2",
		"https://b.example/0", "https://b.example/1", "https://b.example/2",
	}
	if got := collect(false); fmt.Sprint(got) != fmt.Sprint(wantFile) {
		t.Errorf("file order = %v, want %v", got, wantFile)
	}
}

// A run that dies partway through has to pick up where it stopped. The second
// run is given the same locations and must fetch only what the first one did
// not, which is checked on what it asked the server for rather than on the
// ledger, since the ledger being right and the run still refetching is the
// failure worth catching.
func TestBatchFetchResumes(t *testing.T) {
	data, locs := warcFile(t, "a.example.warc.gz", "a.example", 6, 1<<20) // filler past the gap, so one request each
	h, base, _, takeSpans := serveWARCs(t, map[string][]byte{"a.example.warc.gz": data})
	locs = rebase(locs, base)
	path := filepath.Join(t.TempDir(), "fetched.txt")

	// First run: stop after three records, the way a kill would.
	ledger, err := OpenKeyLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	stop := fmt.Errorf("killed")
	n := 0
	_, err = RunBatchFetch(context.Background(), h, BatchFetchConfig{
		Locations: locs, Gap: 0, Workers: 1, Window: 1, Ledger: ledger,
		OnRecord: func(Location, WARCRecord) error {
			n++
			if n == 3 {
				return stop
			}
			return nil
		},
	})
	if err != stop {
		t.Fatalf("first run err = %v, want the stop", err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	// The record that returned the error was not emitted, so it is not done.
	if got := lineCount(t, path); got != 2 {
		t.Fatalf("ledger holds %d keys after 2 emitted records, want 2", got)
	}

	// Second run: same input, and it must only fetch what is left. The window
	// opens here, and it is measured in distinct spans rather than in requests
	// because the first run left a cancelled fetch in flight whose handler can
	// still be running. See serveWARCs.
	takeSpans()
	ledger2, err := OpenKeyLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ledger2.Close() }()
	var got []string
	stats, err := RunBatchFetch(context.Background(), h, BatchFetchConfig{
		Locations: locs, Gap: 0, Workers: 1, Window: 1, Ledger: ledger2,
		OnRecord: func(loc Location, _ WARCRecord) error {
			got = append(got, loc.URL)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if stats.Skipped != 2 {
		t.Errorf("skipped %d, want the 2 the first run finished", stats.Skipped)
	}
	if len(got) != 4 {
		t.Errorf("second run emitted %d records, want the remaining 4: %v", len(got), got)
	}
	if spans := takeSpans(); len(spans) != 4 {
		t.Errorf("second run asked for %d distinct spans, want the 4 records it had left; the finished records were refetched: %v", len(spans), spans)
	}
	for _, u := range got {
		if u == "https://a.example/0" || u == "https://a.example/1" {
			t.Errorf("%s was already done and got fetched again", u)
		}
	}
}

// One bad location must not take the group down with it, or a single corrupt
// record in a coalesced range would lose every record beside it.
func TestBatchFetchOneBadLocation(t *testing.T) {
	data, locs := warcFile(t, "a.example.warc.gz", "a.example", 3, 0)
	h, base, _, _ := serveWARCs(t, map[string][]byte{"a.example.warc.gz": data})
	locs = rebase(locs, base)
	// Point the middle location at bytes that are not the start of a member.
	locs[1].Offset += 3

	var ok, bad int
	stats, err := RunBatchFetch(context.Background(), h, BatchFetchConfig{
		Locations: locs, Gap: 1 << 20, Workers: 1, Window: 1,
		OnRecord: func(Location, WARCRecord) error { ok++; return nil },
		OnError:  func(Location, error) { bad++ },
	})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if ok != 2 || bad != 1 {
		t.Errorf("got %d good and %d bad, want 2 and 1", ok, bad)
	}
	if stats.Records != 2 || stats.Failed != 1 {
		t.Errorf("stats = %+v, want 2 records and 1 failure", stats)
	}
}

// With no OnError a failure is fatal, so a caller that did not opt into
// tolerating losses does not get a short answer that looks complete.
func TestBatchFetchFailsWithoutOnError(t *testing.T) {
	h, base, _, _ := serveWARCs(t, map[string][]byte{})
	_, err := RunBatchFetch(context.Background(), h, BatchFetchConfig{
		Locations: []Location{{Filename: base + "absent.warc.gz", Offset: 0, Length: 10}},
		Workers:   1, Window: 1,
		OnRecord: func(Location, WARCRecord) error { return nil },
	})
	if err == nil {
		t.Error("a failed group with no OnError should fail the run")
	}
}

func TestKeyLedger(t *testing.T) {
	path := filepath.Join(t.TempDir(), "l.txt")
	l, err := OpenKeyLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"a@1", "b@2", "a@1"} {
		if err := l.Mark(k); err != nil {
			t.Fatal(err)
		}
	}
	if l.Count() != 2 {
		t.Errorf("count = %d, want 2 after one repeat", l.Count())
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if got := lineCount(t, path); got != 2 {
		t.Errorf("file holds %d lines, want 2", got)
	}

	l2, err := OpenKeyLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l2.Close() }()
	if !l2.Has("a@1") || !l2.Has("b@2") || l2.Has("c@3") {
		t.Error("a reopened ledger did not carry the keys")
	}

	// An empty path is a nil ledger every method tolerates, so a caller with no
	// --ledger does not need a branch around every call.
	nilLedger, err := OpenKeyLedger("")
	if err != nil || nilLedger != nil {
		t.Fatalf("empty path = %v %v, want a nil ledger and no error", nilLedger, err)
	}
	if nilLedger.Has("x") || nilLedger.Count() != 0 || nilLedger.Mark("x") != nil ||
		nilLedger.Sync() != nil || nilLedger.Close() != nil {
		t.Error("the nil ledger should be inert rather than a panic")
	}
}

func TestLocationKey(t *testing.T) {
	a := LocationKey(Location{Filename: "x.warc.gz", Offset: 10, Length: 5})
	b := LocationKey(Location{Filename: "x.warc.gz", Offset: 10, Length: 900})
	if a != b {
		t.Errorf("the length should not be part of the key: %s vs %s", a, b)
	}
	if c := LocationKey(Location{Filename: "x.warc.gz", Offset: 11}); c == a {
		t.Error("two offsets in one file collided")
	}
}

func lineCount(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, l := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(l) != "" {
			n++
		}
	}
	return n
}

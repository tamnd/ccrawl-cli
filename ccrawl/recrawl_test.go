package ccrawl

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

// recrawlSite serves a page per path and counts every request, so a test can
// ask what was fetched and how often.
type recrawlSite struct {
	mu     sync.Mutex
	hits   map[string]int
	robots string
}

func newRecrawlSite() *recrawlSite {
	return &recrawlSite{hits: map[string]int{}}
}

func (s *recrawlSite) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.hits[r.URL.Path]++
	robots := s.robots
	s.mu.Unlock()

	if r.URL.Path == "/robots.txt" {
		if robots == "" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(robots))
		return
	}
	w.Header().Set("Content-Type", "text/html")
	_, _ = fmt.Fprintf(w, "<html><body><p>%s</p></body></html>", r.URL.Path)
}

func (s *recrawlSite) count(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[path]
}

// fetched returns the paths that were fetched, robots.txt aside, in sorted
// order with duplicates kept out.
func (s *recrawlSite) fetched() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for p, n := range s.hits {
		if p != "/robots.txt" && n > 0 {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

// total counts every page request including repeats, which is how a resume test
// measures what it refetched.
func (s *recrawlSite) total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for p, c := range s.hits {
		if p != "/robots.txt" {
			n += c
		}
	}
	return n
}

// urlSource is a work list of URLs, which is the shape the published URL index
// has and the one a test can point at a local server.
func urlSource() WorkSource {
	return WorkSource{Repo: "open-index/ccrawl-urls", Dir: "data/test", Column: "url"}
}

// writeWorkList lays out paths across parts of the given size and returns the
// directory holding them.
func writeWorkList(t *testing.T, base string, paths []string, perPart int) string {
	t.Helper()
	dir := t.TempDir()
	for i := 0; i < len(paths); i += perPart {
		end := min(i+perPart, len(paths))
		urls := make([]string, 0, end-i)
		for _, p := range paths[i:end] {
			urls = append(urls, base+p)
		}
		writeURLPart(t, filepath.Join(dir, fmt.Sprintf("part-%03d.parquet", i/perPart)), urls)
	}
	return dir
}

func testRecrawlConfig(state string) RecrawlConfig {
	cfg := DefaultRecrawlConfig
	cfg.Source = urlSource()
	cfg.Shard = Shard{Count: 1}
	cfg.StatePath = state
	cfg.Workers = 4
	cfg.Delay = 0
	cfg.Robots = false
	cfg.Batch = 4
	cfg.Crawl = DefaultCrawlConfig
	cfg.Crawl.Timeout = 5 * time.Second
	return cfg
}

// newTestRecrawler builds a recrawler reading parts off the local disk.
func newTestRecrawler(t *testing.T, cfg RecrawlConfig, parts string) *Recrawler {
	t.Helper()
	r, err := NewRecrawler(cfg, crawlClient(), crawlClient())
	if err != nil {
		t.Fatal(err)
	}
	localParts(r.wl, parts)
	return r
}

func paths(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("/p%02d", i)
	}
	return out
}

func TestRecrawlFetchesTheWholeWorkList(t *testing.T) {
	site := newRecrawlSite()
	srv := httptest.NewServer(site)
	defer srv.Close()

	want := paths(10)
	parts := writeWorkList(t, srv.URL, want, 4)
	state := filepath.Join(t.TempDir(), "state.json")

	cfg := testRecrawlConfig(state)
	cfg.OutDir = t.TempDir()
	r := newTestRecrawler(t, cfg, parts)
	defer func() { _ = r.Close() }()

	var emitted int
	stats, err := r.Run(context.Background(), func(CrawlPage) error {
		emitted++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Fetched != 10 || stats.Failed != 0 {
		t.Fatalf("fetched %d failed %d, want 10 and 0", stats.Fetched, stats.Failed)
	}
	if emitted != 10 {
		t.Fatalf("emitted %d pages, want 10", emitted)
	}
	if got := site.fetched(); len(got) != 10 {
		t.Fatalf("the site saw %v, want all ten paths", got)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	ck, err := LoadCheckpoint(state)
	if err != nil {
		t.Fatal(err)
	}
	if !ck.Done {
		t.Fatalf("the work list was read to its end and the checkpoint says %+v", ck)
	}
	if ck.Source != urlSource().Key() {
		t.Fatalf("the checkpoint records source %q", ck.Source)
	}

	// A finished work list has to stop a supervisor restarting the unit rather
	// than start the whole hundred day run again.
	if _, err := NewRecrawler(cfg, crawlClient(), crawlClient()); !errors.Is(err, ErrRecrawlDone) {
		t.Fatalf("restarting a finished run gave %v, want ErrRecrawlDone", err)
	}
}

// TestRecrawlResumesAfterAKill is the resume done-when. The run is killed part
// way through, restarted from its checkpoint, and between them the two runs have
// to cover every URL and refetch at most one batch.
func TestRecrawlResumesAfterAKill(t *testing.T) {
	site := newRecrawlSite()
	srv := httptest.NewServer(site)
	defer srv.Close()

	want := paths(20)
	parts := writeWorkList(t, srv.URL, want, 6)
	state := filepath.Join(t.TempDir(), "state.json")
	cfg := testRecrawlConfig(state)

	// First run, killed after nine pages the way a signal or a reboot kills one,
	// in the middle of a batch rather than tidily between two.
	ctx, kill := context.WithCancel(context.Background())
	r := newTestRecrawler(t, cfg, parts)
	var first int
	if _, err := r.Run(ctx, func(CrawlPage) error {
		first++
		if first == 9 {
			kill()
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	kill()
	if first < 9 {
		t.Fatalf("the first run fetched %d pages before the kill, want at least 9", first)
	}
	if first == len(want) {
		t.Fatal("the kill did not interrupt anything, so the test proves nothing")
	}

	ck, err := LoadCheckpoint(state)
	if err != nil {
		t.Fatal(err)
	}
	if ck.Done {
		t.Fatal("a killed run wrote a finished checkpoint")
	}

	// Second run, from the checkpoint the first one left.
	r2 := newTestRecrawler(t, cfg, parts)
	defer func() { _ = r2.Close() }()
	if _, err := r2.Run(context.Background(), func(CrawlPage) error { return nil }); err != nil {
		t.Fatal(err)
	}

	got := site.fetched()
	if len(got) != len(want) {
		t.Fatalf("the two runs covered %d of %d URLs, so the resume skipped work", len(got), len(want))
	}
	sorted := append([]string(nil), want...)
	sort.Strings(sorted)
	for i := range sorted {
		if got[i] != sorted[i] {
			t.Fatalf("URL %s was never fetched", sorted[i])
		}
	}

	// Refetching is allowed, up to the batch the kill threw away. Much more than
	// that means the checkpoint is not holding the place it claims to.
	if over := site.total() - len(want); over > cfg.Batch {
		t.Fatalf("the resume refetched %d pages, and a batch is %d", over, cfg.Batch)
	}
}

// TestRecrawlDoesNotAdvancePastWorkItDidNotDo is the other half of the resume
// story. A batch cut short by the page limit must leave the checkpoint where it
// was, because a checkpoint past unfetched rows skips them silently and nobody
// ever finds out.
func TestRecrawlDoesNotAdvancePastWorkItDidNotDo(t *testing.T) {
	site := newRecrawlSite()
	srv := httptest.NewServer(site)
	defer srv.Close()

	want := paths(20)
	parts := writeWorkList(t, srv.URL, want, 20)
	state := filepath.Join(t.TempDir(), "state.json")

	cfg := testRecrawlConfig(state)
	cfg.Batch = 20 // one batch for the whole work list
	cfg.MaxPages = 5
	r := newTestRecrawler(t, cfg, parts)
	stats, err := r.Run(context.Background(), func(CrawlPage) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if stats.Fetched != 5 {
		t.Fatalf("--max-pages 5 fetched %d pages", stats.Fetched)
	}

	ck, err := LoadCheckpoint(state)
	if err != nil {
		t.Fatal(err)
	}
	if ck.Row != 0 || ck.Part != 0 || ck.Done {
		t.Fatalf("the run fetched 5 of 20 rows and the checkpoint reads %+v, which claims work it did not do", ck)
	}

	// And the proof that it matters: a second run from that checkpoint covers
	// the whole list.
	cfg.MaxPages = 0
	r2 := newTestRecrawler(t, cfg, parts)
	defer func() { _ = r2.Close() }()
	if _, err := r2.Run(context.Background(), func(CrawlPage) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := site.fetched(); len(got) != len(want) {
		t.Fatalf("after the resume the site saw %d of %d URLs", len(got), len(want))
	}
}

// TestRecrawlMaxPagesIsExact pins the page limit against the workers racing it.
// Reading the counter and then deciding lets every worker read four at once and
// fetch a page each, which is how --max-pages 5 turns into ten fetches.
func TestRecrawlMaxPagesIsExact(t *testing.T) {
	site := newRecrawlSite()
	srv := httptest.NewServer(site)
	defer srv.Close()

	parts := writeWorkList(t, srv.URL, paths(200), 200)
	cfg := testRecrawlConfig(filepath.Join(t.TempDir(), "state.json"))
	cfg.Workers = 16
	cfg.Batch = 200
	cfg.MaxPages = 5

	r := newTestRecrawler(t, cfg, parts)
	defer func() { _ = r.Close() }()
	stats, err := r.Run(context.Background(), func(CrawlPage) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if stats.Fetched != 5 {
		t.Fatalf("--max-pages 5 with 16 workers fetched %d pages", stats.Fetched)
	}
	if got := site.total(); got != 5 {
		t.Fatalf("the site served %d pages for --max-pages 5", got)
	}
}

// TestRecrawlSmallBatchStillHoldsItsPlace covers the case where the batch is
// small enough that every item is handed to a worker before any of them finish.
// The send loop then runs to the end and looks like a whole batch, while the
// last items were refused a page slot and never fetched.
func TestRecrawlSmallBatchStillHoldsItsPlace(t *testing.T) {
	site := newRecrawlSite()
	srv := httptest.NewServer(site)
	defer srv.Close()

	parts := writeWorkList(t, srv.URL, paths(60), 60)
	state := filepath.Join(t.TempDir(), "state.json")
	cfg := testRecrawlConfig(state)
	cfg.Workers = 16
	cfg.Batch = 6
	cfg.MaxPages = 5

	r := newTestRecrawler(t, cfg, parts)
	stats, err := r.Run(context.Background(), func(CrawlPage) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if stats.Fetched != 5 {
		t.Fatalf("fetched %d pages for --max-pages 5", stats.Fetched)
	}
	ck, err := LoadCheckpoint(state)
	if err != nil {
		t.Fatal(err)
	}
	if ck.Row != 0 {
		t.Fatalf("the checkpoint sits at row %d after fetching 5 of a 6 row batch", ck.Row)
	}

	cfg.MaxPages = 0
	r2 := newTestRecrawler(t, cfg, parts)
	defer func() { _ = r2.Close() }()
	if _, err := r2.Run(context.Background(), func(CrawlPage) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := site.fetched(); len(got) != 60 {
		t.Fatalf("after the resume the site saw %d of 60 URLs", len(got))
	}
}

func TestRecrawlHonoursRobots(t *testing.T) {
	site := newRecrawlSite()
	site.robots = "User-agent: *\nDisallow: /p01\nDisallow: /p02\n"
	srv := httptest.NewServer(site)
	defer srv.Close()

	parts := writeWorkList(t, srv.URL, paths(5), 5)
	cfg := testRecrawlConfig(filepath.Join(t.TempDir(), "state.json"))
	cfg.Robots = true
	r := newTestRecrawler(t, cfg, parts)
	defer func() { _ = r.Close() }()

	stats, err := r.Run(context.Background(), func(CrawlPage) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if stats.Disallowed != 2 {
		t.Fatalf("robots disallowed %d pages, want 2", stats.Disallowed)
	}
	if stats.Fetched != 3 {
		t.Fatalf("fetched %d pages, want 3", stats.Fetched)
	}
	if site.count("/p01") != 0 || site.count("/p02") != 0 {
		t.Fatal("a disallowed path was fetched anyway")
	}
	// robots.txt is read once for the host and not once per URL, which is the
	// difference between one request and six here and between one and a hundred
	// thousand on a real shard.
	if n := site.count("/robots.txt"); n != 1 {
		t.Fatalf("robots.txt was fetched %d times for one host", n)
	}
}

// TestRecrawlRobotsDoesNotUseTheBulkBudget is the client split. The bulk client
// spaces every request by a delay meant for one file host, and putting robots
// through it makes the fleet as slow as that delay no matter how many hosts it
// is talking to.
func TestRecrawlRobotsDoesNotUseTheBulkBudget(t *testing.T) {
	site := newRecrawlSite()
	site.robots = "User-agent: *\nAllow: /\n"
	srv := httptest.NewServer(site)
	defer srv.Close()

	parts := writeWorkList(t, srv.URL, paths(4), 4)
	cfg := testRecrawlConfig(filepath.Join(t.TempDir(), "state.json"))
	cfg.Robots = true

	// The bulk client is given a delay nobody would wait for. If robots goes
	// through it the run takes a minute, and if it goes through the crawl client
	// the delay never applies.
	bulk := DefaultConfig()
	bulk.Retries = 0
	bulk.Delay = time.Minute
	r, err := NewRecrawler(cfg, NewHTTPClient(bulk), crawlClient())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	localParts(r.wl, parts)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stats, err := r.Run(ctx, func(CrawlPage) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if stats.Fetched != 4 {
		t.Fatalf("fetched %d pages, want 4, so robots was queued behind the bulk delay", stats.Fetched)
	}
}

func TestRecrawlTakesOnlyItsShard(t *testing.T) {
	site := newRecrawlSite()
	srv := httptest.NewServer(site)
	defer srv.Close()

	all := paths(30)
	parts := writeWorkList(t, srv.URL, all, 30)

	// Every URL here is on one host, so one shard of three owns the lot and the
	// other two own nothing. That is the partition working as designed: the key
	// is the registered domain, so a host and its politeness clock stay whole.
	counts := make([]int64, 3)
	for i := range counts {
		cfg := testRecrawlConfig(filepath.Join(t.TempDir(), "state.json"))
		cfg.Shard = Shard{Index: i, Count: 3}
		r := newTestRecrawler(t, cfg, parts)
		stats, err := r.Run(context.Background(), func(CrawlPage) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		_ = r.Close()
		counts[i] = stats.Fetched
	}
	var sum, owners int64
	for _, c := range counts {
		sum += c
		if c > 0 {
			owners++
		}
	}
	if sum != int64(len(all)) {
		t.Fatalf("three shards fetched %d URLs between them, want %d", sum, len(all))
	}
	if owners != 1 {
		t.Fatalf("%d shards claimed a slice of one host, and a host belongs to exactly one", owners)
	}
}

func TestRecrawlWithNoStatePathStillRuns(t *testing.T) {
	site := newRecrawlSite()
	srv := httptest.NewServer(site)
	defer srv.Close()

	parts := writeWorkList(t, srv.URL, paths(6), 3)
	cfg := testRecrawlConfig("")
	r := newTestRecrawler(t, cfg, parts)
	defer func() { _ = r.Close() }()

	stats, err := r.Run(context.Background(), func(CrawlPage) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if stats.Fetched != 6 {
		t.Fatalf("fetched %d pages, want 6", stats.Fetched)
	}
}

// TestRecrawlStateStaysBounded is the disk done-when. The state a run keeps has
// to be the same size after a hundred batches as after one, or it is the
// frontier again wearing a different name.
func TestRecrawlStateStaysBounded(t *testing.T) {
	site := newRecrawlSite()
	srv := httptest.NewServer(site)
	defer srv.Close()

	parts := writeWorkList(t, srv.URL, paths(400), 100)
	dir := t.TempDir()
	state := filepath.Join(dir, "state.json")
	cfg := testRecrawlConfig(state)
	cfg.Batch = 4 // a hundred checkpoints over the run

	r := newTestRecrawler(t, cfg, parts)
	defer func() { _ = r.Close() }()

	var sizes []int64
	if _, err := r.Run(context.Background(), func(CrawlPage) error {
		if st, err := os.Stat(state); err == nil {
			sizes = append(sizes, st.Size())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(sizes) < 50 {
		t.Fatalf("only saw the checkpoint %d times", len(sizes))
	}
	for _, s := range sizes {
		if s > 512 {
			t.Fatalf("the checkpoint reached %d bytes", s)
		}
	}
	// And nothing else was written next to it.
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 1 {
		t.Fatalf("the run left %d files in its state directory, want just the checkpoint", len(ents))
	}
}

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
	"strings"
	"sync"
	"testing"
	"time"
)

// recrawlSite serves a page per path and counts every request, so a test can
// ask what was fetched and how often.
type recrawlSite struct {
	mu           sync.Mutex
	hits         map[string]int
	robots       string
	robotsStatus int // non-zero to answer /robots.txt with a bare status
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
		s.mu.Lock()
		status := s.robotsStatus
		s.mu.Unlock()
		if status != 0 {
			w.WriteHeader(status)
			return
		}
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

// assertRowsBehindCheckpointWereFetched is the invariant every stopped run has
// to hold: a checkpoint at row N is a promise that rows 0 to N-1 are done. It is
// checked against what the site was actually asked for rather than against a
// count, because a concurrent pool can fetch row 7 and be refused row 3, and
// then the honest checkpoint is 3 and a count would say 5.
func assertRowsBehindCheckpointWereFetched(t *testing.T, site *recrawlSite, want []string, row int64) {
	t.Helper()
	if row < 0 || row > int64(len(want)) {
		t.Fatalf("the checkpoint reads row %d, which is not a row of a %d row work list", row, len(want))
	}
	got := make(map[string]bool, len(want))
	for _, p := range site.fetched() {
		got[p] = true
	}
	for i := range int(row) {
		if !got[want[i]] {
			t.Fatalf("the checkpoint reads row %d but %s at row %d was never fetched, so the resume will skip it", row, want[i], i)
		}
	}
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
// story. A run cut short by the page limit must not leave a checkpoint past a
// row it never fetched, because that skips the row silently and nobody ever
// finds out.
//
// It used to rewind to the start of the batch, which was safe and wasteful. The
// flight set knows which row was actually refused, so the checkpoint now names
// that row: everything before it was fetched, and the assertion below is that
// claim rather than an offset.
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
	if ck.Part != 0 || ck.Done {
		t.Fatalf("the run fetched 5 of 20 rows and the checkpoint reads %+v", ck)
	}
	assertRowsBehindCheckpointWereFetched(t, site, want, ck.Row)

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

// TestRecrawlSmallBatchStillHoldsItsPlace covers the case where the window is
// small enough that every item is handed to a worker before any of them finish.
// The feed loop then runs to the end and looks like it walked the list out,
// while the last items were refused a page slot and never fetched.
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
	assertRowsBehindCheckpointWereFetched(t, site, paths(60), ck.Row)

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

// TestRecrawlWritesParquetCaptures is the end to end done-when for the
// publishing format. What the site served has to come back out of the shard,
// body and headers included, because that is the file the publish step uploads.
func TestRecrawlWritesParquetCaptures(t *testing.T) {
	site := newRecrawlSite()
	srv := httptest.NewServer(site)
	defer srv.Close()

	want := paths(10)
	parts := writeWorkList(t, srv.URL, want, 4)
	out := t.TempDir()

	cfg := testRecrawlConfig(filepath.Join(t.TempDir(), "state.json"))
	cfg.OutDir = out
	cfg.Format = FormatParquet
	cfg.Prefix = "recrawl"
	r := newTestRecrawler(t, cfg, parts)
	stats, err := r.Run(context.Background(), func(CrawlPage) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if len(stats.OutFiles) == 0 {
		t.Fatal("the run reported no output files")
	}

	byURL := map[string]Capture{}
	for _, f := range stats.OutFiles {
		if !strings.HasSuffix(f, ".parquet") {
			t.Fatalf("a parquet run wrote %s", f)
		}
		rows, err := ReadCaptures(f)
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		for _, c := range rows {
			byURL[c.URL] = c
		}
	}
	if len(byURL) != len(want) {
		t.Fatalf("the shards hold %d distinct URLs, want %d", len(byURL), len(want))
	}
	for _, p := range want {
		c, ok := byURL[srv.URL+p]
		if !ok {
			t.Fatalf("%s is not in any shard", p)
		}
		body := fmt.Sprintf("<html><body><p>%s</p></body></html>", p)
		if string(c.Body) != body {
			t.Fatalf("%s came back as %q, want %q", p, c.Body, body)
		}
		if c.Status != 200 || c.BodyLength != int64(len(body)) {
			t.Fatalf("%s came back status %d length %d", p, c.Status, c.BodyLength)
		}
		if !strings.Contains(c.RespHeaders, "200 OK") {
			t.Fatalf("%s came back with response headers %q", p, c.RespHeaders)
		}
		if !strings.Contains(c.ReqHeaders, "GET ") {
			t.Fatalf("%s came back with request headers %q", p, c.ReqHeaders)
		}
		if c.Host == "" || c.Digest == "" || c.FetchedAt == 0 {
			t.Fatalf("%s came back host %q digest %q fetched_at %d", p, c.Host, c.Digest, c.FetchedAt)
		}
	}
}

// TestRecrawlParquetCheckpointHoldsAtAShardBoundary is the durability gate.
// A Parquet file is unreadable until its footer is written, so a run whose open
// shard is nowhere near full has nothing durable to point a checkpoint at, and
// while it is going it must say so rather than record a position that reads back
// as an error.
//
// The stop is the exception, and the one place the shard is sealed whether it is
// full or not, because a stop is the last chance to seal it. What the checkpoint
// records there is the oldest row that never finished, never a row past it, so
// the replay is what was in the air rather than the whole open shard.
func TestRecrawlParquetCheckpointHoldsAtAShardBoundary(t *testing.T) {
	site := newRecrawlSite()
	srv := httptest.NewServer(site)
	defer srv.Close()

	want := paths(20)
	parts := writeWorkList(t, srv.URL, want, 20)
	state := filepath.Join(t.TempDir(), "state.json")

	cfg := testRecrawlConfig(state)
	cfg.OutDir = t.TempDir()
	cfg.Format = FormatParquet
	cfg.ShardSize = 1 << 30 // far more than this run will ever write
	cfg.Batch = 4

	ctx, kill := context.WithCancel(context.Background())
	r := newTestRecrawler(t, cfg, parts)
	var n int
	if _, err := r.Run(ctx, func(CrawlPage) error {
		n++
		if n == 9 {
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
	if n >= len(want) {
		t.Fatal("the kill did not interrupt anything, so the test proves nothing")
	}

	ck, err := LoadCheckpoint(state)
	if err != nil {
		t.Fatal(err)
	}
	if ck.Done {
		t.Fatalf("a killed run wrote a finished checkpoint: %+v", ck)
	}
	if ck.Row > int64(n) {
		t.Fatalf("the checkpoint reads row %d after %d fetches, which claims work it did not do", ck.Row, n)
	}
	assertRowsBehindCheckpointWereFetched(t, site, want, ck.Row)
	// Whatever it does claim has to be readable, which is the gate itself.
	stored := 0
	shards, err := filepath.Glob(filepath.Join(cfg.OutDir, "*.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range shards {
		rows, rerr := ReadCaptures(f)
		if rerr != nil {
			t.Fatalf("%s: %v", f, rerr)
		}
		stored += len(rows)
	}
	if int64(stored) < ck.Row {
		t.Fatalf("the checkpoint reads row %d and only %d captures are readable", ck.Row, stored)
	}

	// The cost is refetching, and the point is that the second run covers
	// everything, so between them no URL is missed.
	r2 := newTestRecrawler(t, cfg, parts)
	defer func() { _ = r2.Close() }()
	if _, err := r2.Run(context.Background(), func(CrawlPage) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := site.fetched(); len(got) != len(want) {
		t.Fatalf("the two runs covered %d of %d URLs", len(got), len(want))
	}
	// And that the refetch is bounded by the batch the kill threw away rather
	// than by everything sitting in the shard that had not filled.
	if over := site.total() - len(want); over > cfg.Batch {
		t.Fatalf("the resume refetched %d pages, and a batch is %d", over, cfg.Batch)
	}
}

// TestRecrawlStillWritesWARC is the promise that adding a format did not take
// one away.
func TestRecrawlStillWritesWARC(t *testing.T) {
	site := newRecrawlSite()
	srv := httptest.NewServer(site)
	defer srv.Close()

	want := paths(6)
	parts := writeWorkList(t, srv.URL, want, 6)
	out := t.TempDir()

	cfg := testRecrawlConfig(filepath.Join(t.TempDir(), "state.json"))
	cfg.OutDir = out
	cfg.Format = FormatWARC
	cfg.Prefix = "recrawl"
	r := newTestRecrawler(t, cfg, parts)
	stats, err := r.Run(context.Background(), func(CrawlPage) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if len(stats.OutFiles) != 1 || !strings.HasSuffix(stats.OutFiles[0], ".warc.gz") {
		t.Fatalf("a warc run wrote %v", stats.OutFiles)
	}
	st, err := os.Stat(stats.OutFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() == 0 {
		t.Fatalf("%s is empty", stats.OutFiles[0])
	}
	if stats.Fetched != int64(len(want)) {
		t.Fatalf("fetched %d, want %d", stats.Fetched, len(want))
	}
}

// TestRecrawlParquetCheckpointAdvancesOnceShardsSeal is the regression for the
// other way of getting this wrong. Holding the checkpoint until a shard is
// sealed is only safe if shards actually seal, and if rotation happened inside a
// batch rather than between two, a run would almost never find its writer at a
// boundary and would checkpoint nothing across a hundred days.
func TestRecrawlParquetCheckpointAdvancesOnceShardsSeal(t *testing.T) {
	site := newRecrawlSite()
	srv := httptest.NewServer(site)
	defer srv.Close()

	want := paths(20)
	parts := writeWorkList(t, srv.URL, want, 20)
	state := filepath.Join(t.TempDir(), "state.json")

	cfg := testRecrawlConfig(state)
	cfg.OutDir = t.TempDir()
	cfg.Format = FormatParquet
	cfg.ShardSize = 256 // small enough that every batch fills one
	cfg.Batch = 4

	ctx, kill := context.WithCancel(context.Background())
	r := newTestRecrawler(t, cfg, parts)
	var n int
	stats, err := r.Run(ctx, func(CrawlPage) error {
		n++
		if n == 13 {
			kill()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	kill()

	ck, err := LoadCheckpoint(state)
	if err != nil {
		t.Fatal(err)
	}
	if ck.Row == 0 {
		t.Fatalf("shards were sealed on the way and the checkpoint still reads %+v", ck)
	}
	if ck.Row > int64(n) {
		t.Fatalf("the checkpoint reads row %d after %d fetches, which claims work it did not do", ck.Row, n)
	}

	// Everything the checkpoint claims has to be readable, since that is the
	// whole reason for holding it back.
	var stored int
	for _, f := range stats.OutFiles {
		rows, err := ReadCaptures(f)
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		stored += len(rows)
	}
	if int64(stored) < ck.Row {
		t.Fatalf("the checkpoint reads row %d and only %d captures are readable", ck.Row, stored)
	}
}

// TestRecrawlPageLimitCheckpointsWhatItFetched covers the third way a run stops.
// The end of the work list sealed the output and saved a checkpoint, and the
// page limit returned without doing either, so a run capped with --max-pages
// wrote its pages and then claimed it had fetched nothing. Restarting it
// refetched every page and wrote a second copy.
func TestRecrawlPageLimitCheckpointsWhatItFetched(t *testing.T) {
	site := newRecrawlSite()
	srv := httptest.NewServer(site)
	defer srv.Close()

	want := paths(20)
	parts := writeWorkList(t, srv.URL, want, 20)
	state := filepath.Join(t.TempDir(), "state.json")

	cfg := testRecrawlConfig(state)
	cfg.OutDir = t.TempDir()
	cfg.Format = FormatParquet
	// Big enough that no shard fills on its own, so the only seal is the one the
	// stop forces. That is the case the bug hid in.
	cfg.ShardSize = 1 << 30
	cfg.Batch = 4
	cfg.MaxPages = 8

	r := newTestRecrawler(t, cfg, parts)
	stats, err := r.Run(t.Context(), func(CrawlPage) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	ck, err := LoadCheckpoint(state)
	if err != nil {
		t.Fatal(err)
	}
	if ck.Row != 8 {
		t.Fatalf("8 pages were fetched under the limit and the checkpoint reads %+v", ck)
	}
	if ck.Done {
		t.Fatal("stopping at the page limit was recorded as finishing the work list")
	}
	if ck.Fetched != stats.Fetched {
		t.Fatalf("the checkpoint counted %d fetches and the run made %d", ck.Fetched, stats.Fetched)
	}
	// The rows the checkpoint claims have to be readable, which means the stop
	// sealed the shard rather than leaving it open for Close to find.
	var stored int
	for _, f := range stats.OutFiles {
		rows, err := ReadCaptures(f)
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		stored += len(rows)
	}
	if int64(stored) < ck.Row {
		t.Fatalf("the checkpoint reads row %d and only %d captures are readable", ck.Row, stored)
	}
}

// TestRecrawlHonoursACrawlDelayLongerThanOurs is the Crawl-delay done-when on
// the recrawl side. Our own delay is a floor and not a ceiling: a host that asks
// for more gets more, or the politeness setting is the one we chose rather than
// the one the site asked for.
func TestRecrawlHonoursACrawlDelayLongerThanOurs(t *testing.T) {
	site := newRecrawlSite()
	site.robots = "User-agent: *\nCrawl-delay: 0.4\n"
	srv := httptest.NewServer(site)
	defer srv.Close()

	want := paths(3)
	parts := writeWorkList(t, srv.URL, want, 3)

	cfg := testRecrawlConfig(filepath.Join(t.TempDir(), "state.json"))
	cfg.Robots = true
	cfg.Delay = 0 // our own figure, which robots is about to beat
	cfg.Workers = 3
	r := newTestRecrawler(t, cfg, parts)
	defer func() { _ = r.Close() }()

	start := time.Now()
	stats, err := r.Run(context.Background(), func(CrawlPage) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	took := time.Since(start)
	if stats.Fetched != 3 {
		t.Fatalf("fetched %d of 3 pages", stats.Fetched)
	}
	// Three pages on one host at four hundred milliseconds apart is two gaps,
	// so eight hundred milliseconds is the floor. Measured a little under, since
	// the point is that the delay was applied and not that the clock is exact.
	if took < 700*time.Millisecond {
		t.Fatalf("three pages on a host asking for 0.4s apart took %s, so the Crawl-delay was ignored", took)
	}
}

// TestRecrawlCountsUnreachableRobotsApartFromRefused is the reporting
// done-when, end to end. Both stop the page and they are not the same event.
func TestRecrawlCountsUnreachableRobotsApartFromRefused(t *testing.T) {
	site := newRecrawlSite()
	site.robotsStatus = 500 // the host cannot tell us anything
	srv := httptest.NewServer(site)
	defer srv.Close()

	want := paths(4)
	parts := writeWorkList(t, srv.URL, want, 4)

	cfg := testRecrawlConfig(filepath.Join(t.TempDir(), "state.json"))
	cfg.Robots = true
	r := newTestRecrawler(t, cfg, parts)
	defer func() { _ = r.Close() }()

	stats, err := r.Run(context.Background(), func(CrawlPage) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if stats.Fetched != 0 {
		t.Fatalf("a host whose robots.txt is failing was crawled %d times", stats.Fetched)
	}
	if stats.Unreachable != 4 {
		t.Fatalf("%d pages counted unreachable, want 4", stats.Unreachable)
	}
	if stats.Disallowed != 0 {
		t.Fatalf("%d pages counted refused, and nothing refused them", stats.Disallowed)
	}
	// And the cost of asking is reported: one host, one request, however many
	// pages went through it.
	if stats.Robots.Fetches != 1 {
		t.Fatalf("robots.txt was fetched %d times for one host", stats.Robots.Fetches)
	}
	if stats.Robots.Unreachable != 1 {
		t.Fatalf("the cache reports %d unreachable hosts", stats.Robots.Unreachable)
	}
}

// TestRecrawlAsksEachHostForRobotsOnce is the cost half. On a domain corpus this
// is one extra request for every three pages, so the run has to be able to say
// what it spent.
func TestRecrawlAsksEachHostForRobotsOnce(t *testing.T) {
	site := newRecrawlSite()
	site.robots = "User-agent: *\nDisallow: /p01\n"
	srv := httptest.NewServer(site)
	defer srv.Close()

	want := paths(8)
	parts := writeWorkList(t, srv.URL, want, 8)

	cfg := testRecrawlConfig(filepath.Join(t.TempDir(), "state.json"))
	cfg.Robots = true
	r := newTestRecrawler(t, cfg, parts)
	defer func() { _ = r.Close() }()

	stats, err := r.Run(context.Background(), func(CrawlPage) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if got := site.count("/robots.txt"); got != 1 {
		t.Fatalf("eight pages on one host asked for robots.txt %d times, want 1", got)
	}
	if stats.Robots.Fetches != 1 || stats.Robots.Hits != 7 {
		t.Fatalf("the run reports %d fetches and %d cache hits over eight pages on one host",
			stats.Robots.Fetches, stats.Robots.Hits)
	}
	if stats.Disallowed != 1 || stats.Unreachable != 0 {
		t.Fatalf("%d refused and %d unreachable, want 1 and 0", stats.Disallowed, stats.Unreachable)
	}
	if stats.Fetched != 7 {
		t.Fatalf("fetched %d of the 7 pages robots allowed", stats.Fetched)
	}
}

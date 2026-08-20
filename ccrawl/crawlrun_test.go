package ccrawl

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testSite is a small web with links, so a crawl has somewhere to go.
type testSite struct {
	mu     sync.Mutex
	hits   map[string]int
	when   map[string][]time.Time // request times per path, for the spacing check
	robots string
}

func newTestSite() *testSite {
	return &testSite{hits: map[string]int{}, when: map[string][]time.Time{}}
}

func (s *testSite) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.hits[r.URL.Path]++
	s.when[r.URL.Path] = append(s.when[r.URL.Path], time.Now())
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
	switch r.URL.Path {
	case "/":
		_, _ = fmt.Fprint(w, `<a href="/a">a</a> <a href="/b">b</a> <a href="/private/x">x</a>`)
	case "/a":
		_, _ = fmt.Fprint(w, `<a href="/c">c</a> <a href="/">home</a>`)
	default:
		_, _ = fmt.Fprintf(w, "<p>%s</p>", r.URL.Path)
	}
}

func (s *testSite) count(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[path]
}

func (s *testSite) total() int {
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

// testRunConfig is a crawl with the delays taken out, so a test that is not
// about politeness does not have to wait for it.
func testRunConfig(dir string) RunConfig {
	cfg := DefaultRunConfig
	cfg.OutDir = dir
	cfg.Delay = time.Millisecond
	cfg.RetryDelay = time.Millisecond
	cfg.Workers = 4
	cfg.Robots = false
	cfg.MaxDepth = 0
	cfg.Crawl = DefaultCrawlConfig
	cfg.Crawl.Timeout = 5 * time.Second
	return cfg
}

// crawlClient is the client the crawler uses for robots.txt, with the retries
// off so a test is not waiting on backoff.
func crawlClient() *HTTPClient {
	cfg := DefaultConfig()
	cfg.Retries = 0
	cfg.Delay = 0
	return NewHTTPClient(cfg)
}

func runCrawl(t *testing.T, cfg RunConfig, seeds ...string) (CrawlStats, []CrawlPage) {
	t.Helper()
	c, err := NewCrawler(cfg, crawlClient())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	for _, s := range seeds {
		if !c.Seed(s, 1) {
			t.Fatalf("seed %s was rejected", s)
		}
	}
	var pages []CrawlPage
	stats, err := c.Run(context.Background(), func(p CrawlPage) error {
		pages = append(pages, p)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return stats, pages
}

func TestCrawlRunFetchesTheSeedsAndWritesWARC(t *testing.T) {
	site := newTestSite()
	srv := httptest.NewServer(site)
	defer srv.Close()

	dir := t.TempDir()
	stats, pages := runCrawl(t, testRunConfig(dir), srv.URL+"/", srv.URL+"/a", srv.URL+"/b")

	if stats.Fetched != 3 || stats.Failed != 0 {
		t.Fatalf("fetched %d, failed %d, want 3 and 0", stats.Fetched, stats.Failed)
	}
	if len(pages) != 3 {
		t.Fatalf("emitted %d pages, want 3", len(pages))
	}
	for _, p := range pages {
		if p.Status != 200 || p.Digest == "" {
			t.Errorf("page %s came back status %d digest %q", p.URL, p.Status, p.Digest)
		}
	}
	if len(stats.OutFiles) != 1 {
		t.Fatalf("wrote %d WARC files, want 1", len(stats.OutFiles))
	}
	var responses int
	f, err := os.Open(stats.OutFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if err := IterateWARC(f, func(r WARCRecord) error {
		if r.Header.Type == "response" {
			responses++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if responses != 3 {
		t.Errorf("WARC holds %d response records, want 3", responses)
	}
}

func TestCrawlRunFollowsLinksToMaxDepth(t *testing.T) {
	site := newTestSite()
	srv := httptest.NewServer(site)
	defer srv.Close()

	cfg := testRunConfig(t.TempDir())
	cfg.MaxDepth = 1
	cfg.SameHost = true
	stats, _ := runCrawl(t, cfg, srv.URL+"/")

	// Depth 0 is the root, depth 1 is /a, /b and /private/x. /c is two hops away
	// and must not be reached.
	if stats.Fetched != 4 {
		t.Fatalf("fetched %d pages, want the root and its three links", stats.Fetched)
	}
	if site.count("/c") != 0 {
		t.Errorf("/c is two hops from the seed and was fetched %d times", site.count("/c"))
	}
	if stats.Discovered < 3 {
		t.Errorf("discovered %d outlinks, want at least 3", stats.Discovered)
	}
	if site.count("/") != 1 {
		t.Errorf("the root was fetched %d times, want 1: /a links back to it", site.count("/"))
	}
}

func TestCrawlRunStaysOffDisallowedPaths(t *testing.T) {
	site := newTestSite()
	site.robots = "User-agent: *\nDisallow: /private/\n"
	srv := httptest.NewServer(site)
	defer srv.Close()

	cfg := testRunConfig(t.TempDir())
	cfg.Robots = true
	cfg.MaxDepth = 1
	cfg.SameHost = true
	stats, _ := runCrawl(t, cfg, srv.URL+"/")

	if site.count("/private/x") != 0 {
		t.Errorf("a disallowed path was fetched %d times", site.count("/private/x"))
	}
	if stats.Disallowed != 1 {
		t.Errorf("disallowed count is %d, want 1", stats.Disallowed)
	}
	if stats.Fetched != 3 {
		t.Errorf("fetched %d pages, want the root plus /a and /b", stats.Fetched)
	}
	if site.count("/robots.txt") != 1 {
		t.Errorf("robots.txt was fetched %d times, want once for the whole host", site.count("/robots.txt"))
	}
}

func TestCrawlRunKeepsHostRequestsApart(t *testing.T) {
	site := newTestSite()
	srv := httptest.NewServer(site)
	defer srv.Close()

	const delay = 120 * time.Millisecond
	cfg := testRunConfig(t.TempDir())
	cfg.Delay = delay
	cfg.Workers = 8 // more workers than URLs, so only the frontier is pacing this

	var seeds []string
	for i := range 4 {
		seeds = append(seeds, fmt.Sprintf("%s/p%d", srv.URL, i))
	}
	start := time.Now()
	stats, _ := runCrawl(t, cfg, seeds...)
	elapsed := time.Since(start)

	if stats.Fetched != 4 {
		t.Fatalf("fetched %d, want 4", stats.Fetched)
	}
	// Four requests to one host at one per delay cannot finish faster than three
	// delays, whatever the worker count says.
	if min := 3 * delay; elapsed < min {
		t.Errorf("four requests took %s, which is under the %s the delay requires", elapsed, min)
	}

	site.mu.Lock()
	defer site.mu.Unlock()
	var times []time.Time
	for p, ts := range site.when {
		if p != "/robots.txt" {
			times = append(times, ts...)
		}
	}
	for i := range times {
		for j := i + 1; j < len(times); j++ {
			gap := times[j].Sub(times[i])
			if gap < 0 {
				gap = -gap
			}
			if gap < delay-10*time.Millisecond {
				t.Errorf("two requests to the same host were %s apart, under the %s delay", gap, delay)
			}
		}
	}
}

func TestCrawlRunHonoursCrawlDelay(t *testing.T) {
	site := newTestSite()
	site.robots = "User-agent: *\nCrawl-delay: 1\n"
	srv := httptest.NewServer(site)
	defer srv.Close()

	cfg := testRunConfig(t.TempDir())
	cfg.Robots = true
	cfg.Delay = time.Millisecond // the host asks for a whole second, we asked for nothing

	start := time.Now()
	stats, _ := runCrawl(t, cfg, srv.URL+"/", srv.URL+"/a")
	elapsed := time.Since(start)

	if stats.Fetched != 2 {
		t.Fatalf("fetched %d, want 2", stats.Fetched)
	}
	if elapsed < time.Second {
		t.Errorf("two fetches under a Crawl-delay of 1 took %s, which ignores it", elapsed)
	}
}

func TestCrawlRunStopsAtMaxPages(t *testing.T) {
	site := newTestSite()
	srv := httptest.NewServer(site)
	defer srv.Close()

	cfg := testRunConfig(t.TempDir())
	cfg.MaxPages = 2
	cfg.Workers = 1
	stats, _ := runCrawl(t, cfg, srv.URL+"/", srv.URL+"/a", srv.URL+"/b", srv.URL+"/c")

	if stats.Fetched != 2 {
		t.Errorf("fetched %d pages under a limit of 2", stats.Fetched)
	}
	if site.total() > 2 {
		t.Errorf("the site saw %d requests under a limit of 2", site.total())
	}
}

// A restart is the interesting failure. The frontier is on disk, so a second
// crawler pointed at the same state file must skip what the first one finished
// and pick up what it did not.
func TestCrawlRunResumesWithoutRefetching(t *testing.T) {
	site := newTestSite()
	srv := httptest.NewServer(site)
	defer srv.Close()

	state := filepath.Join(t.TempDir(), "crawl.db")
	seeds := []string{srv.URL + "/", srv.URL + "/a", srv.URL + "/b", srv.URL + "/c"}

	cfg := testRunConfig(t.TempDir())
	cfg.StatePath = state
	cfg.MaxPages = 2
	cfg.Workers = 1
	first, _ := runCrawl(t, cfg, seeds...)
	if first.Fetched != 2 {
		t.Fatalf("first run fetched %d, want 2", first.Fetched)
	}
	afterFirst := site.total()

	cfg2 := testRunConfig(t.TempDir())
	cfg2.StatePath = state
	cfg2.Workers = 1
	second, _ := runCrawl(t, cfg2, seeds...)
	if second.Fetched != 2 {
		t.Fatalf("second run fetched %d, want the 2 the first run did not", second.Fetched)
	}
	if total := site.total(); total != 4 {
		t.Errorf("the site saw %d requests across two runs of 4 URLs, so %d were refetched", total, total-4)
	}
	if afterFirst != 2 {
		t.Errorf("first run made %d requests, want 2", afterFirst)
	}
}

func TestCrawlRunRetriesThenGivesUp(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("test server does not support hijacking")
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			return
		}
		// Drop the connection without answering, which is a transient failure as
		// far as a crawler can tell.
		_ = conn.Close()
	}))
	defer srv.Close()

	cfg := testRunConfig(t.TempDir())
	cfg.Retries = 2
	stats, pages := runCrawl(t, cfg, srv.URL+"/")

	if stats.Failed != 1 {
		t.Errorf("failed count is %d, want 1", stats.Failed)
	}
	if stats.Retried != 2 {
		t.Errorf("retried %d times, want 2", stats.Retried)
	}
	if n := hits.Load(); n != 3 {
		t.Errorf("the server saw %d attempts, want the first plus 2 retries", n)
	}
	if len(pages) != 0 {
		t.Errorf("emitted %d pages for a URL that never answered", len(pages))
	}
	if stats.ErrDNS+stats.ErrTimeout+stats.ErrRefused+stats.ErrSkip+stats.ErrOther != 3 {
		t.Errorf("the error breakdown counts %d attempts, want 3",
			stats.ErrDNS+stats.ErrTimeout+stats.ErrRefused+stats.ErrSkip+stats.ErrOther)
	}
}

func TestCrawlRunCancels(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte("slow"))
	}))
	defer srv.Close()

	cfg := testRunConfig(t.TempDir())
	cfg.Workers = 2
	c, err := NewCrawler(cfg, crawlClient())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	for i := range 50 {
		c.Seed(fmt.Sprintf("%s/p%d", srv.URL, i), 1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	stats, err := c.Run(ctx, nil)
	if err == nil {
		t.Fatal("a cancelled run should report the context error")
	}
	if stats.Fetched >= 50 {
		t.Errorf("a run cancelled after 100ms fetched %d of 50 pages", stats.Fetched)
	}
}

func TestCrawlRunRejectsBadSeeds(t *testing.T) {
	c, err := NewCrawler(testRunConfig(""), crawlClient())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	for _, s := range []string{"", "not a url", "mailto:someone@example.com", "ftp://example.com/x", "/relative"} {
		if c.Seed(s, 1) {
			t.Errorf("seed %q was accepted", s)
		}
	}
	if !c.Seed("https://example.com/", 1) {
		t.Error("a plain https URL was rejected")
	}
}

func TestRobotsCacheFetchesOncePerHostUnderLoad(t *testing.T) {
	var fetches atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		time.Sleep(20 * time.Millisecond) // long enough for the others to pile up
		_, _ = w.Write([]byte("User-agent: *\nDisallow: /nope\n"))
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	rc := NewRobotsCache(time.Hour, "ccrawl")
	h := crawlClient()
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := rc.Fetch(context.Background(), h, u.Host, "http"); e.IsAllowed("/nope") {
				t.Error("the disallowed path came back allowed")
			}
		}()
	}
	wg.Wait()
	if n := fetches.Load(); n != 1 {
		t.Errorf("32 workers on one host fetched robots.txt %d times, want 1", n)
	}
}

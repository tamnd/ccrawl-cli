package ccrawl

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// The path matching table is RFC 9309 section 2.2.2, transcribed. Every case
// below is one the RFC spells out, which is the point: this is the part of the
// spec sites actually write against, and a parser that gets it approximately
// right blocks pages it should fetch and fetches pages it was told not to.
func TestRobotsPathMatching(t *testing.T) {
	cases := []struct {
		pattern string
		match   []string
		miss    []string
	}{{
		pattern: "/",
		match:   []string{"/", "/anything", "/deep/path.html"},
	}, {
		pattern: "/*",
		match:   []string{"/", "/anything", "/deep/path.html"},
	}, {
		pattern: "/fish",
		match: []string{"/fish", "/fish.html", "/fish/salmon.html", "/fishheads",
			"/fishheads/yummy.html", "/fish.html?id=anything"},
		miss: []string{"/Fish.asp", "/catfish", "/?id=fish", "/desert/fish"},
	}, {
		pattern: "/fish*",
		match:   []string{"/fish", "/fish.html", "/fish/salmon.html", "/fishheads"},
		miss:    []string{"/Fish.asp", "/catfish", "/?id=fish"},
	}, {
		pattern: "/fish/",
		match:   []string{"/fish/", "/fish/?id=anything", "/fish/salmon.html"},
		miss:    []string{"/fish", "/fish.html", "/Fish/Salmon.asp"},
	}, {
		pattern: "/*.php",
		match: []string{"/index.php", "/filename.php", "/folder/filename.php",
			"/folder/filename.php?parameters", "/folder/any.php.file.html", "/filename.php/"},
		miss: []string{"/", "/windows.PHP"},
	}, {
		pattern: "/*.php$",
		match:   []string{"/filename.php", "/folder/filename.php"},
		miss:    []string{"/filename.php?parameters", "/filename.php/", "/filename.php5", "/windows.PHP"},
	}, {
		pattern: "/fish*.php",
		match:   []string{"/fish.php", "/fishheads/catfish.php?parameters"},
		miss:    []string{"/Fish.PHP"},
	}}

	for _, c := range cases {
		e := &RobotsEntry{Rules: []RobotsRule{{Pattern: c.pattern}}}
		for _, p := range c.match {
			if e.IsAllowed(p) {
				t.Errorf("Disallow: %s should have matched %s", c.pattern, p)
			}
		}
		for _, p := range c.miss {
			if !e.IsAllowed(p) {
				t.Errorf("Disallow: %s should not have matched %s", c.pattern, p)
			}
		}
	}
}

// The file is the example from RFC 9309 section 5.2, and so are the four
// verdicts. quxbot is the interesting one: an empty group naming a crawler
// still counts as that crawler's group, so it does not fall back to the
// wildcard rules and everything is allowed.
const rfc9309Example = `User-Agent: *
Disallow: *.gif$
Disallow: /example/
Allow: /publications/

User-Agent: foobot
Disallow:/
Allow:/example/page.html
Allow:/example/allowed.gif

User-Agent: barbot
User-Agent: bazbot
Disallow: /example/page.html

User-Agent: quxbot
`

func TestRobotsGroupSelection(t *testing.T) {
	cases := []struct {
		ua      string
		allowed []string
		denied  []string
	}{{
		// Nothing names this one, so the wildcard group applies.
		ua:      "randombot",
		allowed: []string{"/publications/paper.html", "/index.html"},
		denied:  []string{"/example/page.html", "/img/logo.gif"},
	}, {
		ua:      "foobot",
		allowed: []string{"/example/page.html", "/example/allowed.gif"},
		denied:  []string{"/", "/index.html", "/publications/paper.html", "/img/logo.gif"},
	}, {
		// Matching is case insensitive and runs on the product token, so the
		// version and the contact URL in a real User-Agent header do not matter.
		ua:      "FooBot/2.1 (+https://example.com/bot)",
		allowed: []string{"/example/page.html"},
		denied:  []string{"/index.html"},
	}, {
		ua:      "barbot",
		allowed: []string{"/index.html", "/img/logo.gif", "/example/other.html"},
		denied:  []string{"/example/page.html"},
	}, {
		ua:      "bazbot",
		allowed: []string{"/index.html", "/img/logo.gif"},
		denied:  []string{"/example/page.html"},
	}, {
		ua:      "quxbot",
		allowed: []string{"/", "/example/page.html", "/img/logo.gif"},
	}}

	for _, c := range cases {
		e := parseRobots(strings.NewReader(rfc9309Example), c.ua)
		for _, p := range c.allowed {
			if !e.IsAllowed(p) {
				t.Errorf("%s: %s should be allowed", c.ua, p)
			}
		}
		for _, p := range c.denied {
			if e.IsAllowed(p) {
				t.Errorf("%s: %s should be disallowed", c.ua, p)
			}
		}
	}
}

func TestRobotsMostSpecificGroupWins(t *testing.T) {
	const txt = `User-agent: *
Disallow: /

User-agent: ccrawl
Disallow: /private/

User-agent: ccrawl-images
Disallow: /
`
	// A group naming a prefix of our token applies to us, which is how a site
	// writes one rule for a crawler and all of its variants. The longest such
	// token wins, and a more specific group for a sibling crawler is not ours.
	e := parseRobots(strings.NewReader(txt), "CCrawl/2.0 (+https://example.com/bot)")
	if !e.IsAllowed("/public/page.html") {
		t.Error("the ccrawl group should have replaced the wildcard group")
	}
	if e.IsAllowed("/private/page.html") {
		t.Error("/private/ should be disallowed by the ccrawl group")
	}

	img := parseRobots(strings.NewReader(txt), "ccrawl-images")
	if img.IsAllowed("/public/page.html") {
		t.Error("ccrawl-images should have taken its own group, which disallows everything")
	}
}

func TestRobotsMergesGroupsNamingTheSameAgent(t *testing.T) {
	const txt = `User-agent: ccrawl
Disallow: /a/

User-agent: ccrawl
Disallow: /b/
Crawl-delay: 3
`
	e := parseRobots(strings.NewReader(txt), "ccrawl")
	if e.IsAllowed("/a/x") || e.IsAllowed("/b/x") {
		t.Error("rules from both groups naming ccrawl should be followed")
	}
	if e.CrawlDelay != 3*time.Second {
		t.Errorf("CrawlDelay = %s, want 3s", e.CrawlDelay)
	}
}

func TestRobotsLongestMatchWinsAndAllowBreaksTies(t *testing.T) {
	const txt = `User-agent: *
Disallow: /private/
Allow: /private/public.html
Disallow: /both
Allow: /both
`
	e := parseRobots(strings.NewReader(txt), "ccrawl")
	if e.IsAllowed("/private/secret.html") {
		t.Error("/private/secret.html should be disallowed")
	}
	if !e.IsAllowed("/private/public.html") {
		t.Error("the longer Allow should win over the shorter Disallow")
	}
	if !e.IsAllowed("/both") {
		t.Error("an Allow and a Disallow of equal length should resolve to allow")
	}
	if !e.IsAllowed("/elsewhere") {
		t.Error("a path no rule matches should be allowed")
	}
}

func TestRobotsEmptyDisallowAllowsEverything(t *testing.T) {
	e := parseRobots(strings.NewReader("User-agent: *\nDisallow:\n"), "ccrawl")
	if !e.IsAllowed("/anything") {
		t.Error("an empty Disallow means nothing is disallowed")
	}
}

func TestRobotsIgnoresNamelessGroup(t *testing.T) {
	const txt = `User-agent:
Disallow: /

User-agent: *
Disallow: /admin/
`
	e := parseRobots(strings.NewReader(txt), "ccrawl")
	if !e.IsAllowed("/page.html") {
		t.Error("a User-agent line naming nobody should not disallow the site")
	}
	if e.IsAllowed("/admin/x") {
		t.Error("the wildcard group should still apply")
	}
}

func TestRobotsIgnoresRulesBeforeAnyGroup(t *testing.T) {
	e := parseRobots(strings.NewReader("Disallow: /\n\nUser-agent: *\nAllow: /\n"), "ccrawl")
	if !e.IsAllowed("/page.html") {
		t.Error("a rule outside any group should be ignored")
	}
}

func TestRobotsComments(t *testing.T) {
	const txt = `# leading comment
User-agent: *   # trailing comment
Disallow: /admin/ # another
`
	e := parseRobots(strings.NewReader(txt), "ccrawl")
	if e.IsAllowed("/admin/x") {
		t.Error("/admin/ should be disallowed with comments stripped")
	}
	if !e.IsAllowed("/admin") {
		t.Error("the trailing comment should not have become part of the pattern")
	}
}

func TestRobotsPercentEncoding(t *testing.T) {
	// The rule and the URL spell the same path differently. RFC 9309 section
	// 2.2.2 compares the percent encoded forms, so both sides get normalized and
	// the rule bites.
	e := parseRobots(strings.NewReader("User-agent: *\nDisallow: /caf%c3%a9/\n"), "ccrawl")
	if e.IsAllowed("/caf%C3%A9/menu") {
		t.Error("an escape written in lower case should match the same path in upper case")
	}
	if e.IsAllowed("/café/menu") {
		t.Error("an unescaped path should match the escaped rule written for it")
	}
}

func TestRobotsCollectsSitemaps(t *testing.T) {
	const txt = `Sitemap: https://example.com/sitemap.xml

User-agent: *
Disallow: /admin/
Sitemap: https://example.com/sitemap-news.xml
`
	e := parseRobots(strings.NewReader(txt), "ccrawl")
	want := []string{"https://example.com/sitemap.xml", "https://example.com/sitemap-news.xml"}
	if len(e.Sitemaps) != len(want) {
		t.Fatalf("Sitemaps = %v, want %v", e.Sitemaps, want)
	}
	for i, w := range want {
		if e.Sitemaps[i] != w {
			t.Errorf("Sitemaps[%d] = %q, want %q", i, e.Sitemaps[i], w)
		}
	}
	// A Sitemap line is not a group record and must not end the group it sits in.
	if e.IsAllowed("/admin/x") {
		t.Error("the Disallow before the second Sitemap should still apply")
	}
}

func TestRobotsStopsAtTheSizeLimit(t *testing.T) {
	// RFC 9309 section 2.5 asks for at least 500 kibibytes and says the rest is
	// discarded. A rule past the limit is not an error and does not invalidate
	// the rules before it.
	var b strings.Builder
	b.WriteString("User-agent: *\nDisallow: /early/\n")
	for b.Len() < robotsMaxBytes {
		b.WriteString("# padding to push the next rule past the parse limit\n")
	}
	b.WriteString("Disallow: /late/\n")

	e := parseRobots(strings.NewReader(b.String()), "ccrawl")
	if e.IsAllowed("/early/x") {
		t.Error("the rule before the limit should have been parsed")
	}
	if !e.IsAllowed("/late/x") {
		t.Error("the rule past the limit should have been discarded")
	}
}

// ── fetching ──────────────────────────────────────────────────────────────────

// robotsServer serves /robots.txt with a fixed status and body.
func robotsServer(t *testing.T, status int, body string) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/robots.txt" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, strings.TrimPrefix(srv.URL, "http://")
}

// robotsClient is an HTTP client with the retries turned off, so a test that
// asks for a 503 gets one back in milliseconds instead of after the default
// five attempts and their backoff.
func robotsClient() *HTTPClient {
	cfg := DefaultConfig()
	cfg.Retries = 0
	cfg.Delay = 0
	return NewHTTPClient(cfg)
}

func TestFetchRobots(t *testing.T) {
	_, host := robotsServer(t, 200, "User-agent: *\nDisallow: /secret/\nCrawl-delay: 4\n")
	e := FetchRobots(context.Background(), robotsClient(), host, "http", "ccrawl")
	if e.IsAllowed("/secret/page") {
		t.Error("/secret/ should be disallowed")
	}
	if !e.IsAllowed("/public/page") {
		t.Error("/public/ should be allowed")
	}
	if e.CrawlDelay != 4*time.Second {
		t.Errorf("CrawlDelay = %s, want 4s", e.CrawlDelay)
	}
}

// The done-when from E5, and the reason the issue is filed as a bug: a host
// whose robots endpoint is failing cannot tell us to stop, so we stop.
func TestFetchRobotsDisallowsWhenUnreachable(t *testing.T) {
	for _, status := range []int{500, 502, 503} {
		_, host := robotsServer(t, status, "")
		e := FetchRobots(context.Background(), robotsClient(), host, "http", "ccrawl")
		if e.IsAllowed("/") || e.IsAllowed("/anything.html") {
			t.Errorf("HTTP %d on robots.txt should disallow the whole host", status)
		}
		if e.TTL != robotsErrorTTL {
			t.Errorf("HTTP %d: TTL = %s, want the short negative TTL %s", status, e.TTL, robotsErrorTTL)
		}
	}
}

func TestFetchRobotsDisallowsWhenHostIsDown(t *testing.T) {
	srv, host := robotsServer(t, 200, "")
	srv.Close() // nothing is listening, which is the network failure case
	e := FetchRobots(context.Background(), robotsClient(), host, "http", "ccrawl")
	if e.IsAllowed("/anything.html") {
		t.Error("a host that cannot be reached at all should be disallowed")
	}
}

func TestFetchRobotsAllowsWhenMissing(t *testing.T) {
	// 404 and 403 both mean there is no robots.txt to read, which RFC 9309
	// section 2.3.1.3 says leaves the site open. 403 is the one worth a test:
	// the HTTP client treats it as retryable, and if the retry gave up with a
	// plain error this would come back as a disallow.
	for _, status := range []int{403, 404, 410} {
		_, host := robotsServer(t, status, "")
		e := FetchRobots(context.Background(), robotsClient(), host, "http", "ccrawl")
		if !e.IsAllowed("/anything.html") {
			t.Errorf("HTTP %d on robots.txt should leave the host allowed", status)
		}
	}
}

func TestRobotsCache(t *testing.T) {
	rc := NewRobotsCache(24*time.Hour, "ccrawl")
	if rc.Get("example.com") != nil {
		t.Error("empty cache should return nil")
	}
	rc.Put("example.com", &RobotsEntry{Rules: []RobotsRule{{Pattern: "/admin/"}}})
	got := rc.Get("example.com")
	if got == nil {
		t.Fatal("Put then Get should return the entry")
	} else if len(got.Rules) != 1 {
		t.Errorf("expected 1 rule, got %d", len(got.Rules))
	}
}

func TestRobotsCacheKeepsFailuresBriefly(t *testing.T) {
	rc := NewRobotsCache(24*time.Hour, "ccrawl")
	rc.Put("example.com", robotsUnreachable())
	// The failure expires on its own short clock rather than the cache's day, so
	// a host that comes back up is crawled again in minutes.
	e := rc.Get("example.com")
	if e == nil {
		t.Fatal("the negative entry should be cached")
	} else if wait := time.Until(time.Unix(e.ExpiresAt, 0)); wait > robotsErrorTTL+time.Second {
		t.Errorf("negative entry expires in %s, want at most %s", wait, robotsErrorTTL)
	}
}

func TestRobotsCacheFetchesOnce(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = w.Write([]byte("User-agent: *\nDisallow: /secret/\n"))
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")

	rc := NewRobotsCache(time.Hour, "ccrawl")
	h := robotsClient()
	for i := 0; i < 3; i++ {
		if rc.Fetch(context.Background(), h, host, "http").IsAllowed("/secret/x") {
			t.Fatal("/secret/ should be disallowed")
		}
	}
	if hits != 1 {
		t.Errorf("fetched robots.txt %d times, want 1", hits)
	}
}

// The last done-when from E5. A Crawl-delay is only worth parsing if something
// enforces it, and the thing that decides when a host may be touched again is
// the frontier.
func TestCrawlDelayHoldsTheHostInTheFrontier(t *testing.T) {
	f := NewFrontier(time.Second) // the crawl's own delay, shorter than robots asks for
	defer func() { _ = f.Close() }()

	f.Add(FrontierEntry{URL: "https://slow.example/a", Host: "slow.example"})
	f.Add(FrontierEntry{URL: "https://slow.example/b", Host: "slow.example"})

	now := time.Now().UnixMilli()
	first, ok, err := f.Pop(now)
	if err != nil || !ok {
		t.Fatalf("first Pop: %v %v", ok, err)
	}

	delay := parseRobots(strings.NewReader("User-agent: *\nCrawl-delay: 10\n"), "ccrawl").CrawlDelay
	if delay != 10*time.Second {
		t.Fatalf("CrawlDelay = %s, want 10s", delay)
	}
	f.Hold(first.Host, now+delay.Milliseconds())

	for _, after := range []int64{1_000, 5_000, 9_000} {
		if _, ok, err := f.Pop(now + after); err != nil {
			t.Fatalf("Pop: %v", err)
		} else if ok {
			t.Fatalf("second URL came out %dms after the first, want at least 10s", after)
		}
	}
	if _, ok, err := f.Pop(now + 10_000); err != nil {
		t.Fatalf("Pop: %v", err)
	} else if !ok {
		t.Error("the second URL should be available once the crawl delay has passed")
	}
}

// ── the cache at fleet speed ─────────────────────────────────────────────────

// TestRobotsCacheIsBoundedByEntries is the memory done-when. A threefold recrawl
// of the domain corpus touches 121M registered domains, and a map that never
// forgets one of them does not fit on a 5 GB server. The cache has to hold a
// working set and let the rest go.
func TestRobotsCacheIsBoundedByEntries(t *testing.T) {
	const limit = 1000
	const hosts = 50000
	rc := NewRobotsCacheWithLimits(time.Hour, "ccrawl", RobotsLimits{MaxEntries: limit})

	var m runtime.MemStats
	var baseline uint64
	for i := range hosts {
		rc.Put(fmt.Sprintf("host%06d.example", i), &RobotsEntry{
			Rules: []RobotsRule{{Pattern: "/admin/"}, {Pattern: "/private/"}},
		})
		if i == limit*2 {
			// Taken after the cache is already full, so it is one steady state
			// against another rather than against an empty map.
			runtime.GC()
			runtime.ReadMemStats(&m)
			baseline = m.HeapAlloc
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&m)

	st := rc.Stats()
	if st.Entries > limit {
		t.Fatalf("the cache holds %d entries against a limit of %d", st.Entries, limit)
	}
	if st.Evictions == 0 {
		t.Fatal("fifty thousand hosts went through a cache of a thousand and nothing was evicted")
	}
	if grew := int64(m.HeapAlloc) - int64(baseline); grew > 4<<20 {
		t.Fatalf("the heap grew %.1f MB over %d hosts, so the cache is not bounded in practice",
			float64(grew)/(1<<20), hosts)
	}
	t.Logf("%d hosts through a cache of %d: %d entries, %d evictions, %d bytes held, heap %.1f MB",
		hosts, limit, st.Entries, st.Evictions, st.Bytes, float64(m.HeapAlloc)/(1<<20))
}

// TestRobotsCacheIsBoundedByBytes is the other bound, and the reason there are
// two. RFC 9309 lets a robots.txt be 500 kibibytes, so a limit counted in
// entries is a memory limit that moves by three orders of magnitude depending on
// who we happen to crawl.
func TestRobotsCacheIsBoundedByBytes(t *testing.T) {
	const budget = 256 << 10
	rc := NewRobotsCacheWithLimits(time.Hour, "ccrawl", RobotsLimits{MaxBytes: budget})

	big := make([]RobotsRule, 200)
	for i := range big {
		big[i] = RobotsRule{Pattern: "/section/" + strings.Repeat("x", 100) + fmt.Sprint(i)}
	}
	for i := range 500 {
		rc.Put(fmt.Sprintf("host%03d.example", i), &RobotsEntry{Rules: big})
	}
	st := rc.Stats()
	if st.Bytes > budget {
		t.Fatalf("the cache holds %d bytes against a budget of %d", st.Bytes, budget)
	}
	if st.Entries >= 500 {
		t.Fatalf("nothing was evicted, and 500 entries of this size do not fit in %d bytes", budget)
	}
}

// TestRobotsCacheEvictsLeastRecentlyUsed pins the policy rather than just the
// bound. The work list is published sorted, so a shard walks through hosts in
// bursts and leaves them behind, and a host still being worked on has to survive
// hosts that were finished with long ago.
func TestRobotsCacheEvictsLeastRecentlyUsed(t *testing.T) {
	rc := NewRobotsCacheWithLimits(time.Hour, "ccrawl", RobotsLimits{MaxEntries: 3})
	for _, h := range []string{"a", "b", "c"} {
		rc.Put(h, &RobotsEntry{})
	}
	// a is used again, so b is now the oldest.
	if rc.Get("a") == nil {
		t.Fatal("a should still be cached")
	}
	rc.Put("d", &RobotsEntry{})

	if rc.Get("b") != nil {
		t.Error("b was the least recently used and should have gone")
	}
	for _, h := range []string{"a", "c", "d"} {
		if rc.Get(h) == nil {
			t.Errorf("%s should still be cached", h)
		}
	}
}

// TestRobotsCacheKeepsAnEntryTooBigForItsOwnBudget is the corner the eviction
// loop has to get right. A single robots.txt larger than the whole byte budget
// must not be evicted the moment it is stored, because the caller is about to
// use it and dropping it turns every page on that host into another fetch.
func TestRobotsCacheKeepsAnEntryTooBigForItsOwnBudget(t *testing.T) {
	rc := NewRobotsCacheWithLimits(time.Hour, "ccrawl", RobotsLimits{MaxBytes: 128})
	rc.Put("huge.example", &RobotsEntry{Rules: []RobotsRule{{Pattern: strings.Repeat("/x", 500)}}})
	if rc.Get("huge.example") == nil {
		t.Fatal("the entry was evicted before anybody could use it")
	}
}

// TestRobotsUnreachableIsCountedApartFromRefused is the done-when about telling
// the two apart. Both stop the page, and they mean opposite things: one is a
// corpus that does not want us and the other is a network that is not working.
func TestRobotsUnreachableIsCountedApartFromRefused(t *testing.T) {
	refused := &RobotsEntry{Rules: []RobotsRule{{Pattern: "/"}}}
	if refused.Unreachable {
		t.Fatal("a robots.txt we read and that refused us is not unreachable")
	}
	un := robotsUnreachable()
	if !un.Unreachable {
		t.Fatal("a host that could not be asked has to be marked as such")
	}
	if un.IsAllowed("/anything") {
		t.Fatal("an unreachable host is a complete disallow, per RFC 9309 section 2.3.1.4")
	}
}

// TestRobotsCacheCountsWhatItCost is the reporting done-when. The extra request
// per host is a third of the fleet's capacity on a domain corpus, so it is
// reported rather than guessed at.
func TestRobotsCacheCountsWhatItCost(t *testing.T) {
	var hits int
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		_, _ = w.Write([]byte("User-agent: *\nDisallow: /secret/\n"))
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")

	rc := NewRobotsCache(time.Hour, "ccrawl")
	h := robotsClient()
	for range 5 {
		rc.Fetch(context.Background(), h, host, "http")
	}
	st := rc.Stats()
	if st.Fetches != 1 {
		t.Errorf("the cache reports %d fetches for one host, want 1", st.Fetches)
	}
	if st.Hits != 4 {
		t.Errorf("the cache reports %d hits over five lookups of one host, want 4", st.Hits)
	}
	if st.Entries != 1 {
		t.Errorf("the cache holds %d entries for one host", st.Entries)
	}
	if hits != 1 {
		t.Errorf("the host was asked %d times, want 1", hits)
	}
}

func TestRobotsCacheCountsAnUnreachableHost(t *testing.T) {
	_, host := robotsServer(t, 500, "")
	rc := NewRobotsCache(time.Hour, "ccrawl")
	e := rc.Fetch(context.Background(), robotsClient(), host, "http")
	if !e.Unreachable {
		t.Fatal("a 500 on robots.txt leaves the host unreachable")
	}
	if st := rc.Stats(); st.Unreachable != 1 {
		t.Errorf("the cache reports %d unreachable hosts, want 1", st.Unreachable)
	}
}

// TestRobotsCacheHonoursAStatedLifetime is the "cached for its stated lifetime"
// half of the first done-when. A host that says how long to hold its robots.txt
// gets held for that long rather than for our default day.
func TestRobotsCacheHonoursAStatedLifetime(t *testing.T) {
	cases := []struct {
		header string
		want   time.Duration
	}{
		{"", 0},
		{"max-age=3600", time.Hour},
		{"public, max-age=1800", 30 * time.Minute},
		{"max-age=0", robotsMinTTL},            // not before every page
		{"max-age=99999999", DefaultRobotsTTL}, // and not for a year either
		{"no-store", robotsMinTTL},
		{"max-age=nonsense", 0},
		{"private", 0},
	}
	for _, c := range cases {
		if got := robotsStatedTTL(c.header); got != c.want {
			t.Errorf("Cache-Control %q gave a lifetime of %s, want %s", c.header, got, c.want)
		}
	}
}

func TestFetchRobotsTakesTheLifetimeFromTheResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=600")
		_, _ = w.Write([]byte("User-agent: *\nDisallow: /x/\n"))
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")

	rc := NewRobotsCache(DefaultRobotsTTL, "ccrawl")
	e := rc.Fetch(context.Background(), robotsClient(), host, "http")
	if e.TTL != 10*time.Minute {
		t.Fatalf("the entry carries a lifetime of %s, want the 10 minutes the host asked for", e.TTL)
	}
	if wait := time.Until(time.Unix(e.ExpiresAt, 0)); wait > 11*time.Minute {
		t.Fatalf("the cache holds it for %s, so the stated lifetime was ignored", wait)
	}
}

// TestRobotsCacheRefetchesAnExpiredEntry is the rest of the lifetime story. The
// entry has to go when its time is up, and the cache has to say so, or a run
// reports a hit rate it did not have.
func TestRobotsCacheRefetchesAnExpiredEntry(t *testing.T) {
	rc := NewRobotsCacheWithLimits(time.Millisecond, "ccrawl", RobotsLimits{MaxEntries: 10})
	rc.Put("example.com", &RobotsEntry{})
	time.Sleep(1100 * time.Millisecond) // ExpiresAt has a one second resolution
	if rc.Get("example.com") != nil {
		t.Fatal("an expired entry came back out of the cache")
	}
	st := rc.Stats()
	if st.Expired != 1 {
		t.Errorf("the cache reports %d expired entries, want 1", st.Expired)
	}
	if st.Entries != 0 {
		t.Errorf("the expired entry is still held, %d entries", st.Entries)
	}
}

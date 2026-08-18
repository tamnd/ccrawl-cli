package ccrawl

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
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

package ccrawl

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFrontierAddDedup(t *testing.T) {
	f := NewFrontier(time.Second)
	added := f.Add(FrontierEntry{URL: "https://example.com/", Host: "example.com", Priority: 1.0})
	if !added {
		t.Error("first Add should return true")
	}
	added2 := f.Add(FrontierEntry{URL: "https://example.com/", Host: "example.com", Priority: 1.0})
	if added2 {
		t.Error("duplicate URL should not be added again")
	}
	if f.Len() != 1 {
		t.Errorf("Frontier.Len = %d, want 1", f.Len())
	}
}

func TestFrontierPriority(t *testing.T) {
	f := NewFrontier(0) // no delay for test
	f.Add(FrontierEntry{URL: "https://low.com/", Host: "low.com", Priority: 1.0})
	f.Add(FrontierEntry{URL: "https://high.com/", Host: "high.com", Priority: 100.0})
	f.Add(FrontierEntry{URL: "https://mid.com/", Host: "mid.com", Priority: 50.0})

	now := time.Now().Unix()
	e1, ok := f.Pop(now)
	if !ok {
		t.Fatal("Pop returned nothing")
	}
	if e1.Host != "high.com" {
		t.Errorf("first Pop should return highest priority (high.com), got %s", e1.Host)
	}
}

func TestFrontierPoliteness(t *testing.T) {
	f := NewFrontier(10 * time.Second)
	f.Add(FrontierEntry{URL: "https://example.com/a", Host: "example.com", Priority: 1.0})
	f.Add(FrontierEntry{URL: "https://example.com/b", Host: "example.com", Priority: 0.5})

	now := time.Now().Unix()
	e1, ok := f.Pop(now)
	if !ok {
		t.Fatal("first Pop should succeed")
	}
	if e1.Host != "example.com" {
		t.Fatalf("unexpected host %s", e1.Host)
	}
	// second Pop for same host should fail (delay not elapsed)
	_, ok2 := f.Pop(now)
	if ok2 {
		t.Error("second Pop for same host within delay should fail")
	}
	// after delay has elapsed it should succeed
	e3, ok3 := f.Pop(now + 15)
	if !ok3 {
		t.Error("Pop after delay elapsed should succeed")
	}
	if e3.Host != "example.com" {
		t.Errorf("unexpected host %s", e3.Host)
	}
}

func TestContentSHA1(t *testing.T) {
	h1 := ContentSHA1([]byte("hello"))
	h2 := ContentSHA1([]byte("hello"))
	h3 := ContentSHA1([]byte("world"))
	if h1 != h2 {
		t.Error("same content should have same SHA-1")
	}
	if h1 == h3 {
		t.Error("different content should have different SHA-1")
	}
	// known SHA-1 for "hello"
	want := "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"
	if h1 != want {
		t.Errorf("SHA-1('hello') = %q, want %q", h1, want)
	}
}

func TestParseRobots(t *testing.T) {
	robotsTxt := `User-agent: *
Disallow: /private/
Allow: /private/public.html
Crawl-delay: 2

User-agent: googlebot
Disallow: /admin/`
	entry := parseRobots(strings.NewReader(robotsTxt))
	if len(entry.Rules) == 0 {
		t.Fatal("expected rules to be parsed")
	}
	if entry.CrawlDelay != 2*time.Second {
		t.Errorf("Crawl-delay = %s, want 2s", entry.CrawlDelay)
	}
	// /private/ should be disallowed
	if entry.IsAllowed("/private/secret.html") {
		t.Error("/private/secret.html should be disallowed")
	}
	// /private/public.html should be allowed (Allow rule)
	if !entry.IsAllowed("/private/public.html") {
		t.Error("/private/public.html should be allowed")
	}
	// /public/ should be allowed (not in rules)
	if !entry.IsAllowed("/public/page.html") {
		t.Error("/public/page.html should be allowed")
	}
}

func TestRobotsCache(t *testing.T) {
	rc := NewRobotsCache(24*time.Hour, "ccrawl")
	if rc.Get("example.com") != nil {
		t.Error("empty cache should return nil")
	}
	e := &RobotsEntry{Rules: []RobotsRule{{Allow: false, Pattern: "/admin/"}}}
	rc.Put("example.com", e)
	got := rc.Get("example.com")
	if got == nil {
		t.Fatal("Put then Get should return entry")
	}
	if len(got.Rules) != 1 {
		t.Errorf("expected 1 rule, got %d", len(got.Rules))
	}
}

func TestFetchRobots(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("User-agent: *\nDisallow: /secret/\n"))
		}
	}))
	defer srv.Close()

	h := NewHTTPClient(DefaultConfig())
	host := strings.TrimPrefix(srv.URL, "http://")
	entry := FetchRobots(context.Background(), h, host, "http")
	if entry == nil {
		t.Fatal("FetchRobots returned nil")
	}
	if entry.IsAllowed("/secret/page") {
		t.Error("/secret/ should be disallowed")
	}
	if !entry.IsAllowed("/public/page") {
		t.Error("/public/ should be allowed")
	}
}

func TestCrawlURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<html><head><title>Test</title></head><body><a href="/about">About</a></body></html>`))
	}))
	defer srv.Close()

	res, err := CrawlURL(context.Background(), srv.URL+"/", DefaultCrawlConfig)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != 200 {
		t.Errorf("Status = %d, want 200", res.Status)
	}
	if len(res.Body) == 0 {
		t.Error("Body should not be empty")
	}
	if res.Digest == "" {
		t.Error("Digest should not be empty")
	}
	if len(res.Links) == 0 {
		t.Error("should have extracted links")
	}
}

func TestNormalizeURL(t *testing.T) {
	cases := []struct{ in, want string }{
		{"HTTPS://Example.COM/page?utm_source=g&page=1#section", "https://example.com/page?page=1"},
		{"http://example.com:80/", "http://example.com/"},
		{"https://example.com:443/", "https://example.com/"},
		{"https://example.com/page?fbclid=abc", "https://example.com/page"},
	}
	for _, c := range cases {
		got := NormalizeURL(c.in)
		if got != c.want {
			t.Errorf("NormalizeURL(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestWriteWARCResponse(t *testing.T) {
	var buf bytes.Buffer
	rec := NewWARCRecord{
		TargetURI: "https://example.com/",
		Date:      "2026-06-17T10:00:00Z",
		RecordID:  "urn:uuid:test-record-id",
		Block:     []byte("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html>body</html>"),
	}
	if err := WriteWARCResponse(&buf, rec); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{
		"WARC/1.0", "WARC-Type: response",
		"https://example.com/", "2026-06-17T10:00:00Z",
		"HTTP/1.1 200 OK",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("WARC output missing %q", want)
		}
	}
}

func TestExtractOutLinks(t *testing.T) {
	html := []byte(`<html><body>
<a href="/about">About</a>
<a href="https://other.com/page">External</a>
<a href="mailto:x@y.com">Email</a>
</body></html>`)
	links := ExtractOutLinks(html, "https://example.com/")
	found := map[string]bool{}
	for _, l := range links {
		found[l] = true
	}
	if !found["https://example.com/about"] {
		t.Error("relative /about link not resolved")
	}
	if !found["https://other.com/page"] {
		t.Error("external link not extracted")
	}
	for l := range found {
		if strings.HasPrefix(l, "mailto:") {
			t.Errorf("mailto: link should be excluded: %s", l)
		}
	}
}

package ccrawl

import (
	"strings"
	"testing"
)

func TestExtractContent(t *testing.T) {
	html := `<!DOCTYPE html>
<html lang="en">
<head>
<title>Test Page</title>
<meta name="description" content="A test description">
<link rel="canonical" href="https://example.com/test">
</head>
<body>
<nav>Navigation links</nav>
<main>
<h1>Hello World</h1>
<p>This is the main content of the page. It has enough words to be meaningful.</p>
<p>Second paragraph with more content here and there.</p>
</main>
<footer>Footer content</footer>
<script>var x = 1;</script>
</body>
</html>`
	tr := ExtractContent([]byte(html))
	if tr.Title != "Test Page" {
		t.Errorf("Title = %q, want 'Test Page'", tr.Title)
	}
	if tr.Description != "A test description" {
		t.Errorf("Description = %q", tr.Description)
	}
	if tr.CanonURL != "https://example.com/test" {
		t.Errorf("CanonURL = %q", tr.CanonURL)
	}
	if tr.Language != "en" {
		t.Errorf("Language = %q, want 'en'", tr.Language)
	}
	if !strings.Contains(tr.Body, "Hello World") {
		t.Errorf("Body does not contain 'Hello World'")
	}
	// script content must not appear
	if strings.Contains(tr.Body, "var x = 1") {
		t.Error("Body should not contain script content")
	}
	if tr.WordCount == 0 {
		t.Error("WordCount should be > 0")
	}
}

func TestExtractContentOGURL(t *testing.T) {
	html := `<html><head>
<meta property="og:url" content="https://example.com/og">
</head><body><p>Content</p></body></html>`
	tr := ExtractContent([]byte(html))
	if tr.CanonURL != "https://example.com/og" {
		t.Errorf("CanonURL from og:url = %q", tr.CanonURL)
	}
}

func TestQualitySignals(t *testing.T) {
	// short content
	tr := TextResult{Title: "Domain for Sale", Body: "Buy this domain now.", WordCount: 4}
	q := QualitySignals(tr)
	if !q.IsParked {
		t.Error("expected IsParked=true for parked domain text")
	}
	if !q.IsShortContent {
		t.Error("expected IsShortContent=true for 4 words")
	}
	if q.HasMainContent {
		t.Error("expected HasMainContent=false for 4 words")
	}

	// real content
	tr2 := TextResult{Title: "Go Programming", Body: strings.Repeat("word ", 100), WordCount: 100}
	q2 := QualitySignals(tr2)
	if !q2.HasMainContent {
		t.Error("expected HasMainContent=true for 100 words")
	}
	if q2.IsParked {
		t.Error("should not be parked")
	}
}

func TestSpamScore(t *testing.T) {
	s := spamScore("Buy now! Limited offer! Click here to earn $1000. Guaranteed casino")
	if s <= 0.3 {
		t.Errorf("spam score too low: %f", s)
	}
	s2 := spamScore("The Go programming language is great for systems programming.")
	if s2 > 0.1 {
		t.Errorf("legit text has high spam score: %f", s2)
	}
}

func TestStripTrackingParams(t *testing.T) {
	cases := []struct{ in, want string }{
		{"utm_source=google&utm_medium=cpc&page=1", "page=1"},
		{"fbclid=abc123", ""},
		{"gclid=def&q=search&ref=home", "q=search"},
		{"", ""},
		{"page=2&size=10", "page=2&size=10"},
	}
	for _, c := range cases {
		got := StripTrackingParams(c.in)
		if got != c.want {
			t.Errorf("StripTrackingParams(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestDocumentID(t *testing.T) {
	id1 := DocumentID("https://example.com/page")
	id2 := DocumentID("https://example.com/page")
	id3 := DocumentID("https://example.com/other")
	if id1 != id2 {
		t.Error("DocumentID not deterministic")
	}
	if id1 == id3 {
		t.Error("different URLs should have different IDs")
	}
}

func TestCanonicalURL(t *testing.T) {
	html := `<html><head>
<link rel="canonical" href="https://example.com/canonical">
<meta property="og:url" content="https://example.com/og">
</head><body></body></html>`
	// link[rel=canonical] should take priority
	got := HTMLCanonicalURL([]byte(html))
	if got != "https://example.com/canonical" {
		t.Errorf("CanonicalURL = %q", got)
	}
}


package ccrawl

import (
	"strings"
	"testing"
)

func TestMarkdownDocID(t *testing.T) {
	a := MarkdownDocID("https://example.com/path")
	b := MarkdownDocID("https://example.com/path")
	if a != b {
		t.Fatalf("MarkdownDocID not deterministic: %q vs %q", a, b)
	}
	if len(a) != 32 {
		t.Errorf("MarkdownDocID length = %d, want 32 (16 bytes hex)", len(a))
	}
	c := MarkdownDocID("https://example.com/other")
	if a == c {
		t.Error("different URLs produced the same doc_id")
	}
}

func TestIsHTMLMIME(t *testing.T) {
	cases := map[string]bool{
		"text/html":                true,
		"text/html; charset=utf-8": true,
		"TEXT/HTML":                true,
		"application/xhtml+xml":    true,
		"application/json":         false,
		"image/png":                false,
		"text/plain":               false,
		"":                         false,
	}
	for mime, want := range cases {
		if got := isHTMLMIME(mime); got != want {
			t.Errorf("isHTMLMIME(%q) = %v, want %v", mime, got, want)
		}
	}
}

func TestHFMarkdownPath(t *testing.T) {
	got := HFMarkdownPath("CC-MAIN-2026-25", 42)
	want := "data/crawl=CC-MAIN-2026-25/000042.parquet"
	if got != want {
		t.Errorf("HFMarkdownPath = %q, want %q", got, want)
	}
}

func TestHTMLToMarkdown(t *testing.T) {
	body := []byte(`<!doctype html><html lang="en">
<head><meta charset="utf-8"><title>Test</title></head>
<body>
<nav><a href="/a">nav noise that should be dropped by readability</a></nav>
<article>
<h1>Real Article Title</h1>
<p>First paragraph with enough words to be treated as article content by the readability extractor rather than boilerplate navigation text to discard.</p>
<p>Second paragraph that also carries enough words to survive extraction as genuine article body text worth storing in the markdown column.</p>
<p>Third paragraph to make the article substantial. It mentions a <a href="/related">related page</a> with a relative link that should become absolute.</p>
</article>
<footer>copyright boilerplate</footer>
</body></html>`)

	md := htmlToMarkdown(body, "https://example.com/page")
	if md == "" {
		t.Fatal("htmlToMarkdown returned empty for a valid article")
	}
	if !strings.Contains(md, "Real Article Title") {
		t.Errorf("heading missing from markdown:\n%s", md)
	}
	if !strings.Contains(md, "First paragraph") {
		t.Errorf("body text missing from markdown:\n%s", md)
	}
	if !strings.Contains(md, "https://example.com/related") {
		t.Errorf("relative link not resolved to absolute:\n%s", md)
	}
}

func TestHTMLToMarkdownEdgeCases(t *testing.T) {
	if got := htmlToMarkdown(nil, "https://example.com/"); got != "" {
		t.Errorf("nil body: expected empty, got %q", got)
	}
	if got := htmlToMarkdown([]byte{}, "https://example.com/"); got != "" {
		t.Errorf("empty body: expected empty, got %q", got)
	}
}

func TestHTMLToMarkdownLatin1(t *testing.T) {
	// é in ISO-8859-1 is 0xE9. Charset meta declares latin-1.
	body := []byte("<html><head><meta charset=\"iso-8859-1\"><title>t</title></head>" +
		"<body><article><h1>Caf\xe9 article</h1>" +
		"<p>A paragraph about a caf\xe9 with enough words to read as an article body by readability and survive extraction into the markdown column cleanly.</p>" +
		"<p>Second paragraph to make the article substantial enough for the extractor heuristics to treat it as real content worth keeping.</p>" +
		"</article></body></html>")
	md := htmlToMarkdown(body, "https://example.com/cafe")
	if !strings.Contains(md, "Café") {
		t.Errorf("latin-1 0xE9 byte not transcoded to UTF-8 é in:\n%s", md)
	}
}

func TestURLHostname(t *testing.T) {
	cases := []struct{ in, want string }{
		{"https://en.wikipedia.org/wiki/Go", "en.wikipedia.org"},
		{"http://example.com:8080/path", "example.com"},
		{"", ""},
		{"not a url", ""},
	}
	for _, tc := range cases {
		if got := urlHostname(tc.in); got != tc.want {
			t.Errorf("urlHostname(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

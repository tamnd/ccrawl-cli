package ccrawl

import (
	"strings"
	"testing"
)

const samplePage = `<!doctype html>
<html><head><title>  Hello   World </title></head>
<body>
<h1>Heading</h1>
<p>First paragraph with a <a href="/rel">relative link</a>.</p>
<p>Second with an <a href="https://other.example/x">absolute link</a>.</p>
<script>ignored()</script>
</body></html>`

func TestExtractTitle(t *testing.T) {
	if got := ExtractTitle([]byte(samplePage)); got != "Hello World" {
		t.Errorf("ExtractTitle = %q", got)
	}
}

func TestExtractText(t *testing.T) {
	got := ExtractText([]byte(samplePage))
	if !strings.Contains(got, "Heading") || !strings.Contains(got, "First paragraph") {
		t.Errorf("ExtractText missing content: %q", got)
	}
	if strings.Contains(got, "ignored") {
		t.Errorf("ExtractText leaked script: %q", got)
	}
}

func TestExtractLinks(t *testing.T) {
	links := ExtractLinks("https://site.example/page", []byte(samplePage))
	var rel, abs bool
	for _, l := range links {
		if l.URL == "https://site.example/rel" {
			rel = true
		}
		if l.URL == "https://other.example/x" {
			abs = true
		}
	}
	if !rel {
		t.Errorf("relative link not resolved against base: %+v", links)
	}
	if !abs {
		t.Errorf("absolute link missing: %+v", links)
	}
}

func TestExtractMarkdown(t *testing.T) {
	md, err := ExtractMarkdown([]byte(samplePage))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(md, "# Heading") {
		t.Errorf("markdown missing heading: %q", md)
	}
	if !strings.Contains(md, "[relative link](https://site.example/rel)") &&
		!strings.Contains(md, "relative link") {
		t.Errorf("markdown missing link text: %q", md)
	}
}

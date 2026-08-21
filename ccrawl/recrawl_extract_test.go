package ccrawl

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

// articleSite serves pages with enough prose on them that an extractor has
// something to find. The recrawl site's one paragraph is right for counting
// fetches and useless for checking extraction, because a boilerplate remover
// handed a page that is all boilerplate is entitled to return nothing.
type articleSite struct{ contentType string }

func (s articleSite) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/robots.txt" {
		http.NotFound(w, r)
		return
	}
	ct := s.contentType
	if ct == "" {
		ct = "text/html; charset=utf-8"
	}
	w.Header().Set("Content-Type", ct)
	_, _ = fmt.Fprintf(w, `<html lang="en"><head><title>The page at %s</title></head><body>
<nav><a href="/">home</a><a href="/about">about</a></nav>
<article><h1>A heading worth keeping</h1>
<p>The quick brown fox jumps over the lazy dog while the whole town watches from the windows of the houses along the road.</p>
<p>Every extractor worth the name should find these two paragraphs and leave the navigation behind, because the navigation is on every page and the paragraphs are not.</p>
</article><footer>copyright nobody</footer></body></html>`, r.URL.Path)
}

// readAllCaptures reads every shard a run wrote, sealed or not, so a test can
// look at the rows rather than at the counters.
func readAllCaptures(t *testing.T, dir string) []Capture {
	t.Helper()
	shards, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	var out []Capture
	for _, f := range shards {
		rows, err := ReadCaptures(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		out = append(out, rows...)
	}
	return out
}

// runExtractRecrawl walks a few pages of the given site and returns the rows.
func runExtractRecrawl(t *testing.T, site http.Handler, tune func(*RecrawlConfig)) []Capture {
	t.Helper()
	srv := httptest.NewServer(site)
	defer srv.Close()

	paths := []string{"/one", "/two", "/three"}
	parts := writeWorkList(t, srv.URL, paths, 4)

	cfg := testRecrawlConfig("")
	cfg.OutDir = t.TempDir()
	cfg.Format = FormatParquet
	if tune != nil {
		tune(&cfg)
	}
	r := newTestRecrawler(t, cfg, parts)
	if _, err := r.Run(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	rows := readAllCaptures(t, cfg.OutDir)
	if len(rows) != len(paths) {
		t.Fatalf("wrote %d rows, want %d", len(rows), len(paths))
	}
	return rows
}

// TestRecrawlFillsTextColumns is the whole point of extracting during the fetch:
// a published shard has the text in it, so nobody has to read the corpus back to
// get at what the pages said.
func TestRecrawlFillsTextColumns(t *testing.T) {
	rows := runExtractRecrawl(t, articleSite{}, nil)
	for _, c := range rows {
		if c.Markdown == "" {
			t.Fatalf("%s rendered to no Markdown", c.URL)
		}
		if c.MarkdownLength != int64(len(c.Markdown)) {
			t.Fatalf("%s says its Markdown is %d bytes and it is %d", c.URL, c.MarkdownLength, len(c.Markdown))
		}
		if !strings.Contains(c.Markdown, "quick brown fox") {
			t.Fatalf("%s lost the body of the page: %q", c.URL, c.Markdown)
		}
		if strings.Contains(c.Markdown, "copyright nobody") {
			t.Fatalf("%s kept the footer, so the extractor is not stripping boilerplate: %q", c.URL, c.Markdown)
		}
		if c.Title == "" || !strings.Contains(c.Title, "The page at") {
			t.Fatalf("%s has title %q", c.URL, c.Title)
		}
		if c.Text == "" || c.TextLength != int64(len(c.Text)) {
			t.Fatalf("%s has %d bytes of text and says %d", c.URL, len(c.Text), c.TextLength)
		}
		if c.WordCount < 20 {
			t.Fatalf("%s counted %d words in two paragraphs", c.URL, c.WordCount)
		}
		if c.Language != "eng" {
			t.Fatalf("%s detected as %q, want eng", c.URL, c.Language)
		}
		if c.Simhash == 0 {
			t.Fatalf("%s has no fingerprint", c.URL)
		}
		if !strings.HasPrefix(c.Extractor, DefaultExtractor+"@") {
			t.Fatalf("%s stamped %q, want %s@version", c.URL, c.Extractor, DefaultExtractor)
		}
		// The body is still there. The rendered columns are added beside it and
		// never in place of it, so a better extractor can be run over the same
		// bytes later.
		if len(c.Body) == 0 {
			t.Fatalf("%s lost its body", c.URL)
		}
	}
	// Three different paths on one template, so the pages differ in one line and
	// agree everywhere else. The fingerprint is meant to notice that.
	if rows[0].Simhash != rows[1].Simhash {
		t.Logf("near duplicate pages fingerprinted differently: %d and %d", rows[0].Simhash, rows[1].Simhash)
	}
}

// TestRecrawlWithoutExtractLeavesTheColumnsEmpty checks --no-extract does what
// it says, and that an empty extractor column is how a reader tells a run that
// did not try from a page that rendered to nothing.
func TestRecrawlWithoutExtractLeavesTheColumnsEmpty(t *testing.T) {
	rows := runExtractRecrawl(t, articleSite{}, func(cfg *RecrawlConfig) { cfg.Extract = false })
	for _, c := range rows {
		if c.Markdown != "" || c.Text != "" || c.Title != "" || c.Extractor != "" || c.Simhash != 0 {
			t.Fatalf("%s was extracted with extraction off: %+v", c.URL, c)
		}
		if len(c.Body) == 0 {
			t.Fatalf("%s has no body either, so the run did nothing", c.URL)
		}
	}
}

// TestRecrawlDoesNotExtractNonHTML is the guard against a column full of
// mojibake. An HTML extractor pointed at a PDF or a JPEG produces something that
// looks like text to a query and is not, and a corpus is better off with the
// column empty and the bytes still there.
func TestRecrawlDoesNotExtractNonHTML(t *testing.T) {
	rows := runExtractRecrawl(t, articleSite{contentType: "application/pdf"}, nil)
	for _, c := range rows {
		if c.Extractor != "" || c.Markdown != "" || c.Text != "" {
			t.Fatalf("%s was rendered despite being %s: extractor %q", c.URL, c.ContentType, c.Extractor)
		}
		if len(c.Body) == 0 {
			t.Fatalf("%s lost its body", c.URL)
		}
	}
}

func TestIsHTMLContent(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"text/html", true},
		{"text/html; charset=utf-8", true},
		{"TEXT/HTML;charset=iso-8859-1", true},
		{"application/xhtml+xml", true},
		// A server that sends no Content-Type at all is common enough on the
		// open web, and the body is HTML often enough, that guessing yes and
		// letting the parser decide beats dropping the page.
		{"", true},
		{"application/pdf", false},
		{"image/jpeg", false},
		{"application/json", false},
		{"text/plain", false},
	} {
		if got := isHTMLContent(tc.in); got != tc.want {
			t.Errorf("isHTMLContent(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestUnknownExtractorFailsAtStartup keeps a typo from costing a hundred days of
// fetching with an empty Markdown column.
func TestUnknownExtractorFailsAtStartup(t *testing.T) {
	cfg := testRecrawlConfig("")
	cfg.Extract = true
	cfg.Extractor = "definitely-not-an-engine"
	if _, err := NewRecrawler(cfg, crawlClient(), crawlClient()); err == nil {
		t.Fatal("an unknown extractor started up fine, so the run would have published empty columns")
	}
}

package ccrawl

import (
	"os"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/tamnd/h2m"
)

// Benchmarking extraction over pages the crawl actually fetched.
//
// The synthetic alternative is worse than useless here. Extraction cost is
// dominated by document size and by how much boilerplate the engine has to walk
// past, and a hand written fixture has neither in the proportions the open web
// has them. The recrawl writes its captures as Parquet with the body inline, so
// a shard off a real run is a ready made corpus to measure against.
//
// Point CCRAWL_BENCH_SHARD at one and run:
//
//	CCRAWL_BENCH_SHARD=/tmp/out/ccrawl-recrawl-00000.parquet go test ./ccrawl/ -run xxx -bench Extract
func benchBodies(b *testing.B) []Capture {
	path := os.Getenv("CCRAWL_BENCH_SHARD")
	if path == "" {
		b.Skip("set CCRAWL_BENCH_SHARD to a captures shard to measure extraction against real pages")
	}
	rows, err := parquet.ReadFile[Capture](path)
	if err != nil {
		b.Fatalf("read %s: %v", path, err)
	}
	var out []Capture
	for _, c := range rows {
		if len(c.Body) > 0 && isHTMLContent(c.ContentType) {
			out = append(out, c)
		}
		if len(out) >= 2000 {
			break
		}
	}
	if len(out) == 0 {
		b.Fatalf("%s holds no HTML bodies", path)
	}
	b.Logf("%d HTML pages", len(out))
	return out
}

// BenchmarkExtractMarkdown is the Markdown conversion on its own, which is one
// parse of the document plus the render.
func BenchmarkExtractMarkdown(b *testing.B) {
	pages := benchBodies(b)
	ex, _ := LookupExtractor("")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := pages[i%len(pages)]
		_ = ex.Convert(c.Body, c.URL)
	}
}

// BenchmarkExtractMarkdownFast is h2m's cheaper path, which is the same engine
// asked to do less work per document.
func BenchmarkExtractMarkdownFast(b *testing.B) {
	pages := benchBodies(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := pages[i%len(pages)]
		_ = h2m.ConvertFast(c.Body, c.URL)
	}
}

// BenchmarkExtractMarkdownReadability and BenchmarkExtractMarkdownRaw are the
// other two engines --extractor accepts, measured on the same pages so the
// choice between them is a number rather than a hunch.
func BenchmarkExtractMarkdownReadability(b *testing.B) {
	pages := benchBodies(b)
	ex, _ := LookupExtractor("readability")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := pages[i%len(pages)]
		_ = ex.Convert(c.Body, c.URL)
	}
}

func BenchmarkExtractMarkdownRaw(b *testing.B) {
	pages := benchBodies(b)
	ex, _ := LookupExtractor("raw")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := pages[i%len(pages)]
		_ = ex.Convert(c.Body, c.URL)
	}
}

// TestExtractorYield reports what each engine keeps, so the cost numbers next to
// it can be read as a trade rather than as a ranking. A cheaper engine that
// drops a third of the pages is not cheaper, it is a smaller corpus.
func TestExtractorYield(t *testing.T) {
	path := os.Getenv("CCRAWL_BENCH_SHARD")
	if path == "" {
		t.Skip("set CCRAWL_BENCH_SHARD to a captures shard to compare extractor yield")
	}
	rows, err := parquet.ReadFile[Capture](path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var pages []Capture
	for _, c := range rows {
		if len(c.Body) > 0 && isHTMLContent(c.ContentType) {
			pages = append(pages, c)
		}
	}
	for _, name := range []string{"h2m", "readability", "raw"} {
		ex, err := LookupExtractor(name)
		if err != nil {
			t.Fatal(err)
		}
		var kept, chars int
		for _, c := range pages {
			md := ex.Convert(c.Body, c.URL)
			if md != "" {
				kept++
				chars += len(md)
			}
		}
		t.Logf("%-12s kept %d of %d pages, %d chars in total, %d a page", name, kept, len(pages), chars, chars/max(kept, 1))
	}
	var fastKept, fastChars int
	for _, c := range pages {
		if r := h2m.ConvertFast(c.Body, c.URL); r.HasContent && r.Markdown != "" {
			fastKept++
			fastChars += len(r.Markdown)
		}
	}
	t.Logf("%-12s kept %d of %d pages, %d chars in total, %d a page", "h2m fast", fastKept, len(pages), fastChars, fastChars/max(fastKept, 1))
}

// BenchmarkExtractText is the text columns on their own, which is a second parse
// of the same bytes.
func BenchmarkExtractText(b *testing.B) {
	pages := benchBodies(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := pages[i%len(pages)]
		_ = ExtractContent(c.Body)
	}
}

// BenchmarkExtractFill is what a worker actually pays per page, both parses and
// the fingerprint and the language detector included.
func BenchmarkExtractFill(b *testing.B) {
	pages := benchBodies(b)
	p, err := newPageExtractor("", "")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := pages[i%len(pages)]
		c.Markdown, c.Text, c.Title = "", "", ""
		p.fill(&c)
	}
}

// BenchmarkExtractSimhash and BenchmarkExtractLanguage are the two passes over
// the rendered Markdown, measured apart because they are cheap enough to assume
// away and that assumption is worth checking rather than making.
func BenchmarkExtractSimhash(b *testing.B) {
	pages := benchBodies(b)
	ex, _ := LookupExtractor("")
	md := make([]string, len(pages))
	for i, c := range pages {
		md[i] = ex.Convert(c.Body, c.URL)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Simhash(md[i%len(md)])
	}
}

func BenchmarkExtractLanguage(b *testing.B) {
	pages := benchBodies(b)
	ex, _ := LookupExtractor("")
	md := make([]string, len(pages))
	for i, c := range pages {
		md[i] = ex.Convert(c.Body, c.URL)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DetectLanguage(md[i%len(md)])
	}
}

package ccrawl

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"runtime/debug"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/tamnd/h2m"
	"github.com/tamnd/yomi/extract"
	"github.com/tamnd/yomi/mdconv"
	"golang.org/x/net/html"
	"golang.org/x/net/html/charset"
)

// Extractor is one way of turning a captured page into the Markdown that lands
// in the dataset. Which one you use is a corpus quality decision, not a build
// time constant: the same shard run through two engines is two different
// corpora, and the only way to find out which one suits a downstream task is to
// build both and compare.
//
// Convert returns "" when the page yields nothing worth keeping, which the
// pipelines treat as a skipped record rather than an error.
type Extractor struct {
	// Name is the value written to the extractor column, before the version.
	Name string
	// Summary is the one line shown in help.
	Summary string
	// Module is the Go module whose version identifies this engine. Empty when
	// the engine is not ours to version, which is the WET case.
	Module string
	// SourceKind is the manifest a run using this extractor has to read, "warc"
	// or "wet". They are not interchangeable: a WET file holds no HTML and a
	// WARC holds no pre-extracted text.
	SourceKind string

	convert func(body []byte, pageURL string) string
}

// Extractors are the engines --extractor accepts, keyed by name.
var Extractors = map[string]*Extractor{
	"h2m": {
		Name:       "h2m",
		Summary:    "go-trafilatura tuned for recall, rendered as GFM (default)",
		Module:     "github.com/tamnd/h2m",
		SourceKind: "warc",
		convert:    htmlToMarkdownH2M,
	},
	"readability": {
		Name:       "readability",
		Summary:    "go-readability extraction, the engine open-markdown-v2 shipped",
		Module:     "github.com/tamnd/yomi",
		SourceKind: "warc",
		convert:    htmlToMarkdownReadability,
	},
	"raw": {
		Name:       "raw",
		Summary:    "the whole document rendered as Markdown, no boilerplate removal",
		Module:     "github.com/tamnd/yomi",
		SourceKind: "warc",
		convert:    htmlToMarkdownRaw,
	},
	"wet": {
		Name:       "wet",
		Summary:    "the plain text Common Crawl already extracted, from the WET files",
		SourceKind: "wet",
		convert:    wetTextPassthrough,
	},
}

// DefaultExtractor is the engine a run uses when nobody says otherwise.
const DefaultExtractor = "h2m"

// LookupExtractor resolves an --extractor value.
func LookupExtractor(name string) (*Extractor, error) {
	if name == "" {
		name = DefaultExtractor
	}
	e, ok := Extractors[strings.ToLower(strings.TrimSpace(name))]
	if !ok {
		return nil, fmt.Errorf("unknown extractor %q, want one of %s", name, strings.Join(ExtractorNames(), ", "))
	}
	return e, nil
}

// ExtractorNames lists the known engines in a stable order.
func ExtractorNames() []string {
	names := make([]string, 0, len(Extractors))
	for n := range Extractors {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Convert turns one captured body into Markdown.
func (e *Extractor) Convert(body []byte, pageURL string) string {
	if e == nil {
		return htmlToMarkdownH2M(body, pageURL)
	}
	return e.convert(body, pageURL)
}

// ID is what goes in the extractor column and the dataset card: the engine name
// and the version of the code that produced the text, as name@version.
//
// The version matters more than the name. Extraction changes between releases,
// sometimes visibly, and a dataset that only records "h2m" cannot answer why two
// shards built three months apart disagree about the same page.
//
// The WET engine is Common Crawl's own and has no version we can read, so it is
// stamped with the crawl instead, which is the closest thing to a version the
// text has.
func (e *Extractor) ID(crawlID string) string {
	if e == nil {
		return DefaultExtractor + "@" + moduleVersion(Extractors[DefaultExtractor].Module)
	}
	if e.Module == "" {
		if crawlID == "" {
			return e.Name
		}
		return e.Name + "@" + crawlID
	}
	return e.Name + "@" + moduleVersion(e.Module)
}

// moduleVersion reads a dependency's version out of the build info of the
// running binary, so the stamp describes the build that actually ran rather
// than whatever go.mod said when this file was written.
func moduleVersion(path string) string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, d := range info.Deps {
		if d.Path == path {
			if d.Replace != nil && d.Replace.Version != "" {
				return d.Replace.Version
			}
			return d.Version
		}
	}
	return "unknown"
}

// htmlToMarkdownH2M converts one HTML body to Markdown via h2m: go-trafilatura
// tuned for recall strips boilerplate and isolates the main content, then h2m
// renders GitHub-flavored Markdown with links resolved against pageURL. h2m
// transcodes non-UTF-8 bodies (GBK, Shift-JIS, Latin-1, and so on) from the
// page's declared charset before parsing, so Common Crawl's mixed-encoding
// pages convert correctly.
func htmlToMarkdownH2M(body []byte, pageURL string) string {
	if len(body) == 0 {
		return ""
	}
	res := h2m.Convert(body, pageURL)
	if !res.HasContent {
		return ""
	}
	return validUTF8(res.Markdown)
}

// htmlToMarkdownReadability is the engine open-markdown-v2 shipped: yomi's
// go-readability extraction plus mdconv rendering. It is faster than h2m and
// keeps less of the page, which on a news article is usually the same text and
// on a listing page is much less of it.
func htmlToMarkdownReadability(body []byte, pageURL string) string {
	utf8Body := transcode(body)
	if len(utf8Body) == 0 {
		return ""
	}
	art, err := extract.FromHTML(utf8Body, pageURL)
	if err != nil || art.Node == nil {
		return ""
	}
	base, _ := url.Parse(pageURL)
	md, err := mdconv.Convert(art.Node, mdconv.Options{Base: base})
	if err != nil {
		return ""
	}
	return validUTF8(strings.TrimSpace(md))
}

// htmlToMarkdownRaw renders the whole document and skips extraction entirely,
// so the nav bars, the footer, and the cookie banner all come along.
//
// That sounds useless and is not. Every extractor is a lossy judgement about
// what the page was for, and on the pages where that judgement goes wrong there
// is no way to tell from the output alone. Raw is the control: it says what was
// on the page, so a corpus built with a real extractor can be measured against
// what it threw away.
func htmlToMarkdownRaw(body []byte, pageURL string) string {
	utf8Body := transcode(body)
	if len(utf8Body) == 0 {
		return ""
	}
	doc, err := html.Parse(bytes.NewReader(utf8Body))
	if err != nil {
		return ""
	}
	base, _ := url.Parse(pageURL)
	md, err := mdconv.Convert(doc, mdconv.Options{Base: base})
	if err != nil {
		return ""
	}
	return validUTF8(strings.TrimSpace(md))
}

// wetTextPassthrough is the WET engine: Common Crawl already extracted the text,
// so there is nothing to do but keep it. It is not Markdown and does not pretend
// to be, which is the trade for text that costs no CPU and no HTML download.
func wetTextPassthrough(body []byte, _ string) string {
	return validUTF8(strings.TrimSpace(string(body)))
}

// transcode converts a body to UTF-8 using its declared charset. A page that
// declares nothing is sniffed, and one that declares a charset it does not
// honour comes out with replacement characters rather than an error.
func transcode(body []byte) []byte {
	if len(body) == 0 {
		return nil
	}
	r, err := charset.NewReader(bytes.NewReader(body), "")
	if err != nil {
		return nil
	}
	out, err := io.ReadAll(r)
	if err != nil {
		return nil
	}
	return out
}

// validUTF8 drops invalid byte sequences. A handful of pages declare one charset
// and serve another, so transcoded text can still carry stray bytes, and parquet
// strings and the Arrow readers on top of them require valid UTF-8.
func validUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	return strings.ToValidUTF8(s, "")
}

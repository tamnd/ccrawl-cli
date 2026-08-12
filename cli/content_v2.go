package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func registerContentV2(app *kit.App) {
	registerContentExtract(app)
	registerContentQuality(app)
	registerContentLinksV2(app)
	registerContentLang(app)
}

// ── the URL argument the four content commands share ──────────────────────────

// contentArgHelp is the same sentence on all four commands, so the stream form
// is discoverable from any of them.
const contentArgHelp = `URL to fetch, or "-" to read URLs from stdin`

// contentURL is what a bare host in an argument means. Scheme-less input is
// common enough at a shell that rejecting it would be pedantry.
func contentURL(raw string) string {
	if !strings.HasPrefix(raw, "http") {
		return "https://" + raw
	}
	return raw
}

// stdinBelongsToCaller reports whether stdin is the caller's to read.
//
// The content commands are registered as operations, so the same handler serves
// the command line, the REST API and the MCP tool. On the command line "-" means
// the caller's stdin. Under 'ccrawl mcp' stdin carries the JSON-RPC session and
// under 'ccrawl serve' or 'ccrawl api' it belongs to whoever started the server,
// neither of which is the client asking for the URL. Reading it there would at
// best hang and at worst eat the protocol, so the argument is refused instead.
//
// The surfaces are told apart by the argv the process was started with, because
// kit does not carry the surface on the context and the server commands are its
// own rather than ours to wrap.
func stdinBelongsToCaller() bool {
	for _, a := range os.Args[1:] {
		if a == "serve" || a == "mcp" || a == "api" {
			return false
		}
	}
	return true
}

// eachContentPage fetches every page the url argument names and hands each one
// to fn. A plain argument is one URL. "-" reads stdin, where a line is either a
// bare URL or a JSON object with a url field, which is what search, columnar
// and crawl fetch write with -o jsonl.
//
// A URL that cannot be fetched ends a single-URL run with an error, because that
// run has nothing else to do. In a stream it is named on stderr and the rest of
// the input carries on, since any real list of seeds has a few dead hosts in it
// and losing the other nine hundred to them helps nobody. A stream where every
// URL failed is an error rather than an empty result, for the same reason a
// search that could not read the index does not exit 3: nothing was learned
// about the pages, they were never seen.
func eachContentPage(ctx context.Context, arg string, fn func(*ccrawl.CrawlResult) error) error {
	if arg != "-" {
		raw := contentURL(arg)
		res, err := ccrawl.CrawlURL(ctx, raw, ccrawl.DefaultCrawlConfig)
		if err != nil {
			return fmt.Errorf("fetch %s: %w", raw, err)
		}
		return fn(res)
	}
	if !stdinBelongsToCaller() {
		return usageErr(`"-" reads URLs from stdin and only works on the command line; give a URL instead`)
	}

	var read, failed int
	err := readLines(os.Stdin, func(line string) error {
		raw, err := urlFromLine(line)
		if err != nil {
			return err
		}
		read++
		res, err := ccrawl.CrawlURL(ctx, contentURL(raw), ccrawl.DefaultCrawlConfig)
		if err != nil {
			failed++
			_, _ = fmt.Fprintf(cmdErr, "fetch %s: %v, skipping it\n", raw, err)
			return nil
		}
		return fn(res)
	})
	if err != nil {
		return err
	}
	if failed > 0 {
		if failed == read {
			return fmt.Errorf("no URL on stdin could be fetched (%d tried)", read)
		}
		_, _ = fmt.Fprintf(cmdErr, "%d of the %d URLs on stdin could not be fetched\n", failed, read)
	}
	return nil
}

// urlFromLine reads one line of stdin as either a bare URL or a JSON object
// carrying one. url wins over final_url so a redirect chain is followed again
// from where the caller started rather than from where it last ended up.
func urlFromLine(line string) (string, error) {
	if !strings.HasPrefix(line, "{") {
		return line, nil
	}
	var rec struct {
		URL      string `json:"url"`
		FinalURL string `json:"final_url"`
	}
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		return "", fmt.Errorf("stdin: %w", err)
	}
	if rec.URL != "" {
		return rec.URL, nil
	}
	if rec.FinalURL != "" {
		return rec.FinalURL, nil
	}
	return "", fmt.Errorf("stdin: a JSON line with no url field: %s", line)
}

// ── content lang ──────────────────────────────────────────────────────────────

type contentLangIn struct {
	App *App   `kit:"inject"`
	URL string `kit:"arg" name:"url" help:"fetch and identify this URL, or read URLs from stdin with \"-\""`
}

// LangReport is what the markdown pipelines decide on for one URL.
type LangReport struct {
	URL        string  `json:"url" table:"url"`
	Language   string  `json:"language" table:"language"`
	Confidence float64 `json:"confidence" table:"confidence"`
	CCLanguage string  `json:"cc_language,omitempty" table:"cc_language"`
	Chars      int     `json:"chars" table:"chars"`

	// Sample is the text the identifier actually saw, truncated. When an answer
	// looks wrong this is the first thing to look at, because it is usually the
	// input that is wrong and not the identifier.
	Sample string `json:"sample" table:"sample"`
}

func registerContentLang(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:    "lang",
		Parent:  "content",
		Summary: "Identify the language of a page the way the markdown pipelines do",
		Long: `Fetch a URL, convert it to Markdown, and run the same identifier that
"markdown export --lang" applies, so a document that was kept or dropped can be
asked about one URL at a time.

The language is detected in the extracted Markdown, not in the raw HTML and not
taken from the page's own lang attribute, because that is what the pipelines
filter on. cc_language is what the page declares, shown alongside so a
disagreement is visible rather than silent.

This is a coarse pre-filter. A trigram identifier tells Vietnamese from Malay
well enough to cut a corpus down, and it is not a substitute for a language
specific classifier.

Pass "-" to read URLs from stdin, one per line or as JSONL with a url field.

Examples:
  ccrawl content lang https://vnexpress.net/
  ccrawl content lang https://example.com/ -o json
  ccrawl search 'vnexpress.net/*' -n 50 -o jsonl | ccrawl content lang - -o jsonl`,
		Args: []kit.Arg{{Name: "url", Help: contentArgHelp}},
	}, func(ctx context.Context, in contentLangIn, emit func(LangReport) error) error {
		return eachContentPage(ctx, in.URL, func(res *ccrawl.CrawlResult) error {
			md, _ := ccrawl.ExtractMarkdown(res.Body)
			code, conf := ccrawl.DetectLanguage(md)
			return emit(LangReport{
				URL:        res.FinalURL,
				Language:   code,
				Confidence: conf,
				CCLanguage: ccrawl.ExtractContent(res.Body).Language,
				Chars:      len([]rune(md)),
				Sample:     ccrawl.LangSample(md, 200),
			})
		})
	})
}

// ── content extract ───────────────────────────────────────────────────────────

type contentExtractIn struct {
	App *App   `kit:"inject"`
	URL string `kit:"arg" name:"url" help:"fetch and extract this URL, or read URLs from stdin with \"-\""`
}

// ContentExtractResult is the output of content extraction.
type ContentExtractResult struct {
	URL         string `json:"url" table:"url"`
	CanonURL    string `json:"canon_url,omitempty" table:"canon_url"`
	Title       string `json:"title" table:"title"`
	Description string `json:"description,omitempty" table:"description"`
	Language    string `json:"language" table:"language"`
	WordCount   int    `json:"word_count" table:"word_count"`
	DocID       uint64 `json:"doc_id" table:"doc_id"`
	Snippet     string `json:"snippet" table:"snippet"`
}

func registerContentExtract(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:    "extract",
		Parent:  "content",
		Summary: "Fetch a URL and extract clean text, title, and metadata",
		Long: `Fetch a URL and run the v2 content processing pipeline: HTML to clean text,
title extraction, canonical URL resolution, and word count.

The snippet is the first 500 characters of the extracted text, which is enough
to tell an article from a nav page and is not the document. Use 'ccrawl get
--text' or 'ccrawl get --markdown' when you want the whole thing.

Pass "-" to read URLs from stdin, one per line or as JSONL with a url field.

Examples:
  ccrawl content extract https://example.com/article
  ccrawl content extract https://blog.golang.org/ -o json
  ccrawl search 'example.com/*' -n 100 -o jsonl | ccrawl content extract - -o jsonl`,
		Args: []kit.Arg{{Name: "url", Help: contentArgHelp}},
	}, func(ctx context.Context, in contentExtractIn, emit func(ContentExtractResult) error) error {
		return eachContentPage(ctx, in.URL, func(res *ccrawl.CrawlResult) error {
			tr := ccrawl.ExtractContent(res.Body)

			canonURL := tr.CanonURL
			if canonURL == "" {
				canonURL = res.FinalURL
			} else if base, err2 := url.Parse(res.FinalURL); err2 == nil {
				if ref, err3 := url.Parse(canonURL); err3 == nil {
					canonURL = base.ResolveReference(ref).String()
				}
			}

			snippet := tr.Body
			if len(snippet) > 500 {
				snippet = snippet[:500] + "..."
			}

			return emit(ContentExtractResult{
				URL:         res.FinalURL,
				CanonURL:    canonURL,
				Title:       tr.Title,
				Description: tr.Description,
				Language:    tr.Language,
				WordCount:   tr.WordCount,
				DocID:       ccrawl.DocumentID(canonURL),
				Snippet:     snippet,
			})
		})
	})
}

// ── content quality ───────────────────────────────────────────────────────────

type contentQualityIn struct {
	App *App   `kit:"inject"`
	URL string `kit:"arg" name:"url" help:"fetch and score this URL, or read URLs from stdin with \"-\""`
}

// QualityReport is the output of content quality analysis.
type QualityReport struct {
	URL            string  `json:"url" table:"url"`
	WordCount      int     `json:"word_count" table:"word_count"`
	TitleLength    int     `json:"title_length" table:"title_length"`
	HasMainContent bool    `json:"has_main_content" table:"has_main_content"`
	SpamScore      float64 `json:"spam_score" table:"spam_score"`
	IsParked       bool    `json:"is_parked" table:"is_parked"`
}

func registerContentQuality(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:    "quality",
		Parent:  "content",
		Summary: "Compute content quality signals for a URL",
		Long: `Fetch a URL and score the extracted text on five signals, which is what a
corpus is filtered on before it is worth indexing.

  word_count         words in the extracted text, after nav, header, footer,
                     aside, script and style are dropped
  title_length       characters in the title, 0 for a page that has none
  has_main_content   word_count is at least 50
  spam_score         0 to 1, one tenth for each of sixteen English sales
                     phrases the page contains, capped at 1
  is_parked          a page under 150 words that says it is for sale, parked,
                     coming soon or under construction

These are cheap and blunt on purpose. spam_score reads English only and a page
that scores 0 is not thereby clean, so use them to throw out the obvious floor
of a crawl and not to rank what is left.

Pass "-" to read URLs from stdin, one per line or as JSONL with a url field.

Examples:
  ccrawl content quality https://example.com/
  ccrawl content quality https://site.com/ -o json
  ccrawl search 'example.com/*' -n 100 -o jsonl \
    | ccrawl content quality - -o jsonl \
    | jq -c 'select(.has_main_content and .spam_score < 0.2)'`,
		Args: []kit.Arg{{Name: "url", Help: contentArgHelp}},
	}, func(ctx context.Context, in contentQualityIn, emit func(QualityReport) error) error {
		return eachContentPage(ctx, in.URL, func(res *ccrawl.CrawlResult) error {
			q := ccrawl.QualitySignals(ccrawl.ExtractContent(res.Body))
			return emit(QualityReport{
				URL:            res.FinalURL,
				WordCount:      q.WordCount,
				TitleLength:    q.TitleLength,
				HasMainContent: q.HasMainContent,
				SpamScore:      q.SpamScore,
				IsParked:       q.IsParked,
			})
		})
	})
}

// ── content links (v2 version emits structured LinkRecord rows) ───────────────

type contentLinksV2In struct {
	App *App   `kit:"inject"`
	URL string `kit:"arg" name:"url" help:"extract links from this URL, or read URLs from stdin with \"-\""`
}

// LinkRecord is one outbound link extracted from a page.
//
// Source is the page the link was found on. It is redundant when one URL was
// asked about and it is the whole point over a stream, where the rows of a
// hundred pages arrive in one file and an edge with no tail is not an edge.
type LinkRecord struct {
	Source string `json:"source" table:"source"`
	URL    string `json:"url" kit:"id" table:"url"`
	Host   string `json:"host" table:"host"`
}

func registerContentLinksV2(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:    "outlinks",
		Parent:  "content",
		Summary: "Extract structured outbound links from a URL",
		Long: `Fetch a URL and emit each outbound hyperlink as a (source, url, host) row.
Use 'ccrawl get --links' for the raw text list.

The links are every http and https anchor href on the page, resolved against the
page's own address. They are not deduplicated and they carry no anchor text, so
a nav bar linked from every article shows up once per page. Pass "-" to read
URLs from stdin and the rows of every page land in one stream, which is a link
graph of your own crawl.

Examples:
  ccrawl content outlinks https://example.com/
  ccrawl content outlinks https://news.ycombinator.com/ -n 20 -o table
  ccrawl search 'example.com/*' -n 100 -o jsonl \
    | ccrawl content outlinks - -o jsonl > links.jsonl`,
		Args: []kit.Arg{{Name: "url", Help: contentArgHelp}},
	}, func(ctx context.Context, in contentLinksV2In, emit func(LinkRecord) error) error {
		return eachContentPage(ctx, in.URL, func(res *ccrawl.CrawlResult) error {
			for _, l := range res.Links {
				u, err := url.Parse(l)
				if err != nil {
					continue
				}
				if err := emit(LinkRecord{Source: res.FinalURL, URL: l, Host: u.Hostname()}); err != nil {
					return err
				}
			}
			return nil
		})
	})
}

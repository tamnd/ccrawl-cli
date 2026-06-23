package cli

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func registerContentV2(app *kit.App) {
	registerContentExtract(app)
	registerContentQuality(app)
	registerContentLinksV2(app)
}

// ── content extract ───────────────────────────────────────────────────────────

type contentExtractIn struct {
	App *App   `kit:"inject"`
	URL string `kit:"arg" name:"url" help:"URL to fetch and extract"`
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
		Single:  true,
		Summary: "Fetch a URL and extract clean text, title, and metadata",
		Long: `Fetch a URL and run the v2 content processing pipeline: HTML to clean text,
title extraction, canonical URL resolution, and word count.

Examples:
  ccrawl content extract https://example.com/article
  ccrawl content extract https://blog.golang.org/ -o json`,
		Args: []kit.Arg{{Name: "url"}},
	}, func(ctx context.Context, in contentExtractIn, emit func(ContentExtractResult) error) error {
		rawURL := in.URL
		if !strings.HasPrefix(rawURL, "http") {
			rawURL = "https://" + rawURL
		}
		res, err := ccrawl.CrawlURL(ctx, rawURL, ccrawl.DefaultCrawlConfig)
		if err != nil {
			return fmt.Errorf("fetch %s: %w", rawURL, err)
		}

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
}

// ── content quality ───────────────────────────────────────────────────────────

type contentQualityIn struct {
	App *App   `kit:"inject"`
	URL string `kit:"arg" name:"url" help:"URL to fetch and score"`
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
		Single:  true,
		Summary: "Compute content quality signals for a URL",
		Long: `Fetch a URL and compute v2 quality signals: word count, spam score,
and parked-domain detection.

Examples:
  ccrawl content quality https://example.com/
  ccrawl content quality https://site.com/ -o json`,
		Args: []kit.Arg{{Name: "url"}},
	}, func(ctx context.Context, in contentQualityIn, emit func(QualityReport) error) error {
		rawURL := in.URL
		if !strings.HasPrefix(rawURL, "http") {
			rawURL = "https://" + rawURL
		}
		res, err := ccrawl.CrawlURL(ctx, rawURL, ccrawl.DefaultCrawlConfig)
		if err != nil {
			return fmt.Errorf("fetch %s: %w", rawURL, err)
		}
		tr := ccrawl.ExtractContent(res.Body)
		q := ccrawl.QualitySignals(tr)
		return emit(QualityReport{
			URL:            res.FinalURL,
			WordCount:      q.WordCount,
			TitleLength:    q.TitleLength,
			HasMainContent: q.HasMainContent,
			SpamScore:      q.SpamScore,
			IsParked:       q.IsParked,
		})
	})
}

// ── content links (v2 version emits structured LinkRecord rows) ───────────────

type contentLinksV2In struct {
	App *App   `kit:"inject"`
	URL string `kit:"arg" name:"url" help:"URL to extract links from"`
}

// LinkRecord is one outbound link extracted from a page.
type LinkRecord struct {
	URL  string `json:"url" kit:"id" table:"url"`
	Host string `json:"host" table:"host"`
}

func registerContentLinksV2(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:    "outlinks",
		Parent:  "content",
		Summary: "Extract structured outbound links from a URL",
		Long: `Fetch a URL and emit each outbound hyperlink as a structured record.
Use 'ccrawl get --links' for the raw text list.

Examples:
  ccrawl content outlinks https://example.com/
  ccrawl content outlinks https://news.ycombinator.com/ -n 20 -o table`,
		Args: []kit.Arg{{Name: "url"}},
	}, func(ctx context.Context, in contentLinksV2In, emit func(LinkRecord) error) error {
		rawURL := in.URL
		if !strings.HasPrefix(rawURL, "http") {
			rawURL = "https://" + rawURL
		}
		res, err := ccrawl.CrawlURL(ctx, rawURL, ccrawl.DefaultCrawlConfig)
		if err != nil {
			return fmt.Errorf("fetch %s: %w", rawURL, err)
		}
		for _, l := range res.Links {
			u, err := url.Parse(l)
			if err != nil {
				continue
			}
			if err := emit(LinkRecord{URL: l, Host: u.Hostname()}); err != nil {
				return err
			}
		}
		return nil
	})
}

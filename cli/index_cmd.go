package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func registerIndex(app *kit.App) {
	app.CommandGroup("index", "Build and query the full-text search index")
	registerIndexBuild(app)
	registerIndexSearch(app)
}

// ── index build ───────────────────────────────────────────────────────────────

// The fetch concurrency is the global -j, read off the App: kit only inherits
// --limit into an op struct, so a Workers field here would declare a flag that
// collides with the global and is never set.
type indexBuildIn struct {
	App   *App   `kit:"inject"`
	Dir   string `kit:"flag" help:"directory to write the index into"`
	Input string `kit:"flag" help:"file of JSONL documents to index, or - for stdin"`
	URLs  string `kit:"flag,name=urls" help:"page URLs to fetch and index, comma separated"`
}

// IndexBuildResult reports the outcome of building an index.
type IndexBuildResult struct {
	IndexDir    string `json:"index_dir" table:"index_dir"`
	DocsAdded   int    `json:"docs_added" table:"docs_added"`
	DocsSkipped int    `json:"docs_skipped" table:"docs_skipped"`
	Terms       int    `json:"terms" table:"terms"`
}

// indexDoc is one line of an --input file: a JSON object with a URL and some
// text. There are three spellings of the language because a file written by
// hand tends to say "language", `ccrawl parse wet -o jsonl` says
// "content_language" for the same reason the parquet column does, and a file
// written by a ccrawl before v0.10.1 says "ContentLanguage". encoding/json
// folds case but not underscores, so each spelling needs its own field.
type indexDoc struct {
	URL             string `json:"url"`
	Title           string `json:"title"`
	Text            string `json:"text"`
	Language        string `json:"language"`
	ContentLanguage string `json:"content_language"`
	OldLanguage     string `json:"ContentLanguage"`
}

func (d indexDoc) lang() string {
	for _, s := range []string{d.Language, d.ContentLanguage, d.OldLanguage} {
		if s != "" {
			return s
		}
	}
	return ""
}

// snippet is the first 500 runes of a document, which is what the forward index
// keeps for display.
func snippet(s string) string {
	rs := []rune(s)
	if len(rs) > 500 {
		return string(rs[:500])
	}
	return s
}

func registerIndexBuild(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:    "build",
		Parent:  "index",
		Single:  true,
		Summary: "Build an inverted index from extracted content",
		Long: `Build a BM25 inverted index from a JSONL file of documents or a set of URLs.

--input reads one JSON object per line, each with a url and the text to index,
and optionally a title and a language. That is the shape
"ccrawl parse file.warc.wet.gz -o jsonl" writes, so a WET file can be indexed
without an intermediate step. Use - to read the JSONL from stdin.

--urls fetches each page live, extracts the readable text, and indexes that.

The whole index is held in memory until it is written, so the corpus is bounded
by RAM. Measured on English WET records from CC-MAIN-2026-30, a build costs
about 26 KB per document, which puts a 16 GB machine at roughly 600,000
documents, or 45 of the 100,000 WET files in a crawl. This is a reference
implementation of BM25, not a production search engine.

Examples:
  ccrawl index build --dir /tmp/idx --input docs.jsonl
  ccrawl parse file.warc.wet.gz --lang eng -o jsonl | ccrawl index build --dir /tmp/idx --input -
  ccrawl index build --dir /tmp/idx --urls https://example.com/,https://golang.org/`,
	}, func(ctx context.Context, in indexBuildIn, emit func(IndexBuildResult) error) error {
		if in.Input == "" && in.URLs == "" {
			return usageErr("nothing to index: pass --input (a JSONL file, or - for stdin) or --urls")
		}
		if in.Dir == "" {
			in.Dir = filepath.Join(dataDir(in.App), "index")
		}
		b, err := ccrawl.NewInvertedIndexBuilder(in.Dir)
		if err != nil {
			return fmt.Errorf("create index builder: %w", err)
		}
		fw, err := ccrawl.NewForwardIndexWriter(filepath.Join(in.Dir, "forward.jsonl"))
		if err != nil {
			return err
		}
		defer func() { _ = fw.Close() }()

		workers := 8
		if in.App != nil && in.App.Workers > 0 {
			workers = in.App.Workers
		}

		var docsAdded, docsSkipped int
		add := func(doc ccrawl.ForwardDoc, tokens []string) {
			b.Add(doc.DocID, tokens)
			_ = fw.Write(doc)
			docsAdded++
		}

		type fetchResult struct {
			doc    ccrawl.ForwardDoc
			tokens []string
		}

		fetchOne := func(rawURL string) (fetchResult, error) {
			res, err := ccrawl.CrawlURL(ctx, rawURL, ccrawl.DefaultCrawlConfig)
			if err != nil {
				return fetchResult{}, err
			}
			tr := ccrawl.ExtractContent(res.Body)
			canonURL := res.FinalURL
			if tr.CanonURL != "" {
				canonURL = tr.CanonURL
			}
			docID := ccrawl.DocumentID(canonURL)
			tokens := ccrawl.Tokenize(tr.Title + " " + tr.Body)
			return fetchResult{
				tokens: tokens,
				doc: ccrawl.ForwardDoc{
					DocID:     docID,
					URL:       res.FinalURL,
					CanonURL:  canonURL,
					Host:      hostFromURL(res.FinalURL),
					Title:     tr.Title,
					Language:  tr.Language,
					WordCount: tr.WordCount,
					Snippet:   snippet(tr.Body),
				},
			}, nil
		}

		if in.Input != "" {
			skipped, err := indexJSONL(in.Input, add)
			if err != nil {
				return err
			}
			docsSkipped += skipped
		}

		if in.URLs != "" {
			var nonEmpty []string
			for u := range strings.SplitSeq(in.URLs, ",") {
				if u = strings.TrimSpace(u); u != "" {
					nonEmpty = append(nonEmpty, u)
				}
			}

			// Fan out N workers; drain results in one goroutine (no lock needed).
			resCh := make(chan fetchResult, workers*2)
			var drainWg sync.WaitGroup
			drainWg.Go(func() {
				for r := range resCh {
					add(r.doc, r.tokens)
				}
			})

			eg, egCtx := errgroup.WithContext(ctx)
			sem := make(chan struct{}, workers)
			for _, u := range nonEmpty {
				sem <- struct{}{}
				eg.Go(func() error {
					defer func() { <-sem }()
					r, err := fetchOne(u)
					if err != nil {
						fmt.Fprintf(os.Stderr, "warn: %s: %v\n", u, err)
						return nil
					}
					select {
					case resCh <- r:
					case <-egCtx.Done():
					}
					return nil
				})
			}
			_ = eg.Wait()
			close(resCh)
			drainWg.Wait()
		}

		if err := b.Flush(); err != nil {
			return fmt.Errorf("flush index: %w", err)
		}
		return emit(IndexBuildResult{
			IndexDir:    in.Dir,
			DocsAdded:   docsAdded,
			DocsSkipped: docsSkipped,
			Terms:       b.TermCount,
		})
	})
}

// indexJSONL reads a JSONL document file (or stdin for "-") and hands every
// usable document to add. A line that is not JSON, or has no URL, or tokenizes
// to nothing, is counted and skipped rather than failing the build: a WET file
// run through a language filter has plenty of both.
func indexJSONL(path string, add func(ccrawl.ForwardDoc, []string)) (int, error) {
	r := io.Reader(os.Stdin)
	if path != "-" {
		f, err := os.Open(path)
		if err != nil {
			return 0, fmt.Errorf("read %s: %w", path, err)
		}
		defer func() { _ = f.Close() }()
		r = f
	}

	var skipped int
	err := readLines(r, func(line string) error {
		var d indexDoc
		if err := json.Unmarshal([]byte(line), &d); err != nil || d.URL == "" {
			skipped++
			return nil
		}
		tokens := ccrawl.Tokenize(d.Title + " " + d.Text)
		if len(tokens) == 0 {
			skipped++
			return nil
		}
		add(ccrawl.ForwardDoc{
			DocID:     ccrawl.DocumentID(d.URL),
			URL:       d.URL,
			CanonURL:  d.URL,
			Host:      hostFromURL(d.URL),
			Title:     d.Title,
			Language:  d.lang(),
			WordCount: len(tokens),
			Snippet:   snippet(d.Text),
		}, tokens)
		return nil
	})
	return skipped, err
}

// ── index search ──────────────────────────────────────────────────────────────

type indexSearchIn struct {
	App   *App   `kit:"inject"`
	Query string `kit:"arg" name:"query" help:"search query"`
	Dir   string `kit:"flag" help:"index directory to search"`
	Limit int    `kit:"flag,inherit" name:"limit"`
}

// SearchHit is one result from index search.
type SearchHit struct {
	DocID   uint64  `json:"doc_id" table:"doc_id"`
	Score   float64 `json:"score" table:"score"`
	URL     string  `json:"url" table:"url"`
	Title   string  `json:"title" table:"title"`
	Snippet string  `json:"snippet" table:"snippet"`
}

func registerIndexSearch(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:    "search",
		Parent:  "index",
		Summary: "Search the local inverted index with BM25 ranking",
		Long: `Query the local inverted index built by 'ccrawl index build'. Results are
ranked by BM25 score (best match first).

A document matches if it holds any query term, so a two-word query returns the
documents that hold either word, with the ones that hold both scoring higher.
The default is the top 100 documents, -n changes it. A query that matches
nothing exits 3.

Examples:
  ccrawl index search "golang web server"
  ccrawl index search "machine learning" --dir /tmp/idx -n 20 -o json`,
		Args: []kit.Arg{{Name: "query"}},
	}, func(ctx context.Context, in indexSearchIn, emit func(SearchHit) error) error {
		if in.Dir == "" {
			in.Dir = filepath.Join(dataDir(in.App), "index")
		}
		idx, err := ccrawl.OpenIndex(in.Dir)
		if err != nil {
			return fmt.Errorf("open index %s: %w", in.Dir, err)
		}
		defer func() { _ = idx.Close() }()

		// load forward index for snippet/title lookup
		forward := loadForwardIndex(filepath.Join(in.Dir, "forward.jsonl"))

		n := in.Limit
		if n <= 0 {
			n = 100
		}
		tokens := ccrawl.Tokenize(in.Query)
		hits := idx.Search(tokens, n)
		if len(hits) == 0 {
			return noResults(fmt.Sprintf("nothing in %s matches %q", in.Dir, in.Query))
		}
		for _, h := range hits {
			sh := SearchHit{DocID: h.DocID, Score: h.Score}
			if fd, ok := forward[h.DocID]; ok {
				sh.URL = fd.URL
				sh.Title = fd.Title
				sh.Snippet = fd.Snippet
			}
			if err := emit(sh); err != nil {
				return err
			}
		}
		return nil
	})
}

// ── helpers ───────────────────────────────────────────────────────────────────

func dataDir(app *App) string {
	if app != nil {
		if app.Cfg.DataDir != "" {
			return app.Cfg.DataDir
		}
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "data", "ccrawl")
}

func hostFromURL(rawURL string) string {
	// fast path: extract host without full parsing
	s := strings.TrimPrefix(rawURL, "https://")
	s = strings.TrimPrefix(s, "http://")
	if idx := strings.IndexByte(s, '/'); idx >= 0 {
		s = s[:idx]
	}
	return s
}

func loadForwardIndex(path string) map[uint64]ccrawl.ForwardDoc {
	m := make(map[uint64]ccrawl.ForwardDoc)
	f, err := os.Open(path)
	if err != nil {
		return m
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var fd ccrawl.ForwardDoc
		if err := json.Unmarshal(line, &fd); err != nil {
			continue
		}
		if fd.DocID == 0 {
			continue
		}
		m[fd.DocID] = fd
	}
	return m
}

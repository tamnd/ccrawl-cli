package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
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

type indexBuildIn struct {
	App     *App   `kit:"inject"`
	Dir     string `kit:"flag" help:"directory to write the index into"`
	Input   string `kit:"flag" help:"JSONL file of ForwardDoc records to index (or WET file URL)"`
	URLs    string `kit:"flag,name=urls" help:"comma-separated WET file URLs to index"`
	Workers int    `kit:"flag" help:"parallel fetch workers (default 8)"`
}

// IndexBuildResult reports the outcome of building an index.
type IndexBuildResult struct {
	IndexDir  string `json:"index_dir" table:"index_dir"`
	DocsAdded int    `json:"docs_added" table:"docs_added"`
	Terms     int    `json:"terms" table:"terms"`
}

func registerIndexBuild(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:    "build",
		Parent:  "index",
		Single:  true,
		Summary: "Build an inverted index from extracted content",
		Long: `Build a BM25 inverted index from a set of URLs or a JSONL file of documents.

For each URL, the command fetches the page, extracts text, tokenizes it, and
adds it to the inverted index. The index is written to --dir.

Examples:
  ccrawl index build --dir /tmp/idx --urls https://example.com/,https://golang.org/
  ccrawl index build --dir /tmp/idx --input docs.jsonl`,
	}, func(ctx context.Context, in indexBuildIn, emit func(IndexBuildResult) error) error {
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

		workers := in.Workers
		if workers <= 0 {
			workers = 8
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
			snippet := tr.Body
			if len(snippet) > 500 {
				rs := []rune(snippet)
				if len(rs) > 500 {
					snippet = string(rs[:500])
				}
			}
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
					Snippet:   snippet,
				},
			}, nil
		}

		var docsAdded int

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
					b.Add(r.doc.DocID, r.tokens)
					_ = fw.Write(r.doc)
					docsAdded++
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
			IndexDir:  in.Dir,
			DocsAdded: docsAdded,
			Terms:     b.TermCount,
		})
	})
}

// ── index search ──────────────────────────────────────────────────────────────

type indexSearchIn struct {
	App   *App   `kit:"inject"`
	Query string `kit:"arg" name:"query" help:"search query"`
	Dir   string `kit:"flag" help:"index directory to search"`
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

		tokens := ccrawl.Tokenize(in.Query)
		hits := idx.Search(tokens, 100)
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
	defer f.Close()
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

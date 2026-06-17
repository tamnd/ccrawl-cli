package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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
	App    *App   `kit:"inject"`
	Dir    string `kit:"flag" help:"directory to write the index into"`
	Input  string `kit:"flag" help:"JSONL file of ForwardDoc records to index (or WET file URL)"`
	URLs   string `kit:"flag,name=urls" help:"comma-separated WET file URLs to index"`
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

		var docsAdded int

		addURL := func(rawURL string) error {
			rawURL = strings.TrimSpace(rawURL)
			if rawURL == "" {
				return nil
			}
			res, err := ccrawl.CrawlURL(ctx, rawURL, ccrawl.DefaultCrawlConfig)
			if err != nil {
				return err // non-fatal: caller decides
			}
			tr := ccrawl.ExtractContent(res.Body)
			canonURL := res.FinalURL
			if tr.CanonURL != "" {
				canonURL = tr.CanonURL
			}
			docID := ccrawl.DocumentID(canonURL)
			tokens := ccrawl.Tokenize(tr.Title + " " + tr.Body)
			b.Add(docID, tokens)

			snippet := tr.Body
			if len(snippet) > 500 {
				snippet = snippet[:500]
			}
			_ = fw.Write(ccrawl.ForwardDoc{
				DocID:   docID,
				URL:     res.FinalURL,
				CanonURL: canonURL,
				Host:    hostFromURL(res.FinalURL),
				Title:   tr.Title,
				Language: tr.Language,
				WordCount: tr.WordCount,
				Snippet: snippet,
			})
			docsAdded++
			return nil
		}

		if in.URLs != "" {
			for _, u := range strings.Split(in.URLs, ",") {
				if err := addURL(u); err != nil {
					fmt.Fprintf(os.Stderr, "warn: %s: %v\n", u, err)
				}
			}
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
		Name:   "search",
		Parent: "index",
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
	data, err := os.ReadFile(path)
	if err != nil {
		return m
	}
	// Each line is a JSON object; parse minimal fields
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var fd ccrawl.ForwardDoc
		// simple scan for known fields
		fd.URL = jsonStringField(line, "url")
		fd.Title = jsonStringField(line, "title")
		fd.Snippet = jsonStringField(line, "snippet")
		var docID uint64
		fmt.Sscan(jsonStringField(line, "doc_id"), &docID)
		if docID == 0 {
			continue
		}
		fd.DocID = docID
		m[docID] = fd
	}
	return m
}

// jsonStringField extracts a JSON string field by name from a flat JSON object.
// This is a best-effort parser for the forward index lines, not a full JSON parser.
func jsonStringField(line, key string) string {
	needle := `"` + key + `":`
	idx := strings.Index(line, needle)
	if idx < 0 {
		return ""
	}
	rest := strings.TrimSpace(line[idx+len(needle):])
	if rest == "" {
		return ""
	}
	if rest[0] == '"' {
		// string value
		rest = rest[1:]
		var val strings.Builder
		for i := 0; i < len(rest); i++ {
			if rest[i] == '\\' && i+1 < len(rest) {
				i++
				switch rest[i] {
				case '"':
					val.WriteByte('"')
				case '\\':
					val.WriteByte('\\')
				case 'n':
					val.WriteByte('\n')
				default:
					val.WriteByte(rest[i])
				}
				continue
			}
			if rest[i] == '"' {
				break
			}
			val.WriteByte(rest[i])
		}
		return val.String()
	}
	// numeric value
	end := strings.IndexAny(rest, ",}")
	if end < 0 {
		return rest
	}
	return strings.TrimSpace(rest[:end])
}

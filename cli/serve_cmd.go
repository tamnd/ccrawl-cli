package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func registerServe(app *kit.App) {
	registerServeAPI(app)
}

// ── serve ─────────────────────────────────────────────────────────────────────

type serveIn struct {
	App      *App   `kit:"inject"`
	Addr     string `kit:"flag" help:"listen address (default :8080)"`
	IndexDir string `kit:"flag,name=index-dir" help:"path to inverted index directory"`
}

// ServeResult is emitted once when the server starts.
type ServeResult struct {
	Addr     string `json:"addr" table:"addr"`
	IndexDir string `json:"index_dir,omitempty" table:"index_dir"`
}

func registerServeAPI(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:    "api",
		Single:  true,
		Summary: "Start the ccrawl v2 REST API server",
		Long: `Start the ccrawl v2 HTTP API server. Exposes:

  GET /v2/host/{host}         enriched host profile
  GET /v2/hosts?tld=&n=       top N hosts (optionally filtered by TLD)
  GET /v2/search?q=&k=        full-text search (requires --index-dir)
  GET /v2/health              health check

The host store is populated from the web-graph rank table on startup.
For full enrichment run 'ccrawl host enrich' first.

Examples:
  ccrawl api --addr :8080
  ccrawl api --addr :8080 --index-dir /data/idx`,
	}, func(ctx context.Context, in serveIn, emit func(ServeResult) error) error {
		addr := in.Addr
		if addr == "" {
			addr = ":8080"
		}

		// Build an in-memory host store from the rank table (top 1M).
		// If no graph is available, start without the host store.
		var hostStore ccrawl.HostStore
		if g, err := resolveGraph(ctx, in.App, ""); err == nil {
			var recs []ccrawl.HostRecord
			if err := ccrawl.RankStream(ctx, in.App.HTTP, g.HostRankURL(), "", func(r ccrawl.Rank) error {
				recs = append(recs, ccrawl.HostFromRank(r))
				if len(recs) >= 1_000_000 {
					return errStop
				}
				return nil
			}); err != nil && err != errStop {
				fmt.Fprintf(os.Stderr, "warn: load rank table: %v\n", err)
			}
			hostStore = ccrawl.NewMemHostStore(recs)
		}

		// Build search store if index dir is available
		var searchStore ccrawl.SearchStore
		if in.IndexDir != "" {
			idx, err := ccrawl.OpenIndex(in.IndexDir)
			if err != nil {
				return fmt.Errorf("open index: %w", err)
			}
			defer func() { _ = idx.Close() }()
			forward := loadForwardIndex(filepath.Join(in.IndexDir, "forward.jsonl"))
			searchStore = ccrawl.NewIndexSearchStore(idx, forward)
		}

		cfg := ccrawl.ServeConfig{Addr: addr, IndexDir: in.IndexDir}
		srv := ccrawl.NewAPIServer(cfg, hostStore, searchStore)

		if err := emit(ServeResult{Addr: addr, IndexDir: in.IndexDir}); err != nil {
			return err
		}
		return srv.ListenAndServe(ctx)
	})
}

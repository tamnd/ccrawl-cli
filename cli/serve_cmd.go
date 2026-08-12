package cli

import (
	"context"
	"fmt"
	"net"
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
	Addr     string `kit:"flag" help:"listen address (default 127.0.0.1:8080)"`
	IndexDir string `kit:"flag,name=index-dir" help:"path to inverted index directory"`
}

// defaultAPIAddr is loopback on purpose. The server has no authentication, no
// rate limiting and no request log, so the default has to be an address only
// this machine can reach. Binding anywhere else is a deliberate act.
const defaultAPIAddr = "127.0.0.1:8080"

// reachableOffThisMachine reports whether addr accepts connections from other
// hosts. An empty host, which is what ":8080" means, is every interface.
func reachableOffThisMachine(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	if host == "" {
		return true
	}
	if host == "localhost" {
		return false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		// A name we cannot read as an address. Assume the worst and say so.
		return true
	}
	// 0.0.0.0 and :: are every interface, and neither is loopback.
	return !ip.IsLoopback()
}

// ServeResult is emitted once when the server starts.
type ServeResult struct {
	Addr     string `json:"addr" table:"addr"`
	IndexDir string `json:"index_dir,omitempty" table:"index_dir"`
}

func registerServeAPI(app *kit.App) {
	handle(app, kit.OpMeta{
		Name:    "api",
		Single:  true,
		Summary: "Start the ccrawl v2 REST API server",
		Long: `Start the ccrawl v2 HTTP API server. Exposes:

  GET /v2/host/{host}         enriched host profile
  GET /v2/hosts?tld=&n=       top N hosts (optionally filtered by TLD)
  GET /v2/search?q=&k=        full-text search (requires --index-dir)
  GET /v2/health              health check, and which stores are loaded

This is a local exploration tool, not a service. There is no authentication, no
rate limiting, no request log and no pagination, so it binds 127.0.0.1 by
default and warns if you point it anywhere else. Put a reverse proxy in front
of it if it has to be reachable.

The host store is the top million hosts of the web-graph rank table, read into
memory on every start, which takes a minute or two on a good connection. A rank
table that fails to load is fatal: a server that answers host queries from half
a table is worse than one that did not start. For full enrichment run
'ccrawl host enrich' first.

Examples:
  ccrawl api
  ccrawl api --addr 127.0.0.1:9090 --index-dir /data/idx`,
	}, func(ctx context.Context, in serveIn, emit func(ServeResult) error) error {
		addr := in.Addr
		if addr == "" {
			addr = defaultAPIAddr
		}
		if reachableOffThisMachine(addr) {
			fmt.Fprintf(os.Stderr,
				"warn: %s is reachable from other machines, and this server has no authentication, rate limiting or request log\n", addr)
		}

		// The top million hosts of the rank table, held in memory. Anything that
		// goes wrong on the way is fatal, because the alternative is a server that
		// reports itself healthy and answers host lookups from a partial table.
		g, err := resolveGraph(ctx, in.App, "")
		if err != nil {
			return fmt.Errorf("resolve the web graph for the host store: %w", err)
		}
		var recs []ccrawl.HostRecord
		if err := ccrawl.RankStream(ctx, in.App.HTTP, g.HostRankURL(), "", func(r ccrawl.Rank) error {
			recs = append(recs, ccrawl.HostFromRank(r))
			if len(recs) >= 1_000_000 {
				return errStop
			}
			return nil
		}); err != nil && err != errStop {
			return fmt.Errorf("load the rank table for the host store (%d hosts read): %w", len(recs), err)
		}
		hostStore := ccrawl.NewMemHostStore(recs)

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

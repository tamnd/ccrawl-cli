package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// registerHost wires all host/* operations and escape-hatch commands.
func registerHost(app *kit.App) {
	app.CommandGroup("host", "Enumerate and enrich hosts from the CC web graph")
	registerHostTop(app)
	registerHostGet(app)
	registerHostVertices(app)
	registerHostDegrees(app)
	registerHostCDX(app)
	app.AddCommandUnder("host", newHostEnrichCmd())
}

// ── host top ──────────────────────────────────────────────────────────────────

type hostTopIn struct {
	App   *App   `kit:"inject"`
	Graph string `kit:"flag" help:"web-graph release ID (default: latest)"`
	TLD   string `kit:"flag,name=tld" help:"restrict to hosts under a TLD"`
	Limit int    `kit:"flag,inherit" name:"limit"`
}

func registerHostTop(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:    "top",
		Parent:  "host",
		Summary: "Top hosts by harmonic centrality from the web graph",
		Long: `Stream the most-central hosts from the CC host-level web graph rank table.

Hosts are sorted by harmonic centrality (most important first). Use --tld to
restrict to a single top-level domain and --limit (-n) to cap output.

Examples:
  ccrawl host top -n 20
  ccrawl host top --tld gov -n 50
  ccrawl host top -n 1000 -o jsonl > top_hosts.jsonl`,
	}, func(ctx context.Context, in hostTopIn, emit func(ccrawl.HostRecord) error) error {
		g, err := resolveGraph(ctx, in.App, in.Graph)
		if err != nil {
			return err
		}
		n := in.Limit
		if n == 0 {
			n = 20
		}
		ranks, err := ccrawl.RankTop(ctx, in.App.HTTP, g.HostRankURL(), in.TLD, n)
		if err != nil {
			return err
		}
		for _, r := range ranks {
			rec := ccrawl.HostFromRank(r)
			if err := emit(rec); err != nil {
				return err
			}
		}
		return nil
	})
}

// ── host get ──────────────────────────────────────────────────────────────────

type hostGetIn struct {
	App   *App   `kit:"inject"`
	Host  string `kit:"arg" help:"hostname (e.g. example.com or www.example.com)"`
	Graph string `kit:"flag" help:"web-graph release ID (default: latest)"`
	CDX   bool   `kit:"flag,name=cdx" help:"enrich with CDX statistics (requires DuckDB)"`
}

func registerHostGet(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:    "get",
		Parent:  "host",
		Single:  true,
		Summary: "Get the enriched profile for one host",
		Long: `Look up a host in the CC web graph and return its full profile: rank signals
(harmonic centrality, PageRank), and optionally CDX statistics (URL count,
status breakdown, MIME type, language, first/last seen) with --cdx.

Examples:
  ccrawl host get example.com
  ccrawl host get www.github.com --cdx
  ccrawl host get bbc.co.uk -o json`,
		Args: []kit.Arg{{Name: "hostname"}},
	}, func(ctx context.Context, in hostGetIn, emit func(ccrawl.HostRecord) error) error {
		g, err := resolveGraph(ctx, in.App, in.Graph)
		if err != nil {
			return err
		}
		r, err := ccrawl.RankLookup(ctx, in.App.HTTP, g.HostRankURL(), in.Host)
		if err != nil {
			return fmt.Errorf("%s: %w", in.Host, err)
		}
		rec := ccrawl.HostFromRank(r)
		rec.RegisteredDomain = registeredDomain(rec.Host)

		if in.CDX {
			if !ccrawl.DuckDBAvailable() {
				return fmt.Errorf("--cdx requires the duckdb binary on PATH")
			}
			crawlID, err := in.App.Crawl(ctx)
			if err != nil {
				return err
			}
			urls, err := ccrawl.ColumnarParquetURLs(ctx, in.App.HTTP, in.App.Cache, crawlID, "warc", in.App.Cfg.Source)
			if err != nil {
				return err
			}
			if err := ccrawl.HostCDXAgg(ctx, urls, crawlID, in.Host, func(s ccrawl.HostCDXStats) error {
				applyHostCDXStats(&rec, s)
				return nil
			}); err != nil {
				return err
			}
		}
		return emit(rec)
	})
}

// ── host vertices ─────────────────────────────────────────────────────────────

type hostVerticesIn struct {
	App   *App   `kit:"inject"`
	Graph string `kit:"flag" help:"web-graph release ID (default: latest)"`
}

func registerHostVertices(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:    "vertices",
		Parent:  "host",
		Summary: "Stream the host vertex ID → hostname mapping",
		Long: `Stream every host vertex from the CC web graph: numeric node ID and hostname.
The vertex IDs are used as keys in the edge files.

Examples:
  ccrawl host vertices -n 100
  ccrawl host vertices -o jsonl | head -5`,
	}, func(ctx context.Context, in hostVerticesIn, emit func(ccrawl.VertexRecord) error) error {
		g, err := resolveGraph(ctx, in.App, in.Graph)
		if err != nil {
			return err
		}
		return ccrawl.VertexStream(ctx, in.App.HTTP, g.HostVerticesManifestURL(), emit)
	})
}

// ── host degrees ──────────────────────────────────────────────────────────────

// DegreeRecord is one row of per-host in/out degree output.
type DegreeRecord struct {
	Host      string `json:"host" kit:"id" table:"host"`
	NodeID    int64  `json:"node_id" table:"node_id"`
	InDegree  int32  `json:"in_degree" table:"in_degree"`
	OutDegree int32  `json:"out_degree" table:"out_degree"`
}

type hostDegreesIn struct {
	App   *App   `kit:"inject"`
	Graph string `kit:"flag" help:"web-graph release ID (default: latest)"`
}

func registerHostDegrees(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:    "degrees",
		Parent:  "host",
		Summary: "Compute in-degree and out-degree for every host from the edge files",
		Long: `Stream all edge files and emit per-host in-degree (how many hosts link to it)
and out-degree (how many hosts it links to). This reads ~35 GB of edge data; it
takes several minutes on a fast connection.

Examples:
  ccrawl host degrees -n 20
  ccrawl host degrees -o jsonl > degrees.jsonl`,
	}, func(ctx context.Context, in hostDegreesIn, emit func(DegreeRecord) error) error {
		g, err := resolveGraph(ctx, in.App, in.Graph)
		if err != nil {
			return err
		}

		// First build ID→host map from vertices (fits in memory)
		idToHost := make(map[int64]string, 1<<20)
		maxID := int64(0)
		if err := ccrawl.VertexStream(ctx, in.App.HTTP, g.HostVerticesManifestURL(), func(v ccrawl.VertexRecord) error {
			idToHost[v.ID] = v.Host
			if v.ID > maxID {
				maxID = v.ID
			}
			return nil
		}); err != nil {
			return fmt.Errorf("stream vertices: %w", err)
		}
		nodeCount := maxID + 1

		inDeg, outDeg, err := ccrawl.ComputeEdgeDegrees(ctx, in.App.HTTP, g.HostEdgesManifestURL(), nodeCount)
		if err != nil {
			return fmt.Errorf("compute degrees: %w", err)
		}
		for id, host := range idToHost {
			if err := emit(DegreeRecord{
				Host:      host,
				NodeID:    id,
				InDegree:  inDeg[id],
				OutDegree: outDeg[id],
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

// ── host cdx ─────────────────────────────────────────────────────────────────

type hostCDXIn struct {
	App    *App   `kit:"inject"`
	Filter string `kit:"flag" help:"restrict to one host (url_host_name)"`
}

func registerHostCDX(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:    "cdx",
		Parent:  "host",
		Summary: "Aggregate CDX statistics per host via DuckDB (requires duckdb on PATH)",
		Long: `Run a DuckDB GROUP BY query over the columnar Parquet index and emit per-host
statistics: URL count, HTTP status breakdown, top MIME type, language, first/last
seen, and total bytes. Without --host this scans ~184 GB of Parquet.

Examples:
  ccrawl host cdx --host example.com
  ccrawl host cdx -n 100 -o jsonl`,
	}, func(ctx context.Context, in hostCDXIn, emit func(ccrawl.HostCDXStats) error) error {
		if !ccrawl.DuckDBAvailable() {
			return fmt.Errorf("this command requires the duckdb binary on PATH")
		}
		crawlID, err := in.App.Crawl(ctx)
		if err != nil {
			return err
		}
		urls, err := ccrawl.ColumnarParquetURLs(ctx, in.App.HTTP, in.App.Cache, crawlID, "warc", in.App.Cfg.Source)
		if err != nil {
			return err
		}
		return ccrawl.HostCDXAgg(ctx, urls, crawlID, in.Filter, emit)
	})
}

// ── host enrich (escape-hatch) ────────────────────────────────────────────────

type hostEnrichCmd struct {
	graph   string
	degrees bool
	cdx     bool
}

func newHostEnrichCmd() kit.Command {
	e := &hostEnrichCmd{}
	return kit.Command{
		Use:   "enrich",
		Short: "Run the host enrichment pipeline (rank + optional degrees + CDX)",
		Long: `Run the host enrichment pipeline, streaming enriched HostRecord rows.

Phases:
  1+5: Stream rank table and emit HostRecord rows (always)
  2:   Build vertex ID map (always, used for degree join)
  3:   Compute in/out-degree from edge files (~35 GB) [--degrees]
  4:   Aggregate CDX statistics via DuckDB (~184 GB) [--cdx]

Examples:
  ccrawl host enrich -n 20
  ccrawl host enrich --graph cc-main-2026-mar-apr-may -n 100
  ccrawl host enrich --degrees --cdx -o jsonl > enriched.jsonl`,
		Flags: e.flags,
		Run:   e.run,
	}
}

func (e *hostEnrichCmd) flags(f *kit.FlagSet) {
	f.StringVar(&e.graph, "graph", "", "web-graph release ID (default: latest)")
	f.BoolVar(&e.degrees, "degrees", false, "compute in/out-degree from edge files (~35 GB)")
	f.BoolVar(&e.cdx, "cdx", false, "aggregate CDX statistics via DuckDB (~184 GB)")
}

func (e *hostEnrichCmd) run(ctx context.Context, _ []string) error {
	app := appFromCtx(ctx)

	g, err := resolveGraph(ctx, app, e.graph)
	if err != nil {
		return err
	}

	// Phase 2: vertex ID → host map (needed for degree join)
	hostToID := make(map[string]int64, 1<<18)
	maxID := int64(0)
	if err := ccrawl.VertexStream(ctx, app.HTTP, g.HostVerticesManifestURL(), func(v ccrawl.VertexRecord) error {
		hostToID[v.Host] = v.ID
		if v.ID > maxID {
			maxID = v.ID
		}
		return nil
	}); err != nil {
		return fmt.Errorf("phase 2 vertices: %w", err)
	}

	// Phase 3: edge degrees (optional)
	var inDeg, outDeg []int32
	if e.degrees {
		inDeg, outDeg, err = ccrawl.ComputeEdgeDegrees(ctx, app.HTTP, g.HostEdgesManifestURL(), maxID+1)
		if err != nil {
			return fmt.Errorf("phase 3 edges: %w", err)
		}
	}

	// Phase 4: CDX aggregation (optional)
	cdxStats := make(map[string]ccrawl.HostCDXStats)
	if e.cdx {
		if !ccrawl.DuckDBAvailable() {
			return fmt.Errorf("--cdx requires duckdb binary on PATH")
		}
		crawlID, err := app.Crawl(ctx)
		if err != nil {
			return err
		}
		urls, err := ccrawl.ColumnarParquetURLs(ctx, app.HTTP, app.Cache, crawlID, "warc", app.Cfg.Source)
		if err != nil {
			return fmt.Errorf("phase 4 parquet URLs: %w", err)
		}
		if err := ccrawl.HostCDXAgg(ctx, urls, crawlID, "", func(s ccrawl.HostCDXStats) error {
			cdxStats[s.Host] = s
			return nil
		}); err != nil {
			return fmt.Errorf("phase 4 CDX agg: %w", err)
		}
	}

	// Phase 1 + 5: stream rank table, join phases, emit
	return ccrawl.RankStream(ctx, app.HTTP, g.HostRankURL(), "", func(r ccrawl.Rank) error {
		rec := ccrawl.HostFromRank(r)
		rec.RegisteredDomain = registeredDomain(rec.Host)

		// Join graph topology
		if inDeg != nil {
			if id, ok := hostToID[rec.Host]; ok && id < int64(len(inDeg)) {
				rec.InDegree = int64(inDeg[id])
				rec.OutDegree = int64(outDeg[id])
			}
		}

		// Join CDX stats
		if s, ok := cdxStats[rec.Host]; ok {
			applyHostCDXStats(&rec, s)
		}

		return app.Out.Emit(rec)
	})
}

// ── helpers ───────────────────────────────────────────────────────────────────

// resolveGraph returns the WebGraph for the given ID, or fetches the latest if
// graphID is empty.
func resolveGraph(ctx context.Context, app *App, graphID string) (ccrawl.WebGraph, error) {
	if graphID != "" {
		return ccrawl.WebGraph{
			ID:      graphID,
			BaseURL: "https://data.commoncrawl.org/projects/hyperlinkgraph/" + graphID + "/",
		}, nil
	}
	return ccrawl.LatestWebGraph(ctx, app.HTTP, app.Cache)
}

// registeredDomain returns the last two dot-separated labels of a host.
func registeredDomain(host string) string {
	parts := strings.Split(host, ".")
	if len(parts) <= 2 {
		return host
	}
	return strings.Join(parts[len(parts)-2:], ".")
}

// applyHostCDXStats merges CDX stats into a HostRecord in-place.
func applyHostCDXStats(rec *ccrawl.HostRecord, s ccrawl.HostCDXStats) {
	if s.RegisteredDomain != "" {
		rec.RegisteredDomain = s.RegisteredDomain
	}
	rec.URLCount = s.URLCount
	rec.Status2xx = s.Status2xx
	rec.Status3xx = s.Status3xx
	rec.Status4xx = s.Status4xx
	rec.Status5xx = s.Status5xx
	rec.TopMIME = s.TopMIME
	rec.Language = s.Language
	rec.FirstSeen = s.FirstSeen
	rec.LastSeen = s.LastSeen
	rec.TotalBytes = s.TotalBytes
}

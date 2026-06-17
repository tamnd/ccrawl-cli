package cli

import (
	"context"
	"math"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func registerSched(app *kit.App) {
	app.CommandGroup("sched", "Recrawl scheduling: tier assignment and differential CDX analysis")
	registerSchedAssign(app)
	registerSchedDiff(app)
}

// ── sched assign ──────────────────────────────────────────────────────────────

type schedAssignIn struct {
	App        *App    `kit:"inject"`
	Graph      string  `kit:"flag" help:"web-graph release ID (default: latest)"`
	ChangeRate float64 `kit:"flag,name=change-rate" help:"assume this change rate for all hosts (0–1, default 0.5)"`
}

func registerSchedAssign(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:   "assign",
		Parent: "sched",
		Summary: "Assign crawl tiers to hosts from the web-graph rank table",
		Long: `Stream the host rank table and assign each host to a crawl tier based on
its harmonic rank position and (assumed or measured) change rate.

Tier 1 = 24h recrawl (top 100K + high change)
Tier 2 = 3 days (top 1M + moderate change)
Tier 3 = 7 days (top 5M + low change)
Tier 4 = 30 days (top 10M)
Tier 5 = on-demand (long tail)

Examples:
  ccrawl sched assign --graph cc-main-2026-mar-apr-may -n 20
  ccrawl sched assign --graph cc-main-2026-mar-apr-may --change-rate 0.5 -o jsonl`,
	}, func(ctx context.Context, in schedAssignIn, emit func(ccrawl.HostSchedule) error) error {
		g, err := resolveGraph(ctx, in.App, in.Graph)
		if err != nil {
			return err
		}
		changeRate := in.ChangeRate
		if changeRate <= 0 || changeRate > 1 {
			changeRate = 0.5 // default assumption
		}
		return ccrawl.RankStream(ctx, in.App.HTTP, g.HostRankURL(), "", func(r ccrawl.Rank) error {
			hs := ccrawl.HostScheduleFrom(r, changeRate)
			return emit(hs)
		})
	})
}

// ── sched diff ───────────────────────────────────────────────────────────────

type schedDiffIn struct {
	App    *App   `kit:"inject"`
	CrawlA string `kit:"flag,name=crawl-a" help:"older crawl ID (e.g. CC-MAIN-2026-17)"`
	CrawlB string `kit:"flag,name=crawl-b" help:"newer crawl ID (e.g. CC-MAIN-2026-21)"`
}

func registerSchedDiff(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:   "diff",
		Parent: "sched",
		Summary: "Compute per-host change rates between two CC crawl snapshots",
		Long: `Join two CC columnar Parquet indexes on URL and compare content digests
to compute a per-host change rate. This drives tier re-assignment and freshness
scheduling.

Requires DuckDB on PATH. Scans ~184 GB × 2 = ~368 GB of Parquet data.

Examples:
  ccrawl sched diff --crawl-a CC-MAIN-2026-17 --crawl-b CC-MAIN-2026-21 -n 20
  ccrawl sched diff --crawl-a CC-MAIN-2026-12 --crawl-b CC-MAIN-2026-17 -o jsonl > changes.jsonl`,
	}, func(ctx context.Context, in schedDiffIn, emit func(ccrawl.HostDiffEntry) error) error {
		if in.CrawlA == "" || in.CrawlB == "" {
			return usageErr("--crawl-a and --crawl-b are required")
		}
		if !ccrawl.DuckDBAvailable() {
			return usageErr("DuckDB binary not found on PATH")
		}
		urlsA, err := ccrawl.ColumnarParquetURLs(ctx, in.App.HTTP, in.App.Cache, in.CrawlA, "warc", in.App.Cfg.Source)
		if err != nil {
			return err
		}
		urlsB, err := ccrawl.ColumnarParquetURLs(ctx, in.App.HTTP, in.App.Cache, in.CrawlB, "warc", in.App.Cfg.Source)
		if err != nil {
			return err
		}
		return ccrawl.DiffCDX(ctx, urlsA, urlsB, in.CrawlA, in.CrawlB, emit)
	})
}

// crawlBudget computes daily page budget allocation across tiers.
// It is exported for use in serve status output.
func crawlBudget(totalPagesPerDay int64) map[int]int64 {
	fractions := map[int]float64{1: 0.023, 2: 0.052, 3: 0.093, 4: 0.029}
	out := make(map[int]int64)
	for tier, f := range fractions {
		out[tier] = int64(math.Round(f * float64(totalPagesPerDay)))
	}
	return out
}

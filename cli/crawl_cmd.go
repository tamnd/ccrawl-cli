package cli

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func registerCrawl(app *kit.App) {
	app.CommandGroup("crawl", "Recrawl engine: seed, fetch, and write WARC output")
	registerCrawlSeed(app)
	registerCrawlFetch(app)
	registerCrawlStatus(app)
}

// ── crawl seed ────────────────────────────────────────────────────────────────

type crawlSeedIn struct {
	App        *App   `kit:"inject"`
	Graph      string `kit:"flag" help:"web-graph release ID (default: latest)"`
	MaxSeeds   int    `kit:"flag,name=max-seeds" help:"max hosts to seed (default 10000000)"`
	MinTier    int    `kit:"flag,name=min-tier" help:"only emit hosts at or above this tier (1–5)"`
}

// SeedRecord is one crawl seed URL derived from the host rank table.
type SeedRecord struct {
	Host     string  `json:"host" table:"host"`
	URL      string  `json:"url" kit:"id" table:"url"`
	Tier     int     `json:"tier" table:"tier"`
	Priority float32 `json:"priority" table:"priority"`
}

func registerCrawlSeed(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:   "seed",
		Parent: "crawl",
		Summary: "Generate crawl seed URLs from the web-graph host rank table",
		Long: `Stream the top hosts from the CC web-graph rank table and emit one seed URL
per host (https://{host}/) as a SeedRecord. Use --min-tier to restrict to
high-priority hosts.

Examples:
  ccrawl crawl seed --graph cc-main-2026-mar-apr-may -n 100 -o table
  ccrawl crawl seed --graph cc-main-2026-mar-apr-may --min-tier 2 -n 1000000 -o jsonl > seeds.jsonl`,
	}, func(ctx context.Context, in crawlSeedIn, emit func(SeedRecord) error) error {
		g, err := resolveGraph(ctx, in.App, in.Graph)
		if err != nil {
			return err
		}
		maxSeeds := in.MaxSeeds
		if maxSeeds <= 0 {
			maxSeeds = 10_000_000
		}
		minTier := in.MinTier
		if minTier <= 0 {
			minTier = 1
		}
		count := 0
		return ccrawl.RankStream(ctx, in.App.HTTP, g.HostRankURL(), "", func(r ccrawl.Rank) error {
			if count >= maxSeeds {
				return errStop
			}
			tier := ccrawl.CrawlTier(r.HarmonicPos, 0.5) // default change rate
			if tier > minTier {
				return nil // skip lower-priority hosts
			}
			count++
			return emit(SeedRecord{
				Host:     r.Key,
				URL:      "https://" + r.Key + "/",
				Tier:     tier,
				Priority: float32(r.HarmonicVal),
			})
		})
	})
}

// ── crawl fetch ───────────────────────────────────────────────────────────────

type crawlFetchIn struct {
	App      *App   `kit:"inject"`
	URL      string `kit:"arg" name:"url" help:"URL to crawl"`
	Robots   bool   `kit:"flag" help:"check robots.txt before fetching"`
}

// FetchRecord is the result of crawling one URL.
type FetchRecord struct {
	URL         string `json:"url" table:"url"`
	FinalURL    string `json:"final_url,omitempty" table:"final_url"`
	Status      int    `json:"status" table:"status"`
	ContentType string `json:"content_type" table:"content_type"`
	Digest      string `json:"digest" table:"digest"`
	BodySize    int    `json:"body_size" table:"body_size"`
	LinkCount   int    `json:"link_count" table:"link_count"`
	FetchedAt   string `json:"fetched_at" table:"fetched_at"`
}

func registerCrawlFetch(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:   "fetch",
		Parent: "crawl",
		Single: true,
		Summary: "Crawl a single URL with robots.txt checking and digest",
		Long: `Fetch a single URL using the v2 crawler config (user-agent, redirect following,
body limit). Optionally check robots.txt before fetching.

Examples:
  ccrawl crawl fetch https://example.com/
  ccrawl crawl fetch https://example.com/ --robots -o json`,
		Args: []kit.Arg{{Name: "url"}},
	}, func(ctx context.Context, in crawlFetchIn, emit func(FetchRecord) error) error {
		rawURL := in.URL
		if !strings.HasPrefix(rawURL, "http") {
			rawURL = "https://" + rawURL
		}

		if in.Robots {
			u, err := url.Parse(rawURL)
			if err != nil {
				return err
			}
			h := in.App.HTTP
			rc := ccrawl.NewRobotsCache(24*time.Hour, "ccrawl")
			entry := FetchRobotsForHost(ctx, h, rc, u.Hostname(), u.Scheme)
			if !entry.IsAllowed(u.Path) {
				return fmt.Errorf("robots.txt disallows %s", rawURL)
			}
		}

		res, err := ccrawl.CrawlURL(ctx, rawURL, ccrawl.DefaultCrawlConfig)
		if err != nil {
			return fmt.Errorf("fetch %s: %w", rawURL, err)
		}
		return emit(FetchRecord{
			URL:         rawURL,
			FinalURL:    res.FinalURL,
			Status:      res.Status,
			ContentType: res.ContentType,
			Digest:      res.Digest,
			BodySize:    len(res.Body),
			LinkCount:   len(res.Links),
			FetchedAt:   res.FetchedAt.Format(time.RFC3339),
		})
	})
}

// FetchRobotsForHost retrieves (with caching) the robots.txt for a host.
func FetchRobotsForHost(ctx context.Context, h *ccrawl.HTTPClient, rc *ccrawl.RobotsCache, host, scheme string) *ccrawl.RobotsEntry {
	if e := rc.Get(host); e != nil {
		return e
	}
	e := ccrawl.FetchRobots(ctx, h, host, scheme)
	rc.Put(host, e)
	return e
}

// ── crawl status ──────────────────────────────────────────────────────────────

type crawlStatusIn struct {
	App *App `kit:"inject"`
}

// CrawlStatus reports the crawl budget allocation across tiers.
type CrawlStatus struct {
	Tier          int   `json:"tier" table:"tier"`
	PagesPerDay   int64 `json:"pages_per_day" table:"pages_per_day"`
	TargetHosts   int64 `json:"target_hosts" table:"target_hosts"`
	IntervalHours int   `json:"interval_hours" table:"interval_hours"`
}

func registerCrawlStatus(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:   "status",
		Parent: "crawl",
		Summary: "Show crawl budget allocation across recrawl tiers",
		Long: `Display the daily page crawl budget allocation across the 5 recrawl tiers.
Total assumed capacity: 864 million pages per day (10,000 pages/sec).

Examples:
  ccrawl crawl status
  ccrawl crawl status -o json`,
	}, func(ctx context.Context, in crawlStatusIn, emit func(CrawlStatus) error) error {
		const totalPerDay = 864_000_000
		budget := crawlBudget(totalPerDay)
		targets := map[int]int64{1: 100_000, 2: 900_000, 3: 4_000_000, 4: 5_000_000, 5: 252_000_000}
		for tier := 1; tier <= 5; tier++ {
			if err := emit(CrawlStatus{
				Tier:          tier,
				PagesPerDay:   budget[tier],
				TargetHosts:   targets[tier],
				IntervalHours: ccrawl.TierInterval(tier),
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

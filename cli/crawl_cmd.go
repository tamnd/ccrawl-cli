package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func registerCrawl(app *kit.App) {
	app.CommandGroup("crawl", "Recrawl engine: seed, fetch, and write WARC output")
	registerCrawlSeed(app)
	registerCrawlFetch(app)
	registerCrawlRun(app)
	registerCrawlStatus(app)
}

// ── crawl seed ────────────────────────────────────────────────────────────────

type crawlSeedIn struct {
	App      *App   `kit:"inject"`
	Graph    string `kit:"flag" help:"release ID of the web graph (default: latest)"`
	MaxSeeds int    `kit:"flag,name=max-seeds" help:"max hosts to seed (default 10000000)"`
	MaxTier  int    `kit:"flag,name=max-tier" help:"skip hosts at tiers higher than this (2=top million, 5=all)"`
}

// SeedRecord is one crawl seed URL derived from the host rank table.
type SeedRecord struct {
	Host     string  `json:"host" table:"host"`
	URL      string  `json:"url" kit:"id" table:"url"`
	Tier     int     `json:"tier" table:"tier"`
	Priority float32 `json:"priority" table:"priority"`
}

// seedChangeRate is the change rate crawl seed assumes. The rank table says how
// central a host is and nothing about how often it changes, and a tier needs
// both, so every seed is tiered as if it changed at the middle rate.
//
// seedTierFloor follows from that: 0.5 clears the 0.5 that tier 2 asks for and
// not the 0.8 that tier 1 asks for, so tier 2 is the best a seed can do.
const (
	seedChangeRate = 0.5
	seedTierFloor  = 2
)

func registerCrawlSeed(app *kit.App) {
	handle(app, kit.OpMeta{
		Name:    "seed",
		Parent:  "crawl",
		Summary: "Generate crawl seed URLs from the web-graph host rank table",
		Long: `Stream the top hosts from the CC web-graph rank table and emit one seed URL
per host (https://{host}/) as a SeedRecord. Use --max-tier to restrict to
high-priority hosts. Tier 2 is the top million hosts and the best a seed can
reach, since a rank says nothing about how often a host changes and tier 1
needs that.

Examples:
  ccrawl crawl seed --graph cc-main-2026-mar-apr-may -n 100 -o table
  ccrawl crawl seed --graph cc-main-2026-mar-apr-may --max-tier 2 -n 1000000 -o jsonl > seeds.jsonl`,
	}, func(ctx context.Context, in crawlSeedIn, emit func(SeedRecord) error) error {
		g, err := resolveGraph(ctx, in.App, in.Graph)
		if err != nil {
			return err
		}
		maxSeeds := in.MaxSeeds
		if maxSeeds <= 0 {
			maxSeeds = 10_000_000
		}
		maxTier := in.MaxTier
		if maxTier <= 0 || maxTier > 5 {
			maxTier = 5 // emit all tiers by default
		}
		// A seed carries a rank and no measured change rate, and tier 1 wants a
		// change rate above 0.8, so no host this command sees can reach it. Said
		// here rather than found out at the end of a 262 million row scan that
		// emits nothing.
		if maxTier < seedTierFloor {
			return usageErr("seeds carry no measured change rate, so no host reaches tier 1; use --max-tier 2 for the top million hosts, or feed real change rates in with ccrawl sched diff")
		}
		// Seeding the full rank table is a ten million row stream, so the ticks
		// carry an ETA against --max-seeds and one line per seed would be noise.
		rep, stopRun, err := in.App.StartRun("crawl seed", "")
		if err != nil {
			return err
		}
		defer stopRun()
		sp := ccrawl.StartStreamProgress(rep, "seeds", maxSeeds, 0)
		defer sp.Stop()

		count := 0
		return ccrawl.RankStream(ctx, in.App.HTTP, g.HostRankURL(), "", func(r ccrawl.Rank) error {
			if count >= maxSeeds {
				return errStop
			}
			tier := ccrawl.CrawlTier(r.HarmonicPos, seedChangeRate)
			if tier > maxTier {
				return nil // skip hosts below the requested tier ceiling
			}
			count++
			sp.Add(1, 1, 0)
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
	App     *App   `kit:"inject"`
	URL     string `kit:"arg" name:"url" help:"page to crawl, as a URL"`
	Robots  bool   `kit:"flag" help:"check robots.txt before fetching"`
	WARCDir string `kit:"flag,name=warc-dir" help:"write the fetch to a WARC file in this directory"`
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
	WARCFile    string `json:"warc_file,omitempty" table:"warc_file,omitempty"`
}

// crawlWARCInfo is the provenance a crawl writes into every WARC file it opens.
func crawlWARCInfo() ccrawl.WARCInfo {
	return ccrawl.WARCInfo{
		Software:    "ccrawl/" + strings.TrimPrefix(Version, "v"),
		IsPartOf:    "ccrawl-crawl",
		Description: "crawl generated with: " + strings.Join(os.Args, " "),
		Format:      "WARC file version 1.0",
	}
}

func registerCrawlFetch(app *kit.App) {
	handle(app, kit.OpMeta{
		Name:    "fetch",
		Parent:  "crawl",
		Single:  true,
		Summary: "Crawl a single URL with robots.txt checking and digest",
		Long: `Fetch a single URL using the v2 crawler config (user-agent, redirect following,
body limit). Optionally check robots.txt before fetching.

Pass --warc-dir to archive the fetch as a WARC/1.0 request and response pair,
with digests, the server IP, and a warcinfo record naming the command that made
it. The file reads back with ccrawl parse and with any WARC tool.

Examples:
  ccrawl crawl fetch https://example.com/
  ccrawl crawl fetch https://example.com/ --robots -o json
  ccrawl crawl fetch https://example.com/ --warc-dir warc/`,
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
			rc := ccrawl.NewRobotsCache(24*time.Hour, ccrawl.DefaultCrawlConfig.UserAgent)
			entry := FetchRobotsForHost(ctx, h, rc, u.Hostname(), u.Scheme)
			if !entry.IsAllowed(u.Path) {
				return fmt.Errorf("robots.txt disallows %s", rawURL)
			}
		}

		res, err := ccrawl.CrawlURL(ctx, rawURL, ccrawl.DefaultCrawlConfig)
		if err != nil {
			return fmt.Errorf("fetch %s: %w", rawURL, err)
		}
		warcFile := ""
		if in.WARCDir != "" {
			w := ccrawl.NewWARCWriter(in.WARCDir, "ccrawl-crawl", ccrawl.DefaultCrawlWARCSize, crawlWARCInfo())
			if err := w.Write(ccrawl.NewWARCCapture(res)); err != nil {
				return fmt.Errorf("write WARC: %w", err)
			}
			if err := w.Close(); err != nil {
				return fmt.Errorf("close WARC: %w", err)
			}
			warcFile = w.Files()[0]
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
			WARCFile:    warcFile,
		})
	})
}

// FetchRobotsForHost retrieves (with caching) the robots.txt for a host.
func FetchRobotsForHost(ctx context.Context, h *ccrawl.HTTPClient, rc *ccrawl.RobotsCache, host, scheme string) *ccrawl.RobotsEntry {
	return rc.Fetch(ctx, h, host, scheme)
}

// ── crawl run ─────────────────────────────────────────────────────────────────

type crawlRunIn struct {
	App      *App          `kit:"inject"`
	Seeds    string        `kit:"flag" help:"seed file: crawl seed JSONL or one URL per line, - for stdin"`
	Out      string        `kit:"flag" help:"directory to write WARC files into (empty fetches without archiving)"`
	State    string        `kit:"flag" help:"frontier state file, so a run resumes after a restart"`
	Delay    time.Duration `kit:"flag" default:"1s" help:"minimum spacing between two requests to the same host (0 for none)"`
	MaxDepth int           `kit:"flag,name=max-depth" help:"how far from a seed to follow links (default 0, the seeds only)"`
	MaxPages int64         `kit:"flag,name=max-pages" help:"stop after this many fetches (0 = no limit)"`
	SameHost bool          `kit:"flag,name=same-host" help:"stay on the hosts the seeds named"`
	NoRobots bool          `kit:"flag,name=no-robots" help:"do not check robots.txt, which you had better have a reason for"`
	Robots   bool          `kit:"flag" help:"check robots.txt, which is already the default"`
	WARCSize int64         `kit:"flag,name=warc-size" help:"rotate to a new WARC file past this many bytes"`
	Prefix   string        `kit:"flag" help:"file name prefix for the WARC output"`
	Shard    int           `kit:"flag" help:"which partition of the seed list this process takes, 0-based"`
	Shards   int           `kit:"flag" default:"1" help:"how many machines are splitting the seed list"`
}

func registerCrawlRun(app *kit.App) {
	handle(app, kit.OpMeta{
		Name:    "run",
		Parent:  "crawl",
		Summary: "Crawl a seed list, following links, writing WARC",
		Long: `Drive a crawl from a seed file. The frontier orders URLs by priority and keeps
each host to one request per --delay, robots.txt is fetched once per host and
enforced, and every fetch is written to WARC as a request and response pair.

The frontier lives in --state, so a run that is killed resumes from where it
stopped instead of starting over. Point a second run at the same file with the
same seeds and it picks up the queue rather than refetching what is done.

robots.txt is checked unless you pass --no-robots.

Examples:
  ccrawl crawl seed -n 1000 -o jsonl > seeds.jsonl
  ccrawl crawl run --seeds seeds.jsonl --out warc/ --state crawl.db --max-depth 2
  ccrawl crawl run --seeds - --out warc/ --workers 64 --delay 1s --max-pages 100000`,
	}, func(ctx context.Context, in crawlRunIn, emit func(ccrawl.CrawlPage) error) error {
		if in.Seeds == "" {
			return usageErr("provide a seed file with --seeds, or - to read seeds on stdin")
		}
		shard := ccrawl.Shard{Index: in.Shard, Count: in.Shards}
		if err := shard.Validate(); err != nil {
			return usageErr(err.Error())
		}
		cfg := ccrawl.DefaultRunConfig
		cfg.StatePath = in.State
		cfg.OutDir = in.Out
		cfg.Workers = in.App.Workers
		cfg.Robots = !in.NoRobots
		cfg.SameHost = in.SameHost
		cfg.MaxPages = in.MaxPages
		cfg.Crawl = ccrawl.DefaultCrawlConfig
		cfg.Info = crawlWARCInfo()
		// Taken as given, including zero. The flag carries its own default of one
		// second, so an unset --delay arrives here as a second and an explicit
		// --delay 0 arrives as zero. Assigning only when positive is what used to
		// make those two the same input and left no way to say no delay at all.
		cfg.Delay = in.Delay
		// Depth is taken as given rather than defaulted, because the safe reading
		// of an unset depth is the seeds and nothing else. Following links is
		// something you ask for.
		cfg.MaxDepth = in.MaxDepth
		if in.WARCSize > 0 {
			cfg.WARCSize = in.WARCSize
		}
		if in.Prefix != "" {
			cfg.Prefix = in.Prefix
		}

		// A crawl of the open web gets its own client. in.App.HTTP is the Common
		// Crawl client, and its delay and host-wide budget exist to be polite to
		// data.commoncrawl.org, not to the sites we are about to visit.
		c, err := ccrawl.NewCrawler(cfg, ccrawl.NewCrawlClient(in.App.Cfg))
		if err != nil {
			return err
		}
		defer func() { _ = c.Close() }()

		seeded, err := loadSeeds(in.Seeds, c, shard)
		if err != nil {
			return err
		}
		if seeded == 0 && c.Frontier().Len() == 0 {
			if !shard.Single() {
				return fmt.Errorf("no seeds in %s fell in shard %d of %d and nothing left in the frontier", in.Seeds, shard.Index, shard.Count)
			}
			return fmt.Errorf("no seeds in %s and nothing left in the frontier", in.Seeds)
		}

		rep, stopRun, err := in.App.StartRun("crawl run", "")
		if err != nil {
			return err
		}
		defer stopRun()
		sp := ccrawl.StartStreamProgress(rep, "pages", int(cfg.MaxPages), 0)
		defer sp.Stop()

		stats, runErr := c.Run(ctx, func(p ccrawl.CrawlPage) error {
			sp.Add(1, 1, int64(p.BodySize))
			return emit(p)
		})
		fmt.Fprintf(os.Stderr,
			"crawl run: %d fetched, %d failed, %d retried, %d disallowed, %d discovered, %s, %d WARC files\n",
			stats.Fetched, stats.Failed, stats.Retried, stats.Disallowed, stats.Discovered,
			humanBytes(stats.Bytes), len(stats.OutFiles))
		if stats.Failed > 0 {
			fmt.Fprintf(os.Stderr, "crawl run: failures by class: dns %d, timeout %d, refused %d, skipped %d, other %d\n",
				stats.ErrDNS, stats.ErrTimeout, stats.ErrRefused, stats.ErrSkip, stats.ErrOther)
		}
		fmt.Fprintln(os.Stderr, "crawl run: "+robotsLine(stats))
		return runErr
	})
}

// loadSeeds reads a seed file into the crawler. It takes crawl seed JSONL and
// it takes a plain list of URLs, because the second is what anyone typing this
// by hand will reach for and telling them off for it would be silly.
//
// The shard filter is applied here rather than at fetch time so a URL this
// machine does not own never reaches the frontier. Three servers reading the
// same file then keep a third of the state each instead of three full copies of
// it, which is the difference between a frontier that fits on server2 and one
// that does not.
func loadSeeds(path string, c *ccrawl.Crawler, shard ccrawl.Shard) (int, error) {
	f := os.Stdin
	if path != "-" {
		var err error
		if f, err = os.Open(path); err != nil {
			return 0, err
		}
		defer func() { _ = f.Close() }()
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 4<<20)
	var n int
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		rec := SeedRecord{URL: line}
		if strings.HasPrefix(line, "{") {
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				return n, fmt.Errorf("seed line %d: %w", n+1, err)
			}
		}
		if rec.URL == "" {
			continue
		}
		if !strings.HasPrefix(rec.URL, "http") {
			rec.URL = "https://" + rec.URL
		}
		// After the scheme is filled in, because the key comes from the parsed
		// host and a bare domain has no host until it has a scheme.
		if !shard.Owns(rec.URL) {
			continue
		}
		if c.Seed(rec.URL, rec.Priority) {
			n++
		}
	}
	if err := sc.Err(); err != nil {
		return n, err
	}
	return n, nil
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
	handle(app, kit.OpMeta{
		Name:    "status",
		Parent:  "crawl",
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

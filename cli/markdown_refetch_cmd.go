package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/tamnd/ami/config"
	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func init() {
	// registerMarkdown is called by commands.go; we hook in via the package init
	// so the refetch sub-command is always registered alongside export.
	// The actual registration happens inside registerMarkdownRefetch which
	// is called from registerMarkdown below.
}

// registerMarkdownRefetch attaches the refetch sub-command under markdown.
// Called by registerMarkdown in markdown_cmd.go.
func registerMarkdownRefetch(app *kit.App) {
	app.AddCommandUnder("markdown", newMarkdownRefetchCmd())
}

// markdownRefetchCmd holds the flags for `ccrawl markdown refetch`.
type markdownRefetchCmd struct {
	shards       string
	outDir       string
	repo         string
	workers      int // conversion workers (NumCPU default)
	fetchWorkers int // ami fetch workers
	maxRedirects int // per-fetch redirect limit
	rate         int // per-host rate limit (req/s, 0 = unlimited)
	push         bool
	limit        int
	parallel     int
	commitBatch  int
	keepParquet  bool
	minFreeGB    int
	ledger       string
	warcCacheDir string // where to cache downloaded WARC shards ("" = default)
	noWARCCache  bool   // disable WARC caching entirely
	fetchOnly    bool   // store raw HTML, skip the convert phase
}

func newMarkdownRefetchCmd() kit.Command {
	v := &markdownRefetchCmd{push: true}
	return kit.Command{
		Use:   "refetch",
		Short: "Re-fetch CC URLs live, convert HTML to Markdown, write Parquet to HuggingFace",
		Long: `Extract every HTML URL from a Common Crawl WARC shard, re-fetch each page live
with ami's high-concurrency engine, convert HTML bodies to Markdown, and write
the results to a zstd-compressed Parquet file.

Output schema (open-markdown-refetch-v1):
  doc_id            stable SHA-256 URL hash (16 bytes hex)
  url               original page URL
  final_url         URL after redirects
  host              hostname
  ip_address        server IP address
  crawl_id          CC crawl ID (e.g. CC-MAIN-2026-25)
  crawl_date        WARC record date (YYYY-MM-DD)
  warc_record_id    WARC-Record-ID of the original CC record
  fetched_at        Unix ms timestamp of the live fetch
  status            HTTP status code
  content_type      Content-Type header
  fetch_duration_ms total fetch wall-clock in milliseconds
  ttfb_ms           time to first byte in milliseconds
  etag              ETag response header
  last_modified     Last-Modified response header
  resp_headers      full HTTP response head (status line + headers)
  body_length       raw body bytes
  digest            SHA-1 hex of the raw body
  html_length       HTML body bytes (set only when content is HTML)
  markdown_length   converted Markdown bytes
  markdown          converted Markdown text
  error             fetch error string (empty on success)

HF path layout (same as open-markdown):
  data/crawl=CC-MAIN-YYYY-WW/NNNNNN.parquet

Examples:
  ccrawl markdown refetch --shards 0 --repo open-index/open-markdown-refetch-v1
  ccrawl markdown refetch --shards 0-9 --fetch-workers 400 --repo open-index/open-markdown-refetch-v1
  ccrawl markdown refetch --shards all --parallel 2 --commit-batch 5
  ccrawl markdown refetch --shards 0-99 --no-push --out ~/data/refetch
  HF_TOKEN=hf_... ccrawl markdown refetch --shards 0 -c 2026-25`,
		Flags: v.flags,
		Run:   v.run,
	}
}

func (v *markdownRefetchCmd) flags(f *kit.FlagSet) {
	f.StringVar(&v.shards, "shards", "0", "shard range: N, N-M, N,M, or all")
	f.StringVar(&v.outDir, "out", "", "directory for parquet files (default: <data-dir>/refetch)")
	f.StringVar(&v.repo, "repo", "open-index/open-markdown-refetch-v1", "HuggingFace dataset repo (org/name)")
	f.IntVar(&v.workers, "workers", 0, "HTML-to-Markdown conversion workers (0 = NumCPU)")
	f.IntVar(&v.fetchWorkers, "fetch-workers", 0, "ami concurrent fetch workers per shard (0 = auto from fd limit)")
	f.IntVar(&v.maxRedirects, "max-redirects", 5, "maximum HTTP redirects per fetch")
	f.IntVar(&v.rate, "rate", 0, "per-host request rate limit (req/s, 0 = unlimited)")
	f.IntVar(&v.limit, "limit", 0, "process at most this many shards (0 = all)")
	f.BoolVar(&v.push, "push", true, "commit each parquet shard to HuggingFace after writing")
	f.IntVar(&v.parallel, "parallel", 2, "shards downloaded/re-fetched concurrently")
	f.IntVar(&v.commitBatch, "commit-batch", 1, "parquet files per HuggingFace commit")
	f.BoolVar(&v.keepParquet, "keep-parquet", false, "keep local parquet files after they are committed")
	f.IntVar(&v.minFreeGB, "min-free-gb", 2, "pause new downloads when free disk drops below this many GiB")
	f.StringVar(&v.ledger, "ledger", "", "resume ledger file (default: <out>/.committed)")
	f.StringVar(&v.warcCacheDir, "warc-cache-dir", "", "cache downloaded WARC shards here (default: <data-dir>/ami/warc)")
	f.BoolVar(&v.noWARCCache, "no-warc-cache", false, "do not cache downloaded WARC shards to disk")
	f.BoolVar(&v.fetchOnly, "fetch-only", false, "store raw HTML and skip the convert phase, so fetch runs at full speed (convert offline later over the html column)")
}

func (v *markdownRefetchCmd) run(ctx context.Context, _ []string) error {
	app := appFromCtx(ctx)

	crawlID, err := app.Crawl(ctx)
	if err != nil {
		return err
	}

	outDir := v.outDir
	if outDir == "" {
		outDir = filepath.Join(app.Cfg.DataDir, "refetch", crawlID)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "refetch: fetching WARC manifest for %s ...\n", crawlID)
	paths, err := ccrawl.FetchPaths(ctx, app.HTTP, app.Cache, crawlID, "warc")
	if err != nil {
		return fmt.Errorf("fetch WARC manifest: %w", err)
	}
	fmt.Fprintf(os.Stderr, "refetch: manifest has %d WARC files\n", len(paths))

	indices, err := parseShardRange(v.shards, len(paths))
	if err != nil {
		return err
	}
	if v.limit > 0 && len(indices) > v.limit {
		indices = indices[:v.limit]
	}

	hf := ccrawl.NewHFClient("")
	if v.push {
		if !hf.Valid() {
			return fmt.Errorf("HF_TOKEN not set -- set it or pass --push=false")
		}
		if err := hf.CreateDatasetRepo(ctx, v.repo, false); err != nil {
			return fmt.Errorf("create HF repo: %w", err)
		}
	}

	ledgerPath := v.ledger
	if ledgerPath == "" {
		ledgerPath = filepath.Join(outDir, ".committed")
	}
	ledger, err := ccrawl.OpenLedger(ledgerPath)
	if err != nil {
		return fmt.Errorf("open ledger: %w", err)
	}
	defer func() { _ = ledger.Close() }()
	if done := ledger.Count(); done > 0 {
		fmt.Fprintf(os.Stderr, "refetch: ledger %s already records %d committed shards\n", ledgerPath, done)
	}

	// Lift the open-file soft limit before fetching: the in-flight ceiling is
	// bounded by file descriptors (one per live socket, plus DNS sockets and open
	// files), and the stock 1024 soft limit caps concurrency well below what the
	// engine can drive. The hard limit is typically 1048576, so this unlocks
	// thousands of concurrent connections without root or a shell ulimit change.
	fdLimit, fdErr := ccrawl.RaiseFileLimit()
	if fdErr != nil {
		fmt.Fprintf(os.Stderr, "refetch: could not raise fd limit (%v); concurrency capped at fd=%d\n", fdErr, fdLimit)
	} else {
		fmt.Fprintf(os.Stderr, "refetch: fd limit %d\n", fdLimit)
	}

	fetchWorkers := v.fetchWorkers
	if fetchWorkers <= 0 {
		fetchWorkers = autoFetchWorkers(fdLimit, v.parallel)
		fmt.Fprintf(os.Stderr, "refetch: auto fetch-workers=%d (per shard, %d shards in parallel)\n", fetchWorkers, max(v.parallel, 1))
	}

	fetchCfg := config.Default()
	fetchCfg.Workers = fetchWorkers
	fetchCfg.MaxRedirects = v.maxRedirects
	if v.rate > 0 {
		fetchCfg.PerHostDelay = time.Second / time.Duration(v.rate)
	}
	// Tuning for a batch re-fetch over an unfiltered Common Crawl shard, where a
	// large fraction of hosts are dead (expired domains, blackholed IPs). The
	// goal is to spend as little wall-clock as possible proving a host is dead so
	// the live ones dominate throughput.
	//   - StartInflight high: skip the slow AIMD ramp; a datacenter uplink can
	//     open the full set at once, and the limiter still backs off on real
	//     congestion.
	//   - ProbeTimeout low: an unreachable host fails at the dial in ~1.5s
	//     instead of holding a worker for the full request timeout.
	//   - DomainFailThreshold 4: the breaker now counts a header timeout on a host
	//     that never sent a byte, which is a softer death signal than a DNS or
	//     route failure, so it takes more strikes to shed a host. This keeps a
	//     live-but-slow host that needs a few attempts to warm up from being
	//     skipped, while a host that is genuinely silent still trips after four.
	//   - MaxRetries 1: a congested-retry on a dead-host-heavy shard multiplies
	//     wasted work; one attempt plus a single retry is enough.
	fetchCfg.StartInflight = fetchWorkers
	fetchCfg.MinInflight = 64
	fetchCfg.Timeout = 4000 * time.Millisecond       // whole-exchange deadline, including body read
	fetchCfg.HeaderTimeout = 2000 * time.Millisecond // first-byte deadline: a host that sends a byte by here is immunized as live, so this must be generous enough for a slow-but-real backend; only true silence is abandoned
	fetchCfg.ProbeTimeout = 1500 * time.Millisecond
	fetchCfg.DNSTimeout = 1500 * time.Millisecond
	fetchCfg.DomainFailThreshold = 4
	fetchCfg.MaxRetries = 1

	// Resolve the WARC cache dir. A cached shard skips the ~80s download on a
	// re-run, so the default is on (under <data-dir>/ami/warc) unless the run
	// opts out with --no-warc-cache or overrides the location.
	warcCacheDir := ""
	if !v.noWARCCache {
		warcCacheDir = v.warcCacheDir
		if warcCacheDir == "" {
			warcCacheDir = filepath.Join(app.Cfg.DataDir, "ami", "warc")
		}
	}
	if warcCacheDir != "" {
		fmt.Fprintf(os.Stderr, "refetch: caching WARC shards under %s\n", warcCacheDir)
	}
	if v.fetchOnly {
		fmt.Fprintf(os.Stderr, "refetch: fetch-only mode, storing raw HTML and skipping convert\n")
	}

	run, runErr := ccrawl.RunRefetchExport(ctx, app.HTTP, hf, ccrawl.RefetchExportConfig{
		CrawlID:        crawlID,
		Indices:        indices,
		WARCPaths:      paths,
		OutDir:         outDir,
		Repo:           v.repo,
		Push:           v.push,
		FetchCfg:       fetchCfg,
		ShardParallel:  v.parallel,
		ConvertWorkers: v.workers,
		CommitBatch:    v.commitBatch,
		KeepParquet:    v.keepParquet,
		MinFreeBytes:   int64(v.minFreeGB) << 30,
		Ledger:         ledger,
		CacheDir:       warcCacheDir,
		FetchOnly:      v.fetchOnly,
	})

	n := int64(run.Committed)
	if n == 0 {
		n = 1
	}
	fetchPagesPerS := 0.0
	if run.FetchS > 0 {
		fetchPagesPerS = float64(run.URLsFound) / float64(run.FetchS)
	}
	fmt.Fprintf(os.Stderr,
		"\nrefetch: %d committed, %d skipped, %d failed of %d | %d rows | urls=%d html=%s md=%s parquet=%s | %s elapsed (%.1f shards/hour)\n",
		run.Committed, run.Skipped, run.Failed, run.Total, run.Rows,
		run.URLsFound, humanBytes(run.HTMLBytes), humanBytes(run.MDBytes), humanBytes(run.ParquetBytes),
		run.Elapsed.Round(time.Second), run.ShardsPerHour)
	fmt.Fprintf(os.Stderr,
		"phase totals: extract=%ds fetch=%ds convert=%ds export=%ds publish=%ds\n",
		run.ExtractS, run.FetchS, run.ConvertS, run.ExportS, run.PublishS)
	fmt.Fprintf(os.Stderr,
		"phase avg/shard: extract=%ds fetch=%ds convert=%ds export=%ds | fetch-only %.0f pages/s (urls/fetch-sec)\n",
		run.ExtractS/n, run.FetchS/n, run.ConvertS/n, run.ExportS/n, fetchPagesPerS)
	if run.Failures > 0 {
		fmt.Fprintf(os.Stderr,
			"failures: %d total | dns=%d timeout=%d refused=%d skip=%d other=%d\n",
			run.Failures, run.ErrDNS, run.ErrTimeout, run.ErrRefused, run.ErrSkip, run.ErrOther)
	}

	return runErr
}

// autoFetchWorkers picks a per-shard in-flight ceiling from the open-file limit.
// Every concurrent request holds at least one socket, and the engine also opens
// transient DNS sockets and per-shard output files, so the total descriptor draw
// across all shards running in parallel must stay under the limit with margin.
// The result is also capped at a memory-practical ceiling: each in-flight fetch
// may buffer a response body, so an unbounded socket budget would otherwise let
// peak memory run away on a high-yield shard.
func autoFetchWorkers(fdLimit uint64, parallel int) int {
	if parallel < 1 {
		parallel = 1
	}
	const (
		reserve   = 512  // DNS sockets, open parquet/warc files, stdio, headroom
		perReqFDs = 2    // one socket plus transient DNS-socket headroom per request
		hardCap   = 3000 // memory-practical ceiling regardless of fd budget
		floor     = 200
	)
	usable := int64(fdLimit) - reserve
	if usable < floor*perReqFDs {
		return floor
	}
	w := int(usable / int64(parallel) / perReqFDs)
	if w > hardCap {
		w = hardCap
	}
	if w < floor {
		w = floor
	}
	return w
}

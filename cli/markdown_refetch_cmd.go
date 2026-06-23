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
	f.IntVar(&v.fetchWorkers, "fetch-workers", 400, "ami concurrent fetch workers per shard")
	f.IntVar(&v.maxRedirects, "max-redirects", 5, "maximum HTTP redirects per fetch")
	f.IntVar(&v.rate, "rate", 10, "per-host request rate limit (req/s, 0 = unlimited)")
	f.IntVar(&v.limit, "limit", 0, "process at most this many shards (0 = all)")
	f.BoolVar(&v.push, "push", true, "commit each parquet shard to HuggingFace after writing")
	f.IntVar(&v.parallel, "parallel", 2, "shards downloaded/re-fetched concurrently")
	f.IntVar(&v.commitBatch, "commit-batch", 1, "parquet files per HuggingFace commit")
	f.BoolVar(&v.keepParquet, "keep-parquet", false, "keep local parquet files after they are committed")
	f.IntVar(&v.minFreeGB, "min-free-gb", 2, "pause new downloads when free disk drops below this many GiB")
	f.StringVar(&v.ledger, "ledger", "", "resume ledger file (default: <out>/.committed)")
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

	fetchCfg := config.Default()
	fetchCfg.Workers = v.fetchWorkers
	fetchCfg.MaxRedirects = v.maxRedirects
	if v.rate > 0 {
		fetchCfg.PerHostDelay = time.Second / time.Duration(v.rate)
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
	})

	fmt.Fprintf(os.Stderr,
		"\nrefetch: %d committed, %d skipped, %d failed of %d | %d rows | urls=%d html=%s md=%s parquet=%s | %s elapsed (%.1f shards/hour)\n",
		run.Committed, run.Skipped, run.Failed, run.Total, run.Rows,
		run.URLsFound, humanBytes(run.HTMLBytes), humanBytes(run.MDBytes), humanBytes(run.ParquetBytes),
		run.Elapsed.Round(time.Second), run.ShardsPerHour)

	return runErr
}

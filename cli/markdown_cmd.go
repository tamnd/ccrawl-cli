package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// registerMarkdown attaches the markdown command group.
func registerMarkdown(app *kit.App) {
	app.CommandGroup("markdown", "Build open-index/open-markdown-style Markdown-parquet datasets from CC WARCs")
	app.AddCommandUnder("markdown", newMarkdownExportCmd())
	registerMarkdownRefetch(app)
}

// markdownExportCmd holds the flags for `ccrawl markdown export`.
type markdownExportCmd struct {
	shards      string // "0", "0-49", "1,3,5", "0-2,5" or "all"
	outDir      string
	repo        string
	workers     int
	skip        bool // --skip-errors: continue on per-shard failure
	push        bool // push to HF after each shard (default true)
	limit       int  // stop after this many shards (0 = all)
	parallel    int  // shards in flight at once (P)
	commitBatch int  // parquets per HF commit (K)
	keepParquet bool // keep local parquet after commit
	minFreeGB   int  // pause downloads below this much free disk
	ledger      string
}

func newMarkdownExportCmd() kit.Command {
	v := &markdownExportCmd{push: true}
	return kit.Command{
		Use:   "export",
		Short: "Stream CC WARCs → Markdown → Parquet → HuggingFace",
		Long: `Download one or more Common Crawl WARC files, extract HTML bodies, convert to
Markdown with h2m (go-trafilatura tuned for recall plus a GFM renderer: cleaned
prose, tables, absolute links), and write each shard to a zstd-compressed Parquet
file. After each shard the Parquet is committed to a HuggingFace dataset repo.

Output schema (open-markdown-v2):
  doc_id          stable SHA-256 URL hash (16 bytes hex)
  url             original page URL
  host            hostname
  crawl_date      WARC-Date YYYY-MM-DD
  warc_record_id  WARC record ID
  html_length     raw HTML body bytes before conversion
  markdown_length converted Markdown bytes
  markdown        converted Markdown text

HF path layout:
  data/crawl=CC-MAIN-YYYY-WW/NNNNNN.parquet

Shards stream in parallel: several downloads run at once (--parallel) to hide
network latency, while a single CPU-sized convert pool (--workers) is shared
across them so the cores never oversubscribe. A background committer batches
finished parquets into one HuggingFace commit (--commit-batch), then deletes the
local files, so the slow commit round trip stays off the per-shard critical path.
A ledger file records committed shards, so a killed run resumes where it stopped.

Examples:
  ccrawl markdown export --shards 0 --repo open-index/open-markdown-v2
  ccrawl markdown export --shards 0-9 --repo open-index/open-markdown-v2
  ccrawl markdown export --shards all --parallel 4 --commit-batch 10
  ccrawl markdown export --shards 0-99 --no-push --out ~/data/md
  HF_TOKEN=hf_... ccrawl markdown export --shards 0 -c 2026-25`,
		Flags: v.flags,
		Run:   v.run,
	}
}

func (v *markdownExportCmd) flags(f *kit.FlagSet) {
	f.StringVar(&v.shards, "shards", "0", "shard range: N, N-M, N,M, or all")
	f.StringVar(&v.outDir, "out", "", "directory for parquet files (default: <data-dir>/markdown)")
	f.StringVar(&v.repo, "repo", "open-index/open-markdown-v2", "HuggingFace dataset repo (org/name)")
	f.IntVar(&v.workers, "workers", 0, "total conversion workers shared across shards (0 = NumCPU)")
	f.IntVar(&v.limit, "limit", 0, "process at most this many shards (0 = all)")
	f.BoolVar(&v.skip, "skip-errors", false, "continue past per-shard failures instead of aborting")
	f.BoolVar(&v.push, "push", true, "commit each parquet shard to HuggingFace after writing")
	f.IntVar(&v.parallel, "parallel", 3, "shards downloaded/converted concurrently")
	f.IntVar(&v.commitBatch, "commit-batch", 1, "parquet files per HuggingFace commit")
	f.BoolVar(&v.keepParquet, "keep-parquet", false, "keep local parquet files after they are committed")
	f.IntVar(&v.minFreeGB, "min-free-gb", 2, "pause new downloads when free disk drops below this many GiB")
	f.StringVar(&v.ledger, "ledger", "", "resume ledger file (default: <out>/.committed)")
}

func (v *markdownExportCmd) run(ctx context.Context, _ []string) error {
	app := appFromCtx(ctx)

	crawlID, err := app.Crawl(ctx)
	if err != nil {
		return err
	}

	outDir := v.outDir
	if outDir == "" {
		outDir = filepath.Join(app.Cfg.DataDir, "markdown", crawlID)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	// Resolve the WARC manifest for this crawl (cached after first fetch).
	fmt.Fprintf(os.Stderr, "markdown: fetching WARC manifest for %s …\n", crawlID)
	paths, err := ccrawl.FetchPaths(ctx, app.HTTP, app.Cache, crawlID, "warc")
	if err != nil {
		return fmt.Errorf("fetch WARC manifest: %w", err)
	}
	fmt.Fprintf(os.Stderr, "markdown: manifest has %d WARC files\n", len(paths))

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
			return fmt.Errorf("HF_TOKEN not set — set it or pass --push=false")
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
	defer ledger.Close()
	if done := ledger.Count(); done > 0 {
		fmt.Fprintf(os.Stderr, "markdown: ledger %s already records %d committed shards\n", ledgerPath, done)
	}

	run, runErr := ccrawl.RunMarkdownExport(ctx, app.HTTP, hf, ccrawl.MarkdownExportConfig{
		CrawlID:        crawlID,
		Indices:        indices,
		WARCPaths:      paths,
		OutDir:         outDir,
		Repo:           v.repo,
		Push:           v.push,
		ShardParallel:  v.parallel,
		ConvertWorkers: v.workers,
		CommitBatch:    v.commitBatch,
		KeepParquet:    v.keepParquet,
		MinFreeBytes:   int64(v.minFreeGB) << 30,
		Ledger:         ledger,
	})

	fmt.Fprintf(os.Stderr,
		"\nmarkdown: %d committed, %d skipped, %d failed of %d | %d rows | html=%s md=%s parquet=%s | %s elapsed (%.1f shards/hour)\n",
		run.Committed, run.Skipped, run.Failed, run.Total, run.Rows,
		humanBytes(run.HTMLBytes), humanBytes(run.MDBytes), humanBytes(run.ParquetBytes),
		run.Elapsed.Round(time.Second), run.ShardsPerHour)

	// Per-shard conversion failures never abort the run; they are logged and
	// counted (the committer keeps draining). runErr is only set for a fatal
	// commit failure or a cancelled context, which always propagates.
	return runErr
}

// parseShardRange turns a shard spec into a sorted list of 0-based indices. It
// accepts a single number ("5"), an inclusive range ("0-49"), a comma list
// ("1,3,5"), combinations ("0-9,20"), or "all" for every shard in the manifest.
func parseShardRange(spec string, total int) ([]int, error) {
	if strings.EqualFold(strings.TrimSpace(spec), "all") {
		out := make([]int, total)
		for i := range out {
			out[i] = i
		}
		return out, nil
	}
	seen := make(map[int]bool)
	var out []int
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if dash := strings.Index(part, "-"); dash >= 0 {
			lo, err1 := strconv.Atoi(part[:dash])
			hi, err2 := strconv.Atoi(part[dash+1:])
			if err1 != nil || err2 != nil {
				return nil, fmt.Errorf("invalid shard range %q", part)
			}
			if lo > hi || lo < 0 || hi >= total {
				return nil, fmt.Errorf("shard range %d-%d out of bounds [0, %d)", lo, hi, total)
			}
			for i := lo; i <= hi; i++ {
				if !seen[i] {
					seen[i] = true
					out = append(out, i)
				}
			}
		} else {
			n, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("invalid shard index %q", part)
			}
			if n < 0 || n >= total {
				return nil, fmt.Errorf("shard %d out of bounds [0, %d)", n, total)
			}
			if !seen[n] {
				seen[n] = true
				out = append(out, n)
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("shard spec %q matched no shards", spec)
	}
	return out, nil
}

package ccrawl

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/tamnd/ami/config"
)

// RefetchExportConfig configures a parallel multi-shard refetch export.
type RefetchExportConfig struct {
	CrawlID   string
	Indices   []int    // shard indices to process, in order
	WARCPaths []string // full manifest, indexed by shard index
	OutDir    string
	Repo      string
	Push      bool

	// FetchCfg is the ami config passed to each shard's FetchBatch call.
	// Workers, MaxRedirects, and PerHostDelay are the knobs most callers set.
	FetchCfg config.Config

	// ShardParallel is the number of shards processed concurrently. 0 picks 2.
	ShardParallel int
	// ConvertWorkers caps total concurrent HTML-to-Markdown conversions. 0 picks NumCPU.
	ConvertWorkers int
	// CommitBatch is how many finished parquets go into one HF commit. 0 means 1.
	CommitBatch int
	// KeepParquet leaves local parquet files in place after they are committed.
	KeepParquet bool
	// MinFreeBytes pauses new downloads while free disk is below this. 0 selects 2 GiB.
	MinFreeBytes int64
	// CacheDir caches downloaded WARC shards so a re-run skips the download.
	// Empty disables caching.
	CacheDir string
	// FetchOnly stores raw HTML and skips the HTML-to-Markdown convert, so the
	// fetch step runs at its true ceiling and Markdown is produced by a separate
	// offline pass over the stored html column.
	FetchOnly bool
	// Ledger, when set, skips already-committed shards and records new ones.
	Ledger *Ledger

	// Progress is called once per committed batch with a snapshot of the run.
	Progress func(RefetchRunStats)
}

// RefetchRunStats is a live snapshot of a parallel refetch export run.
type RefetchRunStats struct {
	Total         int
	Skipped       int
	Committed     int
	Failed        int
	Rows          int64
	URLsFound     int64
	WARCBytes     int64
	FetchBytes    int64
	HTMLBytes     int64
	MDBytes       int64
	ParquetBytes  int64
	// Per-phase wall-time sums (in seconds, across all completed shards).
	ExtractS int64 // phase 1: WARC download + URL extraction
	FetchS   int64 // phase 2: ami live refetch
	ConvertS int64 // phase 3: HTML to Markdown
	ExportS  int64 // phase 4: Parquet write
	PublishS int64 // HF commit (off critical path)
	// Failure breakdown summed across shards, for throughput diagnosis.
	Failures   int64
	ErrDNS     int64
	ErrTimeout int64
	ErrRefused int64
	ErrSkip    int64
	ErrOther   int64
	Elapsed       time.Duration
	ShardsPerHour float64
	ETA           time.Duration
	FreeDiskBytes int64
}

// packRefetchFn is the function the orchestrator uses to refetch one shard.
// Tests swap it for a stub to exercise orchestration without hitting the network.
var packRefetchFn = PackRefetchShard

// refetchShardResult is one shard's outcome handed from a worker to the committer.
type refetchShardResult struct {
	idx   int
	path  string
	stats RefetchStats
	err   error
}

// RunRefetchExport streams the requested shards through the refetch pipeline in
// parallel and commits the parquet files to HuggingFace in batches.
//
// Layout mirrors RunMarkdownExport:
//
//	[P shard workers] extract URLs + re-fetch + convert → finished channel →
//	[1 committer] batches K parquets per HF commit, deletes local files,
//	records the ledger, logs an ETA.
func RunRefetchExport(ctx context.Context, h *HTTPClient, hf *HFClient, cfg RefetchExportConfig) (RefetchRunStats, error) {
	p := cfg.ShardParallel
	if p <= 0 {
		p = 2
	}
	c := cfg.ConvertWorkers
	if c <= 0 {
		c = runtime.NumCPU()
	}
	k := cfg.CommitBatch
	if k <= 0 {
		k = 1
	}
	minFree := cfg.MinFreeBytes
	if minFree <= 0 {
		minFree = 2 << 30
	}

	if err := os.MkdirAll(cfg.OutDir, 0o755); err != nil {
		return RefetchRunStats{}, err
	}

	sem := make(chan struct{}, c)
	jobs := make(chan int)
	finished := make(chan refetchShardResult, p+k)

	start := time.Now()
	run := RefetchRunStats{Total: len(cfg.Indices)}

	committerDone := make(chan struct{})
	var commitErr error
	go func() {
		defer close(committerDone)
		commitErr = runRefetchCommitter(ctx, hf, cfg, k, start, finished, &run)
	}()

	var wg sync.WaitGroup
	wg.Add(p)
	for range p {
		go func() {
			defer wg.Done()
			for idx := range jobs {
				if ctx.Err() != nil {
					return
				}
				waitForDisk(ctx, cfg.OutDir, minFree)
				outPath := filepath.Join(cfg.OutDir, fmt.Sprintf("%06d.parquet", idx))
				stats, err := packRefetchFn(ctx, h, RefetchPackConfig{
					CrawlID:    cfg.CrawlID,
					ShardIdx:   idx,
					WARCPath:   cfg.WARCPaths[idx],
					OutPath:    outPath,
					FetchCfg:   cfg.FetchCfg,
					ConvertSem: sem,
					CacheDir:   cfg.CacheDir,
					FetchOnly:  cfg.FetchOnly,
				})
				if err != nil {
					_ = os.Remove(outPath)
					finished <- refetchShardResult{idx: idx, err: err}
					continue
				}
				finished <- refetchShardResult{idx: idx, path: outPath, stats: stats}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, idx := range cfg.Indices {
			if cfg.Ledger != nil && cfg.Ledger.Has(idx) {
				finished <- refetchShardResult{idx: idx, stats: RefetchStats{ShardIdx: idx}, path: "", err: errAlreadyDone}
				continue
			}
			select {
			case <-ctx.Done():
				return
			case jobs <- idx:
			}
		}
	}()

	wg.Wait()
	close(finished)
	<-committerDone

	run.Elapsed = time.Since(start)
	run.FreeDiskBytes = freeDiskBytes(cfg.OutDir)
	if commitErr != nil {
		return run, commitErr
	}
	return run, ctx.Err()
}

// runRefetchCommitter drains finished shards, batches K parquets per HF commit,
// deletes the local files, records the ledger, and reports progress with an ETA.
func runRefetchCommitter(ctx context.Context, hf *HFClient, cfg RefetchExportConfig, k int, start time.Time, finished <-chan refetchShardResult, run *RefetchRunStats) error {
	var batch []refetchShardResult

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		ops := make([]HFOperation, 0, len(batch)+1)
		for _, r := range batch {
			ops = append(ops, HFOperation{
				LocalPath:  r.path,
				PathInRepo: HFRefetchPath(cfg.CrawlID, r.idx),
			})
		}

		for _, r := range batch {
			run.Rows += r.stats.Rows
			run.URLsFound += r.stats.URLsFound
			run.WARCBytes += r.stats.WARCBytes
			run.FetchBytes += r.stats.FetchBytes
			run.HTMLBytes += r.stats.HTMLBytes
			run.MDBytes += r.stats.MDBytes
			run.ParquetBytes += r.stats.ParquetBytes
			run.ExtractS += int64(r.stats.DurExtract.Seconds())
			run.FetchS += int64(r.stats.DurFetch.Seconds())
			run.ConvertS += int64(r.stats.DurConvert.Seconds())
			run.ExportS += int64(r.stats.DurExport.Seconds())
			run.Failures += r.stats.Failed
			run.ErrDNS += r.stats.ErrDNS
			run.ErrTimeout += r.stats.ErrTimeout
			run.ErrRefused += r.stats.ErrRefused
			run.ErrSkip += r.stats.ErrSkip
			run.ErrOther += r.stats.ErrOther
		}

		if cfg.Push {
			readmeTmp, err := writeRefetchTempREADME(RefetchDatasetStats{
				CrawlID:         cfg.CrawlID,
				CommittedShards: run.Committed + len(batch),
				TotalShards:     len(cfg.WARCPaths),
				Rows:            run.Rows,
				URLsFound:       run.URLsFound,
				WARCBytes:       run.WARCBytes,
				FetchBytes:      run.FetchBytes,
				HTMLBytes:       run.HTMLBytes,
				MDBytes:         run.MDBytes,
				ParquetBytes:    run.ParquetBytes,
				ConvertS:        run.ConvertS,
				PublishS:        run.PublishS,
			})
			if err != nil {
				return err
			}
			defer func() { _ = os.Remove(readmeTmp) }()
			ops = append(ops, HFOperation{LocalPath: readmeTmp, PathInRepo: "README.md"})

			lo, hi := batch[0].idx, batch[len(batch)-1].idx
			msg := fmt.Sprintf("Add %s shards %06d-%06d (%d files)", cfg.CrawlID, lo, hi, len(batch))
			tPush := time.Now()
			if _, err := hf.CommitWithRetry(ctx, cfg.Repo, msg, ops, 5); err != nil {
				return fmt.Errorf("commit batch %06d-%06d: %w", lo, hi, err)
			}
			run.PublishS += int64(time.Since(tPush).Seconds())
		}

		for _, r := range batch {
			if cfg.Ledger != nil {
				if err := cfg.Ledger.Mark(r.idx); err != nil {
					return err
				}
			}
			if !cfg.KeepParquet {
				_ = os.Remove(r.path)
			}
		}
		run.Committed += len(batch)
		batch = batch[:0]

		updateRefetchRunRates(run, start)
		logRefetchProgress(cfg, run)
		if cfg.Progress != nil {
			cfg.Progress(*run)
		}
		return nil
	}

	for r := range finished {
		switch {
		case r.err == errAlreadyDone:
			run.Skipped++
		case r.err != nil:
			run.Failed++
			fmt.Fprintf(os.Stderr, "refetch: shard %06d failed: %v\n", r.idx, r.err)
		default:
			batch = append(batch, r)
			if len(batch) >= k {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	return flush()
}

// updateRefetchRunRates recomputes throughput and ETA from committed progress.
func updateRefetchRunRates(run *RefetchRunStats, start time.Time) {
	elapsed := time.Since(start)
	run.Elapsed = elapsed
	if elapsed > 0 && run.Committed > 0 {
		run.ShardsPerHour = float64(run.Committed) / elapsed.Hours()
		remaining := run.Total - run.Skipped - run.Committed - run.Failed
		if remaining > 0 && run.ShardsPerHour > 0 {
			run.ETA = time.Duration(float64(remaining) / run.ShardsPerHour * float64(time.Hour))
		} else {
			run.ETA = 0
		}
	}
}

// logRefetchProgress prints a two-line status: top-level throughput + per-phase breakdown.
func logRefetchProgress(cfg RefetchExportConfig, run *RefetchRunStats) {
	done := run.Committed + run.Skipped + run.Failed
	pct := 0.0
	if run.Total > 0 {
		pct = float64(done) / float64(run.Total) * 100
	}
	run.FreeDiskBytes = freeDiskBytes(cfg.OutDir)

	// Per-phase average over committed shards (0 guard avoids div-by-zero).
	n := int64(run.Committed)
	if n == 0 {
		n = 1
	}
	// Fetch throughput is URLs processed per second of the fetch phase: that is
	// the rate the refetch step disposes of pages, whether they answer or time
	// out. Markdown rows are a downstream yield, not a fetch rate, so reporting
	// rows/s would understate fetch throughput on a dead-host-heavy shard where
	// most URLs fail.
	fetchPerS := 0.0
	if run.FetchS > 0 {
		fetchPerS = float64(run.URLsFound) / float64(run.FetchS)
	}
	fmt.Fprintf(os.Stderr,
		"refetch: %d/%d shards (%.1f%%) | %d rows | %.1f shards/hr | ETA %s | disk %s\n",
		done, run.Total, pct, run.Rows,
		run.ShardsPerHour, fmtETA(run.ETA), fmtBytes(run.FreeDiskBytes))
	fmt.Fprintf(os.Stderr,
		"  phases/shard avg: extract=%ds fetch=%ds convert=%ds export=%ds | fetch %.0f pages/s (%d urls)\n",
		run.ExtractS/n, run.FetchS/n, run.ConvertS/n, run.ExportS/n, fetchPerS, run.URLsFound)
	if run.Failures > 0 {
		fmt.Fprintf(os.Stderr,
			"  failures: %d total | dns=%d timeout=%d refused=%d skip=%d other=%d\n",
			run.Failures, run.ErrDNS, run.ErrTimeout, run.ErrRefused, run.ErrSkip, run.ErrOther)
	}
}

// RefetchDatasetStats holds cumulative stats for the refetch README card.
type RefetchDatasetStats struct {
	CrawlID         string
	CommittedShards int
	TotalShards     int
	Rows            int64
	URLsFound       int64
	WARCBytes       int64
	FetchBytes      int64
	HTMLBytes       int64
	MDBytes         int64
	ParquetBytes    int64
	ConvertS        int64
	PublishS        int64
}

// writeRefetchTempREADME renders the refetch dataset card to a temp file for a commit.
func writeRefetchTempREADME(s RefetchDatasetStats) (string, error) {
	f, err := os.CreateTemp("", "open-markdown-refetch-readme-*.md")
	if err != nil {
		return "", err
	}
	w := bufio.NewWriter(f)
	if _, err := w.WriteString(generateRefetchREADME(s)); err != nil {
		_ = f.Close()
		return "", err
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return "", err
	}
	return f.Name(), f.Close()
}

// generateRefetchREADME produces the HuggingFace dataset card for the refetch dataset.
func generateRefetchREADME(s RefetchDatasetStats) string {
	shards := s.CommittedShards
	if s.TotalShards > shards {
		shards = s.TotalShards
	}
	scaled := shards > s.CommittedShards && s.CommittedShards > 0

	rows, warc, fetch, html, md, pq := s.Rows, s.WARCBytes, s.FetchBytes, s.HTMLBytes, s.MDBytes, s.ParquetBytes
	if scaled {
		rows = scaleEst(rows, s.CommittedShards, shards)
		warc = scaleEst(warc, s.CommittedShards, shards)
		fetch = scaleEst(fetch, s.CommittedShards, shards)
		html = scaleEst(html, s.CommittedShards, shards)
		md = scaleEst(md, s.CommittedShards, shards)
		pq = scaleEst(pq, s.CommittedShards, shards)
	}
	_ = warc
	_ = fetch

	approx := ""
	if scaled {
		approx = "~"
	}

	var b strings.Builder
	fmt.Fprintf(&b, `---
configs:
- config_name: default
  data_files:
  - split: train
    path: "data/crawl=%s/**/*.parquet"
- config_name: %s
  data_files:
  - split: train
    path: "data/crawl=%s/**/*.parquet"
license: odc-by
task_categories:
- text-generation
- feature-extraction
language:
- multilingual
pretty_name: Open Markdown Refetch
size_categories:
- %s
tags:
- common-crawl
- web
- markdown
- refetch
- parquet
- open-data
---

`, s.CrawlID, s.CrawlID, s.CrawlID, sizeCategory(rows))

	fmt.Fprintf(&b, "# Open Markdown Refetch\n\n")
	fmt.Fprintf(&b, "Re-fetched live web pages from Common Crawl %s URLs, converted to Markdown.\n\n", s.CrawlID)
	fmt.Fprintf(&b, "Committed %s%d of %d shards. %sRows: %s. HTML: %s. Markdown: %s. Parquet: %s.\n\n",
		approx, s.CommittedShards, s.TotalShards,
		approx, fmtInt(rows), fmtBytes(html), fmtBytes(md), fmtBytes(pq))
	return b.String()
}

// scaleEst and fmtBytes are defined in hf_readme.go.
// fmtInt and fmtETA are defined in markdown_readme.go and markdown_run.go.

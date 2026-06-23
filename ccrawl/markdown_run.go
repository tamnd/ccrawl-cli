package ccrawl

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// Ledger is an append-only record of which shard indices have been committed.
// It lets a killed run resume without re-doing finished shards and without a
// database: the file is one shard index per line. It is safe for concurrent
// use; the committer is the only writer in practice.
type Ledger struct {
	mu   sync.Mutex
	path string
	done map[int]bool
	f    *os.File
}

// OpenLedger loads any existing ledger at path and opens it for appending.
func OpenLedger(path string) (*Ledger, error) {
	l := &Ledger{path: path, done: make(map[int]bool)}
	if data, err := os.ReadFile(path); err == nil {
		for _, line := range splitLines(string(data)) {
			if n, err := strconv.Atoi(line); err == nil {
				l.done[n] = true
			}
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l.f = f
	return l, nil
}

// Has reports whether shard idx is already committed.
func (l *Ledger) Has(idx int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.done[idx]
}

// Mark records shard idx as committed and flushes the line to disk.
func (l *Ledger) Mark(idx int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done[idx] {
		return nil
	}
	if _, err := fmt.Fprintf(l.f, "%d\n", idx); err != nil {
		return err
	}
	if err := l.f.Sync(); err != nil {
		return err
	}
	l.done[idx] = true
	return nil
}

// Count returns how many shards the ledger has recorded.
func (l *Ledger) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.done)
}

// Close closes the underlying file.
func (l *Ledger) Close() error {
	if l.f == nil {
		return nil
	}
	return l.f.Close()
}

// MarkdownExportConfig configures a parallel multi-shard markdown export.
type MarkdownExportConfig struct {
	CrawlID   string
	Indices   []int    // shard indices to process, in order
	WARCPaths []string // full manifest, indexed by shard index
	OutDir    string
	Repo      string
	Push      bool

	// ShardParallel is the number of shards processed at once (P). More than a
	// couple only helps when downloads are slow relative to conversion. 0 picks
	// a small default.
	ShardParallel int
	// ConvertWorkers caps total concurrent conversions across all shards (C).
	// 0 selects runtime.NumCPU().
	ConvertWorkers int
	// CommitBatch is how many finished parquets go into one HF commit (K). The
	// committer runs off the critical path, so batching keeps commit round
	// trips from dominating wall-clock on long runs. 0 means 1.
	CommitBatch int
	// KeepParquet leaves local parquet files in place after they are committed.
	// The default deletes them, which is required on small disks.
	KeepParquet bool
	// MinFreeBytes pauses new downloads while free disk is below this. 0 selects
	// 2 GiB.
	MinFreeBytes int64
	// Ledger, when set, skips already-committed shards and records new ones.
	Ledger *Ledger

	// Progress is called once per committed batch with a snapshot of the run.
	// It may be nil.
	Progress func(MarkdownRunStats)
}

// MarkdownRunStats is a live snapshot of a parallel export run.
type MarkdownRunStats struct {
	Total          int   // shards requested this run
	Skipped        int   // shards skipped via the ledger
	Committed      int   // shards committed so far
	Failed         int   // shards that errored
	Rows           int64 // cumulative rows across committed shards
	WARCBytes      int64
	HTMLBytes      int64
	MDBytes        int64
	ParquetBytes   int64
	ConvertS       int64 // cumulative per-shard conversion wall-clock, seconds
	PublishS       int64 // cumulative HF commit wall-clock, seconds
	Elapsed        time.Duration
	ShardsPerHour  float64
	ETA            time.Duration // estimated time to finish the remaining shards
	FreeDiskBytes  int64
}

// packShardFn is the function the orchestrator uses to convert one shard. It
// points at PackMarkdownShard in production; tests swap it for a stub so the
// orchestration logic can be exercised without hitting the network.
var packShardFn = PackMarkdownShard

// shardResult is one shard's outcome handed from a shard worker to the committer.
type shardResult struct {
	idx   int
	path  string
	stats MarkdownStats
	err   error
}

// RunMarkdownExport streams the requested shards through the conversion pipeline
// in parallel and commits the parquet files to HuggingFace in batches.
//
// Layout:
//
//	[P shard workers] download+convert each shard (sharing one C-sized convert
//	    semaphore) → finished channel → [1 committer] batches K parquets per HF
//	    commit, deletes the local files, records the ledger, logs an ETA.
//
// Moving commits onto their own goroutine keeps the ~30 s HF round trip off the
// per-shard critical path, which is what makes a full-crawl run practical.
func RunMarkdownExport(ctx context.Context, h *HTTPClient, hf *HFClient, cfg MarkdownExportConfig) (MarkdownRunStats, error) {
	p := cfg.ShardParallel
	if p <= 0 {
		p = 3
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
		return MarkdownRunStats{}, err
	}

	sem := make(chan struct{}, c)
	jobs := make(chan int)
	finished := make(chan shardResult, p+k)

	start := time.Now()
	run := MarkdownRunStats{Total: len(cfg.Indices)}

	// Committer: the only goroutine that touches run's cumulative fields and the
	// ledger, so no extra locking is needed there.
	committerDone := make(chan struct{})
	var commitErr error
	go func() {
		defer close(committerDone)
		commitErr = runCommitter(ctx, hf, cfg, k, start, finished, &run)
	}()

	// Shard workers.
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
				t0 := time.Now()
				stats, err := packShardFn(ctx, h, MarkdownPackConfig{
					CrawlID:    cfg.CrawlID,
					ShardIdx:   idx,
					WARCPath:   cfg.WARCPaths[idx],
					OutPath:    outPath,
					Workers:    c,
					ConvertSem: sem,
				})
				// Per-shard wall-clock is the useful convert figure for a parallel
				// run; the streamed download is folded into it.
				stats.DurConvert = time.Since(t0)
				if err != nil {
					_ = os.Remove(outPath)
					finished <- shardResult{idx: idx, err: err}
					continue
				}
				finished <- shardResult{idx: idx, path: outPath, stats: stats}
			}
		}()
	}

	// Feed shard indices, skipping any the ledger already has.
	go func() {
		defer close(jobs)
		for _, idx := range cfg.Indices {
			if cfg.Ledger != nil && cfg.Ledger.Has(idx) {
				finished <- shardResult{idx: idx, stats: MarkdownStats{ShardIdx: idx}, path: "", err: errAlreadyDone}
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

// errAlreadyDone marks a shard the ledger already recorded so the committer can
// count it as skipped without trying to commit it.
var errAlreadyDone = fmt.Errorf("already committed")

// runCommitter drains finished shards, batches K parquets per HF commit, deletes
// the local files, records the ledger, and reports progress with an ETA.
func runCommitter(ctx context.Context, hf *HFClient, cfg MarkdownExportConfig, k int, start time.Time, finished <-chan shardResult, run *MarkdownRunStats) error {
	var batch []shardResult

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		ops := make([]HFOperation, 0, len(batch)+1)
		for _, r := range batch {
			ops = append(ops, HFOperation{
				LocalPath:  r.path,
				PathInRepo: HFMarkdownPath(cfg.CrawlID, r.idx),
			})
		}

		// Fold this batch into the cumulative totals before rendering the card so
		// the README reflects everything committed so far.
		for _, r := range batch {
			run.Rows += r.stats.Rows
			run.WARCBytes += r.stats.WARCBytes
			run.HTMLBytes += r.stats.HTMLBytes
			run.MDBytes += r.stats.MDBytes
			run.ParquetBytes += r.stats.ParquetBytes
			run.ConvertS += int64(r.stats.DurConvert.Seconds())
		}

		var readmeTmp string
		if cfg.Push {
			committedAfter := run.Committed + len(batch)
			dstats := MarkdownDatasetStats{
				CrawlID:         cfg.CrawlID,
				CommittedShards: committedAfter,
				TotalShards:     len(cfg.WARCPaths),
				Rows:            run.Rows,
				WARCBytes:       run.WARCBytes,
				HTMLBytes:       run.HTMLBytes,
				MDBytes:         run.MDBytes,
				ParquetBytes:    run.ParquetBytes,
				ConvertS:        run.ConvertS,
				PublishS:        run.PublishS,
			}
			tmp, err := writeTempREADME(dstats)
			if err != nil {
				return err
			}
			readmeTmp = tmp
			defer os.Remove(readmeTmp)
			ops = append(ops, HFOperation{LocalPath: readmeTmp, PathInRepo: "README.md"})

			lo, hi := batch[0].idx, batch[len(batch)-1].idx
			msg := fmt.Sprintf("Add %s shards %06d-%06d (%d files)", cfg.CrawlID, lo, hi, len(batch))
			tPush := time.Now()
			if _, err := hf.CommitWithRetry(ctx, cfg.Repo, msg, ops, 5); err != nil {
				return fmt.Errorf("commit batch %06d-%06d: %w", lo, hi, err)
			}
			run.PublishS += int64(time.Since(tPush).Seconds())
		}

		// Record the ledger and reclaim disk now that the files are safe on HF.
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

		updateRunRates(run, start)
		logProgress(cfg, run)
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
			fmt.Fprintf(os.Stderr, "markdown: shard %06d failed: %v\n", r.idx, r.err)
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

// updateRunRates recomputes the throughput and ETA from committed progress.
func updateRunRates(run *MarkdownRunStats, start time.Time) {
	elapsed := time.Since(start)
	run.Elapsed = elapsed
	if elapsed > 0 && run.Committed > 0 {
		run.ShardsPerHour = float64(run.Committed) / elapsed.Hours()
		remaining := run.Total - run.Skipped - run.Committed - run.Failed
		if remaining > 0 && run.ShardsPerHour > 0 {
			run.ETA = time.Duration(float64(remaining)/run.ShardsPerHour*float64(time.Hour))
		} else {
			run.ETA = 0
		}
	}
}

// logProgress prints a one-line status with throughput, ETA, and free disk.
func logProgress(cfg MarkdownExportConfig, run *MarkdownRunStats) {
	done := run.Committed + run.Skipped + run.Failed
	pct := 0.0
	if run.Total > 0 {
		pct = float64(done) / float64(run.Total) * 100
	}
	run.FreeDiskBytes = freeDiskBytes(cfg.OutDir)
	fmt.Fprintf(os.Stderr,
		"markdown: %d/%d shards (%.1f%%) | %.1f rows total | %.1f shards/hour | ETA %s | disk free %s\n",
		done, run.Total, pct, float64(run.Rows),
		run.ShardsPerHour, fmtETA(run.ETA), fmtBytes(run.FreeDiskBytes))
}

func fmtETA(d time.Duration) string {
	if d <= 0 {
		return "n/a"
	}
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	if days > 0 {
		return fmt.Sprintf("%dd %dh", days, hours)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, int(d.Minutes())%60)
	}
	return fmt.Sprintf("%dm", int(d.Minutes()))
}

// writeTempREADME renders the dataset card to a temp file for a commit.
func writeTempREADME(dstats MarkdownDatasetStats) (string, error) {
	f, err := os.CreateTemp("", "open-markdown-readme-*.md")
	if err != nil {
		return "", err
	}
	w := bufio.NewWriter(f)
	if _, err := w.WriteString(GenerateMarkdownREADME(dstats)); err != nil {
		f.Close()
		return "", err
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return "", err
	}
	return f.Name(), f.Close()
}

// waitForDisk blocks until the filesystem holding dir has at least minFree bytes
// available, so a stalled committer cannot let parquet files fill a small disk.
// It returns immediately when free space is unknown (0) or the context is done.
func waitForDisk(ctx context.Context, dir string, minFree int64) {
	warned := false
	for {
		free := freeDiskBytes(dir)
		if free == 0 || free >= minFree {
			return
		}
		if !warned {
			fmt.Fprintf(os.Stderr, "markdown: disk low (%s free, need %s), pausing downloads\n",
				fmtBytes(free), fmtBytes(minFree))
			warned = true
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

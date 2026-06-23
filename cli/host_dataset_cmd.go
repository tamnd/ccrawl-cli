package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func newHostDatasetCmd() kit.Command {
	d := &hostDatasetCmd{}
	return kit.Command{
		Use:   "dataset",
		Short: "Build and publish the cc-host-dataset Parquet shards",
		Long: `Build and publish all 262M CC hosts as a partitioned Parquet dataset.

The pipeline runs in three phases:

  Phase 1 (CDX prep): DuckDB scans ~184 GB of Parquet once and writes
    per-prefix cdx-{x}.jsonl.gz files into --work-dir. Skipped if files
    already exist.

  Phase 2 (Rank split): Streams the rank table (~2.8 GB) once and writes
    per-prefix rank-{x}.tsv.gz files into --work-dir. Skipped if files
    already exist.

  Phase 3 (Shard): For each prefix (a-z, 0, misc), loads the CDX map
    for that prefix into RAM, joins with rank rows, writes
    hosts-{x}.parquet to --out-dir. Optionally uploads to HuggingFace
    and deletes the local Parquet file.

Run a single prefix first to measure timing, then let the rest run
unattended:

  ccrawl host dataset --prefix a --work-dir /tmp/cc-ds --out-dir /tmp/shards
  ccrawl host dataset --work-dir /tmp/cc-ds --out-dir /tmp/shards --upload`,
		Flags: d.flags,
		Run:   d.run,
	}
}

type hostDatasetCmd struct {
	graph        string
	workDir      string
	outDir       string
	prefix       string
	noCDX        bool
	cdxAgg       bool
	upload       bool
	hfRepo       string
	hfToken      string
	hfPrivate    bool
	skipCDX      bool
	skipRank     bool
	cdxWorkers   int
	cdxLimit     int
	cdxBatchSize int
}

func (d *hostDatasetCmd) flags(f *kit.FlagSet) {
	f.StringVar(&d.graph, "graph", "", "web-graph release ID (default: latest)")
	f.StringVar(&d.workDir, "work-dir", filepath.Join(os.Getenv("HOME"), ".ccrawl", "dataset"), "directory for intermediate prefix files")
	f.StringVar(&d.outDir, "out-dir", ".", "directory for output Parquet shards")
	f.StringVar(&d.prefix, "prefix", "", "process only this prefix (a-z, 0, misc); empty = all")
	f.BoolVar(&d.noCDX, "no-cdx", false, "skip CDX enrichment (rank signals only)")
	f.BoolVar(&d.cdxAgg, "cdx-agg", false, "also write cdx-agg-*.jsonl.gz per-host summary files after raw extract")
	f.BoolVar(&d.upload, "upload", false, "upload each shard to HuggingFace after building")
	f.StringVar(&d.hfRepo, "hf-repo", "open-index/cc-host-dataset", "HuggingFace dataset repository (org/name)")
	f.StringVar(&d.hfToken, "hf-token", "", "HuggingFace token (default: $HUGGINGFACE_TOKEN)")
	f.BoolVar(&d.hfPrivate, "hf-private", false, "create HuggingFace repo as private")
	f.BoolVar(&d.skipCDX, "skip-cdx-raw", false, "skip CDX raw extract phase (assume cdx-raw-*.jsonl.gz present)")
	f.BoolVar(&d.skipRank, "skip-rank-split", false, "skip rank-split phase (assume rank-*.tsv.gz present)")
	f.IntVar(&d.cdxWorkers, "cdx-workers", 8, "concurrent CDX Parquet download workers (lower if CC returns 429/403)")
	f.IntVar(&d.cdxLimit, "cdx-limit", 0, "stop after N CDX files (0=all; for benchmarking only)")
	f.IntVar(&d.cdxBatchSize, "cdx-batch-size", 30, "CDX files per commit batch (0=monolithic, commit only after all CDX done)")
}

func (d *hostDatasetCmd) run(ctx context.Context, _ []string) error {
	app := appFromCtx(ctx)
	runStart := time.Now()

	if err := os.MkdirAll(d.workDir, 0o755); err != nil {
		return fmt.Errorf("create work-dir: %w", err)
	}
	if err := os.MkdirAll(d.outDir, 0o755); err != nil {
		return fmt.Errorf("create out-dir: %w", err)
	}

	g, err := resolveGraph(ctx, app, d.graph)
	if err != nil {
		return err
	}
	crawlID, err := app.Crawl(ctx)
	if err != nil {
		return err
	}

	logf := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "[%s] "+format+"\n", append([]any{time.Since(runStart).Round(time.Second)}, args...)...)
	}

	// Set up HF client if upload requested.
	var hf *ccrawl.HFClient
	if d.upload {
		hf = ccrawl.NewHFClient(d.hfToken)
		if !hf.Valid() {
			return fmt.Errorf("--upload requires HF_TOKEN env var or --hf-token flag")
		}
		logf("HF upload enabled: repo=%s private=%v", d.hfRepo, d.hfPrivate)
		if err := hf.CreateDatasetRepo(ctx, d.hfRepo, d.hfPrivate); err != nil {
			return fmt.Errorf("create HF repo: %w", err)
		}
		logf("HF repo ready: https://huggingface.co/datasets/%s", d.hfRepo)
	}

	// Batched mode: interleave CDX batches with shard builds and HF commits.
	if d.cdxBatchSize > 0 && !d.noCDX && !d.skipCDX && d.prefix == "" {
		return d.runBatched(ctx, app, g, crawlID, hf, logf, runStart)
	}

	// ── Phase 4: CDX raw extract ─────────────────────────────────────────────
	// Downloads all 302 CDX Parquet files with N parallel workers, reads each
	// with parquet-go (no DuckDB, no GROUP BY), and fans each row to its
	// per-prefix cdx-raw-{prefix}.jsonl.gz file in one pass. No aggregation.
	cdxPrefixes := ccrawl.DatasetPrefixes
	if d.prefix != "" {
		cdxPrefixes = []string{d.prefix}
	}
	if !d.noCDX && !d.skipCDX {
		urls, err := ccrawl.ColumnarParquetURLs(ctx, app.HTTP, app.Cache, crawlID, "warc", app.Cfg.Source)
		if err != nil {
			return fmt.Errorf("CDX parquet URLs: %w", err)
		}
		logf("phase 4: CDX raw extract — %d files, %d workers", len(urls), d.cdxWorkers)
		t0 := time.Now()
		if err := ccrawl.ExtractCDXRaw(ctx, app.HTTP, urls, d.workDir, d.cdxWorkers, d.cdxLimit, func(fileN int, rows int64) {
			logf("  CDX file %d/%d done (%d rows total)", fileN, len(urls), rows)
		}); err != nil {
			return fmt.Errorf("phase 4 CDX raw: %w", err)
		}
		logf("phase 4 done in %s", time.Since(t0).Round(time.Second))
	}

	// ── Phase 5: CDX aggregate (opt-in) ──────────────────────────────────────
	// Off by default. Pass --cdx-agg to also write cdx-agg-{prefix}.jsonl.gz
	// per-host summary files alongside the raw files. Shard build (phase 3)
	// reads cdx-raw-* directly and does not need these files.
	if !d.noCDX && d.cdxAgg {
		logf("phase 5: CDX aggregate (opt-in) — %d prefixes", len(cdxPrefixes))
		t0 := time.Now()
		if err := ccrawl.AggregateCDXRaw(ctx, d.workDir, cdxPrefixes, 1, func(prefix string, hosts int64) {
			logf("  CDX agg prefix %q done (%d hosts)", prefix, hosts)
		}); err != nil {
			return fmt.Errorf("phase 5 CDX agg: %w", err)
		}
		logf("phase 5 done in %s", time.Since(t0).Round(time.Second))
	}

	// ── Phase 2: Rank split ───────────────────────────────────────────────────
	// Download the rank table (3-8 GB gzipped TSV) to disk first, then split.
	// Downloading to disk means interrupted downloads resume from where they left off.
	if !d.skipRank {
		markerPath := filepath.Join(d.workDir, "rank.done")
		rankCachePath := filepath.Join(d.workDir, "rank-table.tsv.gz")
		if _, err := os.Stat(markerPath); os.IsNotExist(err) {
			logf("phase 2a: downloading rank table to %s (resumes if interrupted)", rankCachePath)
			t0 := time.Now()
			if err := ccrawl.DownloadRankTable(ctx, g.HostRankURL(), rankCachePath); err != nil {
				return fmt.Errorf("phase 2a rank download: %w", err)
			}
			logf("phase 2a done in %s", time.Since(t0).Round(time.Second))

			logf("phase 2b: rank split from local file")
			t0 = time.Now()
			counts, err := ccrawl.SplitRankFromFile(ctx, rankCachePath, d.workDir, func(total int64) {
				logf("  rank rows written: %d M", total/1_000_000)
			})
			if err != nil {
				return fmt.Errorf("phase 2b rank split: %w", err)
			}
			var total int64
			for _, c := range counts {
				total += c
			}
			logf("phase 2b done: %d rank rows in %s", total, time.Since(t0).Round(time.Second))
			_ = os.WriteFile(markerPath, []byte(fmt.Sprintf("rows=%d graph=%s\n", total, g.ID)), 0o644)
		} else {
			logf("phase 2: rank split already done (marker found), skipping")
		}
	}

	// ── Phase 3: Per-prefix shards ────────────────────────────────────────────
	// For each prefix: build Parquet shard, optionally commit to HF, remove local file.
	// Commit path: data/crawl={crawlID}/subset=urls/hosts-{prefix}.parquet
	// Hive-partition layout lets DuckDB read with hive_partitioning=true.
	prefixes := ccrawl.DatasetPrefixes
	if d.prefix != "" {
		prefixes = []string{d.prefix}
	}

	var (
		shardTimes      []time.Duration
		shardStart      = time.Now()
		committedShards int
		totalURLs       int64
		totalBytes      int64
	)

	// Count already-done shards for accurate README stats on resume.
	for _, p := range prefixes {
		doneM := filepath.Join(d.workDir, fmt.Sprintf("shard-%s.done", p))
		if _, e := os.Stat(doneM); e == nil {
			committedShards++
		}
	}

	readmePath := filepath.Join(d.workDir, "README.md")

	writeReadme := func() {
		if hf == nil {
			return
		}
		readme := ccrawl.GenerateDatasetREADME(ccrawl.DatasetStats{
			CrawlID:         crawlID,
			CommittedShards: committedShards,
			TotalShards:     len(prefixes),
			TotalURLs:       totalURLs,
			TotalBytes:      totalBytes,
		})
		_ = os.WriteFile(readmePath, []byte(readme), 0o644)
	}

	for i, prefix := range prefixes {
		outPath := filepath.Join(d.outDir, fmt.Sprintf("hosts-%s.parquet", prefix))
		doneMarker := filepath.Join(d.workDir, fmt.Sprintf("shard-%s.done", prefix))

		if _, err := os.Stat(doneMarker); err == nil {
			logf("shard %s: already done, skipping", prefix)
			continue
		}

		rankPath := filepath.Join(d.workDir, fmt.Sprintf("rank-%s.tsv.gz", prefix))
		if _, err := os.Stat(rankPath); os.IsNotExist(err) {
			logf("shard %s: rank file missing, skipping", prefix)
			continue
		}

		logf("shard %s (%d/%d): building %s", prefix, i+1, len(prefixes), outPath)
		t0 := time.Now()

		n, err := ccrawl.BuildDatasetShard(ctx, prefix, d.workDir, crawlID, g.ID, outPath, func(n int64) {
			logf("  shard %s: %d rows written", prefix, n)
		})
		if err != nil {
			return fmt.Errorf("shard %s: %w", prefix, err)
		}

		elapsed := time.Since(t0)
		shardTimes = append(shardTimes, elapsed)
		totalURLs += n
		debug.FreeOSMemory() // return rank map memory to OS before next shard
		logf("shard %s done: %d rows in %s", prefix, n, elapsed.Round(time.Second))

		if len(shardTimes) >= 2 {
			var sum time.Duration
			for _, d := range shardTimes {
				sum += d
			}
			avg := sum / time.Duration(len(shardTimes))
			remaining := len(prefixes) - (i + 1)
			eta := time.Duration(remaining) * avg
			logf("estimate: avg %.0fs/shard, %d shards left, ETA ~%s (running %.0fs total)",
				avg.Seconds(), remaining, eta.Round(time.Minute), time.Since(shardStart).Seconds())
		}

		if hf != nil {
			committedShards++
			writeReadme()

			remotePath := ccrawl.HFShardPath(crawlID, "urls", prefix)
			msg := fmt.Sprintf("Add crawl=%s/subset=urls/prefix=%s (%d rows)", crawlID, prefix, n)
			logf("shard %s: committing to HF %s ...", prefix, remotePath)
			t1 := time.Now()

			ops := []ccrawl.HFOperation{
				{LocalPath: outPath, PathInRepo: remotePath},
				{LocalPath: readmePath, PathInRepo: "README.md"},
			}
			commitURL, err := hf.CommitWithRetry(ctx, d.hfRepo, msg, ops, 5)
			if err != nil {
				logf("WARNING: HF commit failed for shard %s: %v", prefix, err)
				committedShards-- // undo — marker not written
			} else {
				logf("shard %s: committed in %s — %s", prefix, time.Since(t1).Round(time.Second), commitURL)
				if removeErr := os.Remove(outPath); removeErr != nil {
					logf("WARNING: failed to remove local shard %s: %v", outPath, removeErr)
				}
				_ = os.WriteFile(doneMarker, []byte(fmt.Sprintf("rows=%d elapsed=%s\n", n, elapsed)), 0o644)
				continue
			}
		}

		_ = os.WriteFile(doneMarker, []byte(fmt.Sprintf("rows=%d elapsed=%s\n", n, elapsed)), 0o644)
	}

	logf("all shards done in %s", time.Since(runStart).Round(time.Second))
	return nil
}

// runBatched implements the incremental pipeline:
//
//  1. Rank download + split starts in a goroutine concurrently with CDX batch 1.
//  2. For each CDX batch (chunk 1..N):
//     a. ExtractCDXBatch → 28 per-prefix JSONL files for this chunk only.
//     b. Wait for rank goroutine to finish (usually done by end of chunk 1).
//     c. Build 28 Parquet shards (one prefix at a time: load rank, write, unload).
//     d. Commit all 28 shards + README in one HF commit.
//     e. Delete local JSONL + Parquet, write batch-{N}.done marker.
//
// First HF commit lands ~30-50 min in instead of ~5 hours.
func (d *hostDatasetCmd) runBatched(ctx context.Context, app *App, g ccrawl.WebGraph, crawlID string, hf *ccrawl.HFClient, logf func(string, ...any), runStart time.Time) error {
	urls, err := ccrawl.ColumnarParquetURLs(ctx, app.HTTP, app.Cache, crawlID, "warc", app.Cfg.Source)
	if err != nil {
		return fmt.Errorf("CDX parquet URLs: %w", err)
	}

	// Divide URLs into batches.
	bs := d.cdxBatchSize
	var batches [][]string
	for i := 0; i < len(urls); i += bs {
		end := i + bs
		if end > len(urls) {
			end = len(urls)
		}
		batches = append(batches, urls[i:end])
	}
	totalBatches := len(batches)
	logf("batched mode: %d CDX files → %d batches of ~%d, %d workers", len(urls), totalBatches, bs, d.cdxWorkers)

	// Start rank download + split in the background.
	rankReady := make(chan error, 1)
	rankDone := false
	rankMarker := filepath.Join(d.workDir, "rank.done")
	if _, err := os.Stat(rankMarker); err == nil {
		logf("rank: already done (marker found), skipping")
		rankDone = true
		rankReady <- nil
	} else if !d.skipRank {
		go func() {
			// If all per-prefix rank files already exist (from a previous run that was
			// killed after split but before rank.done was written), skip re-splitting.
			if allRankPrefixFilesExist(d.workDir) {
				logf("rank: all per-prefix rank files present, skipping re-split")
				_ = os.WriteFile(rankMarker, []byte(fmt.Sprintf("rows=skipped graph=%s\n", g.ID)), 0o644)
				rankReady <- nil
				return
			}
			rankCachePath := filepath.Join(d.workDir, "rank-table.tsv.gz")
			logf("rank: downloading in background (~5 GB, runs parallel to CDX batch 1)")
			if err := ccrawl.DownloadRankTable(ctx, g.HostRankURL(), rankCachePath); err != nil {
				rankReady <- fmt.Errorf("rank download: %w", err)
				return
			}
			logf("rank: download done, splitting")
			counts, err := ccrawl.SplitRankFromFile(ctx, rankCachePath, d.workDir, func(total int64) {
				logf("  rank rows written: %d M", total/1_000_000)
			})
			if err != nil {
				rankReady <- fmt.Errorf("rank split: %w", err)
				return
			}
			var total int64
			for _, c := range counts {
				total += c
			}
			logf("rank: split done (%d rows)", total)
			_ = os.WriteFile(rankMarker, []byte(fmt.Sprintf("rows=%d graph=%s\n", total, g.ID)), 0o644)
			rankReady <- nil
		}()
	} else {
		rankDone = true
		rankReady <- nil
	}

	readmePath := filepath.Join(d.workDir, "README.md")
	prefixes := ccrawl.DatasetPrefixes
	totalShards := totalBatches * len(prefixes)
	committedShards := 0

	// Count already-committed shards for accurate README on resume.
	for b := 1; b <= totalBatches; b++ {
		for _, p := range prefixes {
			if _, e := os.Stat(shardDoneMarker(d.workDir, b, p)); e == nil {
				committedShards++
			}
		}
	}

	writeReadme := func() {
		if hf == nil {
			return
		}
		readme := ccrawl.GenerateDatasetREADME(ccrawl.DatasetStats{
			CrawlID:         crawlID,
			CommittedShards: committedShards,
			TotalShards:     totalShards,
			TotalBatches:    totalBatches,
		})
		_ = os.WriteFile(readmePath, []byte(readme), 0o644)
	}

	for b, batchURLs := range batches {
		chunkNum := b + 1 // 1-based
		batchMarker := filepath.Join(d.workDir, fmt.Sprintf("batch-%03d.done", chunkNum))

		if _, err := os.Stat(batchMarker); err == nil {
			logf("batch %d/%d: already done, skipping", chunkNum, totalBatches)
			continue
		}

		// ── CDX extract for this batch ────────────────────────────────────────
		// Check whether all JSONL chunk files already exist (partial resume within batch).
		needExtract := false
		for _, p := range prefixes {
			if _, e := os.Stat(ccrawl.CDXBatchPath(d.workDir, p, chunkNum)); os.IsNotExist(e) {
				needExtract = true
				break
			}
		}
		if needExtract {
			logf("batch %d/%d: CDX extract — %d files", chunkNum, totalBatches, len(batchURLs))
			t0 := time.Now()
			if err := ccrawl.ExtractCDXBatch(ctx, app.HTTP, batchURLs, d.workDir, chunkNum, d.cdxWorkers,
				func(fileN, total int, rows int64) {
					logf("  batch %d: CDX file %d/%d done (%d rows)", chunkNum, fileN, total, rows)
				}); err != nil {
				return fmt.Errorf("batch %d CDX extract: %w", chunkNum, err)
			}
			logf("batch %d/%d: CDX done in %s", chunkNum, totalBatches, time.Since(t0).Round(time.Second))
		} else {
			logf("batch %d/%d: CDX JSONL already present, skipping extract", chunkNum, totalBatches)
		}

		// ── Wait for rank ─────────────────────────────────────────────────────
		if !rankDone {
			logf("batch %d/%d: waiting for rank goroutine...", chunkNum, totalBatches)
			if rankErr := <-rankReady; rankErr != nil {
				return rankErr
			}
			rankDone = true
		}

		// ── Build + commit each shard immediately ─────────────────────────────
		logf("batch %d/%d: building and committing shards one by one", chunkNum, totalBatches)
		var batchRows int64
		for _, prefix := range prefixes {
			shardMarker := shardDoneMarker(d.workDir, chunkNum, prefix)
			if _, err := os.Stat(shardMarker); err == nil {
				logf("  batch %d prefix %s: already committed, skipping", chunkNum, prefix)
				continue
			}

			outPath := filepath.Join(d.outDir, fmt.Sprintf("hosts-%s-chunk%03d.parquet", prefix, chunkNum))
			t0 := time.Now()
			n, err := ccrawl.BuildDatasetShardFromChunk(ctx, prefix, d.workDir, crawlID, g.ID, outPath, chunkNum, func(n int64) {
				logf("  batch %d prefix %s: %d rows written", chunkNum, prefix, n)
			})
			if err != nil {
				return fmt.Errorf("batch %d shard %s: %w", chunkNum, prefix, err)
			}
			debug.FreeOSMemory() // return rank map pages to OS before next prefix
			batchRows += n
			logf("  batch %d prefix %s: built %d rows in %s", chunkNum, prefix, n, time.Since(t0).Round(time.Second))

			if hf != nil {
				committedShards++
				writeReadme()
				remotePath := ccrawl.HFShardPathChunk(crawlID, "urls", prefix, chunkNum)
				msg := fmt.Sprintf("Add chunk=%03d/prefix=%s crawl=%s (%d rows)", chunkNum, prefix, crawlID, n)
				ops := []ccrawl.HFOperation{
					{LocalPath: outPath, PathInRepo: remotePath},
					{LocalPath: readmePath, PathInRepo: "README.md"},
				}
				t1 := time.Now()
				commitURL, commitErr := hf.CommitWithRetry(ctx, d.hfRepo, msg, ops, 5)
				if commitErr != nil {
					logf("  WARNING: batch %d prefix %s HF commit failed: %v", chunkNum, prefix, commitErr)
					committedShards--
				} else {
					logf("  batch %d prefix %s: committed in %s — %s", chunkNum, prefix, time.Since(t1).Round(time.Second), commitURL)
					_ = os.Remove(outPath)
					_ = os.WriteFile(shardMarker, []byte(fmt.Sprintf("rows=%d\n", n)), 0o644)
				}
			} else {
				_ = os.WriteFile(shardMarker, []byte(fmt.Sprintf("rows=%d\n", n)), 0o644)
			}
		}

		// ── Cleanup JSONL chunk files, write batch marker ─────────────────────
		for _, p := range prefixes {
			_ = os.Remove(ccrawl.CDXBatchPath(d.workDir, p, chunkNum))
		}
		_ = os.WriteFile(batchMarker, []byte(fmt.Sprintf("rows=%d\n", batchRows)), 0o644)
		logf("batch %d/%d: done — %d total rows", chunkNum, totalBatches, batchRows)
	}

	logf("all batches done in %s", time.Since(runStart).Round(time.Second))
	return nil
}

// allRankPrefixFilesExist returns true if all 28 per-prefix rank split
// completion markers (rank-*.tsv.gz.done) are present — indicating a completed
// rank split from a prior run. Using markers avoids false-positives from partial
// gzip files left behind by a process that was killed mid-split.
func allRankPrefixFilesExist(workDir string) bool {
	for _, p := range ccrawl.DatasetPrefixes {
		marker := fmt.Sprintf("%s/rank-%s.tsv.gz.done", workDir, p)
		if _, err := os.Stat(marker); err != nil {
			return false
		}
	}
	return true
}

// shardDoneMarker returns the path to a per-shard done marker file.
// It is written after a shard is built and committed, enabling resume
// within a batch without rebuilding already-committed shards.
func shardDoneMarker(workDir string, chunk int, prefix string) string {
	return fmt.Sprintf("%s/shard-chunk%03d-%s.done", workDir, chunk, prefix)
}

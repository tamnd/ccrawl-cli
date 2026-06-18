package cli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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
	graph      string
	workDir    string
	outDir     string
	prefix     string
	noCDX      bool
	cdxAgg     bool
	upload     bool
	hfRepo     string
	skipCDX    bool
	skipRank   bool
	cdxWorkers int
	cdxLimit   int
}

func (d *hostDatasetCmd) flags(f *kit.FlagSet) {
	f.StringVar(&d.graph, "graph", "", "web-graph release ID (default: latest)")
	f.StringVar(&d.workDir, "work-dir", filepath.Join(os.Getenv("HOME"), ".ccrawl", "dataset"), "directory for intermediate prefix files")
	f.StringVar(&d.outDir, "out-dir", ".", "directory for output Parquet shards")
	f.StringVar(&d.prefix, "prefix", "", "process only this prefix (a-z, 0, misc); empty = all")
	f.BoolVar(&d.noCDX, "no-cdx", false, "skip CDX enrichment (rank signals only)")
	f.BoolVar(&d.cdxAgg, "cdx-agg", false, "also write cdx-agg-*.jsonl.gz per-host summary files after raw extract")
	f.BoolVar(&d.upload, "upload", false, "upload each shard to HuggingFace after building")
	f.StringVar(&d.hfRepo, "hf-repo", "open-index/cc-host-dataset", "HuggingFace dataset repository")
	f.BoolVar(&d.skipCDX, "skip-cdx-raw", false, "skip CDX raw extract phase (assume cdx-raw-*.jsonl.gz present)")
	f.BoolVar(&d.skipRank, "skip-rank-split", false, "skip rank-split phase (assume rank-*.tsv.gz present)")
	f.IntVar(&d.cdxWorkers, "cdx-workers", 8, "concurrent CDX Parquet download workers (lower if CC returns 429/403)")
	f.IntVar(&d.cdxLimit, "cdx-limit", 0, "stop after N CDX files (0=all; for benchmarking only)")
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
	// Downloading to disk with curl's --continue-at means interrupted downloads
	// resume from where they left off rather than restarting from zero.
	if !d.skipRank {
		markerPath := filepath.Join(d.workDir, "rank.done")
		rankCachePath := filepath.Join(d.workDir, "rank-table.tsv.gz")
		if _, err := os.Stat(markerPath); os.IsNotExist(err) {
			// Step 2a: download with resume support.
			logf("phase 2a: downloading rank table to %s (resumes if interrupted)", rankCachePath)
			t0 := time.Now()
			if err := ccrawl.DownloadRankTable(ctx, g.HostRankURL(), rankCachePath); err != nil {
				return fmt.Errorf("phase 2a rank download: %w", err)
			}
			logf("phase 2a done in %s", time.Since(t0).Round(time.Second))

			// Step 2b: split from local file.
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
			// Keep the local cache — it's needed if shards are re-built later.
		} else {
			logf("phase 2: rank split already done (marker found), skipping")
		}
	}

	// ── Phase 3: Per-prefix shards ────────────────────────────────────────────
	prefixes := ccrawl.DatasetPrefixes
	if d.prefix != "" {
		prefixes = []string{d.prefix}
	}

	var shardTimes []time.Duration
	shardStart := time.Now()

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
		logf("shard %s done: %d rows in %s", prefix, n, elapsed.Round(time.Second))

		// Estimate remaining time after at least 2 shards
		if len(shardTimes) >= 2 {
			var sum time.Duration
			for _, d := range shardTimes {
				sum += d
			}
			avg := sum / time.Duration(len(shardTimes))
			remaining := len(prefixes) - (i + 1)
			eta := time.Duration(remaining) * avg
			totalElapsed := time.Since(shardStart)
			logf("estimate: avg %.0fs/shard, %d shards left, ETA ~%s (running %.0fs total)",
				avg.Seconds(), remaining, eta.Round(time.Minute), totalElapsed.Seconds())
		}

		// Upload to HuggingFace
		if d.upload {
			if err := hfUpload(ctx, d.hfRepo, outPath, fmt.Sprintf("data/train/hosts-%s.parquet", prefix)); err != nil {
				logf("WARNING: upload failed for %s: %v", prefix, err)
			} else {
				logf("shard %s: uploaded to %s", prefix, d.hfRepo)
				if err := os.Remove(outPath); err != nil {
					logf("WARNING: failed to remove %s: %v", outPath, err)
				}
			}
		}

		_ = os.WriteFile(doneMarker, []byte(fmt.Sprintf("rows=%d elapsed=%s\n", n, elapsed)), 0o644)
	}

	logf("all shards done in %s", time.Since(runStart).Round(time.Second))
	return nil
}

// hfUpload uploads a local file to a HuggingFace dataset repository.
// Requires huggingface-cli on PATH.
func hfUpload(ctx context.Context, repo, localPath, remotePath string) error {
	_, err := exec.LookPath("huggingface-cli")
	if err != nil {
		return fmt.Errorf("huggingface-cli not found on PATH")
	}
	cmd := exec.CommandContext(ctx, "huggingface-cli", "upload",
		"--repo-type", "dataset",
		repo, localPath, remotePath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("huggingface-cli upload: %w", err)
	}
	return nil
}


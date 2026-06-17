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
	graph   string
	workDir string
	outDir  string
	prefix  string
	noCDX   bool
	upload  bool
	hfRepo  string
	skipCDX bool
	skipRank bool
}

func (d *hostDatasetCmd) flags(f *kit.FlagSet) {
	f.StringVar(&d.graph, "graph", "", "web-graph release ID (default: latest)")
	f.StringVar(&d.workDir, "work-dir", filepath.Join(os.Getenv("HOME"), ".ccrawl", "dataset"), "directory for intermediate prefix files")
	f.StringVar(&d.outDir, "out-dir", ".", "directory for output Parquet shards")
	f.StringVar(&d.prefix, "prefix", "", "process only this prefix (a-z, 0, misc); empty = all")
	f.BoolVar(&d.noCDX, "no-cdx", false, "skip CDX enrichment (rank signals only)")
	f.BoolVar(&d.upload, "upload", false, "upload each shard to HuggingFace after building")
	f.StringVar(&d.hfRepo, "hf-repo", "open-index/cc-host-dataset", "HuggingFace dataset repository")
	f.BoolVar(&d.skipCDX, "skip-cdx-prep", false, "skip CDX phase (assume cdx-*.jsonl.gz already present)")
	f.BoolVar(&d.skipRank, "skip-rank-split", false, "skip rank-split phase (assume rank-*.tsv.gz already present)")
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

	// ── Phase 1: CDX prep ────────────────────────────────────────────────────
	// cdx-{prefix}.jsonl.gz files are the per-prefix markers; SaveCDXSplitByPrefix
	// skips any file that already exists, so re-running is always safe.
	if !d.noCDX && !d.skipCDX {
		if !ccrawl.DuckDBAvailable() {
			return fmt.Errorf("phase 1 requires duckdb on PATH; use --no-cdx to skip")
		}
		{
			cdxPrefixes := ccrawl.DatasetPrefixes
			if d.prefix != "" {
				cdxPrefixes = []string{d.prefix}
			}
			logf("phase 1: CDX prep — %d prefix(es) via DuckDB (~184 GB Parquet total, ~1/26 per prefix)", len(cdxPrefixes))
			t0 := time.Now()
			urls, err := ccrawl.ColumnarParquetURLs(ctx, app.HTTP, app.Cache, crawlID, "warc", app.Cfg.Source)
			if err != nil {
				return fmt.Errorf("phase 1 parquet URLs: %w", err)
			}
			var counts map[string]int64
			counts, err = ccrawl.SaveCDXSplitByPrefix(ctx, urls, crawlID, d.workDir, cdxPrefixes, func(prefix string, total int64) {
				logf("  CDX prefix %q done (%d total rows so far)", prefix, total)
				_ = counts // reference to avoid unused warning before assignment
			})
			if err != nil {
				return fmt.Errorf("phase 1 CDX prep: %w", err)
			}
			var total int64
			for _, c := range counts {
				total += c
			}
			logf("phase 1 done: %d CDX rows in %s", total, time.Since(t0).Round(time.Second))
		}
	}

	// ── Phase 2: Rank split ───────────────────────────────────────────────────
	if !d.skipRank {
		markerPath := filepath.Join(d.workDir, "rank.done")
		if _, err := os.Stat(markerPath); os.IsNotExist(err) {
			logf("phase 2: rank split — streaming rank table (%s)", g.HostRankURL())
			t0 := time.Now()
			counts, err := ccrawl.SplitRankByPrefix(ctx, app.HTTP, g.HostRankURL(), d.workDir, func(total int64) {
				logf("  rank rows written: %d M", total/1_000_000)
			})
			if err != nil {
				return fmt.Errorf("phase 2 rank split: %w", err)
			}
			var total int64
			for _, c := range counts {
				total += c
			}
			logf("phase 2 done: %d rank rows in %s", total, time.Since(t0).Round(time.Second))
			_ = os.WriteFile(markerPath, []byte(fmt.Sprintf("rows=%d graph=%s\n", total, g.ID)), 0o644)
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


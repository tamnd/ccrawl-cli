package ccrawl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/seed"
)

// seedURLRow is the columnar schema a published shard carries: the URL and the
// host it was sharded on. The host is derived from the URL the same way meguri
// derives it on ingest, so a reader can filter by host without parsing and a
// rebuild keys every URL back into the shard it came from.
type seedURLRow struct {
	URL  string `parquet:"url"`
	Host string `parquet:"host,dict"`
}

// HFSeedShardPath returns the HF repo path for one published seed shard. All
// shards for a crawl land under data/crawl=CC-MAIN-YYYY-WW/ so HF's partition
// tooling can filter by crawl without a full scan.
func HFSeedShardPath(crawlID string, shardIdx int) string {
	return fmt.Sprintf("data/crawl=%s/shard-%05d.parquet", crawlID, shardIdx)
}

// SeedPublishConfig drives RunSeedPublish: which .seed directory to read, where
// to stage Parquet, and which HF dataset repo to push to.
type SeedPublishConfig struct {
	SeedDir string // the .seed directory a `ccrawl seed cc` run wrote
	CrawlID string // crawl label for the Hive partition (data/crawl=<id>/)
	Subset  string // index subset the seed came from, for the card
	OutDir  string // staging dir for the transcoded Parquet files
	Repo    string // HF dataset repo, org/name
	Push    bool   // when false, transcode locally and skip the upload

	// ShardParallel is how many shards transcode at once. Transcoding is CPU and
	// disk bound (decode a seed block, recompress as Parquet), so this scales
	// with cores. 0 picks a small default.
	ShardParallel int
	// CommitBatch is how many finished Parquet shards go into one HF commit. The
	// committer runs off the transcode critical path, so batching keeps commit
	// round trips from dominating a long run. 0 means 1.
	CommitBatch int
	// KeepParquet leaves the staged Parquet in place after a commit. The default
	// deletes it once it is safe on HF, which is what lets a full seed publish
	// from a box that cannot hold the whole Parquet copy at once.
	KeepParquet bool
	// MinFreeBytes pauses new transcodes while free disk is below this. 0 selects
	// 2 GiB.
	MinFreeBytes int64
	// Ledger, when set, skips already-published shards and records new ones, so a
	// killed run resumes where it stopped.
	Ledger *Ledger

	// Progress is called once per committed batch with a snapshot of the run.
	Progress func(SeedPublishStats)
}

// SeedPublishStats is a live snapshot of a publish run.
type SeedPublishStats struct {
	Total         int   // shards requested this run
	Skipped       int   // shards skipped via the ledger
	Published     int   // shards committed so far
	Failed        int   // shards that errored
	Rows          int64 // cumulative URLs across published shards
	URLBytes      int64 // cumulative uncompressed URL bytes
	ParquetBytes  int64 // cumulative Parquet bytes
	TranscodeS    int64 // cumulative per-shard transcode wall-clock, seconds
	PublishS      int64 // cumulative HF commit wall-clock, seconds
	Elapsed       time.Duration
	ShardsPerHour float64
	ETA           time.Duration
	FreeDiskBytes int64
}

// seedShardResult is one shard's outcome, handed from a transcode worker to the
// committer.
type seedShardResult struct {
	idx      int
	path     string
	rows     int64
	urlBytes int64
	pqBytes  int64
	dur      time.Duration
	err      error
}

// RunSeedPublish transcodes every shard of a .seed directory into partitioned
// Parquet and commits the files to a HuggingFace dataset repo in batches.
//
// Layout mirrors the markdown export:
//
//	[P transcode workers] decode one seed shard, write url+host Parquet →
//	    finished channel → [1 committer] batches K shards per HF commit, pushes
//	    the shard map once and a fresh dataset card each batch, deletes the local
//	    Parquet, records the ledger, logs an ETA.
//
// Moving the commit onto its own goroutine keeps the HF round trip off the
// per-shard critical path, so a full-seed publish is bounded by transcode plus
// upload bandwidth, not by their sum.
func RunSeedPublish(ctx context.Context, hf *HFClient, cfg SeedPublishConfig) (SeedPublishStats, error) {
	man, err := seed.ReadManifest(cfg.SeedDir)
	if err != nil {
		return SeedPublishStats{}, fmt.Errorf("read seed manifest: %w", err)
	}
	if len(man.Shards) == 0 {
		return SeedPublishStats{}, fmt.Errorf("seed %s has no shards", cfg.SeedDir)
	}

	p := cfg.ShardParallel
	if p <= 0 {
		p = 4
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
		return SeedPublishStats{}, err
	}

	jobs := make(chan seed.ShardMeta)
	finished := make(chan seedShardResult, p+k)
	start := time.Now()
	run := SeedPublishStats{Total: len(man.Shards)}

	committerDone := make(chan struct{})
	var commitErr error
	go func() {
		defer close(committerDone)
		commitErr = runSeedCommitter(ctx, hf, cfg, man, k, start, finished, &run)
	}()

	var wg sync.WaitGroup
	wg.Add(p)
	for range p {
		go func() {
			defer wg.Done()
			for sm := range jobs {
				if ctx.Err() != nil {
					return
				}
				waitForDisk(ctx, cfg.OutDir, minFree)
				outPath := filepath.Join(cfg.OutDir, fmt.Sprintf("shard-%05d.parquet", sm.Index))
				seedPath := filepath.Join(cfg.SeedDir, sm.Path)
				t0 := time.Now()
				rows, urlBytes, terr := transcodeSeedShard(seedPath, outPath)
				if terr != nil {
					_ = os.Remove(outPath)
					finished <- seedShardResult{idx: sm.Index, err: terr}
					continue
				}
				var pqBytes int64
				if fi, serr := os.Stat(outPath); serr == nil {
					pqBytes = fi.Size()
				}
				finished <- seedShardResult{
					idx: sm.Index, path: outPath,
					rows: rows, urlBytes: urlBytes, pqBytes: pqBytes,
					dur: time.Since(t0),
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, sm := range man.Shards {
			if cfg.Ledger != nil && cfg.Ledger.Has(sm.Index) {
				finished <- seedShardResult{idx: sm.Index, err: errAlreadyDone}
				continue
			}
			select {
			case <-ctx.Done():
				return
			case jobs <- sm:
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

// transcodeSeedShard reads one .seed shard and writes its URLs as a Parquet file
// with a url and a derived host column. It returns the row count and the
// uncompressed URL byte total. URLs are written in batches so the generic
// writer's per-call overhead does not dominate a shard of millions of rows.
func transcodeSeedShard(seedPath, outPath string) (rows, urlBytes int64, err error) {
	r, err := seed.Open(seedPath)
	if err != nil {
		return 0, 0, fmt.Errorf("open seed shard: %w", err)
	}
	defer func() { _ = r.Close() }()

	w, err := NewParquetWriter[seedURLRow](outPath)
	if err != nil {
		return 0, 0, err
	}
	// Close the writer on every path; a transcode error still flushes what was
	// read so the failure is the transcode, not a half-open file.
	defer func() {
		if cerr := w.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	const batchSize = 8192
	buf := make([]seedURLRow, 0, batchSize)
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if werr := w.WriteRows(buf); werr != nil {
			return werr
		}
		buf = buf[:0]
		return nil
	}

	for i := 0; i < r.Blocks(); i++ {
		br, berr := r.BlockReader(i)
		if berr != nil {
			return rows, urlBytes, fmt.Errorf("block %d: %w", i, berr)
		}
		for {
			u, ok := br.Next()
			if !ok {
				break
			}
			// Next's bytes alias the block body, so the string copy that string()
			// makes is what keeps the URL past the next call.
			url := string(u)
			buf = append(buf, seedURLRow{URL: url, Host: frontier.HostOf(url)})
			rows++
			urlBytes += int64(len(u))
			if len(buf) >= batchSize {
				if ferr := flush(); ferr != nil {
					return rows, urlBytes, ferr
				}
			}
		}
	}
	if ferr := flush(); ferr != nil {
		return rows, urlBytes, ferr
	}
	return rows, urlBytes, nil
}

// runSeedCommitter drains transcoded shards, batches K per HF commit, pushes the
// shard map once and a refreshed dataset card each batch, deletes the local
// Parquet, records the ledger, and reports progress with an ETA.
func runSeedCommitter(ctx context.Context, hf *HFClient, cfg SeedPublishConfig, man seed.Manifest, k int, start time.Time, finished <-chan seedShardResult, run *SeedPublishStats) error {
	manifestPushed := false
	var batch []seedShardResult

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		ops := make([]HFOperation, 0, len(batch)+2)
		for _, r := range batch {
			ops = append(ops, HFOperation{
				LocalPath:  r.path,
				PathInRepo: HFSeedShardPath(cfg.CrawlID, r.idx),
			})
		}

		for _, r := range batch {
			run.Rows += r.rows
			run.URLBytes += r.urlBytes
			run.ParquetBytes += r.pqBytes
			run.TranscodeS += int64(r.dur.Seconds())
		}

		var tmpFiles []string
		defer func() {
			for _, f := range tmpFiles {
				_ = os.Remove(f)
			}
		}()

		if cfg.Push {
			// Push the shard map once so a puller can reconstruct the exact
			// frontier partitions, then a fresh card every batch so the counts on
			// HF track what has actually landed.
			if !manifestPushed {
				manPath := filepath.Join(cfg.SeedDir, seed.ManifestName)
				if _, serr := os.Stat(manPath); serr == nil {
					ops = append(ops, HFOperation{LocalPath: manPath, PathInRepo: seed.ManifestName})
				}
			}
			card, cerr := writeTempSeedREADME(SeedDatasetStats{
				Repo:            cfg.Repo,
				CrawlID:         cfg.CrawlID,
				Subset:          cfg.Subset,
				PublishedShards: run.Published + len(batch),
				TotalShards:     len(man.Shards),
				Rows:            run.Rows,
				URLBytes:        run.URLBytes,
				ParquetBytes:    run.ParquetBytes,
			})
			if cerr != nil {
				return cerr
			}
			tmpFiles = append(tmpFiles, card)
			ops = append(ops, HFOperation{LocalPath: card, PathInRepo: "README.md"})

			lo, hi := batch[0].idx, batch[len(batch)-1].idx
			msg := fmt.Sprintf("Add %s URL shards %05d-%05d (%d files)", cfg.CrawlID, lo, hi, len(batch))
			tPush := time.Now()
			if _, err := hf.CommitWithRetry(ctx, cfg.Repo, msg, ops, 5); err != nil {
				return fmt.Errorf("commit shards %05d-%05d: %w", lo, hi, err)
			}
			run.PublishS += int64(time.Since(tPush).Seconds())
			manifestPushed = true
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
		run.Published += len(batch)
		batch = batch[:0]

		updateSeedRates(run, start)
		logSeedProgress(cfg, run)
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
			fmt.Fprintf(os.Stderr, "seed publish: shard %05d failed: %v\n", r.idx, r.err)
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

// updateSeedRates recomputes the throughput and ETA from published progress.
func updateSeedRates(run *SeedPublishStats, start time.Time) {
	elapsed := time.Since(start)
	run.Elapsed = elapsed
	if elapsed > 0 && run.Published > 0 {
		run.ShardsPerHour = float64(run.Published) / elapsed.Hours()
		remaining := run.Total - run.Skipped - run.Published - run.Failed
		if remaining > 0 && run.ShardsPerHour > 0 {
			run.ETA = time.Duration(float64(remaining) / run.ShardsPerHour * float64(time.Hour))
		} else {
			run.ETA = 0
		}
	}
}

// logSeedProgress prints a one-line status with throughput, ETA, and free disk.
func logSeedProgress(cfg SeedPublishConfig, run *SeedPublishStats) {
	done := run.Published + run.Skipped + run.Failed
	pct := 0.0
	if run.Total > 0 {
		pct = float64(done) / float64(run.Total) * 100
	}
	run.FreeDiskBytes = freeDiskBytes(cfg.OutDir)
	fmt.Fprintf(os.Stderr,
		"seed publish: %d/%d shards (%.1f%%) | %s URLs | %.1f shards/hour | ETA %s | disk free %s\n",
		done, run.Total, pct, fmtInt(run.Rows),
		run.ShardsPerHour, fmtETA(run.ETA), fmtBytes(run.FreeDiskBytes))
}

// writeTempSeedREADME renders the URL-seed dataset card to a temp file.
func writeTempSeedREADME(s SeedDatasetStats) (string, error) {
	f, err := os.CreateTemp("", "cc-url-seed-readme-*.md")
	if err != nil {
		return "", err
	}
	if _, err := f.WriteString(GenerateSeedREADME(s)); err != nil {
		_ = f.Close()
		return "", err
	}
	return f.Name(), f.Close()
}

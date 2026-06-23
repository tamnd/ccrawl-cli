package ccrawl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLedgerPersistsAndResumes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".committed")

	l, err := OpenLedger(path)
	if err != nil {
		t.Fatalf("OpenLedger: %v", err)
	}
	if l.Count() != 0 {
		t.Fatalf("fresh ledger Count = %d, want 0", l.Count())
	}
	for _, idx := range []int{0, 5, 5, 42} { // 5 twice on purpose
		if err := l.Mark(idx); err != nil {
			t.Fatalf("Mark(%d): %v", idx, err)
		}
	}
	if l.Count() != 3 {
		t.Fatalf("Count after marks = %d, want 3 (dedup)", l.Count())
	}
	if !l.Has(5) || l.Has(7) {
		t.Fatalf("Has wrong: Has(5)=%v Has(7)=%v", l.Has(5), l.Has(7))
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: the marks must survive.
	l2, err := OpenLedger(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if l2.Count() != 3 {
		t.Fatalf("reopened Count = %d, want 3", l2.Count())
	}
	for _, idx := range []int{0, 5, 42} {
		if !l2.Has(idx) {
			t.Fatalf("reopened ledger missing shard %d", idx)
		}
	}
}

// stubPack replaces the real shard packer for the duration of a test.
func stubPack(t *testing.T, fn func(ctx context.Context, h *HTTPClient, cfg MarkdownPackConfig) (MarkdownStats, error)) {
	t.Helper()
	prev := packShardFn
	packShardFn = fn
	t.Cleanup(func() { packShardFn = prev })
}

func TestRunMarkdownExportConvertsAndDeletes(t *testing.T) {
	dir := t.TempDir()

	var packed int64
	stubPack(t, func(ctx context.Context, h *HTTPClient, cfg MarkdownPackConfig) (MarkdownStats, error) {
		atomic.AddInt64(&packed, 1)
		// Write a placeholder parquet so the delete-after-commit path has a file.
		if err := os.WriteFile(cfg.OutPath, []byte("parquet"), 0o644); err != nil {
			return MarkdownStats{}, err
		}
		return MarkdownStats{ShardIdx: cfg.ShardIdx, Rows: 10, HTMLBytes: 100, MDBytes: 30, ParquetBytes: 7}, nil
	})

	ledger, err := OpenLedger(filepath.Join(dir, ".committed"))
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()

	indices := []int{0, 1, 2, 3, 4}
	paths := make([]string, 5)
	for i := range paths {
		paths[i] = fmt.Sprintf("crawl-data/x/%d.warc.gz", i)
	}

	run, err := RunMarkdownExport(context.Background(), nil, nil, MarkdownExportConfig{
		CrawlID:        "CC-MAIN-2026-25",
		Indices:        indices,
		WARCPaths:      paths,
		OutDir:         dir,
		Push:           false, // no HF; committer just records the ledger + deletes
		ShardParallel:  3,
		ConvertWorkers: 2,
		CommitBatch:    2,
		Ledger:         ledger,
	})
	if err != nil {
		t.Fatalf("RunMarkdownExport: %v", err)
	}

	if packed != 5 {
		t.Fatalf("packed %d shards, want 5", packed)
	}
	if run.Committed != 5 {
		t.Fatalf("run.Committed = %d, want 5", run.Committed)
	}
	if run.Rows != 50 {
		t.Fatalf("run.Rows = %d, want 50", run.Rows)
	}
	if ledger.Count() != 5 {
		t.Fatalf("ledger.Count = %d, want 5", ledger.Count())
	}
	// Parquet files must be gone (delete-after-commit is the default).
	for _, idx := range indices {
		p := filepath.Join(dir, fmt.Sprintf("%06d.parquet", idx))
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("parquet %s should have been deleted", p)
		}
	}
}

func TestRunMarkdownExportSkipsLedgered(t *testing.T) {
	dir := t.TempDir()

	var packed int64
	stubPack(t, func(ctx context.Context, h *HTTPClient, cfg MarkdownPackConfig) (MarkdownStats, error) {
		atomic.AddInt64(&packed, 1)
		os.WriteFile(cfg.OutPath, []byte("p"), 0o644)
		return MarkdownStats{ShardIdx: cfg.ShardIdx, Rows: 1}, nil
	})

	ledger, err := OpenLedger(filepath.Join(dir, ".committed"))
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	// Pretend shards 0 and 2 were committed by an earlier run.
	ledger.Mark(0)
	ledger.Mark(2)

	paths := make([]string, 4)
	run, err := RunMarkdownExport(context.Background(), nil, nil, MarkdownExportConfig{
		CrawlID:       "CC-MAIN-2026-25",
		Indices:       []int{0, 1, 2, 3},
		WARCPaths:     paths,
		OutDir:        dir,
		Push:          false,
		ShardParallel: 2,
		CommitBatch:   1,
		Ledger:        ledger,
	})
	if err != nil {
		t.Fatalf("RunMarkdownExport: %v", err)
	}
	if packed != 2 {
		t.Fatalf("packed %d shards, want 2 (0 and 2 skipped)", packed)
	}
	if run.Skipped != 2 || run.Committed != 2 {
		t.Fatalf("Skipped=%d Committed=%d, want 2 and 2", run.Skipped, run.Committed)
	}
}

func TestRunMarkdownExportCountsFailures(t *testing.T) {
	dir := t.TempDir()

	stubPack(t, func(ctx context.Context, h *HTTPClient, cfg MarkdownPackConfig) (MarkdownStats, error) {
		if cfg.ShardIdx%2 == 1 {
			return MarkdownStats{}, fmt.Errorf("boom on shard %d", cfg.ShardIdx)
		}
		os.WriteFile(cfg.OutPath, []byte("p"), 0o644)
		return MarkdownStats{ShardIdx: cfg.ShardIdx, Rows: 1}, nil
	})

	run, err := RunMarkdownExport(context.Background(), nil, nil, MarkdownExportConfig{
		CrawlID:       "CC-MAIN-2026-25",
		Indices:       []int{0, 1, 2, 3},
		WARCPaths:     make([]string, 4),
		OutDir:        dir,
		Push:          false,
		ShardParallel: 2,
		CommitBatch:   1,
	})
	if err != nil {
		t.Fatalf("RunMarkdownExport: %v", err)
	}
	if run.Failed != 2 || run.Committed != 2 {
		t.Fatalf("Failed=%d Committed=%d, want 2 and 2", run.Failed, run.Committed)
	}
}

func TestRunMarkdownExportSharedConvertCap(t *testing.T) {
	dir := t.TempDir()

	var inFlight, peak int64
	var mu sync.Mutex
	stubPack(t, func(ctx context.Context, h *HTTPClient, cfg MarkdownPackConfig) (MarkdownStats, error) {
		// The orchestrator hands every shard the same convert semaphore. Acquire a
		// slot here to model one in-progress conversion and assert the cap holds.
		cfg.ConvertSem <- struct{}{}
		n := atomic.AddInt64(&inFlight, 1)
		mu.Lock()
		if n > peak {
			peak = n
		}
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt64(&inFlight, -1)
		<-cfg.ConvertSem

		os.WriteFile(cfg.OutPath, []byte("p"), 0o644)
		return MarkdownStats{ShardIdx: cfg.ShardIdx, Rows: 1}, nil
	})

	run, err := RunMarkdownExport(context.Background(), nil, nil, MarkdownExportConfig{
		CrawlID:        "CC-MAIN-2026-25",
		Indices:        []int{0, 1, 2, 3, 4, 5, 6, 7},
		WARCPaths:      make([]string, 8),
		OutDir:         dir,
		Push:           false,
		ShardParallel:  8, // 8 shards want to convert at once
		ConvertWorkers: 2, // but the shared cap is 2
		CommitBatch:    4,
	})
	if err != nil {
		t.Fatalf("RunMarkdownExport: %v", err)
	}
	if run.Committed != 8 {
		t.Fatalf("Committed=%d, want 8", run.Committed)
	}
	if peak > 2 {
		t.Fatalf("peak concurrent conversions = %d, want <= 2 (shared cap)", peak)
	}
}

func TestFmtETA(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{0, "n/a"},
		{-time.Hour, "n/a"},
		{30 * time.Minute, "30m"},
		{90 * time.Minute, "1h 30m"},
		{50 * time.Hour, "2d 2h"},
	}
	for _, c := range cases {
		if got := fmtETA(c.d); got != c.want {
			t.Errorf("fmtETA(%s) = %q, want %q", c.d, got, c.want)
		}
	}
}

func TestUpdateRunRates(t *testing.T) {
	run := &MarkdownRunStats{Total: 100, Committed: 10}
	// Start an hour ago so 10 committed implies 10 shards/hour and a 9h ETA.
	start := time.Now().Add(-time.Hour)
	updateRunRates(run, start)
	if run.ShardsPerHour < 9.5 || run.ShardsPerHour > 10.5 {
		t.Fatalf("ShardsPerHour = %.2f, want ~10", run.ShardsPerHour)
	}
	// 90 remaining at 10/hour ≈ 9h.
	if h := run.ETA.Hours(); h < 8.5 || h > 9.5 {
		t.Fatalf("ETA = %.2fh, want ~9h", h)
	}
}

package ccrawl

import (
	"context"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"
)

// TestPackShardThroughput measures the full extract+encode pipeline (WARC gzip
// decompress, h2m HTML->Markdown, zstd parquet write) over a local WARC shard,
// with no network. It is the reproducible before/after harness for the export
// throughput work: the number that matters on a saturated box is rows/s.
//
// It is skipped unless CCRAWL_WARC points at a local .warc.gz file. Knobs:
//
//	CCRAWL_WARC      path to a local .warc.gz shard (required)
//	CCRAWL_WORKERS   convert workers / convert-sem size (default runtime.NumCPU)
//	CCRAWL_CPUPROFILE  write a CPU profile here while packing
//
//	CCRAWL_WARC=~/data/shard.warc.gz CCRAWL_WORKERS=6 \
//	  go test ./ccrawl -run TestPackShardThroughput -v -timeout 30m
func TestPackShardThroughput(t *testing.T) {
	path := os.Getenv("CCRAWL_WARC")
	if path == "" {
		t.Skip("set CCRAWL_WARC to a local .warc.gz file to run the pack throughput test")
	}

	workers := 0
	if v := os.Getenv("CCRAWL_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			workers = n
		}
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open WARC: %v", err)
	}
	defer func() { _ = f.Close() }()
	fi, _ := f.Stat()

	out := filepath.Join(t.TempDir(), "shard.parquet")
	sem := make(chan struct{}, max(workers, 1))
	if workers == 0 {
		sem = nil // packStream sizes its own workers; leave the sem ungated
	}

	if pp := os.Getenv("CCRAWL_CPUPROFILE"); pp != "" {
		pf, err := os.Create(pp)
		if err != nil {
			t.Fatalf("create cpuprofile: %v", err)
		}
		defer func() { _ = pf.Close() }()
		if err := pprof.StartCPUProfile(pf); err != nil {
			t.Fatalf("start cpuprofile: %v", err)
		}
		defer pprof.StopCPUProfile()
	}

	cfg := MarkdownPackConfig{
		CrawlID:    "CC-MAIN-bench",
		ShardIdx:   0,
		OutPath:    out,
		Workers:    workers,
		ConvertSem: sem,
	}

	t0 := time.Now()
	stats, err := packStream(context.Background(), f, cfg, MarkdownStats{}, t0)
	if err != nil {
		t.Fatalf("packStream: %v", err)
	}
	elapsed := time.Since(t0)

	rowsPerSec := float64(stats.Rows) / elapsed.Seconds()
	htmlMBPerSec := float64(stats.HTMLBytes) / 1e6 / elapsed.Seconds()
	t.Logf("WARC: %s (%.0f MB compressed)", path, float64(fi.Size())/1e6)
	if workers == 0 {
		workers = -1
	}
	t.Logf("workers=%d rows=%d elapsed=%s", workers, stats.Rows, elapsed.Round(time.Millisecond))
	t.Logf("throughput: %.1f rows/s | %.1f HTML MB/s in | parquet %.1f MB (%.1fx vs html)",
		rowsPerSec, htmlMBPerSec, float64(stats.ParquetBytes)/1e6,
		ratioOf(stats.HTMLBytes, stats.ParquetBytes))
}

func ratioOf(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

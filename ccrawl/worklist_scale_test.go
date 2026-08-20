package ccrawl

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// TestWorkListMemoryOnRealDataset streams the published domain list and watches
// what the process holds while it does.
//
// This is the measurement the whole streaming design was argued from, so it is
// worth taking against the real thing rather than a generated one. The frontier
// it replaces costs about 135 bytes a URL and its admit rate falls fourfold
// between two million and five million rows, so five million is the size at
// which the two approaches visibly part company. A work list that holds one part
// and one fixed buffer should read five million rows in the same resident set it
// read the first thousand in.
//
// Off by default because it pulls real parts over the network.
//
//	CCRAWL_WORKLIST_ROWS=5000000 go test ./ccrawl -run TestWorkListMemoryOnRealDataset -v -timeout 1h
func TestWorkListMemoryOnRealDataset(t *testing.T) {
	raw := os.Getenv("CCRAWL_WORKLIST_ROWS")
	if raw == "" {
		t.Skip("set CCRAWL_WORKLIST_ROWS to stream the published domain list and measure what it holds")
	}
	rows, err := strconv.Atoi(raw)
	if err != nil || rows <= 0 {
		t.Fatalf("CCRAWL_WORKLIST_ROWS=%q is not a positive count", raw)
	}

	src := WorkSource{Repo: "open-index/ccrawl-domains", Dir: "data/cc-main-2026-apr-may-jun", Column: "domain"}
	if d := os.Getenv("CCRAWL_WORKLIST_DIR"); d != "" {
		src.Dir = d
	}
	cfg := DefaultConfig()
	w, err := NewWorkList(src, Shard{Count: 1}, NewHTTPClient(cfg), Checkpoint{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	buf := make([]WorkItem, 4096)
	var read int
	var baseline, peak uint64
	var m runtime.MemStats
	start := time.Now()
	for read < rows {
		n, err := w.Next(ctx, buf)
		if err != nil {
			t.Fatalf("after %d rows: %v", read, err)
		}
		if n == 0 {
			t.Fatalf("the work list ended after %d rows, short of the %d asked for", read, rows)
		}
		read += n

		// The baseline is taken after a thousand rows rather than at zero, so it
		// is one steady state against another and not against the buffers the
		// first read allocates.
		if baseline == 0 && read >= 1000 {
			runtime.GC()
			runtime.ReadMemStats(&m)
			baseline = m.HeapAlloc
			t.Logf("baseline after %d rows: %.1f MB held", read, float64(baseline)/(1<<20))
		}
		if read%1000000 < n {
			runtime.GC()
			runtime.ReadMemStats(&m)
			if m.HeapAlloc > peak {
				peak = m.HeapAlloc
			}
			t.Logf("%d rows, part %d, %.1f MB held, %.0f rows/s",
				read, w.part, float64(m.HeapAlloc)/(1<<20), float64(read)/time.Since(start).Seconds())
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&m)
	if m.HeapAlloc > peak {
		peak = m.HeapAlloc
	}
	t.Logf("read %d rows in %s, held %.1f MB at the start and peaked at %.1f MB",
		read, time.Since(start).Round(time.Second), float64(baseline)/(1<<20), float64(peak)/(1<<20))

	// A generous ceiling on purpose. The claim being tested is that the work
	// list does not accumulate, and accumulation at 135 bytes a row would be
	// hundreds of megabytes over five million rows, not tens.
	if grew := int64(peak) - int64(baseline); grew > 64<<20 {
		t.Fatalf("the heap grew %.1f MB over %d rows, so the work list is accumulating rather than streaming",
			float64(grew)/(1<<20), read)
	}
}

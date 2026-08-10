package ccrawl

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// TestFrontierScale is the harness behind the numbers in the frontier's design
// notes. It is off by default because it writes gigabytes and takes minutes.
//
//	CCRAWL_SCALE_URLS=10000000 go test ./ccrawl -run TestFrontierScale -v -timeout 2h
//
// Set CCRAWL_SCALE_DIR to put the database somewhere with room for it. Ten
// million URLs is about 1.2 GB, and it is close enough to linear to extrapolate
// from.
func TestFrontierScale(t *testing.T) {
	raw := os.Getenv("CCRAWL_SCALE_URLS")
	if raw == "" {
		t.Skip("set CCRAWL_SCALE_URLS to run the scale harness")
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		t.Fatalf("CCRAWL_SCALE_URLS=%q is not a positive count", raw)
	}
	dir := os.Getenv("CCRAWL_SCALE_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	path := filepath.Join(dir, "frontier-scale.db")
	_ = os.Remove(path)

	f, err := OpenFrontier(FrontierConfig{Path: path, SeenURLs: 1 << 22})
	if err != nil {
		t.Fatalf("OpenFrontier: %v", err)
	}
	report := func(stage string, done int, since time.Duration) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		t.Logf("%-8s %9d  %7.0f/s  heap %4.0f MB  sys %4.0f MB",
			stage, done, float64(done)/since.Seconds(),
			float64(m.HeapAlloc)/(1<<20), float64(m.Sys)/(1<<20))
	}

	// Two million hosts, so the politeness cache is exercised well past its
	// capacity rather than answering everything out of the hot set.
	start := time.Now()
	for i := 0; i < n; i++ {
		host := fmt.Sprintf("h%d.example", i%2000000)
		f.Add(FrontierEntry{
			URL:      fmt.Sprintf("https://%s/page/%d", host, i),
			Host:     host,
			Priority: float32(i % 100000),
		})
		if i > 0 && i%2000000 == 0 {
			report("admit", i, time.Since(start))
		}
	}
	if err := f.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	report("admit", n, time.Since(start))
	if fi, err := os.Stat(path); err == nil {
		t.Logf("database %.2f GB for %d URLs", float64(fi.Size())/(1<<30), n)
	}

	now := time.Now().UnixMilli()
	start = time.Now()
	var popped int
	for popped < n {
		e, ok, err := f.Pop(now)
		if err != nil {
			t.Fatalf("Pop: %v", err)
		}
		if !ok {
			break
		}
		popped++
		if err := f.Done(e.URL); err != nil {
			t.Fatalf("Done: %v", err)
		}
		if popped%2000000 == 0 {
			report("pop", popped, time.Since(start))
		}
	}
	popRate := float64(popped) / time.Since(start).Seconds()
	report("pop", popped, time.Since(start))
	if popped != n {
		t.Errorf("popped %d of %d admitted, the frontier lost URLs", popped, n)
	}
	// The floor from E4. It is asserted here rather than in the ordinary test
	// suite because this only runs when someone asks for it on a machine they
	// chose, which is the only place a throughput number means anything.
	if popRate < 5000 {
		t.Errorf("pop rate %.0f/s over %d URLs, want at least 5000/s", popRate, n)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

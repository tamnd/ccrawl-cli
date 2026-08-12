package ccrawl

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestGlobalLimiterSpacesSlotsInOneProcess(t *testing.T) {
	g := newGlobalLimiter(t.TempDir(), 10*time.Millisecond)
	defer func() { _ = g.Close() }()

	const slots = 12
	start := time.Now()
	for i := range slots {
		granted, err := g.wait(context.Background())
		if err != nil {
			t.Fatalf("slot %d: %v", i, err)
		}
		if !granted {
			t.Fatalf("slot %d was not granted by the shared limiter", i)
		}
	}
	// The first slot is free, so N slots cost N-1 intervals.
	want := time.Duration(slots-1) * 10 * time.Millisecond
	if got := time.Since(start); got < want {
		t.Fatalf("%d slots took %s, want at least %s", slots, got, want)
	}
	grants, err := limiterGrants(g.Path())
	if err != nil {
		t.Fatal(err)
	}
	if grants != slots {
		t.Fatalf("the file counted %d grants, want %d", grants, slots)
	}
}

// TestGlobalLimiterIsSharedAcrossGoroutines is the in-process half of the
// guarantee: many callers, one budget.
func TestGlobalLimiterIsSharedAcrossGoroutines(t *testing.T) {
	g := newGlobalLimiter(t.TempDir(), 5*time.Millisecond)
	defer func() { _ = g.Close() }()

	const workers, each = 4, 10
	start := time.Now()
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				if _, err := g.wait(context.Background()); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	want := time.Duration(workers*each-1) * 5 * time.Millisecond
	if got := time.Since(start); got < want {
		t.Fatalf("%d slots across %d goroutines took %s, want at least %s", workers*each, workers, got, want)
	}
}

func TestGlobalLimiterHonorsContext(t *testing.T) {
	g := newGlobalLimiter(t.TempDir(), time.Hour)
	defer func() { _ = g.Close() }()

	// Burn the free first slot, so the second one is an hour out.
	if _, err := g.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := g.wait(ctx); err == nil {
		t.Fatal("waiting for a slot an hour out returned nil, want the context error")
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("the cancelled wait took %s, want it to give up with the context", el)
	}
}

// TestGlobalLimiterDegradesWhenTheFileIsUnusable checks the fallback: a host
// where the shared file cannot be opened keeps running on the per process delay
// instead of failing the run.
func TestGlobalLimiterDegradesWhenTheFileIsUnusable(t *testing.T) {
	// A regular file where the limiter wants a directory, so MkdirAll fails.
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	g := newGlobalLimiter(filepath.Join(blocker, "ccrawl"), 10*time.Millisecond)
	defer func() { _ = g.Close() }()

	for i := range 3 {
		granted, err := g.wait(context.Background())
		if err != nil {
			t.Fatalf("attempt %d returned %v, want a silent fallback", i, err)
		}
		if granted {
			t.Fatalf("attempt %d claimed a shared slot from a file it cannot open", i)
		}
	}
	if g.degraded == "" {
		t.Fatal("the limiter degraded without recording why")
	}
}

func TestGlobalLimiterOffWhenRateIsZero(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		if g := newGlobalLimiter(t.TempDir(), d); g != nil {
			t.Fatalf("--global-rate %s built a limiter, want it off", d)
		}
	}
	var nilG *globalLimiter
	granted, err := nilG.wait(context.Background())
	if err != nil || granted {
		t.Fatalf("a nil limiter returned (%v, %v), want (false, nil)", granted, err)
	}
	if !strings.Contains(nilG.Describe(), "no global rate limit") {
		t.Fatalf("a nil limiter describes itself as %q", nilG.Describe())
	}
}

func TestIsCommonCrawlURL(t *testing.T) {
	cases := []struct {
		url  string
		want bool
	}{
		{"https://index.commoncrawl.org/CC-MAIN-2026-25-index?url=a", true},
		{"https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-25/x.warc.gz", true},
		{"https://commoncrawl.org/robots.txt", true},
		{"https://commoncrawl.s3.us-east-1.amazonaws.com/a/b.parquet", true},
		{"s3://commoncrawl/crawl-data/CC-MAIN-2026-25/x.warc.gz", true},
		// A live crawl fetches whatever the caller asked for. That is not Common
		// Crawl's bandwidth, so it must not queue behind their budget.
		{"https://example.com/page.html", false},
		{"https://huggingface.co/api/datasets/open-index/ccrawl-urls", false},
		// Close enough to look right and not theirs.
		{"https://commoncrawl.org.evil.example/x", false},
		{"https://notcommoncrawl.org/x", false},
		{"https://other.s3.us-east-1.amazonaws.com/x", false},
		{"::not a url::", false},
	}
	for _, c := range cases {
		if got := isCommonCrawlURL(c.url); got != c.want {
			t.Errorf("isCommonCrawlURL(%q) = %v, want %v", c.url, got, c.want)
		}
	}
}

// TestThrottleSpendsTheBudgetOnlyOnCommonCrawl is the wiring check: the shared
// limiter gates Common Crawl requests and nothing else.
func TestThrottleSpendsTheBudgetOnlyOnCommonCrawl(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.Delay = 0
	cfg.GlobalRate = 10 * time.Millisecond
	h := NewHTTPClient(cfg)
	defer func() { _ = h.global.Close() }()

	for range 5 {
		if err := h.throttle(context.Background(), "https://example.com/x"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, limiterFile)); err == nil {
		t.Fatal("throttling a non Common Crawl URL touched the shared budget file")
	}

	for range 4 {
		if err := h.throttle(context.Background(), "https://data.commoncrawl.org/x"); err != nil {
			t.Fatal(err)
		}
	}
	grants, err := limiterGrants(filepath.Join(dir, limiterFile))
	if err != nil {
		t.Fatal(err)
	}
	if grants != 4 {
		t.Fatalf("the budget recorded %d grants, want 4", grants)
	}

	// A columnar scan is thousands of small footer reads and is deliberately
	// exempt, so the copy must not carry the limiter.
	if h.WithoutDelay().global != nil {
		t.Fatal("WithoutDelay kept the shared limiter, which would throttle columnar scans")
	}
}

func TestGlobalRateDescribesTheEffectiveRate(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.GlobalRate = 200 * time.Millisecond
	h := NewHTTPClient(cfg)
	defer func() { _ = h.global.Close() }()
	got := h.GlobalRate()
	if !strings.Contains(got, "5 requests per second") {
		t.Fatalf("a 200ms gap describes itself as %q, want 5 requests per second", got)
	}

	cfg.GlobalRate = 0
	if got := NewHTTPClient(cfg).GlobalRate(); !strings.Contains(got, "no global rate limit") {
		t.Fatalf("--global-rate 0 describes itself as %q", got)
	}
}

// Env keys the multi-process test passes to its children. Flags would be
// cleaner, but the child is this same test binary and adding flags to it would
// leak into every other package's test run.
const (
	envLimiterDir      = "CCRAWL_TEST_LIMITER_DIR"
	envLimiterInterval = "CCRAWL_TEST_LIMITER_INTERVAL"
	envLimiterSlots    = "CCRAWL_TEST_LIMITER_SLOTS"
	envLimiterPeers    = "CCRAWL_TEST_LIMITER_PEERS"
	envLimiterOut      = "CCRAWL_TEST_LIMITER_OUT"
)

// TestGlobalLimiterHoldsTheRateAcrossProcesses is the done-when for the shared
// budget: several ccrawl processes running at once on one host add up to the
// configured rate, not a multiple of it.
//
// Three children take slots from one lock file as fast as they can. They line up
// on a barrier first, so the whole run really is concurrent rather than three
// runs in a row, and each reports the time of its first and last slot. The rate
// is then measured over the window every child was inside, which is the number
// the issue asks about.
func TestGlobalLimiterHoldsTheRateAcrossProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("re-execs this binary three times and runs for a few seconds")
	}
	const (
		procs    = 3
		perProc  = 40
		interval = 20 * time.Millisecond
	)
	dir := t.TempDir()

	type result struct{ first, last time.Time }
	results := make([]result, procs)
	errCh := make(chan error, procs)
	var wg sync.WaitGroup
	for i := range procs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out := filepath.Join(dir, fmt.Sprintf("child-%d.txt", i))
			cmd := exec.Command(os.Args[0], "-test.run=TestGlobalLimiterChild", "-test.timeout=5m")
			cmd.Env = append(os.Environ(),
				envLimiterDir+"="+dir,
				envLimiterInterval+"="+interval.String(),
				envLimiterSlots+"="+strconv.Itoa(perProc),
				envLimiterPeers+"="+strconv.Itoa(procs),
				envLimiterOut+"="+out,
			)
			if b, err := cmd.CombinedOutput(); err != nil {
				errCh <- fmt.Errorf("child %d: %v: %s", i, err, b)
				return
			}
			first, last, err := readChildWindow(out)
			if err != nil {
				errCh <- fmt.Errorf("child %d: %w", i, err)
				return
			}
			results[i] = result{first, last}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// The run is from the earliest slot anyone got to the latest, and every slot
	// handed out falls inside it. The barrier keeps the children from drifting
	// apart at the ends, so this is a saturated window rather than a ramp.
	first, lastFirst := results[0].first, results[0].first
	last, firstLast := results[0].last, results[0].last
	for _, r := range results[1:] {
		if r.first.Before(first) {
			first = r.first
		}
		if r.first.After(lastFirst) {
			lastFirst = r.first
		}
		if r.last.After(last) {
			last = r.last
		}
		if r.last.Before(firstLast) {
			firstLast = r.last
		}
	}
	if !firstLast.After(lastFirst) {
		t.Fatalf("the children did not overlap: the last one started at %s, the first finished at %s", lastFirst, firstLast)
	}

	span := last.Sub(first)
	// N slots at one interval apart span N-1 intervals, so the first slot of the
	// run is free and must not be counted against the elapsed time.
	measured := float64(procs*perProc-1) / span.Seconds()
	want := float64(time.Second) / float64(interval)
	t.Logf("%d processes, %d slots each over %s: %.2f requests per second combined, configured %.2f",
		procs, perProc, span.Round(time.Millisecond), measured, want)

	if measured > want*1.10 || measured < want*0.90 {
		t.Fatalf("three concurrent processes served %.2f requests per second combined, want %.2f within 10 percent", measured, want)
	}

	grants, err := limiterGrants(filepath.Join(dir, limiterFile))
	if err != nil {
		t.Fatal(err)
	}
	if grants != procs*perProc {
		t.Fatalf("the shared file counted %d grants, want %d, so somebody bypassed it", grants, procs*perProc)
	}
}

// TestGlobalLimiterChild is one process of the test above. It does nothing
// unless the parent asked for it by env.
func TestGlobalLimiterChild(t *testing.T) {
	dir := os.Getenv(envLimiterDir)
	if dir == "" {
		t.Skip("run by TestGlobalLimiterHoldsTheRateAcrossProcesses, not on its own")
	}
	interval, err := time.ParseDuration(os.Getenv(envLimiterInterval))
	if err != nil {
		t.Fatal(err)
	}
	slots, err := strconv.Atoi(os.Getenv(envLimiterSlots))
	if err != nil {
		t.Fatal(err)
	}
	peers, err := strconv.Atoi(os.Getenv(envLimiterPeers))
	if err != nil {
		t.Fatal(err)
	}
	if err := waitAtBarrier(dir, peers); err != nil {
		t.Fatal(err)
	}

	g := newGlobalLimiter(dir, interval)
	defer func() { _ = g.Close() }()
	var first, last time.Time
	for i := range slots {
		granted, err := g.wait(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !granted {
			t.Fatalf("slot %d fell back to the per process delay, so this host cannot share a budget", i)
		}
		last = time.Now()
		if i == 0 {
			first = last
		}
	}
	line := fmt.Sprintf("%d %d\n", first.UnixNano(), last.UnixNano())
	if err := os.WriteFile(os.Getenv(envLimiterOut), []byte(line), 0o644); err != nil {
		t.Fatal(err)
	}
}

// waitAtBarrier blocks until peers processes have reached this point, so the
// measured window is one where all of them are actually competing.
func waitAtBarrier(dir string, peers int) error {
	bdir := filepath.Join(dir, "barrier")
	if err := os.MkdirAll(bdir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(bdir, strconv.Itoa(os.Getpid())), nil, 0o644); err != nil {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		ents, err := os.ReadDir(bdir)
		if err != nil {
			return err
		}
		if len(ents) >= peers {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("only %d of %d processes reached the barrier", len(ents), peers)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func readChildWindow(path string) (time.Time, time.Time, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	parts := strings.Fields(string(b))
	if len(parts) != 2 {
		return time.Time{}, time.Time{}, fmt.Errorf("%s holds %q, want two timestamps", path, b)
	}
	first, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	last, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return time.Unix(0, first), time.Unix(0, last), nil
}

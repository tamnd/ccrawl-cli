package ccrawl

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// seedFrontier admits n URLs spread over hosts hosts and flushes them.
func seedFrontier(tb testing.TB, f *Frontier, n, hosts int) {
	tb.Helper()
	for i := 0; i < n; i++ {
		host := fmt.Sprintf("h%d.example", i%hosts)
		f.Add(FrontierEntry{
			URL:      fmt.Sprintf("https://%s/page/%d", host, i),
			Host:     host,
			Priority: float32(n - i),
		})
	}
	if err := f.Flush(); err != nil {
		tb.Fatalf("Flush: %v", err)
	}
}

// drain pops until nothing is eligible and returns the URLs in order.
func drain(tb testing.TB, f *Frontier, now int64) []string {
	tb.Helper()
	var got []string
	for {
		e, ok, err := f.Pop(now)
		if err != nil {
			tb.Fatalf("Pop: %v", err)
		}
		if !ok {
			return got
		}
		got = append(got, e.URL)
		if err := f.Done(e.URL); err != nil {
			tb.Fatalf("Done: %v", err)
		}
	}
}

// TestFrontierResumesWithoutRefetching is the property the whole thing is for:
// a run that dies mid crawl and starts again picks up what it had not done and
// does not hand back what it had.
func TestFrontierResumesWithoutRefetching(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	now := time.Now().Unix()

	f, err := OpenFrontier(FrontierConfig{Path: path})
	if err != nil {
		t.Fatalf("OpenFrontier: %v", err)
	}
	seedFrontier(t, f, 200, 40)

	done := map[string]bool{}
	for i := 0; i < 60; i++ {
		e, ok, err := f.Pop(now)
		if err != nil || !ok {
			t.Fatalf("Pop %d: ok=%v err=%v", i, ok, err)
		}
		if err := f.Done(e.URL); err != nil {
			t.Fatalf("Done: %v", err)
		}
		done[e.URL] = true
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f2, err := OpenFrontier(FrontierConfig{Path: path})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = f2.Close() }()

	if got := f2.Len(); got != 140 {
		t.Errorf("reopened Len = %d, want 140 (200 seeded minus 60 done)", got)
	}
	rest := drain(t, f2, now)
	if len(rest) != 140 {
		t.Fatalf("second run popped %d URLs, want 140", len(rest))
	}
	seen := map[string]bool{}
	for _, u := range rest {
		if done[u] {
			t.Fatalf("refetched %s, which the first run already finished", u)
		}
		if seen[u] {
			t.Fatalf("popped %s twice in one run", u)
		}
		seen[u] = true
	}
}

// TestFrontierReclaimsClaimsLeftInFlight covers the other half of a crash: URLs
// a worker had taken but not finished. They were fetched or they were not, and
// only one of those readings is safe, so they go back in the queue.
func TestFrontierReclaimsClaimsLeftInFlight(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	now := time.Now().Unix()

	f, err := OpenFrontier(FrontierConfig{Path: path})
	if err != nil {
		t.Fatalf("OpenFrontier: %v", err)
	}
	seedFrontier(t, f, 20, 20)
	var claimed []string
	for i := 0; i < 5; i++ {
		e, ok, err := f.Pop(now) // popped and never marked done, as a kill would leave it
		if err != nil || !ok {
			t.Fatalf("Pop: ok=%v err=%v", ok, err)
		}
		claimed = append(claimed, e.URL)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f2, err := OpenFrontier(FrontierConfig{Path: path})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = f2.Close() }()
	if got := f2.Stats().Reclaimed; got != 5 {
		t.Errorf("Reclaimed = %d, want 5", got)
	}
	back := map[string]bool{}
	for _, u := range drain(t, f2, now) {
		back[u] = true
	}
	for _, u := range claimed {
		if !back[u] {
			t.Errorf("%s was claimed when the run died and never came back", u)
		}
	}
}

// TestFrontierDedupsAcrossRestart pins that the dedup is the primary key rather
// than the memory cache. The cache is empty on open, so a reseed after a
// restart is exactly the case where a cache only design quietly doubles the
// queue.
func TestFrontierDedupsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")

	f, err := OpenFrontier(FrontierConfig{Path: path})
	if err != nil {
		t.Fatalf("OpenFrontier: %v", err)
	}
	seedFrontier(t, f, 100, 10)
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f2, err := OpenFrontier(FrontierConfig{Path: path})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = f2.Close() }()
	seedFrontier(t, f2, 100, 10) // the same 100 URLs again
	if s := f2.Stats(); s.Admitted != 0 || s.Duplicates != 100 {
		t.Errorf("reseed Stats = %+v, want Admitted 0 and Duplicates 100", s)
	}
	if got := f2.Len(); got != 100 {
		t.Errorf("Len after reseed = %d, want 100", got)
	}
}

// TestFrontierPolitenessSurvivesRestart checks that a restart does not hit a
// host it was mid delay on. The clocks are buffered and drained per claim
// batch, so this only holds for a clean Close, which is what the test does.
func TestFrontierPolitenessSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frontier.db")
	now := time.Now().Unix()

	f, err := OpenFrontier(FrontierConfig{Path: path, Delay: 30 * time.Second})
	if err != nil {
		t.Fatalf("OpenFrontier: %v", err)
	}
	f.Add(FrontierEntry{URL: "https://slow.example/a", Host: "slow.example"})
	f.Add(FrontierEntry{URL: "https://slow.example/b", Host: "slow.example"})
	e, ok, err := f.Pop(now)
	if err != nil || !ok {
		t.Fatalf("Pop: ok=%v err=%v", ok, err)
	}
	if err := f.Done(e.URL); err != nil {
		t.Fatalf("Done: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f2, err := OpenFrontier(FrontierConfig{Path: path, Delay: 30 * time.Second})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = f2.Close() }()
	if _, ok, err := f2.Pop(now + 5); err != nil {
		t.Fatalf("Pop: %v", err)
	} else if ok {
		t.Error("a restart popped a host it was 5 seconds into a 30 second delay on")
	}
	if _, ok, err := f2.Pop(now + 31); err != nil {
		t.Fatalf("Pop: %v", err)
	} else if !ok {
		t.Error("Pop after the delay elapsed should succeed")
	}
}

// TestFrontierDeferredURLsAreNotLost is the politeness write back: an entry
// claimed while its host is inside the delay goes back to pending rather than
// evaporating with the claim buffer.
func TestFrontierDeferredURLsAreNotLost(t *testing.T) {
	f := NewFrontier(60 * time.Second)
	defer func() { _ = f.Close() }()
	now := time.Now().Unix()

	// One busy host and one quiet one. The busy host's second URL has to be
	// deferred to reach the quiet host's, and then still be there afterwards.
	f.Add(FrontierEntry{URL: "https://busy.example/1", Host: "busy.example", Priority: 10})
	f.Add(FrontierEntry{URL: "https://busy.example/2", Host: "busy.example", Priority: 9})
	f.Add(FrontierEntry{URL: "https://quiet.example/1", Host: "quiet.example", Priority: 1})

	got := drain(t, f, now)
	if len(got) != 2 || got[0] != "https://busy.example/1" || got[1] != "https://quiet.example/1" {
		t.Fatalf("first pass popped %v, want busy/1 then quiet/1", got)
	}
	if f.Len() != 1 {
		t.Fatalf("Len = %d, want 1 deferred URL still queued", f.Len())
	}
	if s := f.Stats(); s.Deferred == 0 {
		t.Error("Stats.Deferred = 0, want the write back to have been counted")
	}
	later := drain(t, f, now+61)
	if len(later) != 1 || later[0] != "https://busy.example/2" {
		t.Fatalf("after the delay popped %v, want busy/2", later)
	}
}

func TestFrontierRetryRequeues(t *testing.T) {
	f := NewFrontier(0)
	defer func() { _ = f.Close() }()
	now := time.Now().Unix()

	f.Add(FrontierEntry{URL: "https://flaky.example/", Host: "flaky.example"})
	e, ok, err := f.Pop(now)
	if err != nil || !ok {
		t.Fatalf("Pop: ok=%v err=%v", ok, err)
	}
	if err := f.Retry(e, now+10); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if _, ok, _ := f.Pop(now); ok {
		t.Error("a URL retried for later should not be eligible now")
	}
	e2, ok, err := f.Pop(now + 11)
	if err != nil || !ok {
		t.Fatalf("Pop after retry time: ok=%v err=%v", ok, err)
	}
	if e2.Retries != 1 {
		t.Errorf("Retries = %d, want 1", e2.Retries)
	}
}

// TestFrontierThroughput is the done when box from the issue, at the scale this
// machine has disk for. The rates are asserted well under what the run reports
// so the test fails on a regression rather than on a busy CI runner.
func TestFrontierThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("throughput measurement, skipped under -short")
	}
	const n = 200000
	path := filepath.Join(t.TempDir(), "frontier.db")
	f, err := OpenFrontier(FrontierConfig{Path: path, SeenURLs: n})
	if err != nil {
		t.Fatalf("OpenFrontier: %v", err)
	}
	defer func() { _ = f.Close() }()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	start := time.Now()
	seedFrontier(t, f, n, 5000)
	admit := time.Since(start)

	now := time.Now().Unix()
	start = time.Now()
	var popped int
	for {
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
	}
	pop := time.Since(start)

	runtime.ReadMemStats(&after)
	admitRate := float64(n) / admit.Seconds()
	popRate := float64(popped) / pop.Seconds()
	t.Logf("admitted %d in %s (%.0f/s), popped %d in %s (%.0f/s), heap %.1f MB for %d URLs",
		n, admit.Round(time.Millisecond), admitRate,
		popped, pop.Round(time.Millisecond), popRate,
		float64(after.HeapAlloc-before.HeapAlloc)/(1<<20), n)

	if popped != n {
		t.Fatalf("popped %d of %d admitted", popped, n)
	}
	if popRate < 5000 {
		t.Errorf("pop rate %.0f/s, want at least 5000/s", popRate)
	}
	if admitRate < 20000 {
		t.Errorf("admit rate %.0f/s, want at least 20000/s", admitRate)
	}
}

func BenchmarkFrontierAdd(b *testing.B) {
	f, err := OpenFrontier(FrontierConfig{Path: filepath.Join(b.TempDir(), "f.db"), SeenURLs: b.N})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		host := fmt.Sprintf("h%d.example", i%5000)
		f.Add(FrontierEntry{URL: fmt.Sprintf("https://%s/p/%d", host, i), Host: host, Priority: float32(i)})
	}
	if err := f.Flush(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkFrontierPop(b *testing.B) {
	f, err := OpenFrontier(FrontierConfig{Path: filepath.Join(b.TempDir(), "f.db"), SeenURLs: b.N})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	seedFrontier(b, f, b.N, 5000)
	now := time.Now().Unix()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e, ok, err := f.Pop(now)
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			b.Fatalf("frontier ran dry after %d of %d pops", i, b.N)
		}
		if err := f.Done(e.URL); err != nil {
			b.Fatal(err)
		}
	}
}

// TestSeenCacheNeverInventsAHit is the property that made this an exact cache
// instead of a Bloom filter. A key that was never added has to come back false,
// every time, because the caller drops the URL on a true.
func TestSeenCacheNeverInventsAHit(t *testing.T) {
	const n = 50000
	c := newSeenCache(n)
	for i := 0; i < n; i++ {
		if c.add(uint64(i) * 0x9e3779b97f4a7c15) {
			t.Fatalf("key %d was reported as seen the first time it was added", i)
		}
	}
	for i := n; i < 2*n; i++ {
		if c.add(uint64(i) * 0x9e3779b97f4a7c15) {
			t.Fatalf("key %d was never added and the cache claimed it had been", i)
		}
	}
}

func TestSeenCacheRemembersAndBounds(t *testing.T) {
	c := newSeenCache(1000)
	if c.add(42) {
		t.Error("first add reported the key as already seen")
	}
	if !c.add(42) {
		t.Error("second add of the same key reported it as new")
	}
	// Well past the limit: the cache forgets rather than growing, and forgetting
	// is safe because the primary key still catches what falls out.
	for i := 0; i < 10000; i++ {
		c.add(uint64(i) | 1<<40)
	}
	if got := c.len(); got > 2000 {
		t.Errorf("resident keys = %d, want at most twice the 1000 limit", got)
	}
}

// TestSeenCacheKeepsHotKeysResident checks the promotion path: a URL that keeps
// being rediscovered should not age out just because the generation turned.
func TestSeenCacheKeepsHotKeysResident(t *testing.T) {
	c := newSeenCache(100)
	const hot = uint64(0xdeadbeef)
	c.add(hot)
	for i := 0; i < 500; i++ {
		c.add(uint64(i))
		if !c.add(hot) {
			t.Fatalf("hot key was forgotten after %d other insertions", i)
		}
	}
}

package ccrawl

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// The politeness delay used to live entirely in HTTPClient, which made it per
// process. The pipelines are meant to run at the same time, urls publish and
// domains publish and markdown export together, so three of them meant three
// times the request rate against a budget nobody was accounting for. Common
// Crawl is a nonprofit serving free bandwidth, and this is the one place in the
// tool where being sloppy has an external victim.
//
// globalLimiter moves the reservation out of process memory and into a small
// file under the data dir that every ccrawl process on the host shares. Taking a
// slot is: lock the file, read the time the next slot comes free, push that time
// forward by one interval, unlock, then sleep until the slot you were handed.
// The lock is held for one read and one write, microseconds, so processes do not
// queue on the lock, they queue on the timestamps it hands out. That gives an
// exact aggregate rate no matter how the requests are spread across processes.
//
// A host where the file cannot be locked degrades to the per process delay,
// which is what shipped before this existed, and says so once.

// limiterFile is the name of the shared state under the data dir.
const limiterFile = "ratelimit.lock"

// limiterStateSize is the state: the time the next slot comes free, and how many
// slots have been handed out since the file was created. The counter is what
// lets an operator, or a test, measure the rate that was actually served.
const limiterStateSize = 16

// globalLimiter hands out request slots at a fixed rate to every process on the
// host that shares its file.
type globalLimiter struct {
	path     string
	interval time.Duration

	mu sync.Mutex
	f  *os.File
	// degraded is why the file cannot be used, empty while it works. Once set it
	// stays set: a limiter that has failed once is not retried per request.
	degraded string
	tried    bool
	warned   bool
}

// newGlobalLimiter returns a limiter over dir, or nil when the interval is zero
// or negative, which is how --global-rate 0 turns the thing off.
func newGlobalLimiter(dir string, interval time.Duration) *globalLimiter {
	if interval <= 0 || dir == "" {
		return nil
	}
	return &globalLimiter{path: filepath.Join(dir, limiterFile), interval: interval}
}

// Path is where the shared state lives.
func (g *globalLimiter) Path() string {
	if g == nil {
		return ""
	}
	return g.path
}

// Describe renders the effective rate in the form an operator reads, which is
// requests per second rather than the delay between them.
func (g *globalLimiter) Describe() string {
	if g == nil {
		return "no global rate limit, each process keeps its own delay"
	}
	rate := float64(time.Second) / float64(g.interval)
	return fmt.Sprintf("global rate limit %.4g requests per second, shared by every ccrawl process using %s", rate, g.path)
}

// wait blocks until this process may make its next Common Crawl request. It
// reports whether the shared limiter was the thing that granted the slot, so the
// caller knows whether it still has to apply its own delay.
func (g *globalLimiter) wait(ctx context.Context) (bool, error) {
	if g == nil {
		return false, nil
	}
	slot, ok := g.reserve()
	if !ok {
		return false, nil
	}
	if slot <= 0 {
		return true, nil
	}
	t := time.NewTimer(slot)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return true, ctx.Err()
	case <-t.C:
		return true, nil
	}
}

// reserve takes the next slot and returns how long to wait for it. ok is false
// when the shared file is unusable, which leaves the caller on its own delay.
func (g *globalLimiter) reserve() (time.Duration, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	f, err := g.open()
	if err != nil {
		return 0, false
	}
	if err := lockFile(f); err != nil {
		g.degrade(fmt.Sprintf("cannot lock %s: %v", g.path, err))
		return 0, false
	}
	defer func() { _ = unlockFile(f) }()

	var buf [limiterStateSize]byte
	// A short read is a file that was just created, or one somebody truncated.
	// Either way the zero value is the right starting point: the next slot is
	// now, and nothing has been handed out.
	n, err := f.ReadAt(buf[:], 0)
	if err != nil && n != limiterStateSize {
		for i := n; i < limiterStateSize; i++ {
			buf[i] = 0
		}
	}
	next := time.Unix(0, int64(binary.BigEndian.Uint64(buf[0:8])))
	granted := binary.BigEndian.Uint64(buf[8:16])

	now := time.Now()
	if next.Before(now) {
		next = now
	}
	wait := next.Sub(now)

	binary.BigEndian.PutUint64(buf[0:8], uint64(next.Add(g.interval).UnixNano()))
	binary.BigEndian.PutUint64(buf[8:16], granted+1)
	if _, err := f.WriteAt(buf[:], 0); err != nil {
		g.degrade(fmt.Sprintf("cannot write %s: %v", g.path, err))
		return 0, false
	}
	return wait, true
}

// open returns the shared file, creating it on first use. Caller holds g.mu.
func (g *globalLimiter) open() (*os.File, error) {
	if g.f != nil {
		return g.f, nil
	}
	if g.tried {
		return nil, fmt.Errorf("%s", g.degraded)
	}
	g.tried = true
	if err := os.MkdirAll(filepath.Dir(g.path), 0o755); err != nil {
		g.degrade(fmt.Sprintf("cannot create %s: %v", filepath.Dir(g.path), err))
		return nil, err
	}
	f, err := os.OpenFile(g.path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		g.degrade(fmt.Sprintf("cannot open %s: %v", g.path, err))
		return nil, err
	}
	g.f = f
	return f, nil
}

// degrade records why the shared limiter is off and says so once, because a rate
// limit that silently stopped being global is exactly the failure this whole
// file exists to prevent.
func (g *globalLimiter) degrade(reason string) {
	g.degraded = reason
	if g.warned {
		return
	}
	g.warned = true
	fmt.Fprintf(os.Stderr, "ccrawl: %s, falling back to a per process delay; concurrent runs will exceed the global rate\n", reason)
}

// Close releases the shared file.
func (g *globalLimiter) Close() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.f == nil {
		return nil
	}
	err := g.f.Close()
	g.f = nil
	return err
}

// limiterGrants reads how many slots the file at path has handed out. It is what
// turns "the rate looked about right" into a number.
func limiterGrants(path string) (uint64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	if len(b) < limiterStateSize {
		return 0, fmt.Errorf("%s holds %d bytes, want %d", path, len(b), limiterStateSize)
	}
	return binary.BigEndian.Uint64(b[8:16]), nil
}

// isCommonCrawlURL reports whether a URL is one of Common Crawl's, which is what
// the shared budget covers. A live crawl fetches arbitrary hosts and pays its own
// per host politeness, so it must not draw on this.
func isCommonCrawlURL(raw string) bool {
	if strings.HasPrefix(raw, "s3://"+s3Bucket+"/") {
		return true
	}
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	if host == "commoncrawl.org" || strings.HasSuffix(host, ".commoncrawl.org") {
		return true
	}
	// The bulk data is also reachable as the bucket's REST endpoint, which is
	// the same bytes off the same nonprofit's budget.
	return strings.HasPrefix(host, s3Bucket+".s3.") && strings.HasSuffix(host, ".amazonaws.com")
}

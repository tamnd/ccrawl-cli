package ccrawl

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrCommitStall is returned by a publish run when no forward progress happens
// within the stall window. The CLI maps it to exit code 75 (EX_TEMPFAIL) so a
// supervisor restarts the command and the remote-truth resume picks up cleanly.
var ErrCommitStall = errors.New("commit stall: no forward progress within the stall window")

// DefaultMinFreeGB is the free-disk floor that gates new downloads.
const DefaultMinFreeGB = 30

// DefaultMaxStall is how long a run may make no progress before it restarts.
const DefaultMaxStall = 45 * time.Minute

// stallClock tracks the time of the last forward progress. A watcher cancels the
// run context if the clock is not marked within max.
type stallClock struct {
	max    time.Duration
	cancel context.CancelFunc

	mu    sync.Mutex
	last  time.Time
	fired bool
}

func newStallClock(max time.Duration, cancel context.CancelFunc) *stallClock {
	if max <= 0 {
		max = DefaultMaxStall
	}
	s := &stallClock{max: max, cancel: cancel, last: time.Now()}
	return s
}

// mark records forward progress.
func (s *stallClock) mark() {
	s.mu.Lock()
	s.last = time.Now()
	s.mu.Unlock()
}

// stalled reports whether the watcher fired.
func (s *stallClock) stalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fired
}

// watch ticks each minute and cancels the run if the clock has gone stale.
func (s *stallClock) watch(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.mu.Lock()
			stale := time.Since(s.last) > s.max
			if stale {
				s.fired = true
			}
			s.mu.Unlock()
			if stale {
				fmt.Fprintf(os.Stderr, "commit stall: no progress for %s, restarting\n", s.max.Round(time.Second))
				s.cancel()
				return
			}
		}
	}
}

// committer serializes commits to one dataset repo. The hub commit API is
// one-at-a-time per repo, so exactly one committer runs per publish. It batches
// finished shards, commits every commitEvery, deletes the local files after a
// successful real commit, and persists shard-level progress.
type committer struct {
	hf           *HFClient
	repo         string
	scope        string // crawl or graph id, blank if a batch can span units
	kind         string // "url" | "domain"
	width        int    // zero-pad width of the shard index (5 urls, 3 domains)
	commitEvery  int
	keep         bool
	doCommit     bool // false = dry run, stage and print but never touch the hub
	progressKey  string
	progressPath string
	clock        *stallClock
	logf         func(string, ...any)

	// sidecar, when set, is called during flush with the shard, row, and byte
	// totals the batch will bring the unit to. It refreshes the ledger and
	// dataset card for that progress and returns the extra files (stats.csv,
	// README.md) to commit alongside the shards, so the hub shows current
	// coverage after every batch instead of only when the whole run finishes.
	sidecar func(shards int, rows, bytes int64) ([]HFOperation, error)

	batch     []shard
	committed int
	rows      int64
	bytes     int64
	flushes   int // real commits made, so the finalize refresh can be skipped
}

// add appends a finished shard and flushes when the batch is full.
func (c *committer) add(ctx context.Context, s shard) error {
	c.batch = append(c.batch, s)
	if len(c.batch) >= c.commitEvery {
		return c.flush(ctx)
	}
	return nil
}

// flush commits the current batch and clears it.
func (c *committer) flush(ctx context.Context) error {
	if len(c.batch) == 0 {
		return nil
	}
	batch := c.batch
	summary := commitSummary(c.scope, c.kind, c.width, batch)
	msg := summary
	if body := commitBody(batch); body != "" {
		msg = summary + "\n\n" + body
	}

	// Totals this batch will bring the unit to once committed. The card and
	// ledger are generated for this projected state and committed with the
	// shards, so a reader always sees a coherent snapshot.
	shards := c.committed + len(batch)
	rows, bytes := c.rows, c.bytes
	for _, s := range batch {
		rows += s.Rows
		bytes += s.Bytes
	}

	if c.doCommit {
		ops := make([]HFOperation, 0, len(batch)+2)
		for _, s := range batch {
			ops = append(ops, HFOperation{LocalPath: s.Local, PathInRepo: s.RepoPath})
		}
		if c.sidecar != nil {
			extra, err := c.sidecar(shards, rows, bytes)
			if err != nil {
				return fmt.Errorf("refresh card %q: %w", summary, err)
			}
			ops = append(ops, extra...)
		}
		if _, err := c.hf.CommitWithRetry(ctx, c.repo, msg, ops, 5); err != nil {
			return fmt.Errorf("commit %q: %w", summary, err)
		}
		c.flushes++
	} else {
		c.logf("[dry-run] would commit: %s", summary)
	}

	c.committed = shards
	c.rows = rows
	c.bytes = bytes
	// Delete local shards right after a real commit so disk stays flat. On a dry
	// run the files are kept for inspection.
	if c.doCommit && !c.keep {
		for _, s := range batch {
			_ = os.Remove(s.Local)
		}
	}
	if err := c.saveProgress(); err != nil {
		return err
	}
	c.clock.mark()
	if c.doCommit {
		c.logf("committed: %s", summary)
	}
	c.batch = nil
	return nil
}

// saveProgress persists the committer's shard-level progress for this unit.
func (c *committer) saveProgress() error {
	if c.progressPath == "" {
		return nil
	}
	m, err := ReadProgress(c.progressPath)
	if err != nil {
		return err
	}
	m[c.progressKey] = ProgressEntry{Shards: c.committed, Rows: c.rows, Bytes: c.bytes}
	return WriteProgress(c.progressPath, m)
}

// waitForDiskFloor blocks new downloads while free disk is under the floor,
// marking the stall clock so a deliberate hold is not counted as a stall.
func waitForDiskFloor(ctx context.Context, dir string, minFreeGB int, clock *stallClock) error {
	if minFreeGB <= 0 {
		return nil
	}
	floor := int64(minFreeGB) << 30
	warned := false
	for {
		free := freeDiskBytes(dir)
		if free == 0 || free >= floor {
			return nil
		}
		if !warned {
			fmt.Fprintf(os.Stderr, "waiting for disk: %s free, need %d GB\n", humanBytes(free), minFreeGB)
			warned = true
		}
		clock.mark()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(15 * time.Second):
		}
	}
}

// sweepTemps removes stray .parquet.tmp files left by a killed run under root.
func sweepTemps(root string) {
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() && strings.HasSuffix(p, ".parquet.tmp") {
			_ = os.Remove(p)
		}
		return nil
	})
}

// budgetProcess sizes the download-and-convert worker pool. override wins when
// positive; otherwise it is CPU-2 clamped to [1,8].
func budgetProcess(override int) int {
	if override > 0 {
		return override
	}
	if v := envInt("CCRAWL_MAX_PROCESS", 0); v > 0 {
		return v
	}
	return min(max(runtime.NumCPU()-2, 1), 8)
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

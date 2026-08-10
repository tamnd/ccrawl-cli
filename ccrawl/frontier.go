package ccrawl

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite" // pure Go driver, no cgo
)

// Frontier states. A URL is pending until something claims it, claimed while a
// worker holds it, and then done or failed for good.
const (
	frontierPending = 0
	frontierClaimed = 1
	frontierDone    = 2
	frontierFailed  = 3
)

// FrontierEntry is one URL in the crawl frontier.
type FrontierEntry struct {
	URL      string  // normalized URL
	Host     string  // hostname for politeness grouping
	Priority float32 // harmonic centrality (higher = crawl sooner)
	NextAt   int64   // earliest Unix timestamp to fetch
	Depth    uint8   // BFS depth from seed
	Retries  uint8
}

// FrontierConfig configures a frontier. The zero value is a usable in memory
// frontier, which is what the tests and small runs want.
type FrontierConfig struct {
	// Path is the SQLite file holding the state. Empty means a private in
	// memory database, which is gone when the process is.
	Path string
	// Delay is the minimum spacing between two requests to the same host.
	Delay time.Duration
	// MergeSize is how many staged URLs pile up before they are merged into the
	// frontier. The merge is one ordered sweep of the frontier B-tree whatever it
	// carries, so a bigger number is cheaper per URL and costs a longer wait
	// before a newly discovered URL becomes poppable.
	MergeSize int
	// SeenURLs is how many URL hashes the in memory seen cache holds per
	// generation, so the resident set is between one and two times it. Under
	// sizing it does not lose URLs, it just sends more of the seen check to the
	// disk. Roughly 50 bytes a key, so the default is around 100 MB.
	SeenURLs int
	// BatchSize is how many admissions accumulate before a transaction. One
	// insert per URL is roughly two orders of magnitude slower than a batch.
	BatchSize int
	// ClaimSize is how many rows one refill claims. Larger amortises the query
	// over more pops and leaves more work to reclaim after a crash.
	ClaimSize int
	// HotHosts is the size of the in memory host politeness cache.
	HotHosts int
	// SyncEvery is how many completions buffer before they are written, and so
	// how many URLs a kill can cost. It defaults to ClaimSize. Set it to 1 for a
	// frontier where a kill must never cause a refetch, at roughly a quarter of
	// the pop rate.
	SyncEvery int
	// CacheMB is the SQLite page cache in megabytes. The frontier is a B-tree
	// that a crawl walks in priority order while inserting in URL hash order, so
	// the cache is what keeps the upper levels of both off the disk.
	CacheMB int
}

// FrontierStats counts what the frontier did, as opposed to what it holds.
type FrontierStats struct {
	Admitted   int64 // URLs that were new and went in
	Duplicates int64 // URLs already known, dropped by the primary key
	Claimed    int64 // pops handed out
	Deferred   int64 // claims written back because their host was not eligible
	Reclaimed  int64 // rows a previous run left claimed and this one took back
	Filtered   int64 // URLs the seen cache rejected without touching the disk
}

// Frontier is a disk backed URL frontier with per host politeness.
//
// The state lives in SQLite because the alternative does not survive contact
// with the job it is for: the gao seed set is around 280M URLs, and a Go map
// with 280M string keys is tens of gigabytes of live heap and a garbage
// collector that never finishes. On disk the same set is a file, and the parts
// that have to be fast, the seen check and the pop, are held in front of it by
// bounded memory caches.
//
// The three moving parts:
//
//   - Admission batches. Add buffers rows and writes them in transactions of
//     BatchSize, because SQLite's cost is per transaction far more than per row.
//     A bounded exact cache answers the rediscoveries without touching the disk
//     and the primary key resolves whatever falls through it.
//   - Claim buffering. Pop refills from one query that claims ClaimSize rows at
//     once, then serves from memory. The pop rate is therefore a memory
//     operation with a query amortised over hundreds of it.
//   - Politeness write back. A claimed URL whose host is inside its delay is
//     written back with next_at set to when the host is next eligible, so it
//     stops being a candidate until then rather than being rescanned.
//
// A Frontier is safe for concurrent use.
type Frontier struct {
	mu    sync.Mutex
	db    *sql.DB
	cfg   FrontierConfig
	delay int64 // politeness delay in seconds

	seen    *seenCache
	pending []FrontierEntry // admissions not yet written
	ready   []FrontierEntry // claimed rows not yet handed out
	readyAt int             // how far into ready the scan has walked
	defer_  []FrontierEntry // claimed rows going back because of politeness

	finished []finishedURL // completions not yet written

	hosts      map[string]*list.Element // host -> LRU element holding hostAt
	hostLRU    *list.List
	dirtyHosts map[string]int64 // politeness clocks not yet written

	staged int64 // rows in the staging table, not yet merged
	queued int64 // pending rows, tracked rather than counted
	stats  FrontierStats
}

// finishedURL is one buffered completion.
type finishedURL struct {
	hash  []byte
	state int
}

// hostAt is one host's next eligible time, held in the LRU.
type hostAt struct {
	host   string
	nextAt int64
}

// NewFrontier opens an in memory frontier with the given politeness delay.
// It is the small case: a test, or a crawl that fits in one process lifetime.
func NewFrontier(delay time.Duration) *Frontier {
	f, err := OpenFrontier(FrontierConfig{Delay: delay})
	if err != nil {
		// An in memory SQLite database with a fixed schema does not fail to
		// open for any reason a caller could handle.
		panic(fmt.Sprintf("ccrawl: in memory frontier: %v", err))
	}
	return f
}

// OpenFrontier opens or creates a frontier at cfg.Path.
//
// Reopening an existing file resumes it. Rows a previous run left claimed are
// returned to pending, because a claim that outlived its process is a URL that
// was fetched or was not, and the only safe reading is that it was not. Rows
// already marked done stay done, which is what stops a restart refetching.
func OpenFrontier(cfg FrontierConfig) (*Frontier, error) {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 10000
	}
	if cfg.ClaimSize <= 0 {
		cfg.ClaimSize = 512
	}
	if cfg.HotHosts <= 0 {
		cfg.HotHosts = 1 << 16
	}
	if cfg.SeenURLs <= 0 {
		cfg.SeenURLs = 1 << 21
	}
	if cfg.MergeSize <= 0 {
		cfg.MergeSize = 1 << 20
	}
	if cfg.SyncEvery <= 0 {
		cfg.SyncEvery = cfg.ClaimSize
	}
	if cfg.CacheMB <= 0 {
		cfg.CacheMB = 128
	}

	dsn := cfg.Path
	if dsn == "" {
		dsn = ":memory:"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open frontier %s: %w", dsn, err)
	}
	// One connection, because an in memory database is per connection and
	// because a frontier is written by one process that already serialises
	// itself behind a mutex. WAL and NORMAL are the usual pair: a crash can
	// lose the last transaction, which for a frontier means recrawling a few
	// URLs, and that is a much better trade than an fsync per batch.
	db.SetMaxOpenConns(1)
	// wal_autocheckpoint is the one that matters. The default checkpoints every
	// 1000 pages, which on a write heavy frontier means copying the WAL back
	// into the database several times a second and costs about five times the
	// throughput. Thirty two megabytes of WAL is a cheap thing to trade for it.
	if _, err := db.Exec(fmt.Sprintf(`
		PRAGMA journal_mode=WAL;
		PRAGMA synchronous=NORMAL;
		PRAGMA temp_store=MEMORY;
		PRAGMA cache_size=-%d;
		PRAGMA wal_autocheckpoint=8192;`, cfg.CacheMB*1024) + `
		CREATE TABLE IF NOT EXISTS frontier (
			url_hash BLOB PRIMARY KEY,
			url      TEXT    NOT NULL,
			host     TEXT    NOT NULL,
			priority REAL    NOT NULL DEFAULT 0,
			depth    INTEGER NOT NULL DEFAULT 0,
			retries  INTEGER NOT NULL DEFAULT 0,
			next_at  INTEGER NOT NULL DEFAULT 0,
			state    INTEGER NOT NULL DEFAULT 0
		) WITHOUT ROWID;
		-- Priority before next_at on purpose. The claim query wants the highest
		-- priority eligible rows, and an index that sorts by next_at first makes
		-- SQLite sort every pending row on every refill, which is quadratic over
		-- a crawl. This order lets it walk in priority order and stop at the
		-- limit.
		CREATE INDEX IF NOT EXISTS frontier_claim ON frontier(state, priority DESC, next_at);
		-- Admissions land here first and are merged in url_hash order. A rowid
		-- table with no index on it, so writing to it is an append.
		CREATE TABLE IF NOT EXISTS stage (
			seq      INTEGER PRIMARY KEY,
			url_hash BLOB    NOT NULL,
			url      TEXT    NOT NULL,
			host     TEXT    NOT NULL,
			priority REAL    NOT NULL DEFAULT 0,
			depth    INTEGER NOT NULL DEFAULT 0,
			retries  INTEGER NOT NULL DEFAULT 0,
			next_at  INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS hosts (
			host    TEXT    PRIMARY KEY,
			next_at INTEGER NOT NULL DEFAULT 0
		) WITHOUT ROWID;
	`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("frontier schema: %w", err)
	}

	f := &Frontier{
		db:         db,
		cfg:        cfg,
		delay:      int64(cfg.Delay / time.Second),
		seen:       newSeenCache(cfg.SeenURLs),
		hosts:      make(map[string]*list.Element),
		hostLRU:    list.New(),
		dirtyHosts: make(map[string]int64),
	}

	res, err := db.Exec(`UPDATE frontier SET state = ? WHERE state = ?`, frontierPending, frontierClaimed)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("reclaim in flight rows: %w", err)
	}
	if n, err := res.RowsAffected(); err == nil {
		f.stats.Reclaimed = n
	}
	// One count at open, then tracked by delta. Counting 280M rows on every
	// call to Len would make the progress line the slowest thing in the crawl.
	if err := db.QueryRow(`SELECT count(*) FROM frontier WHERE state = ?`, frontierPending).Scan(&f.queued); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("count pending: %w", err)
	}
	// A run that died between a stage and a merge left rows here. They are
	// admissions that were never deduped, so the merge on the next flush picks
	// them up as though nothing had happened.
	if err := db.QueryRow(`SELECT count(*) FROM stage`).Scan(&f.staged); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("count staged: %w", err)
	}
	return f, nil
}

// Close flushes pending admissions and closes the database.
func (f *Frontier) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.flushLocked()
	if merr := f.mergeLocked(); err == nil {
		err = merr
	}
	if derr := f.flushFinishedLocked(); err == nil {
		err = derr
	}
	if herr := f.flushHostsLocked(); err == nil {
		err = herr
	}
	// Only the part of the claim buffer nothing has looked at yet, plus the
	// write backs. Anything before readyAt was handed to a caller and is that
	// caller's business, not the frontier's.
	if rerr := f.releaseLocked(append(f.ready[f.readyAt:], f.defer_...)); err == nil {
		err = rerr
	}
	f.ready, f.readyAt, f.defer_ = nil, 0, nil
	if cerr := f.db.Close(); err == nil {
		err = cerr
	}
	return err
}

// Add admits a URL. It reports whether the URL looks new.
//
// The answer is exact in one direction and provisional in the other. False
// means the seen cache has this URL and nothing is written, which is a fact
// rather than a guess. True means the cache has not got it, so the row is
// buffered and the primary key decides at flush time, and a URL the cache had
// forgotten is counted as a duplicate then. Callers that want the exact totals
// read FrontierStats after a Flush.
//
// No return value drops a URL. That is the property worth stating plainly,
// because the usual structure in this position is a Bloom filter, and a Bloom
// filter here would discard genuinely new URLs at its false positive rate.
func (f *Frontier) Add(e FrontierEntry) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.seen.add(seenKey(e.URL)) {
		f.stats.Duplicates++
		f.stats.Filtered++
		return false
	}
	f.pending = append(f.pending, e)
	if len(f.pending) >= f.cfg.BatchSize {
		if err := f.flushLocked(); err != nil {
			return true
		}
		if f.staged >= f.mergeThreshold() {
			_ = f.mergeLocked()
		}
	}
	return true
}

// Flush stages buffered admissions and merges them into the frontier, so that
// afterwards every admitted URL is queryable and Stats is exact. Call it when
// the seeding is done or before reading Stats. Add stages on its own every
// BatchSize URLs and merges every MergeSize, so a long seed load does not need
// this at all until the end.
func (f *Frontier) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushLocked(); err != nil {
		return err
	}
	return f.mergeLocked()
}

// flushLocked appends the buffer to the staging table.
//
// Appends, not inserts into the frontier. This is the whole reason the staging
// table exists: url_hash order has nothing to do with the order URLs are
// discovered in, so inserting straight into the frontier writes to a random
// page of a B-tree that is far larger than any page cache, and the admit rate
// falls off a cliff as the table grows. Measured on this design, four million
// rows went in at under 3k a second and still slowing, against 170k a second
// for the first two hundred thousand. Staging is a rowid table written in
// increasing rowid order, which is an append to the right hand edge and costs
// the same at four million rows as at four hundred.
func (f *Frontier) flushLocked() error {
	if len(f.pending) == 0 {
		return nil
	}
	tx, err := f.db.Begin()
	if err != nil {
		return fmt.Errorf("frontier flush: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare(`INSERT INTO stage
		(url_hash, url, host, priority, depth, retries, next_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("frontier flush: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, e := range f.pending {
		if _, err := stmt.Exec(urlHashBytes(e.URL), e.URL, e.Host, e.Priority, e.Depth, e.Retries, e.NextAt); err != nil {
			return fmt.Errorf("frontier stage %s: %w", e.URL, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("frontier flush: %w", err)
	}
	f.staged += int64(len(f.pending))
	f.pending = f.pending[:0]
	return nil
}

// mergeThreshold is how many staged rows are worth a merge.
//
// A sweep costs a pass over the whole frontier, so a fixed threshold makes the
// total merge cost grow with the square of the crawl: ten merges into a table
// that is ten times bigger by the end. Scaling the threshold with what is
// already queued keeps each sweep carrying a fixed fraction of the table, which
// turns that into a handful of sweeps over the life of a crawl.
func (f *Frontier) mergeThreshold() int64 {
	n := int64(f.cfg.MergeSize)
	if eighth := f.queued / 8; eighth > n {
		return eighth
	}
	return n
}

// mergeLocked moves the staging table into the frontier in url_hash order.
//
// One ORDER BY turns a pile of random writes into a single left to right sweep
// of the frontier's B-tree, so every leaf page is read and dirtied once instead
// of once per row that happens to land on it. The sweep costs the same whether
// it carries ten thousand rows or ten million, which is why the merge is worth
// deferring until MergeSize rows have piled up rather than running per batch.
//
// The dedup is here too, and it is exact: INSERT OR IGNORE against the primary
// key resolves both URLs already in the frontier and URLs staged twice.
func (f *Frontier) mergeLocked() error {
	if f.staged == 0 {
		return nil
	}
	tx, err := f.db.Begin()
	if err != nil {
		return fmt.Errorf("frontier merge: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.Exec(`INSERT OR IGNORE INTO frontier
		(url_hash, url, host, priority, depth, retries, next_at, state)
		SELECT url_hash, url, host, priority, depth, retries, next_at, ?
		FROM stage ORDER BY url_hash`, frontierPending)
	if err != nil {
		return fmt.Errorf("frontier merge: %w", err)
	}
	admitted, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("frontier merge: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM stage`); err != nil {
		return fmt.Errorf("frontier merge: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("frontier merge: %w", err)
	}
	dups := f.staged - admitted
	f.staged = 0
	f.stats.Admitted += admitted
	f.stats.Duplicates += dups
	f.queued += admitted
	return nil
}

// Len returns the number of URLs waiting to be crawled, counting buffered
// admissions and claimed rows nothing has been handed yet.
//
// The counter is maintained rather than queried. Counting hundreds of millions
// of rows would make the progress line the slowest thing in the crawl, and a
// crawler asks how much is left far more often than it needs the answer to be
// exact to the row.
func (f *Frontier) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	// queued still counts rows sitting in the claim buffer, since a pop is what
	// decrements it, so adding the buffer here would count it twice. Staged rows
	// have not been deduped yet, so this is an upper bound until the next merge.
	return int(f.queued) + int(f.staged) + len(f.pending)
}

// Stats returns the counters. Admitted and Duplicates only count URLs that have
// been merged, so read them after a Flush.
func (f *Frontier) Stats() FrontierStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats
}

// Pop returns the highest priority URL that is eligible to crawl at now, and
// records the fetch against its host's politeness clock.
//
// A false second return means nothing is eligible yet, which is a wait rather
// than an end: check Len to tell "come back in a moment" from "finished".
func (f *Frontier) Pop(now int64) (FrontierEntry, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for {
		// readyAt walks the claim buffer once. An entry it steps over was either
		// handed out or written back, so there is nothing to compact and the scan
		// stays O(1) per pop rather than O(claim size).
		for f.readyAt < len(f.ready) {
			e := f.ready[f.readyAt]
			f.readyAt++
			at := f.hostNextAt(e.Host)
			if now < at || now < e.NextAt {
				e.NextAt = maxInt64(at, e.NextAt)
				f.defer_ = append(f.defer_, e)
				continue
			}
			f.setHostNextAt(e.Host, now+f.delay)
			f.stats.Claimed++
			f.queued--
			return e, true, nil
		}
		f.ready, f.readyAt = f.ready[:0], 0
		if err := f.releaseLocked(f.defer_); err != nil {
			return FrontierEntry{}, false, err
		}
		f.defer_ = f.defer_[:0]

		// Buffered admissions have to land before the query looks, or a crawler
		// that adds an outlink and immediately pops would be told the frontier
		// is empty for as long as it takes to fill a batch.
		if err := f.flushLocked(); err != nil {
			return FrontierEntry{}, false, err
		}
		n, err := f.refillLocked(now)
		if err != nil {
			return FrontierEntry{}, false, err
		}
		if n == 0 {
			return FrontierEntry{}, false, nil
		}
	}
}

// refillLocked claims a batch of eligible rows and returns how many it got.
//
// Claiming marks the rows so a second worker, or a second process pointed at
// the same file, cannot hand out the same URL. It is also what makes a crash
// recoverable: whatever is still marked claimed when the process dies is
// exactly the set the next run has to put back.
func (f *Frontier) refillLocked(now int64) (int, error) {
	ctx := context.Background()
	// Completions first. A row that finished has to stop being claimed before
	// the next claim query runs, or the frontier keeps a growing tail of rows
	// that are done in memory and in flight on disk.
	if err := f.flushFinishedLocked(); err != nil {
		return 0, err
	}
	if err := f.flushHostsLocked(); err != nil {
		return 0, err
	}
	rows, err := f.db.QueryContext(ctx, `SELECT url, host, priority, depth, retries, next_at
		FROM frontier WHERE state = ? AND next_at <= ?
		ORDER BY priority DESC LIMIT ?`,
		frontierPending, now, f.cfg.ClaimSize)
	if err != nil {
		return 0, fmt.Errorf("frontier refill: %w", err)
	}
	var batch []FrontierEntry
	for rows.Next() {
		var e FrontierEntry
		var depth, retries int64
		if err := rows.Scan(&e.URL, &e.Host, &e.Priority, &depth, &retries, &e.NextAt); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("frontier refill: %w", err)
		}
		e.Depth, e.Retries = uint8(depth), uint8(retries)
		batch = append(batch, e)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("frontier refill: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("frontier refill: %w", err)
	}
	if len(batch) == 0 {
		// Nothing eligible on disk. If admissions are still sitting in staging,
		// merge them and look again, so a crawl that is adding as fast as it pops
		// never reports itself finished with work in hand.
		if f.staged > 0 {
			if err := f.mergeLocked(); err != nil {
				return 0, err
			}
			return f.refillLocked(now)
		}
		return 0, nil
	}

	hashes := make([]any, len(batch))
	for i, e := range batch {
		hashes[i] = urlHashBytes(e.URL)
	}
	args := append([]any{frontierClaimed}, hashes...)
	q := fmt.Sprintf(`UPDATE frontier SET state = ? WHERE url_hash IN (%s)`,
		strings.TrimSuffix(strings.Repeat("?,", len(hashes)), ","))
	if _, err := f.db.ExecContext(ctx, q, args...); err != nil {
		return 0, fmt.Errorf("frontier claim: %w", err)
	}

	// Highest priority first, so the in memory scan hands out in the same order
	// the query chose rather than in whatever order politeness leaves behind.
	sort.SliceStable(batch, func(i, j int) bool { return batch[i].Priority > batch[j].Priority })
	f.ready = append(f.ready, batch...)
	return len(batch), nil
}

// releaseLocked puts claimed rows back as pending, carrying whatever next_at
// politeness decided for them.
func (f *Frontier) releaseLocked(entries []FrontierEntry) error {
	if len(entries) == 0 {
		return nil
	}
	tx, err := f.db.Begin()
	if err != nil {
		return fmt.Errorf("frontier release: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.Prepare(`UPDATE frontier SET state = ?, next_at = ? WHERE url_hash = ?`)
	if err != nil {
		return fmt.Errorf("frontier release: %w", err)
	}
	defer func() { _ = stmt.Close() }()
	for _, e := range entries {
		if e.URL == "" {
			continue
		}
		if _, err := stmt.Exec(frontierPending, e.NextAt, urlHashBytes(e.URL)); err != nil {
			return fmt.Errorf("frontier release %s: %w", e.URL, err)
		}
		f.stats.Deferred++
	}
	return tx.Commit()
}

// Done marks a URL as crawled. It is what a restart reads to know not to fetch
// the URL again.
//
// The write is buffered up to SyncEvery completions, so it is exact across a
// clean Close and bounded across a kill: at most SyncEvery URLs get fetched a
// second time by the run that follows one. Buffering is worth roughly four
// times the pop rate, and a caller who would rather have the exactness sets
// SyncEvery to 1.
func (f *Frontier) Done(url string) error { return f.finish(url, frontierDone) }

// Fail marks a URL as given up on. Same effect as Done for the crawl, kept
// apart so a run can report the difference.
func (f *Frontier) Fail(url string) error { return f.finish(url, frontierFailed) }

func (f *Frontier) finish(url string, state int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.finished = append(f.finished, finishedURL{hash: urlHashBytes(url), state: state})
	if len(f.finished) >= f.cfg.SyncEvery {
		return f.flushFinishedLocked()
	}
	return nil
}

// Sync writes every buffered completion. A caller that has just been asked to
// stop, or that is about to do something it cannot undo, calls this to make the
// frontier's idea of what is done match its own.
func (f *Frontier) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.flushFinishedLocked()
}

func (f *Frontier) flushFinishedLocked() error {
	if len(f.finished) == 0 {
		return nil
	}
	tx, err := f.db.Begin()
	if err != nil {
		return fmt.Errorf("frontier finish: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.Prepare(`UPDATE frontier SET state = ? WHERE url_hash = ?`)
	if err != nil {
		return fmt.Errorf("frontier finish: %w", err)
	}
	defer func() { _ = stmt.Close() }()
	// Same reason the merge sorts: completions arrive in priority order, which
	// is unrelated to where the rows live, and updating in key order walks the
	// B-tree once instead of jumping around it.
	sort.Slice(f.finished, func(i, j int) bool {
		return bytes.Compare(f.finished[i].hash, f.finished[j].hash) < 0
	})
	for _, d := range f.finished {
		if _, err := stmt.Exec(d.state, d.hash); err != nil {
			return fmt.Errorf("frontier finish: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("frontier finish: %w", err)
	}
	f.finished = f.finished[:0]
	return nil
}

// Retry puts a URL back in the queue at a later time with its retry count
// bumped, which is what a transient fetch failure deserves.
func (f *Frontier) Retry(e FrontierEntry, at int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Straight through rather than buffered. A retry is rare next to a
	// completion, and a lost one is a URL dropped from the crawl rather than a
	// URL fetched twice.
	if _, err := f.db.Exec(`UPDATE frontier SET state = ?, next_at = ?, retries = retries + 1 WHERE url_hash = ?`,
		frontierPending, at, urlHashBytes(e.URL)); err != nil {
		return fmt.Errorf("frontier retry %s: %w", e.URL, err)
	}
	f.queued++
	return nil
}

// hostNextAt reads a host's next eligible time, from the LRU when it is hot,
// from the unflushed writes when it is not, and from the database otherwise. A
// crawl touches a small set of hosts hard and a long tail once, which is the
// access pattern an LRU is for.
func (f *Frontier) hostNextAt(host string) int64 {
	if el, ok := f.hosts[host]; ok {
		f.hostLRU.MoveToFront(el)
		return el.Value.(*hostAt).nextAt
	}
	if at, ok := f.dirtyHosts[host]; ok {
		f.cacheHost(host, at)
		return at
	}
	var at int64
	_ = f.db.QueryRow(`SELECT next_at FROM hosts WHERE host = ?`, host).Scan(&at)
	f.cacheHost(host, at)
	return at
}

// setHostNextAt records a fetch against a host.
//
// The write is buffered rather than issued, because a statement per pop is the
// one thing that would put the pop rate back where SQLite can be seen. The
// buffer is drained once per refill, so a kill loses at most one claim batch of
// host timings; the cost of that is a restart being a little eager with the
// hosts it was mid delay on, which is a much smaller price than a write per
// URL.
func (f *Frontier) setHostNextAt(host string, at int64) {
	if el, ok := f.hosts[host]; ok {
		el.Value.(*hostAt).nextAt = at
		f.hostLRU.MoveToFront(el)
	} else {
		f.cacheHost(host, at)
	}
	f.dirtyHosts[host] = at
}

// flushHostsLocked writes buffered politeness clocks in one transaction.
func (f *Frontier) flushHostsLocked() error {
	if len(f.dirtyHosts) == 0 {
		return nil
	}
	tx, err := f.db.Begin()
	if err != nil {
		return fmt.Errorf("frontier hosts: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.Prepare(`INSERT INTO hosts (host, next_at) VALUES (?, ?)
		ON CONFLICT(host) DO UPDATE SET next_at = excluded.next_at`)
	if err != nil {
		return fmt.Errorf("frontier hosts: %w", err)
	}
	defer func() { _ = stmt.Close() }()
	for host, at := range f.dirtyHosts {
		if _, err := stmt.Exec(host, at); err != nil {
			return fmt.Errorf("frontier host %s: %w", host, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("frontier hosts: %w", err)
	}
	clear(f.dirtyHosts)
	return nil
}

func (f *Frontier) cacheHost(host string, at int64) {
	el := f.hostLRU.PushFront(&hostAt{host: host, nextAt: at})
	f.hosts[host] = el
	for f.hostLRU.Len() > f.cfg.HotHosts {
		old := f.hostLRU.Back()
		f.hostLRU.Remove(old)
		delete(f.hosts, old.Value.(*hostAt).host)
	}
}

// urlHashBytes is the frontier's primary key: the first 16 bytes of the URL's
// SHA-1. Sixteen bytes is 128 bits, so a collision across the 280M URLs this
// is sized for is not something that happens, and it is four bytes shorter per
// row than the full digest across a table with hundreds of millions of them.
func urlHashBytes(rawURL string) []byte {
	sum := sha1.Sum([]byte(rawURL))
	return sum[:16]
}

// seenKey folds a URL into the 64 bits the seen cache holds.
//
// A collision here would drop a URL, the same way a Bloom filter false positive
// would, so the number matters. The cache holds a few million keys at a time,
// not the whole frontier, and the odds of any collision among four million 64
// bit hashes are about one in two million. That is six orders of magnitude
// below the one percent a conventionally sized filter would cost, which is the
// difference between a bound worth stating and a bound worth avoiding.
func seenKey(rawURL string) uint64 {
	sum := sha1.Sum([]byte(rawURL))
	return binary.BigEndian.Uint64(sum[:8])
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

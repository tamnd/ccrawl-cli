package ccrawl

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
)

// Defaults for the batch fetch. The gap is what makes the batching worth doing:
// two records 200 KiB apart in the same WARC file are cheaper as one GET that
// reads the 200 KiB between them than as two GETs, because a request costs a
// round trip and the bytes in between arrive at line rate. A megabyte is where
// that stops being true on a home connection, and the span cap keeps one group
// from growing until it will not fit in memory.
const (
	DefaultFetchGap     int64 = 1 << 20
	DefaultFetchMaxSpan int64 = 16 << 20
)

// LocationGroup is a run of locations in one WARC file that sit close enough
// together to be worth a single ranged GET.
type LocationGroup struct {
	Filename string
	Start    int64 // offset of the first record
	End      int64 // one past the last byte of the last record
	Locs     []Location

	// Idx is the position each of Locs held in the input, parallel to it, kept
	// so input order can be put back after the sort that made the grouping
	// possible.
	Idx []int

	// First is the smallest input index in the group, which is what orders the
	// groups when the caller asked for input order.
	First int
}

// Span is how many bytes the one GET for this group reads.
func (g LocationGroup) Span() int64 { return g.End - g.Start }

// GroupLocations sorts locations by file and offset and coalesces neighbours
// into groups. Two records join the same group when they are in the same file
// and the hole between them is at most gap bytes, and a group is closed early
// rather than let its span pass maxSpan.
//
// Duplicates are kept. The same record asked for twice is fetched once and
// handed back twice, which is what the one at a time path does as well.
func GroupLocations(locs []Location, gap, maxSpan int64) []LocationGroup {
	if len(locs) == 0 {
		return nil
	}
	if gap < 0 {
		gap = 0
	}
	if maxSpan <= 0 {
		maxSpan = DefaultFetchMaxSpan
	}

	// The input index rides along so input order can be restored later. Sorting
	// is stable on it, so two locations at the same offset keep their order.
	type indexed struct {
		loc Location
		i   int
	}
	sorted := make([]indexed, len(locs))
	for i, l := range locs {
		sorted[i] = indexed{loc: l, i: i}
	}
	sort.SliceStable(sorted, func(a, b int) bool {
		if sorted[a].loc.Filename != sorted[b].loc.Filename {
			return sorted[a].loc.Filename < sorted[b].loc.Filename
		}
		return sorted[a].loc.Offset < sorted[b].loc.Offset
	})

	var groups []LocationGroup
	cur := LocationGroup{
		Filename: sorted[0].loc.Filename,
		Start:    sorted[0].loc.Offset,
		End:      sorted[0].loc.Offset + sorted[0].loc.Length,
		Locs:     []Location{sorted[0].loc},
		Idx:      []int{sorted[0].i},
		First:    sorted[0].i,
	}
	for _, it := range sorted[1:] {
		end := it.loc.Offset + it.loc.Length
		// A record fully inside the group so far costs nothing to add, whatever
		// the span rule says, so the hole is measured against the current end
		// and never goes negative.
		hole := it.loc.Offset - cur.End
		if hole < 0 {
			hole = 0
		}
		newEnd := cur.End
		if end > newEnd {
			newEnd = end
		}
		if it.loc.Filename == cur.Filename && hole <= gap && newEnd-cur.Start <= maxSpan {
			cur.End = newEnd
			cur.Locs = append(cur.Locs, it.loc)
			cur.Idx = append(cur.Idx, it.i)
			if it.i < cur.First {
				cur.First = it.i
			}
			continue
		}
		groups = append(groups, cur)
		cur = LocationGroup{
			Filename: it.loc.Filename,
			Start:    it.loc.Offset,
			End:      end,
			Locs:     []Location{it.loc},
			Idx:      []int{it.i},
			First:    it.i,
		}
	}
	return append(groups, cur)
}

// FetchGroup reads a group's whole span in one ranged GET and returns the record
// for each location in it.
//
// Each record is parsed from exactly the bytes the one at a time path would have
// asked for, sliced out of the span, so the result is the same records rather
// than merely similar ones. A location whose own bytes do not parse fails on its
// own and leaves the rest of the group alone.
func FetchGroup(ctx context.Context, h *HTTPClient, g LocationGroup) ([]WARCRecord, []error, error) {
	resp, err := h.GetRange(ctx, h.DataURL(g.Filename), g.Start, g.Span())
	if err != nil {
		return nil, nil, fmt.Errorf("range GET: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	span, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("read range: %w", err)
	}

	recs := make([]WARCRecord, len(g.Locs))
	errs := make([]error, len(g.Locs))
	for i, loc := range g.Locs {
		lo := loc.Offset - g.Start
		hi := lo + loc.Length
		if lo < 0 || hi > int64(len(span)) {
			errs[i] = fmt.Errorf("range %d+%d of %s is outside the %d bytes the server returned",
				loc.Offset, loc.Length, loc.Filename, len(span))
			continue
		}
		rec, err := parseOneRecord(span[lo:hi])
		if err != nil {
			errs[i] = fmt.Errorf("%s at %d: %w", loc.Filename, loc.Offset, err)
			continue
		}
		recs[i] = rec
	}
	return recs, errs, nil
}

// parseOneRecord reads the first WARC record out of one record's bytes. It is
// the tail of FetchWARCRecord with the transport taken off, so both paths decode
// the same way.
func parseOneRecord(b []byte) (WARCRecord, error) {
	var out WARCRecord
	found := false
	err := IterateWARC(bytes.NewReader(b), func(r WARCRecord) error {
		out = r
		found = true
		return errStop
	})
	if err != nil && !errors.Is(err, errStop) {
		return WARCRecord{}, err
	}
	if !found {
		return WARCRecord{}, errors.New("no WARC record in the range")
	}
	return out, nil
}

// BatchFetchConfig configures a batched ranged fetch.
type BatchFetchConfig struct {
	Locations []Location
	Gap       int64 // coalesce records this close together, 0 for adjacent only
	MaxSpan   int64 // never read more than this in one GET
	Workers   int
	InOrder   bool // emit in input order rather than in the order records sit on disk

	// Window is how many groups may be fetched ahead of the one being written
	// out. Under file order it is a hard cap on what is held in memory. Under
	// input order it only bounds the fetches in flight; see RunBatchFetch.
	Window int

	// Ledger, when set, skips locations it already holds and records each one as
	// it is emitted, so a killed run resumes where it stopped.
	Ledger *KeyLedger

	// OnRecord receives every fetched record in the chosen order. Returning an
	// error stops the run and is what the run returns.
	OnRecord func(Location, WARCRecord) error

	// OnError receives every location that could not be fetched. A nil OnError
	// makes a failure fatal, which is not what a million location run wants.
	OnError func(Location, error)

	// Progress, when set, is told about each record and each failure.
	Progress *StreamProgress
}

// BatchFetchStats is what a run did, and is the evidence for the batching being
// worth anything: Requests against Records is the ratio the whole feature is for.
type BatchFetchStats struct {
	Records  int   // records handed to OnRecord
	Failed   int   // locations that could not be fetched
	Skipped  int   // locations the ledger had already
	Requests int   // ranged GETs issued
	Bytes    int64 // bytes the ranges covered, holes included
}

// groupResult is one group's worth of fetching, done or failed.
type groupResult struct {
	g    LocationGroup
	recs []WARCRecord
	errs []error
	err  error // the whole GET failed, so every location in the group did
}

// RunBatchFetch fetches every location, coalescing neighbours into shared ranged
// GETs and dispatching those across a worker pool.
//
// File order, the default, streams: groups are written out in the order they sit
// on disk and nothing more than Window groups is ever held. Input order has to
// put back an order the grouping destroyed, so it holds each finished record
// until its turn comes. On an input that is already roughly file ordered that
// costs almost nothing; on one that is not, it can hold a large part of the run,
// which is the price of asking for it and the reason file order is the default.
func RunBatchFetch(ctx context.Context, h *HTTPClient, cfg BatchFetchConfig) (BatchFetchStats, error) {
	var stats BatchFetchStats

	todo := cfg.Locations
	if cfg.Ledger != nil {
		kept := todo[:0:0]
		for _, l := range todo {
			if cfg.Ledger.Has(LocationKey(l)) {
				stats.Skipped++
				continue
			}
			kept = append(kept, l)
		}
		todo = kept
	}
	if len(todo) == 0 {
		return stats, nil
	}

	groups := GroupLocations(todo, cfg.Gap, cfg.MaxSpan)
	if cfg.InOrder {
		// Running the groups in the order their earliest member arrived is what
		// keeps the reorder buffer small when the input was already roughly
		// sorted. It is not needed for correctness, only for the memory.
		sort.SliceStable(groups, func(a, b int) bool { return groups[a].First < groups[b].First })
	}

	workers := cfg.Workers
	if workers < 1 {
		workers = 1
	}
	window := cfg.Window
	if window < 1 {
		window = 1
	}

	e := &batchEmitter{cfg: cfg, stats: &stats}
	if cfg.InOrder {
		e.pending = make(map[int]pendingRecord, len(todo))
		e.total = len(todo)
	}

	// One goroutine writes, so OnRecord is never called from two places at once
	// and the callback needs no locking of its own.
	slots := make([]chan groupResult, len(groups))
	for i := range slots {
		slots[i] = make(chan groupResult, 1)
	}

	fetchCtx, stopFetching := context.WithCancel(ctx)
	defer stopFetching()

	next := make(chan int)
	go func() {
		defer close(next)
		for i := range groups {
			select {
			case next <- i:
			case <-fetchCtx.Done():
				return
			}
		}
	}()

	// A worker takes a token before fetching and the writer returns it after
	// reading the result, so no more than window groups are ever in flight.
	tokens := make(chan struct{}, window)
	for i := 0; i < window; i++ {
		tokens <- struct{}{}
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				select {
				case <-tokens:
				case <-fetchCtx.Done():
					return
				}
				recs, errs, err := FetchGroup(fetchCtx, h, groups[i])
				slots[i] <- groupResult{g: groups[i], recs: recs, errs: errs, err: err}
			}
		}()
	}

	var emitErr error
	for i := range groups {
		var r groupResult
		select {
		case r = <-slots[i]:
		case <-ctx.Done():
			emitErr = ctx.Err()
		}
		if emitErr != nil {
			break
		}
		tokens <- struct{}{}

		stats.Requests++
		stats.Bytes += r.g.Span()
		if emitErr = e.take(r); emitErr != nil {
			break
		}
		// The ledger is flushed once per group rather than once per record. A
		// kill loses at most the group in flight, and a run of a million
		// records does not pay a million fsyncs for it.
		if emitErr = cfg.Ledger.Sync(); emitErr != nil {
			break
		}
	}

	// Every slot is buffered, so a worker whose group nobody will read still
	// finishes its send and exits. Cancelling first releases the ones waiting
	// on a token the writer will never return.
	stopFetching()
	wg.Wait()
	return stats, emitErr
}

// pendingRecord is one finished location waiting for its turn under input order.
type pendingRecord struct {
	loc Location
	rec WARCRecord
	err error
}

// batchEmitter writes finished groups out, in file order as they arrive or in
// input order once the gaps have filled in.
type batchEmitter struct {
	cfg   BatchFetchConfig
	stats *BatchFetchStats

	// pending and cursor are the reorder buffer, used only under input order.
	pending map[int]pendingRecord
	cursor  int
	total   int
}

// take accepts one finished group and writes out whatever is now writable.
func (e *batchEmitter) take(r groupResult) error {
	for j, loc := range r.g.Locs {
		err := r.err
		var rec WARCRecord
		if err == nil {
			err = r.errs[j]
			rec = r.recs[j]
		}
		if e.pending == nil {
			if wErr := e.write(loc, rec, err); wErr != nil {
				return wErr
			}
			continue
		}
		e.pending[r.g.Idx[j]] = pendingRecord{loc: loc, rec: rec, err: err}
	}
	if e.pending == nil {
		return nil
	}
	for e.cursor < e.total {
		p, ok := e.pending[e.cursor]
		if !ok {
			break
		}
		delete(e.pending, e.cursor)
		e.cursor++
		if err := e.write(p.loc, p.rec, p.err); err != nil {
			return err
		}
	}
	return nil
}

// write hands one location's outcome to the callbacks and counts it.
func (e *batchEmitter) write(loc Location, rec WARCRecord, err error) error {
	if err != nil {
		e.stats.Failed++
		e.cfg.Progress.Fail()
		if e.cfg.OnError == nil {
			return err
		}
		e.cfg.OnError(loc, err)
		return nil
	}
	if err := e.cfg.OnRecord(loc, rec); err != nil {
		return err
	}
	e.stats.Records++
	e.cfg.Progress.Add(1, 1, int64(len(rec.Block)))
	return e.cfg.Ledger.Mark(LocationKey(loc))
}

package ccrawl

import (
	"net/url"
	"sync"
)

// Keeping a resumable position without making the pool wait for one.
//
// A recrawl has to be able to say "start again here" after a kill, and the cheap
// way to buy that is a barrier: hand out a batch, wait for all of it, write the
// position down. It is cheap in code and ruinous in throughput, because the
// barrier is only as fast as the slowest item under it. Measured on the live
// domain list with 256 workers, a 600 page run spent its last 92 seconds
// finishing four items with the other 252 workers idle, and reported 87 percent
// of the pool doing nothing across the run.
//
// So the barrier goes and a low water mark takes its place. Items are handed out
// in work list order and each carries its position. An item that finishes is
// retired. The safe position is the oldest item that has not been retired, and
// everything before it is finished by construction, because handing out is
// ordered. A kill resumes there and replays only what was genuinely in flight.
//
// The flight set is the same idea as the barrier in what it promises, which is
// that a resume never skips a row, and the opposite of it in what it costs: the
// pool never waits for anything.

// flight tracks handed-out items that have not finished, so the run can name a
// position it is safe to resume from at any moment.
type flight struct {
	mu   sync.Mutex
	seq  int64               // next sequence number to hand out
	live map[int64]*WorkItem // sequence number to item, for everything in flight
	// past is the position just after the last item retired while nothing else
	// was in flight, which is the safe position when the set is empty.
	past    WorkItem
	pastSet bool
}

func newFlight() *flight { return &flight{live: make(map[int64]*WorkItem)} }

// add records an item as handed out and returns its sequence number.
func (f *flight) add(it WorkItem) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := f.seq
	f.seq++
	copied := it
	f.live[n] = &copied
	return n
}

// retire records an item as finished, meaning it was fetched and written or
// deliberately passed over. An item that was never attempted is left in the set
// on purpose, because the run has to come back to it.
func (f *flight) retire(seq int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	it, ok := f.live[seq]
	if !ok {
		return
	}
	delete(f.live, seq)
	// Remember where the work list had got to, in case this was the last one in
	// flight and the safe position has to come from somewhere.
	if !f.pastSet || it.Part > f.past.Part || (it.Part == f.past.Part && it.Row >= f.past.Row) {
		f.past, f.pastSet = *it, true
	}
}

// safe returns the position to resume from: the oldest item still in flight.
//
// The second return is false when nothing is in flight and nothing has been
// retired either, which is a run that has not started, and the caller should use
// the work list's own position instead. When the set is empty but items have
// been retired, the position is one past the last of them, because that one is
// done and repeating it would be a duplicate for no reason.
func (f *flight) safe() (part int, row int64, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.live) > 0 {
		var oldest int64 = -1
		for seq := range f.live {
			if oldest < 0 || seq < oldest {
				oldest = seq
			}
		}
		it := f.live[oldest]
		return it.Part, it.Row, true
	}
	if f.pastSet {
		return f.past.Part, f.past.Row + 1, true
	}
	return 0, 0, false
}

// stalled reports whether anything handed out is still unfinished, which after a
// run has stopped means it was cut short rather than walked out.
func (f *flight) stalled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.live) > 0
}

// spreadItems reorders work items so consecutive ones belong to different hosts,
// keeping each host's own items in work list order.
//
// The domain work list is one row per host and needs none of this. The URL work
// list is the opposite: it is sorted, so a run of rows shares a host, and the
// live measurement is stark, 120 consecutive URLs from the published index
// covered 7 hosts. Handed to the pool in that order, every worker but a handful
// parks on the politeness clock waiting for the same few sites, and the rate
// collapses to the number of distinct hosts divided by the delay however many
// workers there are. Rotating hosts costs one pass over a slice already in
// memory and is the difference between a wide pool being useful and being
// decoration.
func spreadItems(items []WorkItem) []WorkItem {
	return spreadHosts(items, itemHost)
}

// itemHost is the host a work item will be fetched from, or the raw URL when it
// will not parse, so an unparseable item still lands in one group rather than
// splitting the rotation.
func itemHost(it WorkItem) string {
	u, err := url.Parse(it.URL)
	if err != nil || u.Host == "" {
		return it.URL
	}
	return u.Host
}

// spreadHosts groups a slice by host, keeping each host's entries in their
// original order, then emits one per host round robin until every group is
// drained. The result holds exactly the same entries as the input, interleaved.
//
// It is the reorder buffer from ami's engine with the streaming taken out.
// Everything here reads its work in windows and a window is a slice already in
// memory, so the round robin is a pass over it rather than a bounded buffer with
// two condition variables.
func spreadHosts[T any](items []T, host func(T) string) []T {
	if len(items) < 2 {
		return items
	}
	order := make([]string, 0, len(items)) // first seen host order, for determinism
	groups := make(map[string][]T, len(items))
	for _, it := range items {
		h := host(it)
		if _, seen := groups[h]; !seen {
			order = append(order, h)
		}
		groups[h] = append(groups[h], it)
	}
	if len(order) == 1 || len(order) == len(items) {
		// One host has nothing to rotate, and one host per entry is already
		// spread, which is what the domain work list looks like. Copying either
		// would be busywork.
		return items
	}
	out := make([]T, 0, len(items))
	for len(out) < len(items) {
		for _, h := range order {
			g := groups[h]
			if len(g) == 0 {
				continue
			}
			out = append(out, g[0])
			groups[h] = g[1:]
		}
	}
	return out
}

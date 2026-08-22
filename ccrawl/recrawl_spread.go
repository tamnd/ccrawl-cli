package ccrawl

import "context"

// Reading a work list that is sorted by host.
//
// The domain work list has one row per host, so a batch of it is two thousand
// different hosts and every worker has something to do. The URL work list is the
// opposite: it comes out of Common Crawl's index in SURT order, so it is sorted
// by host and then by path, and every row in a batch belongs to the same site.
//
// That turns the politeness delay into the ceiling. A host gets one request per
// delay no matter how many workers are free, so a window holding one host runs
// at one page a second and the other 31 workers wait. Measured on server3
// against the live URL list at 32 workers, a run fetched 596 pages in ten
// minutes and all 596 came from vkbn.ru.
//
// spreadHosts already rotates a window across the hosts in it, and it is the
// right idea sized wrong: it is handed one batch, and one batch of this list is
// one host, so it finds a single group and hands the batch back untouched. What
// is needed is a window that keeps reading until it has hosts to rotate between,
// which is this file.
//
// The cost is replay. The checkpoint can only name the oldest item that has not
// finished, and holding a site's rows back while the rotation works through
// other sites holds that position back with them, so a kill replays further.
// That is what the row cap bounds: the checkpoint can fall at most a bufferful
// behind, and the default is a few minutes of refetching against a run measured
// in months.

// hostSpread is a reorder buffer between the work list and the pool.
//
// It reads ahead until it holds either enough distinct hosts to keep the pool
// busy or as many rows as it is allowed to hold, then hands items out one host
// at a time round robin. Hosts keep their first seen order and each host keeps
// its rows in work list order, so a run is reordered but not shuffled and two
// runs over the same list hand out the same sequence.
type hostSpread struct {
	wl    *WorkList
	buf   []WorkItem
	hosts int // read ahead until this many distinct hosts are held
	// cap bounds how many rows may be held, and it is a threshold rather than a
	// hard limit: it is tested before a read and a read returns a whole batch,
	// so the buffer can end up holding one batch more than this. Bounding it
	// exactly would mean reading the work list a row at a time, which is a
	// Parquet column scan per row.
	cap int

	order  []string
	groups map[string][]WorkItem
	rows   int
	cur    int
	done   bool
}

// newHostSpread sizes the buffer from the pool it is feeding.
//
// The host target is the worker count, because the thing being bought is one
// host per worker and buying more than that buys nothing. The row cap is a
// multiple of the batch, since the batch is already the unit of replay this run
// accepted, and it is what stops a site with fifty thousand URLs in a row from
// pulling the whole list into memory looking for a second host.
func newHostSpread(wl *WorkList, workers, batch int) *hostSpread {
	if workers < 1 {
		workers = 1
	}
	if batch < 1 {
		batch = 1
	}
	return &hostSpread{
		wl:     wl,
		buf:    make([]WorkItem, batch),
		hosts:  workers,
		cap:    batch * 16,
		groups: make(map[string][]WorkItem),
	}
}

// next hands out the next item, or reports the work list is walked out.
func (s *hostSpread) next(ctx context.Context) (WorkItem, bool, error) {
	if err := s.fill(ctx); err != nil {
		return WorkItem{}, false, err
	}
	if s.rows == 0 {
		return WorkItem{}, false, nil
	}
	if s.cur >= len(s.order) {
		s.cur = 0
	}
	h := s.order[s.cur]
	g := s.groups[h]
	it := g[0]
	if len(g) == 1 {
		// The host is spent. Dropping it from the rotation rather than leaving
		// an empty group behind is what keeps next constant time: a list that
		// kept its dead hosts would be scanned past on every call, and on a work
		// list of two billion rows that list is the whole corpus.
		delete(s.groups, h)
		s.order = append(s.order[:s.cur], s.order[s.cur+1:]...)
	} else {
		s.groups[h] = g[1:]
		s.cur++
	}
	s.rows--
	return it, true, nil
}

// fill reads until there are hosts to rotate between, the buffer is full, or the
// work list ends.
//
// The condition is distinct hosts and not rows held, which is the whole point.
// A read that returns two thousand rows of one site has not made the run any
// wider and the loop goes round again, and a read that returns two thousand
// different sites satisfies it immediately, which is why this costs the domain
// list nothing.
func (s *hostSpread) fill(ctx context.Context) error {
	for !s.done && len(s.order) < s.hosts && s.rows < s.cap {
		n, err := s.wl.Next(ctx, s.buf)
		if err != nil {
			return err
		}
		if n == 0 {
			s.done = true
			return nil
		}
		for _, it := range s.buf[:n] {
			s.push(it)
		}
	}
	return nil
}

// push files one item under its host, remembering when the host was first seen.
func (s *hostSpread) push(it WorkItem) {
	h := itemHost(it)
	if _, seen := s.groups[h]; !seen {
		s.order = append(s.order, h)
	}
	s.groups[h] = append(s.groups[h], it)
	s.rows++
}

// position is the earliest row the run has not handed out yet.
//
// This is why the buffer cannot be a private detail of the feeder. Rows sitting
// in it have already been read off the work list, so the work list's own
// position is past them, and a checkpoint written from that position names rows
// that were read, never fetched, and would never be fetched again. Held rows
// have to hold the position back, and the run is only done when the list is
// walked out and the buffer is empty with it.
//
// The oldest row is the smallest across the heads of the groups, since each
// group keeps its own rows in work list order. That is a scan of at most one
// host per worker, and it happens once a checkpoint rather than once an item.
func (s *hostSpread) position() (part int, row int64, done bool) {
	if s.rows == 0 {
		return s.wl.Position()
	}
	first := true
	for _, h := range s.order {
		g := s.groups[h]
		if len(g) == 0 {
			continue
		}
		it := g[0]
		if first || it.Part < part || (it.Part == part && it.Row < row) {
			part, row, first = it.Part, it.Row, false
		}
	}
	return part, row, false
}

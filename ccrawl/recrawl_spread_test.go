package ccrawl

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

// surtOrderedList builds a work list shaped like the published URL index: sorted
// by host, so every host's rows are consecutive and a small read sees one site.
func surtOrderedList(t *testing.T, hosts, perHost int) *WorkList {
	t.Helper()
	dir := t.TempDir()
	var urls []string
	for h := range hosts {
		for p := range perHost {
			urls = append(urls, fmt.Sprintf("https://site%02d.example/page%03d", h, p))
		}
	}
	writeURLPart(t, filepath.Join(dir, "part-000.parquet"), urls)
	src := WorkSource{Repo: "open-index/ccrawl-urls", Dir: "data/test", Column: "url"}
	w, err := NewWorkList(src, Shard{Count: 1}, nil, Checkpoint{})
	if err != nil {
		t.Fatal(err)
	}
	localParts(w, dir)
	t.Cleanup(func() { w.Close() })
	return w
}

// drainSpread hands out the whole buffer and returns the URLs in the order the
// pool would have seen them.
func drainSpread(t *testing.T, s *hostSpread) []string {
	t.Helper()
	var got []string
	for {
		it, ok, err := s.next(context.Background())
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if !ok {
			return got
		}
		got = append(got, it.URL)
	}
}

// TestSpreadReadsPastOneHostToFindOthers is the measurement this exists for.
//
// A batch of the published URL list is one site, because the index is in SURT
// order and a big site has tens of thousands of rows in a row. Reordering inside
// a batch therefore finds one group and changes nothing, the pool parks on the
// politeness clock, and 32 workers fetch at the rate of one. On server3 against
// the live list that was 596 pages in ten minutes, every one of them from
// vkbn.ru.
//
// The buffer has to keep reading until it holds hosts worth rotating between,
// which is what this checks: with a batch far smaller than one host's run of
// rows, consecutive handouts still come from different sites.
func TestSpreadReadsPastOneHostToFindOthers(t *testing.T) {
	const hosts, perHost, batch, workers = 8, 50, 10, 8
	wl := surtOrderedList(t, hosts, perHost)
	got := drainSpread(t, newHostSpread(wl, workers, batch))

	if len(got) != hosts*perHost {
		t.Fatalf("handed out %d items, want %d: the buffer lost work", len(got), hosts*perHost)
	}

	// Every window the width of the pool should hold as many distinct sites as
	// it can, since that is the number the politeness delay divides into to give
	// the rate.
	//
	// The tail is left out on purpose. A list has only so much variety in it,
	// and once the last few sites are all that is left there is nothing to
	// rotate between however the buffer behaves. What is being checked is that
	// the pool sees the variety while the list still has some.
	worst := hosts
	for i := 0; i+workers <= len(got)/2; i++ {
		seen := map[string]bool{}
		for _, u := range got[i : i+workers] {
			seen[hostOf(u)] = true
		}
		worst = min(worst, len(seen))
	}
	if worst < hosts/2 {
		t.Errorf("some window of %d handouts held only %d distinct hosts, so most of the pool is waiting on the politeness clock", workers, worst)
	}
}

// TestSpreadKeepsEachHostInOrder pins the half of the ordering that is not
// allowed to move.
//
// Rotating between hosts is the point, shuffling within one is not. A site's own
// pages stay in work list order so a resume replays a contiguous run of them,
// and so two runs over the same list hand out the same sequence.
func TestSpreadKeepsEachHostInOrder(t *testing.T) {
	wl := surtOrderedList(t, 6, 20)
	got := drainSpread(t, newHostSpread(wl, 6, 8))

	last := map[string]string{}
	for _, u := range got {
		h := hostOf(u)
		if prev, ok := last[h]; ok && prev >= u {
			t.Fatalf("%s handed out %s after %s, which is out of work list order", h, u, prev)
		}
		last[h] = u
	}
}

// TestSpreadHoldsNoMoreThanItsCap is the bound on how much a kill replays.
//
// The checkpoint can only name the oldest item that has not finished, so rows
// held back in the buffer hold the checkpoint back with them. That is the price
// of the reordering and it has to be a bounded price: without a cap, a site with
// fifty thousand consecutive rows would pull all of them into memory looking for
// a second host, and the checkpoint would sit at the row the buffer started
// from.
func TestSpreadHoldsNoMoreThanItsCap(t *testing.T) {
	const batch, workers = 4, 64 // more hosts wanted than the list will ever have
	wl := surtOrderedList(t, 1, 500)
	s := newHostSpread(wl, workers, batch)

	for range 10 {
		if _, ok, err := s.next(context.Background()); err != nil || !ok {
			t.Fatalf("next: %v, ok %v", err, ok)
		}
		// One batch of slack, because the cap is tested before a read and a
		// read returns a whole batch.
		if s.rows > s.cap+batch {
			t.Fatalf("buffer holds %d rows with a cap of %d, so the checkpoint can fall arbitrarily far behind", s.rows, s.cap)
		}
	}
}

// TestSpreadLeavesTheDomainListAlone checks the case that must not regress.
//
// The domain work list is one row per host already, so there is nothing to
// rotate and the buffer should hand it out in the order it was read. Reordering
// it would cost a fleet run its rank ordering, which is the property that makes
// the early shards the interesting ones.
func TestSpreadLeavesTheDomainListAlone(t *testing.T) {
	dir := t.TempDir()
	want := []string{"a.com", "b.com", "c.com", "d.com", "e.com"}
	writeDomainPart(t, filepath.Join(dir, "part-000.parquet"), want)
	wl, err := NewWorkList(domainSource(), Shard{Count: 1}, nil, Checkpoint{})
	if err != nil {
		t.Fatal(err)
	}
	localParts(wl, dir)
	defer func() { wl.Close() }()

	got := drainSpread(t, newHostSpread(wl, 4, 2))
	for i, u := range got {
		if hostOf(u) != want[i] {
			t.Fatalf("item %d is %s, want %s: the domain list was reordered", i, u, want[i])
		}
	}
}

// TestSpreadPositionDoesNotStepOverHeldRows is the regression for the way this
// buffer can lose work silently.
//
// Rows in the buffer have been read off the work list already, so the work
// list's own position is past them. A checkpoint taken from that position while
// the buffer still holds rows names them as covered, and the next run starts
// after them, so they are read once and fetched never. It cost this test to
// notice: a twenty row run reported done at the end of the list with four rows
// still in hand.
func TestSpreadPositionDoesNotStepOverHeldRows(t *testing.T) {
	wl := surtOrderedList(t, 1, 40)
	s := newHostSpread(wl, 8, 8)

	for i := range 5 {
		if _, ok, err := s.next(context.Background()); err != nil || !ok {
			t.Fatalf("next: %v, ok %v", err, ok)
		}
		part, row, done := s.position()
		if done {
			t.Fatalf("after %d handouts the position says the list is finished while the buffer still holds %d rows", i+1, s.rows)
		}
		if want := int64(i + 1); row != want || part != 0 {
			t.Fatalf("after %d handouts the position is part %d row %d, want part 0 row %d", i+1, part, row, want)
		}
	}
}

// TestFlightSafeIsTheOldestRowAndNotTheFirstHandout pins the premise the reorder
// buffer broke.
//
// The flight set was written when handouts were in work list order, so the first
// sequence number in the set was also the lowest row in it and picking either
// gave the same answer. The buffer holds a site's rows back and hands them out
// later, so an item added second can carry a row that comes first, and picking
// by sequence names a position with an unfinished row behind it.
func TestFlightSafeIsTheOldestRowAndNotTheFirstHandout(t *testing.T) {
	fl := newFlight()
	fl.add(WorkItem{URL: "https://late.example/", Part: 0, Row: 900})
	fl.add(WorkItem{URL: "https://early.example/", Part: 0, Row: 100})

	part, row, ok := fl.safe()
	if !ok {
		t.Fatal("two items are in flight and the set says it has no position")
	}
	if part != 0 || row != 100 {
		t.Errorf("safe is part %d row %d, want part 0 row 100: row 100 is still in flight and a resume past it never comes back for it", part, row)
	}
}

// TestCheckpointStaysBehindRowsHeldInTheBuffer is the regression for the gap
// this pair of positions can write between them.
//
// Rows in the reorder buffer have been read off the work list and never handed
// to the pool, so the flight set has no idea they exist and reports the run as
// having got as far as the last row it retired. On the live URL run the two
// disagreed by 362739 rows and the checkpoint file alternated between them.
// Whichever one a kill happened to leave behind is where the next run starts, so
// half the time it started past rows that had been read and never fetched.
func TestCheckpointStaysBehindRowsHeldInTheBuffer(t *testing.T) {
	wl := surtOrderedList(t, 4, 40)
	s := newHostSpread(wl, 4, 8)
	r := &Recrawler{sp: s}

	// Hand out a few items and finish them, which is what leaves the flight set
	// empty and its position sitting at the last row it saw.
	fl := newFlight()
	for range 3 {
		it, ok, err := s.next(context.Background())
		if err != nil || !ok {
			t.Fatalf("next: %v, ok %v", err, ok)
		}
		fl.retire(fl.add(it))
	}
	if s.rows == 0 {
		t.Fatal("the buffer holds nothing, so this test is not checking what it says it checks")
	}

	_, wantRow, _ := s.position()
	_, gotRow := r.safePosition(fl)
	if gotRow > wantRow {
		t.Errorf("the checkpoint is at row %d with row %d still sitting in the buffer, so those rows are read once and fetched never", gotRow, wantRow)
	}
}

// TestSpreadPositionIsTheWorkListOnceItIsEmpty checks the other half, because a
// position that never says done is a unit that restarts forever.
func TestSpreadPositionIsTheWorkListOnceItIsEmpty(t *testing.T) {
	wl := surtOrderedList(t, 2, 3)
	s := newHostSpread(wl, 4, 4)
	drainSpread(t, s)

	if _, _, done := s.position(); !done {
		t.Error("the buffer is empty and the list is walked out, and the position still does not say done")
	}
}

package ccrawl

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Where a recrawl's time actually goes.
//
// A fleet target is a rate, and a rate is workers divided by the time one item
// takes. Both of those are knobs, so a run that is short of its target is short
// for one of five reasons and they have completely different fixes: it is
// reading robots.txt, it is sitting on the politeness clock, it is fetching, it
// is writing, or it is not doing anything at all because the work list is not
// feeding it. Guessing between those from a page count is how an afternoon
// disappears. Measured on the live domain list, the first guess was the
// politeness clock and the answer was robots.txt at 74 percent, which no amount
// of staring at the code would have settled.
//
// The counters are nanosecond sums across every worker, so they add up to
// worker time rather than wall time and the sum can exceed the run's duration by
// the worker count. That is the useful form: divided by workers times wall it is
// the fraction of the pool that phase was holding.

// recrawlTimers accumulates worker time per phase while a run is happening.
type recrawlTimers struct {
	robots, clock, fetch, extract, write atomic.Int64
	// sink is the writer goroutine's own time, which is not worker time and does
	// not belong in the pool's budget. It is kept because it is the other half of
	// the answer when the write phase grows: a pool queueing to reach a sink that
	// is idle wants a wider writer, and a pool queueing to reach a sink that is
	// busy every second of the run wants a faster one, and the two look identical
	// from the workers' side.
	sink  atomic.Int64
	items atomic.Int64
	wall  atomic.Int64
}

// add records one phase's duration. It is called once per phase per item, so it
// is a handful of atomic adds on a path that has just done a network round trip.
func (t *recrawlTimers) add(c *atomic.Int64, since time.Time) {
	c.Add(int64(time.Since(since)))
}

// RecrawlTiming is where the worker pool spent a run, summed over every worker.
type RecrawlTiming struct {
	// Robots is time inside the robots.txt cache, which is a network fetch on a
	// miss and nothing at all on a hit.
	Robots time.Duration
	// Clock is time held by the per host politeness delay.
	Clock time.Duration
	// Fetch is time in the page request itself, connect and body included.
	Fetch time.Duration
	// Extract is time turning the page into text and Markdown. It is the only
	// phase that is CPU rather than network, so it is the only one that competes
	// with the machine rather than with the far end, and the one to watch when a
	// run stops scaling with workers.
	Extract time.Duration
	// Write is time the workers spent handing captures to the writer. It is
	// normally close to nothing, and when it is not, the sink has fallen behind
	// the pool and the queue between them has backed up.
	Write time.Duration
	// Sink is the writer goroutines' own time, summed the same way the worker
	// phases are. It is not part of Busy and not part of the pool, and it is
	// reported as a share of one writer's wall clock rather than of the workers'.
	// A run where that share approaches the whole run has a sink that is the
	// ceiling, which is what more Writers is for.
	Sink time.Duration
	// Writers is how many sink parts the run had open, which is what Sink has to
	// be divided by to mean anything. A summed figure over four writers reading
	// 106 percent is four writers a quarter busy, not a writer that is over its
	// own wall clock.
	Writers int

	// Items is how many work items went through the pool, which is fetches plus
	// everything robots refused.
	Items int64
	// Wall is how long the run took and Workers is how wide the pool was. The
	// two together are the pool's capacity, which is what the phase sums are
	// worth comparing against.
	Wall    time.Duration
	Workers int
}

// Busy is the worker time the phases account for.
func (t RecrawlTiming) Busy() time.Duration {
	return t.Robots + t.Clock + t.Fetch + t.Extract + t.Write
}

// Capacity is the worker time the run had available.
func (t RecrawlTiming) Capacity() time.Duration {
	return time.Duration(t.Workers) * t.Wall
}

// Idle is capacity the run never used. It is the number that says the pool was
// starved rather than slow, which is a work list problem and not a fetch one,
// and it is clamped at zero because a phase that straddles the final tick can
// otherwise push the sum a hair past capacity.
func (t RecrawlTiming) Idle() time.Duration {
	return max(t.Capacity()-t.Busy(), 0)
}

// Rate is the pages a second the run actually held.
func (t RecrawlTiming) Rate(fetched int64) float64 {
	if t.Wall <= 0 {
		return 0
	}
	return float64(fetched) / t.Wall.Seconds()
}

// Line is the one line a run prints about itself when it stops.
//
// It reads as percentages of the pool rather than as durations because the
// question an operator has is never "how many worker seconds went into robots"
// but "what do I fix first", and the phase holding the largest share of the pool
// is the answer to that.
func (t RecrawlTiming) Line() string {
	cap := t.Capacity()
	if cap <= 0 {
		return "worker time: nothing measured"
	}
	pct := func(d time.Duration) float64 { return 100 * float64(d) / float64(cap) }
	return fmt.Sprintf(
		"worker time: robots %.0f%%, host clock %.0f%%, fetching %.0f%%, extracting %.0f%%, queueing to write %.0f%%, idle %.0f%% of %d workers, with %s",
		pct(t.Robots), pct(t.Clock), pct(t.Fetch), pct(t.Extract), pct(t.Write), pct(t.Idle()), t.Workers, t.writerLine())
}

// writerLine is the sink half of the timing line, said in whichever way is
// honest for the number of writers the run had. One writer is a goroutine and
// reads as one. Several are a pool of their own, and the figure that matters
// then is how busy each of them was, since that is what says whether another one
// would help.
func (t RecrawlTiming) writerLine() string {
	n := max(t.Writers, 1)
	if n == 1 {
		return fmt.Sprintf("the writer busy %.0f%% of the run", share(t.Sink, t.Wall))
	}
	return fmt.Sprintf("%d writers each busy %.0f%% of the run", n, share(t.Sink, time.Duration(n)*t.Wall))
}

// share is one duration as a percentage of another, and zero when there is
// nothing to take a share of.
func share(part, whole time.Duration) float64 {
	if whole <= 0 {
		return 0
	}
	return 100 * float64(part) / float64(whole)
}

// PerItem is the average time one work item held a worker, which is the number
// the fleet target divides into. At 250 pages a second a pool of 512 workers
// has two seconds an item to spend and no more.
func (t RecrawlTiming) PerItem() time.Duration {
	if t.Items <= 0 {
		return 0
	}
	return t.Busy() / time.Duration(t.Items)
}

// Timing reports where the run's worker time went.
func (r *Recrawler) Timing() RecrawlTiming {
	t := RecrawlTiming{
		Robots:  time.Duration(r.timers.robots.Load()),
		Clock:   time.Duration(r.timers.clock.Load()),
		Fetch:   time.Duration(r.timers.fetch.Load()),
		Extract: time.Duration(r.timers.extract.Load()),
		Write:   time.Duration(r.timers.write.Load()),
		Sink:    time.Duration(r.timers.sink.Load()),
		Items:   r.timers.items.Load(),
		Workers: r.cfg.Workers,
		Writers: r.writers(),
	}
	switch {
	case r.timers.wall.Load() > 0:
		t.Wall = time.Duration(r.timers.wall.Load())
	case !r.started.IsZero():
		t.Wall = time.Since(r.started)
	}
	return t
}

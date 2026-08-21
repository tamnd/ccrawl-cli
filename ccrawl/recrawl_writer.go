package ccrawl

import (
	"fmt"
	"time"
)

// Getting the sink off the worker path.
//
// Every worker used to write its own row. It took a mutex, handed the capture to
// the sink, and let go, which is correct and is also the shape that stops a wide
// pool getting any wider. Measured on the live domain list at 256 workers,
// writing was 30 percent of the pool and the run held 23.6 pages a second. The
// same run at 512 workers held 19.4, writing was 61 percent of the pool, and
// fetching had fallen from 61 percent to 27. Twice the workers, less crawling.
// Nearly all of that 61 percent was workers queueing for the mutex rather than
// the sink doing anything, which is what the queued share on the timing line
// says out loud.
//
// So the sink gets its own goroutine and the workers get a channel. A worker
// hands over a finished capture and goes back to the network, which is the only
// thing it is any good at. The writer takes them one at a time, in no particular
// order, and the lock it still holds is now contended with nothing but the
// occasional checkpoint.
//
// The one thing this must not lose is the resume promise. A row is retired from
// the flight set when it is safely with the sink, and the checkpoint never moves
// past an unretired row, so retiring moved from the worker to the writer along
// with the write. That is not a detail: retiring in the worker and writing later
// would let a checkpoint step over a row that is still sitting in a channel, and
// a kill at that moment loses the page with nothing anywhere saying so. A
// checkpoint past unwritten rows is the one failure that is silent.

// writeJob is one finished fetch on its way to the sink.
//
// It carries either a built row or the raw result, because the two sinks want
// different things: Parquet takes a Capture with the text columns already filled
// by the worker, and WARC takes the result and writes the bytes as they came.
type writeJob struct {
	row  Capture
	res  *CrawlResult
	rows bool
	seq  int64
}

// startWriter launches the one goroutine that owns the sink for the run.
//
// A run with no sink starts no writer, and then the workers retire their own
// items, because there is nothing to be durable about.
func (r *Recrawler) startWriter(fl *flight, errs chan<- error, stop func()) {
	if r.w == nil {
		return
	}
	r.rows, _ = r.w.(captureRowSink)
	// One item per worker of slack, capped. Slack absorbs a burst of small pages
	// arriving together; it is not meant to absorb a sink that cannot keep up,
	// and a run where it is permanently full wants a faster sink and not a
	// longer queue. It is also part of what a kill replays, see replayBound.
	r.wq = make(chan writeJob, min(r.cfg.Workers, writeQueueDepth))
	r.wdone = make(chan struct{})
	go func() {
		defer close(r.wdone)
		for j := range r.wq {
			start := time.Now()
			r.wmu.Lock()
			var err error
			if j.rows {
				err = r.rows.Write(j.row)
			} else {
				err = r.w.WriteCapture(j.res)
			}
			r.wmu.Unlock()
			r.timers.add(&r.timers.sink, start)
			if err != nil {
				select {
				case errs <- fmt.Errorf("write capture: %w", err):
				default:
				}
				stop()
				// Keep taking jobs even though none of them will be written.
				// A writer that returns here leaves every worker blocked on a
				// send into a channel nobody is reading, and the run hangs
				// instead of reporting the error it already has. Nothing is
				// retired from here on, so the checkpoint stays behind the row
				// that failed.
				continue
			}
			fl.retire(j.seq)
		}
	}()
}

// stopWriter closes the queue and waits for the sink to drain.
//
// It is called after the pool has stopped, so nothing can be sent after the
// close, and before the run seals its output, so every row a worker handed over
// is with the sink before the footer is written.
func (r *Recrawler) stopWriter() {
	if r.wq == nil {
		return
	}
	close(r.wq)
	<-r.wdone
	r.wq = nil
}

// writeQueueDepth is how many finished captures may wait for the sink, capped
// because the queue is bodies in memory and a page averages 300 KB.
const writeQueueDepth = 256

// replayBound is the most work items a kill can cost, which is not --batch.
//
// A checkpoint sits at the oldest item still in the air, so what a kill replays
// is everything the pool had got through behind that one: the batch the feeder
// was handing out, the items waiting in the buffer between the feeder and the
// pool, the one each worker is holding, and the rows already fetched and sitting
// in the writer's queue. The last of those is new, and it is why this is written
// down rather than assumed: taking the sink off the worker path let the pool run
// further ahead of the position the checkpoint can safely name.
//
// The difference matters at a small batch and rounds to nothing at a large one.
// At --batch 4 --workers 4 the pool is three quarters of the bound, and at
// --batch 2000 --workers 256 it is a fifth. Replaying is the safe direction,
// costing duplicate rows rather than missing ones, which is the trade the
// checkpoint is built around.
func (c RecrawlConfig) replayBound() int {
	return c.Batch + 2*c.Workers + min(c.Workers, writeQueueDepth)
}

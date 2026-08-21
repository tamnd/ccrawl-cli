package ccrawl

import (
	"strconv"
	"sync"
	"sync/atomic"
)

// More than one file open at once, because one encoder is not enough.
//
// The async writer took the sink off the worker path and the pool stopped
// waiting on a mutex. What it did not do is make the sink faster, and the
// timing line says so: at 256 workers the writer goroutine is busy 91 percent
// of the run. That is the ceiling now, and 33 pages a second is what it is
// worth.
//
// The interesting part is what that goroutine is doing with the time. Measured
// under /usr/bin/time -v, a run that fetches and writes and does not extract
// costs 6.0 ms of CPU a page in the sink. At 34 pages a second the writer is
// occupying 27 ms of wall clock a page to spend 6 of CPU, so four fifths of it
// is waiting: for a core on a box that is running two other crawlers, and for
// the disk at the end of a shard. Work that is mostly waiting is work that
// parallelises, and there is nothing in a Parquet shard that requires the row
// beside it to be in the same file.
//
// So the sink fans out. Each part is a whole CaptureWriter with its own file
// series, its own encoder and its own buffer, and rows go round the parts as
// they arrive. A reader sees more files of roughly the same size and does not
// otherwise notice, since the published corpus was always a directory of
// independently readable shards.

// fanoutSink is a CaptureSink made of several, each writing its own shards.
//
// The mutexes are per part rather than one for the whole thing. A single mutex
// would make this an expensive way to write exactly what one writer wrote, which
// is the thing it exists to stop being.
type fanoutSink struct {
	mu    []sync.Mutex
	parts []CaptureSink
	rows  []captureRowSink
	next  atomic.Uint64
}

// NewCaptureFanout opens n outputs for a run, so n encoders work at once.
//
// n at or below one is a single sink, unwrapped, because a run that does not
// need this should not pay a layer for it.
//
// Each part gets its own prefix, since two writers sharing one would race for
// the same file names. The suffix is part of the published file name and that is
// fine: the name of a shard has never meant anything beyond ordering, and a
// reader globs the directory.
func NewCaptureFanout(f CaptureFormat, dir, prefix string, shardSize int64, info WARCInfo, n int) (CaptureSink, error) {
	if n <= 1 {
		return NewCaptureSink(f, dir, prefix, shardSize, info)
	}
	if err := f.Validate(); err != nil {
		return nil, err
	}
	fan := &fanoutSink{mu: make([]sync.Mutex, n)}
	takesRows := true
	for i := range n {
		p, err := NewCaptureSink(f, dir, partPrefix(prefix, i), shardSize, info)
		if err != nil {
			for _, open := range fan.parts {
				_ = open.Close()
			}
			return nil, err
		}
		fan.parts = append(fan.parts, p)
		r, ok := p.(captureRowSink)
		if !ok {
			takesRows = false
		}
		fan.rows = append(fan.rows, r)
	}
	if takesRows {
		// Only a fanout whose every part takes a built row may claim to, since
		// the assertion for that interface is also what turns extraction on. A
		// WARC fanout that answered it would have the workers render Markdown
		// there is nowhere to put.
		return rowFanout{fan}, nil
	}
	return fan, nil
}

// rowFanout is a fanout every part of which takes a built row.
type rowFanout struct{ *fanoutSink }

// Write stores a row somebody else built, so the columns the worker filled
// survive.
func (f rowFanout) Write(c Capture) error {
	i := f.pick()
	f.mu[i].Lock()
	defer f.mu[i].Unlock()
	return f.rows[i].Write(c)
}

// partPrefix names one part's file series.
func partPrefix(prefix string, i int) string {
	if prefix == "" {
		prefix = "captures"
	}
	return prefix + "-w" + strconv.Itoa(i)
}

// pick chooses the next part, round robin. Round robin rather than a hash of the
// URL because the only thing being balanced here is bytes, and consecutive rows
// off the work list are already spread across hosts by the feeder.
func (f *fanoutSink) pick() int { return int(f.next.Add(1)-1) % len(f.parts) }

// WriteCapture stores one fetch in whichever part is next.
func (f *fanoutSink) WriteCapture(res *CrawlResult) error {
	i := f.pick()
	f.mu[i].Lock()
	defer f.mu[i].Unlock()
	return f.parts[i].WriteCapture(res)
}

// writeAt is the entry point the recrawl's writer goroutines use, one goroutine
// per part, so a slow encoder holds up its own file and nothing else. The lock
// is still taken, because the checkpoint's Sync comes through the same door.
func (f *fanoutSink) writeAt(i int, j writeJob) error {
	f.mu[i].Lock()
	defer f.mu[i].Unlock()
	if j.rows && f.rows[i] != nil {
		return f.rows[i].Write(j.row)
	}
	return f.parts[i].WriteCapture(j.res)
}

// Sync seals every part when any one of them is full, and reports durable only
// when all of them are.
//
// Sealing together rather than each part on its own is what keeps the checkpoint
// moving. A checkpoint may only advance when everything behind it is readable,
// and a Parquet file is unreadable until its footer is written, so a run where
// the parts rotate independently would need all of them to be empty at the same
// sync, which at fleet speed never happens and the checkpoint would sit at row
// zero for the length of the run. Rotating them together costs a few percent of
// the shard size on the parts that were not yet full, and buys back a checkpoint
// that advances once per rotation exactly as the single writer's does.
func (f *fanoutSink) Sync(force bool) (bool, error) {
	for i := range f.mu {
		f.mu[i].Lock()
	}
	defer func() {
		for i := range f.mu {
			f.mu[i].Unlock()
		}
	}()
	if !force {
		for _, p := range f.parts {
			if c, ok := p.(interface{ Full() bool }); ok && c.Full() {
				force = true
				break
			}
		}
	}
	durable := true
	var firstErr error
	for _, p := range f.parts {
		d, err := p.Sync(force)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if !d {
			durable = false
		}
	}
	if firstErr != nil {
		return false, firstErr
	}
	return durable, nil
}

// Files lists every part's shards.
func (f *fanoutSink) Files() []string {
	var out []string
	for i := range f.parts {
		f.mu[i].Lock()
		out = append(out, f.parts[i].Files()...)
		f.mu[i].Unlock()
	}
	return out
}

// Close seals every part, and reports the first failure rather than the last, so
// a run that fails to seal two shards still says which one went first.
func (f *fanoutSink) Close() error {
	var firstErr error
	for i := range f.parts {
		f.mu[i].Lock()
		if err := f.parts[i].Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		f.mu[i].Unlock()
	}
	return firstErr
}

// Full reports whether the open shard has reached the rotation target, which is
// the question the fanout asks before deciding to rotate all of its parts at
// once. A writer with no target never fills.
func (w *CaptureWriter) Full() bool {
	return w.target > 0 && w.accumulated >= w.target
}

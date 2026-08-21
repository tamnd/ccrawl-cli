package ccrawl

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// The recrawl loop. It is the crawl loop with the frontier taken out and a
// streamed work list put in, and everything else about it is the same: robots
// is read once per host and enforced, a host gets one request per delay, and
// every fetch is written to WARC.
//
// Taking the frontier out is the point rather than a simplification. See the
// note at the top of worklist.go for the numbers. What is left has to hold its
// place some other way, and that is a checkpoint of two numbers written after
// the bytes it accounts for are on the platter.

// RecrawlConfig configures one recrawl run.
type RecrawlConfig struct {
	// Source is the published dataset to walk.
	Source WorkSource
	// Shard is this machine's partition of it.
	Shard Shard
	// StatePath is the checkpoint file. Empty means a run that cannot resume,
	// which is fine for a short run and wrong for a long one.
	StatePath string

	// OutDir is where the captures go, empty fetches without storing anything.
	OutDir string
	Prefix string
	// Format is "warc" or "parquet". WARC is the archival format and Parquet is
	// the publishing one, and a fleet feeding a dataset wants the second.
	Format CaptureFormat
	// ShardSize rotates the output once this much has gone into the open file.
	// For WARC it is bytes on disk and for Parquet it is uncompressed payload,
	// because a Parquet file's size is not known until its footer is written and
	// the rotation decision has to be made before then.
	ShardSize int64
	Workers   int
	// Delay is the minimum spacing between two requests to the same host. A
	// robots.txt Crawl-delay longer than this wins for that host.
	Delay time.Duration
	// MaxPages stops the run after this many fetches. 0 means no limit.
	MaxPages int64
	// Robots turns on robots.txt enforcement.
	Robots bool
	// RobotsTimeout bounds one host's robots.txt fetch, retries included. It is
	// short on purpose, see the note on DefaultRecrawlConfig.
	RobotsTimeout time.Duration
	// Batch is how many work items are fetched between checkpoints. It is the
	// only knob that trades resume granularity against checkpoint writes, and
	// the default is a good place to leave it.
	Batch int

	// Conns shapes the connection layer the run fetches through: how many
	// transport shards, how many DNS lookups may be open at once, and the
	// deadlines that price a host which accepts a connection and then says
	// nothing. Every zero value is derived from Workers, so a caller with no
	// opinion is not required to have one. See webpool.go.
	Conns WebPoolConfig

	// Extract renders every captured page into the text and Markdown columns as
	// it is fetched. On by default, because a corpus of bodies nobody has
	// extracted is a corpus somebody has to read twice. See recrawl_extract.go.
	Extract bool
	// Extractor names the engine, empty for the default. It only matters when
	// Extract is on.
	Extractor string

	Crawl CrawlConfig
	Info  WARCInfo
}

// DefaultRecrawlConfig is the shape of a polite recrawl.
//
// Batch defaults to 2000 because a batch is what a kill replays. At 250 pages a
// second that is eight seconds of work to redo after a crash, against one small
// fsync every eight seconds, and both of those numbers are comfortable. Making
// it much smaller buys resume precision nobody needs and pays for it in fsyncs
// on a run measured in months.
var DefaultRecrawlConfig = defaultRecrawlConfig()

func defaultRecrawlConfig() RecrawlConfig {
	cfg := RecrawlConfig{
		Prefix:        "ccrawl-recrawl",
		Format:        FormatParquet,
		ShardSize:     DefaultCaptureShardSize,
		Workers:       32,
		Delay:         time.Second,
		Robots:        true,
		RobotsTimeout: 10 * time.Second,
		Batch:         2000,
		Extract:       true,
		Crawl:         DefaultCrawlConfig,
	}
	// The crawl default is two minutes, which is the right patience for one
	// large file off a bulk host and the wrong patience here. A batch does not
	// checkpoint until every item in it is done, so one host that accepts a
	// connection and then says nothing holds up the whole batch. Measured on the
	// live domain list, a single unanswered robots.txt stalled a twenty page run
	// for over a minute while thirty one workers sat idle. Thirty seconds for a
	// page and ten for a robots.txt are long enough that a slow site still gets
	// crawled and short enough that a dead one costs a batch a few seconds.
	cfg.Crawl.Timeout = 30 * time.Second
	return cfg
}

// Recrawler walks a streamed work list and fetches it.
type Recrawler struct {
	cfg RecrawlConfig
	h   *HTTPClient // the bulk client, for reading the work list
	rh  *HTTPClient // the open web client, for reading robots.txt
	rc  *RobotsCache
	wl  *WorkList
	ck  Checkpoint

	wmu sync.Mutex // held by the writer goroutine and by the checkpoint's Sync
	w   CaptureSink
	// rows is w again when the sink takes a built row rather than a raw result,
	// which is how the text columns the worker filled survive the write.
	rows captureRowSink
	// wq carries finished captures to the writer goroutine and wdone closes when
	// that goroutine has drained. See recrawl_writer.go for why the sink is not
	// on the worker path any more.
	wq    chan writeJob
	wdone chan struct{}
	ex    *pageExtractor // nil when the run was asked not to extract

	emu   sync.Mutex // the emit callback is not asked to be concurrency safe
	pages atomic.Int64
	stats crawlCounters

	// started and timers are the run's own account of where its worker time
	// went. See recrawl_timing.go for why a page count on its own is not enough
	// to tune a fleet with.
	started time.Time
	timers  recrawlTimers

	clock *hostClock
	// pool is the connection layer robots.txt and the pages both go through.
	pool *webPool
}

// DNS reports what the run's resolver did. A crawl that cannot resolve a host
// reports it as a host that refused to talk, which is the same thing from the
// outside and a completely different thing to fix, so the run says which.
func (r *Recrawler) DNS() ResolverStats { return r.pool.stats() }

// ErrRecrawlDone is returned by Resume when the checkpoint says the work list
// was walked to its end. It is not a failure and a supervisor should stop
// restarting the run.
var ErrRecrawlDone = errors.New("the work list is finished")

// NewRecrawler opens the work list at the checkpoint and the WARC writer.
//
// It takes two clients on purpose. bulk reads the work list from the dataset
// host, one host serving big files, and wants the ordinary budget and the
// pooled transport. web reads robots.txt off the open web, a million hosts
// served once each, and wants neither. Handing one client both jobs is the bug
// PR #169 fixed for crawl run, and it costs the same here: a global delay meant
// for one host turns robots into a queue five hosts a second long.
func NewRecrawler(cfg RecrawlConfig, bulk, web *HTTPClient) (*Recrawler, error) {
	h := bulk
	if cfg.Workers <= 0 {
		cfg.Workers = DefaultRecrawlConfig.Workers
	}
	if cfg.Batch <= 0 {
		cfg.Batch = DefaultRecrawlConfig.Batch
	}
	if cfg.RobotsTimeout <= 0 {
		cfg.RobotsTimeout = DefaultRecrawlConfig.RobotsTimeout
	}
	if cfg.Crawl.Timeout <= 0 {
		cfg.Crawl.Timeout = DefaultRecrawlConfig.Crawl.Timeout
	}
	// Delay is taken as given, including zero, for the same reason crawl run
	// takes it as given: no spacing is a thing a caller can mean, on a server
	// they own or behind a robots.txt that asks for nothing.
	if cfg.Delay < 0 {
		cfg.Delay = 0
	}
	ck, err := LoadCheckpoint(cfg.StatePath)
	if cfg.StatePath != "" && err != nil {
		return nil, err
	}
	if ck.Done {
		return nil, ErrRecrawlDone
	}
	wl, err := NewWorkList(cfg.Source, cfg.Shard, h, ck)
	if err != nil {
		return nil, err
	}
	// One connection layer for everything the run fetches off the open web, so
	// robots.txt and the page land on the same transport and the page reuses the
	// connection instead of handshaking again with a host we just spoke to. It
	// also owns the DNS cache and the bound on lookups, which is the difference
	// between asking about a host and finding out about it. See webpool.go.
	cfg.Conns.Workers = cfg.Workers
	pool := newWebPool(cfg.Conns)
	cfg.Crawl.Transport = pool

	r := &Recrawler{cfg: cfg, h: h, wl: wl, ck: ck, clock: newHostClock(), pool: pool}
	if cfg.Extract {
		// The source key is what stamps the extractor version, the same way a
		// crawl ID does in the conversion pipeline: it names the work list this
		// text came out of, which is the closest thing a recrawl has to a crawl.
		ex, err := newPageExtractor(cfg.Extractor, cfg.Source.Key())
		if err != nil {
			return nil, err
		}
		r.ex = ex
	}
	if cfg.Robots {
		// robots.txt comes off the open web and gets the open web's client, for
		// the same reasons crawl run does it: the bulk client spaces every
		// request by a delay that exists to be polite to one host serving files
		// to everybody, and applying it to a million unrelated sites is not
		// politeness, it is a queue five hosts a second long.
		r.rh = web
		// The crawl client is built for this run and nothing else holds it, so
		// pointing it at the pool is safe and is the whole reason the page fetch
		// gets a warm connection.
		r.rh.useTransport(pool)
		// Five retries is the bulk client's number and it is wrong for robots.txt
		// twice over. It cannot fit: the robots fetch has a ten second budget and
		// a dial timeout of five, so six attempts means the budget is always spent
		// in full on a host that is not going to answer, and a dead host is a
		// third of this work list. And it is not polite: a 403 counts as
		// retryable, which on the open web is a site telling us to go away and
		// getting asked six times. One retry covers the transient reset, which is
		// the only failure a second attempt inside the same ten seconds fixes.
		r.rh.useRetries(1)
		r.rc = NewRobotsCache(DefaultRobotsTTL, cfg.Crawl.UserAgent)
	}
	if cfg.OutDir != "" {
		w, err := NewCaptureSink(cfg.Format, cfg.OutDir, cfg.Prefix, cfg.ShardSize, cfg.Info)
		if err != nil {
			return nil, err
		}
		r.w = w
	}
	return r, nil
}

// Checkpoint is where the run currently believes it is.
func (r *Recrawler) Checkpoint() Checkpoint { return r.ck }

// Close releases the work list and the WARC file.
func (r *Recrawler) Close() error {
	r.wl.Close()
	if r.pool != nil {
		// Sockets held open against hosts the run is finished with, which after
		// the last page is all of them.
		r.pool.CloseIdleConnections()
	}
	if r.w != nil {
		err := r.w.Close()
		r.w = nil
		return err
	}
	return nil
}

// Run walks the work list to its end, to MaxPages, or until the context is
// cancelled, whichever comes first.
//
// The pool is started once and fed for the whole run. It used to be started per
// batch, with a barrier at the end of each one so the checkpoint could be
// written between them, and the barrier is what a rate target runs into: a
// window is only as fast as its slowest item, and one host that accepts a
// connection and then says nothing holds every other worker at the line.
// Measured on the live domain list with 256 workers, a 600 page run spent its
// last 92 seconds finishing four items and reported 87 percent of the pool idle
// across the run.
//
// The resume promise is unchanged and it is now kept by the flight set rather
// than by the barrier: items are handed out in work list order, the safe
// position is the oldest one that has not finished, and a kill replays only what
// was genuinely in the air. See recrawl_flight.go.
func (r *Recrawler) Run(ctx context.Context, emit func(CrawlPage) error) (CrawlStats, error) {
	r.started = time.Now()
	defer func() { r.timers.wall.Store(int64(time.Since(r.started))) }()

	ctx, stop := context.WithCancel(ctx)
	defer stop()

	fl := newFlight()
	// The sink runs in its own goroutine for the whole run, so a worker's part
	// in writing a page is a channel send. See recrawl_writer.go.
	errs := make(chan error, r.cfg.Workers)
	r.startWriter(fl, errs, stop)
	// The buffer is what keeps the pool fed while this goroutine is reading the
	// next window out of Parquet over the network, which is the other way a pool
	// goes idle with work left to do. One item per worker covers that and no more
	// on purpose: a buffered item counts as handed out, so a buffer the size of a
	// window would let the feeder dump the whole window and then checkpoint at the
	// row it started from, which is a checkpoint that never moves and a kill that
	// replays two thousand rows.
	work := make(chan flightItem, r.cfg.Workers)
	var wg sync.WaitGroup

	// full says the page limit has been reached, which stops work being handed
	// out but leaves the fetches already in flight alone. Cancelling the context
	// here instead would abort them, and pages we asked a server for and then
	// threw away are neither polite nor what --max-pages 5 means.
	full := make(chan struct{})
	closeFull := sync.OnceFunc(func() { close(full) })

	for range r.cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fi := range work {
				select {
				case <-full:
					// The run has had its fill. This item was handed out but
					// never attempted, so like a refused one it stays in the
					// flight set and the run comes back to it. Skipping here
					// rather than inside process matters more than it sounds:
					// the limit is claimed after robots, so a worker draining
					// the buffer fetches a robots.txt for every row it is
					// about to throw away. Measured on the live domain list, a
					// 600 page run went on for another ninety seconds after
					// its last page and fetched robots for 2300 hosts it was
					// never going to ask for a page.
					continue
				default:
				}
				err := r.process(ctx, fi, emit)
				if errors.Is(err, errBatchStopped) {
					// Refused rather than done, so it stays in the flight set and
					// the run comes back to exactly this row.
					closeFull()
					continue
				}
				if errors.Is(err, errItemAbandoned) {
					continue
				}
				if errors.Is(err, errItemQueued) {
					// Fetched and handed over. The writer retires it.
					continue
				}
				if err != nil {
					errs <- err
					stop()
					return
				}
				fl.retire(fi.seq)
			}
		}()
	}

	feedErr := r.feed(ctx, fl, work, full)
	close(work)
	wg.Wait()
	// The pool has stopped, so nothing more can be queued, and the rows already
	// queued reach the sink before the run seals its output.
	r.stopWriter()
	close(errs)

	if err := <-errs; err != nil {
		return r.snapshot(), err
	}
	if feedErr != nil {
		return r.snapshot(), feedErr
	}
	return r.snapshot(), r.finishFlight(fl)
}

// flightItem is a work item with the sequence number that retires it.
type flightItem struct {
	item WorkItem
	seq  int64
}

// feed reads the work list and hands it to the pool, checkpointing as it goes.
//
// It returns when the work list is walked out, when the page limit stops it, or
// when the run is cancelled. It does not wait for the pool, because the pool is
// what the caller waits for.
func (r *Recrawler) feed(ctx context.Context, fl *flight, work chan<- flightItem, full <-chan struct{}) error {
	buf := make([]WorkItem, r.cfg.Batch)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-full:
			return nil
		default:
		}
		n, err := r.wl.Next(ctx, buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if n == 0 {
			return nil // walked out
		}
		for _, it := range spreadItems(buf[:n]) {
			fi := flightItem{item: it, seq: fl.add(it)}
			select {
			case work <- fi:
			case <-full:
				return nil
			case <-ctx.Done():
				return nil
			}
		}
		if err := r.checkpoint(fl); err != nil {
			return err
		}
	}
}

// checkpoint writes down a position it is safe to resume from, if the output
// says the rows behind it are readable.
//
// The safe position is read before the sync rather than after. Read afterwards
// it could name a row whose page was fetched and written in between, which the
// sync did not cover, and a checkpoint past unwritten rows is the one failure
// that is silent. Read first it can only be behind, which costs a refetch.
func (r *Recrawler) checkpoint(fl *flight) error {
	part, row, ok := fl.safe()
	if !ok {
		part, row, _ = r.wl.Position()
	}
	if r.w != nil {
		r.wmu.Lock()
		durable, err := r.w.Sync(false)
		r.wmu.Unlock()
		if err != nil {
			return err
		}
		if !durable {
			// The sink cannot promise these rows are readable yet, which is what
			// a Parquet shard says until its footer is written. Leaving the
			// checkpoint where it is costs a replay of the open shard on a crash
			// and never costs a gap, and the shard size is the knob that bounds
			// it.
			return nil
		}
	}
	r.ck.Part, r.ck.Row, r.ck.Done = part, row, false
	r.ck.Fetched = r.stats.fetched.Load()
	return r.save()
}

// finishFlight closes out a stopped run at the position the flight set says is
// safe, which is the oldest item that never finished.
//
// The three ways to get here are the end of the work list, the page limit, and a
// cancel. All three used to be handled differently and only the first one sealed
// the output and moved the checkpoint, so a run with --max-pages fetched its
// pages, wrote them, and left a checkpoint saying it had done nothing.
// Restarting it refetched every page and wrote a second copy of all of them,
// which on a fleet run is the difference between a hundred days and never
// finishing. They go through one path now.
//
// The work list being walked out is not enough to call a run done: the pool can
// have been cut off by the page limit or by a cancel with items still unfetched
// behind the reader. Done is claimed only when the reader reached the end and
// nothing was left in the air, because a supervisor reads Done as a reason to
// stop restarting the unit and a wrong one there loses rows for good.
//
// The sync inside finishAt is forced because a stop is the last chance to make
// the open shard readable. Holding it back for a fuller shard means holding it
// back forever.
func (r *Recrawler) finishFlight(fl *flight) error {
	part, row, done := r.wl.Position()
	if p, rw, ok := fl.safe(); ok && fl.stalled() {
		part, row, done = p, rw, false
	}
	return r.finishAt(part, row, done)
}

// finishAt is finish with the position given rather than read off the work list,
// for the one caller that has to record a row the work list has already gone
// past.
//
// Recording a position behind where the reader sits means the rows in between
// are fetched again on the next run and written again, so a shard can hold two
// copies of a page. That is the deliberate half of the trade: replaying costs
// duplicate rows and skipping costs missing ones, and only one of those can be
// noticed after the fact.
func (r *Recrawler) finishAt(part int, row int64, done bool) error {
	if r.w != nil {
		r.wmu.Lock()
		_, err := r.w.Sync(true)
		r.wmu.Unlock()
		if err != nil {
			return err
		}
	}
	r.ck.Part, r.ck.Row, r.ck.Done = part, row, done
	r.ck.Fetched = r.stats.fetched.Load()
	return r.save()
}

// save writes the checkpoint, filling in the identity it has to match on the
// way back in.
func (r *Recrawler) save() error {
	if r.cfg.StatePath == "" {
		return nil
	}
	r.ck.Source = r.cfg.Source.Key()
	r.ck.Shard, r.ck.Shards = r.cfg.Shard.Index, r.cfg.Shard.Count
	return r.ck.Save(r.cfg.StatePath)
}

// errBatchStopped is a worker saying the run has had its fill, not that
// anything went wrong. The item it was carrying is left unretired, so the run
// resumes at that row rather than past it.
var errBatchStopped = errors.New("the page limit was reached")

// errItemAbandoned is a worker saying the run stopped underneath it. Like the
// page limit it is not a failure, and like the page limit the item stays in the
// flight set: a fetch that was cancelled halfway is not a row that was dealt
// with, and retiring it would move the checkpoint past a page nobody has.
var errItemAbandoned = errors.New("the run stopped before this item was fetched")

// errItemQueued is a worker saying it fetched the page and handed it to the
// writer, so the writer will retire it once the row is durable and the worker
// must not. It is not a failure and it never reaches the caller.
var errItemQueued = errors.New("the page is with the writer")

// claimPage takes one of the MaxPages slots and says whether it got one.
//
// The slot is claimed rather than checked because thirty two workers reading a
// counter and then deciding will happily read 4 all at once and fetch thirty
// two pages for --max-pages 5. It is claimed here rather than before robots
// because a page robots would not let us have is not a page we fetched, and
// --max-pages counts fetches.
func (r *Recrawler) claimPage() bool {
	if r.cfg.MaxPages <= 0 {
		r.pages.Add(1)
		return true
	}
	return r.pages.Add(1) <= r.cfg.MaxPages
}

// process fetches one URL. A failure is counted and moved past rather than
// retried: the work list is a hundred days long and a site that is down now
// will be back in the next pass, so blocking on it buys a page and costs a
// queue.
func (r *Recrawler) process(ctx context.Context, fi flightItem, emit func(CrawlPage) error) error {
	it := fi.item
	if ctx.Err() != nil {
		return errItemAbandoned
	}
	u, err := url.Parse(it.URL)
	if err != nil || u.Host == "" {
		r.stats.failed.Add(1)
		r.stats.errOther.Add(1)
		return nil
	}

	r.timers.items.Add(1)
	delay := r.cfg.Delay
	if r.rc != nil {
		// The deadline covers the fetch and its retries together, so a host that
		// answers slowly four times in a row costs one budget and not four.
		start := time.Now()
		rctx, cancel := context.WithTimeout(ctx, r.cfg.RobotsTimeout)
		entry := r.rc.Fetch(rctx, r.rh, u.Host, u.Scheme)
		cancel()
		r.timers.add(&r.timers.robots, start)
		if !entry.IsAllowed(u.RequestURI()) {
			if ctx.Err() != nil {
				// The robots fetch was cut off by the run stopping rather than by
				// the host, and a refusal we manufactured is not a row we dealt
				// with. Leaving it unretired is what makes the resume come back
				// to it.
				return errItemAbandoned
			}
			if entry.Unreachable {
				r.stats.unreachable.Add(1)
			} else {
				r.stats.disallowed.Add(1)
			}
			return nil
		}
		if entry.CrawlDelay > delay {
			delay = entry.CrawlDelay
		}
	}
	clockStart := time.Now()
	err = r.clock.wait(ctx, u.Host, delay)
	r.timers.add(&r.timers.clock, clockStart)
	if err != nil {
		return errItemAbandoned
	}

	if !r.claimPage() {
		return errBatchStopped
	}
	crawlCfg := r.cfg.Crawl
	crawlCfg.OnRequestWritten = func(t time.Time) { r.clock.stamp(u.Host, t) }
	fetchStart := time.Now()
	// The deadline covers the fetch and its retries together, the same way the
	// robots one does, so a host that answers slowly four times in a row costs one
	// budget and not four. Without it the timeout is per attempt and the retry
	// loop multiplies it: measured on the live domain list, a 600 page run reached
	// its last page after sixty seconds and then took another ninety to exit,
	// because a handful of items were still working through a thirty second
	// timeout for the third time.
	fctx, cancel := context.WithTimeout(ctx, r.cfg.Crawl.Timeout)
	res, err := CrawlURL(fctx, it.URL, crawlCfg)
	cancel()
	r.timers.add(&r.timers.fetch, fetchStart)
	if err != nil {
		if ctx.Err() != nil {
			return errItemAbandoned
		}
		classifyCrawlErr(err, &r.stats)
		r.stats.failed.Add(1)
		return nil
	}

	queued := false
	if r.wq != nil {
		// Rendered here and written elsewhere. The two used to happen together
		// under one lock and they are different costs with different fixes, so
		// they keep their own timers.
		r.writeCapture(res, fi.seq)
		queued = true
	}
	r.stats.fetched.Add(1)
	r.stats.bytes.Add(int64(len(res.Body)))

	if emit != nil {
		r.emu.Lock()
		err = emit(CrawlPage{
			URL:         it.URL,
			FinalURL:    res.FinalURL,
			Status:      res.Status,
			ContentType: res.ContentType,
			Digest:      res.Digest,
			BodySize:    len(res.Body),
			LinkCount:   len(res.Links),
			FetchedAt:   res.FetchedAt.Format(time.RFC3339),
		})
		r.emu.Unlock()
		if err != nil {
			return err
		}
	}
	if queued {
		// The writer retires this one, once the row is with the sink. Retiring
		// it here would let the checkpoint step over a row that is still in the
		// channel, and a kill at that moment loses the page silently.
		return errItemQueued
	}
	return nil
}

// snapshot reads the counters into a CrawlStats.
func (r *Recrawler) snapshot() CrawlStats {
	s := CrawlStats{
		Fetched:     r.stats.fetched.Load(),
		Failed:      r.stats.failed.Load(),
		Disallowed:  r.stats.disallowed.Load(),
		Unreachable: r.stats.unreachable.Load(),
		Bytes:       r.stats.bytes.Load(),
		ErrDNS:      r.stats.errDNS.Load(),
		ErrTimeout:  r.stats.errTimeout.Load(),
		ErrRefused:  r.stats.errRefused.Load(),
		ErrSkip:     r.stats.errSkip.Load(),
		ErrOther:    r.stats.errOther.Load(),
	}
	if r.rc != nil {
		s.Robots = r.rc.Stats()
	}
	if r.w != nil {
		s.OutFiles = r.w.Files()
	}
	return s
}

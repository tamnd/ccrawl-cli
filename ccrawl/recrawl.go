package ccrawl

import (
	"context"
	"errors"
	"fmt"
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

	wmu sync.Mutex // one writer, so the WARC file is written by one goroutine
	w   CaptureSink

	emu   sync.Mutex // the emit callback is not asked to be concurrency safe
	pages atomic.Int64
	stats crawlCounters

	clock *hostClock
}

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
	r := &Recrawler{cfg: cfg, h: h, wl: wl, ck: ck, clock: newHostClock()}
	if cfg.Robots {
		// robots.txt comes off the open web and gets the open web's client, for
		// the same reasons crawl run does it: the bulk client spaces every
		// request by a delay that exists to be polite to one host serving files
		// to everybody, and applying it to a million unrelated sites is not
		// politeness, it is a queue five hosts a second long.
		r.rh = web
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
// The shape is a batch at a time rather than a single pipeline, and that is the
// whole resume story. Every item in a batch is fetched and written, the WARC is
// flushed, and only then does the checkpoint move past them. A kill anywhere in
// a batch loses the batch and replays it, which refetches at most Batch pages
// and skips none. A checkpoint written before the flush would do the opposite,
// and the opposite is silent.
func (r *Recrawler) Run(ctx context.Context, emit func(CrawlPage) error) (CrawlStats, error) {
	buf := make([]WorkItem, r.cfg.Batch)
	for {
		if err := ctx.Err(); err != nil {
			return r.snapshot(), r.finish()
		}
		if r.cfg.MaxPages > 0 && r.pages.Load() >= r.cfg.MaxPages {
			return r.snapshot(), r.finish()
		}
		// Where the batch about to be read starts, which is the position a run
		// cut off inside that batch has to fall back to.
		startPart, startRow, _ := r.wl.Position()
		n, err := r.wl.Next(ctx, buf)
		if err != nil {
			return r.snapshot(), err
		}
		if n == 0 {
			// The work list is walked out. finish records that, so a supervisor
			// restarting the unit is told to stop rather than reading the
			// dataset from the top again.
			return r.snapshot(), r.finish()
		}
		whole, err := r.fetchBatch(ctx, buf[:n], emit)
		if err != nil {
			return r.snapshot(), err
		}
		if !whole {
			// The batch was cut short by the page limit or by a cancel, so part
			// of it was never fetched. Advancing the checkpoint past it would
			// claim work that was not done and the next run would skip it without
			// a word, so the checkpoint falls back to where this batch began and
			// the batch is replayed whole.
			//
			// It falls back rather than staying put. Every batch before this one
			// was finished, and on a Parquet run those batches are usually sitting
			// in a shard that has not filled yet, so their checkpoint was held
			// back too. Sealing here and recording the batch boundary turns a
			// replay of the whole open shard into a replay of one batch, which is
			// the difference between --max-pages being usable and being a way to
			// throw away everything the run just did.
			return r.snapshot(), r.finishAt(startPart, startRow, false)
		}
		// The bytes first, then the claim that they exist.
		if r.w != nil {
			r.wmu.Lock()
			durable, serr := r.w.Sync(false)
			r.wmu.Unlock()
			if serr != nil {
				return r.snapshot(), serr
			}
			if !durable {
				// The sink cannot promise these rows are readable yet, which is
				// what a Parquet shard says until its footer is written. Leaving
				// the checkpoint where it is costs a replay of the open shard on
				// a crash and never costs a gap, and the shard size is the knob
				// that bounds it.
				continue
			}
		}
		part, row, done := r.wl.Position()
		r.ck.Part, r.ck.Row, r.ck.Done = part, row, done
		r.ck.Fetched = r.stats.fetched.Load()
		if err := r.save(); err != nil {
			return r.snapshot(), err
		}
	}
}

// finish closes out a run that stopped between batches, which is the one place
// a stop is clean: nothing is in flight, every fetched page is written, and the
// work list is sitting on a row nobody has read.
//
// The three ways to get here are the end of the work list, the page limit, and a
// cancel that landed on the boundary. All three used to be handled differently
// and only the first one sealed the output and moved the checkpoint, so a run
// with --max-pages fetched its pages, wrote them, and left a checkpoint saying
// it had done nothing. Restarting it refetched every page and wrote a second
// copy of all of them, which on a fleet run is the difference between a hundred
// days and never finishing.
//
// The sync is forced because a stop is the last chance to make the open shard
// readable. Holding it back for a fuller shard means holding it back forever.
func (r *Recrawler) finish() error {
	part, row, done := r.wl.Position()
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

// fetchBatch fetches one batch with a fixed pool of workers and returns when
// every item in it has been dealt with.
//
// The barrier at the end of each batch is what makes the checkpoint mean
// something, and it costs less than it looks like it should: the work list is
// sorted, so a batch of two thousand rows is two thousand different hosts and
// the pool is never waiting on one slow site while the rest of the batch sits
// idle behind it.
func (r *Recrawler) fetchBatch(ctx context.Context, items []WorkItem, emit func(CrawlPage) error) (bool, error) {
	work := make(chan WorkItem)
	var wg sync.WaitGroup
	errs := make(chan error, r.cfg.Workers)
	ctx, stop := context.WithCancel(ctx)
	defer stop()

	// full says the page limit has been reached, which stops the batch being
	// handed out but leaves the fetches already in flight alone. Cancelling the
	// context here instead would abort them, and pages we asked a server for and
	// then threw away are neither polite nor what --max-pages 5 means.
	full := make(chan struct{})
	closeFull := sync.OnceFunc(func() { close(full) })

	for range r.cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range work {
				err := r.process(ctx, it, emit)
				if errors.Is(err, errBatchStopped) {
					closeFull()
					continue
				}
				if err != nil {
					errs <- err
					stop()
					return
				}
			}
		}()
	}
	whole := true
	for _, it := range items {
		select {
		case work <- it:
			continue
		case <-full:
		case <-ctx.Done():
		}
		whole = false
		break
	}
	close(work)
	wg.Wait()
	close(errs)

	// A batch small enough to be handed out before any worker finishes can reach
	// the end of the send loop and still contain items the limit refused, so the
	// question is not whether every item was sent but whether every item was
	// done. Either signal means some of this batch was not fetched, and a
	// checkpoint past it would skip those rows for good.
	select {
	case <-full:
		whole = false
	default:
	}
	if ctx.Err() != nil {
		whole = false
	}
	return whole, <-errs
}

// errBatchStopped is a worker saying the run has had its fill, not that
// anything went wrong. It stops the batch and, because the batch did not
// finish, leaves the checkpoint where it was.
var errBatchStopped = errors.New("the page limit was reached")

// process fetches one URL. A failure is counted and moved past rather than
// retried: the work list is a hundred days long and a site that is down now
// will be back in the next pass, so blocking on it buys a page and costs a
// queue.
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

func (r *Recrawler) process(ctx context.Context, it WorkItem, emit func(CrawlPage) error) error {
	u, err := url.Parse(it.URL)
	if err != nil || u.Host == "" {
		r.stats.failed.Add(1)
		r.stats.errOther.Add(1)
		return nil
	}

	delay := r.cfg.Delay
	if r.rc != nil {
		// The deadline covers the fetch and its retries together, so a host that
		// answers slowly four times in a row costs one budget and not four.
		rctx, cancel := context.WithTimeout(ctx, r.cfg.RobotsTimeout)
		entry := r.rc.Fetch(rctx, r.rh, u.Host, u.Scheme)
		cancel()
		if !entry.IsAllowed(u.RequestURI()) {
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
	if err := r.clock.wait(ctx, u.Host, delay); err != nil {
		return nil
	}

	if !r.claimPage() {
		return errBatchStopped
	}
	crawlCfg := r.cfg.Crawl
	crawlCfg.OnRequestWritten = func(t time.Time) { r.clock.stamp(u.Host, t) }
	res, err := CrawlURL(ctx, it.URL, crawlCfg)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		classifyCrawlErr(err, &r.stats)
		r.stats.failed.Add(1)
		return nil
	}

	if r.w != nil {
		r.wmu.Lock()
		err = r.w.WriteCapture(res)
		r.wmu.Unlock()
		if err != nil {
			return fmt.Errorf("write WARC for %s: %w", it.URL, err)
		}
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

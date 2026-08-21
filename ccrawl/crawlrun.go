package ccrawl

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// The crawl loop. Everything it uses already existed and none of it was
// connected: the frontier holds the queue and the politeness clocks, the robots
// cache holds the rules, CrawlURL fetches, WARCWriter archives, and
// ExtractOutLinks finds the next hop. This file is the composition and nothing
// else, which is why it is short.

// RunConfig configures a crawl run.
type RunConfig struct {
	// StatePath is the SQLite frontier file. Empty means in memory, which means
	// a run that cannot be resumed.
	StatePath string
	// OutDir is where the WARC files go. Empty means the pages are fetched and
	// not archived, which is useful for a dry run and for nothing else.
	OutDir   string
	Prefix   string // WARC file name prefix
	WARCSize int64  // rotate past this many bytes
	Workers  int
	// Delay is the minimum spacing between two requests to the same host. A
	// robots.txt Crawl-delay longer than this wins for that host.
	Delay time.Duration
	// MaxDepth is how far from a seed the crawl follows links. 0 fetches the
	// seeds and stops.
	MaxDepth int
	// MaxPages stops the run after this many fetches. 0 means no limit.
	MaxPages int64
	// Robots turns on robots.txt enforcement. It defaults off in the struct and
	// on in the command, because a library caller can be trusted with a list it
	// built and a crawler pointed at the open web cannot.
	Robots bool
	// SameHost keeps the crawl to the hosts the seeds named.
	SameHost bool
	// Retries is how many times a transient failure goes back in the queue.
	Retries int
	// RetryDelay is the wait before the first retry. It doubles each attempt.
	RetryDelay time.Duration

	Crawl CrawlConfig
	Info  WARCInfo
}

// DefaultRunConfig is the shape of a polite crawl.
var DefaultRunConfig = RunConfig{
	Prefix:     "ccrawl-crawl",
	WARCSize:   DefaultCrawlWARCSize,
	Workers:    8,
	Delay:      time.Second,
	MaxDepth:   2,
	Robots:     true,
	Retries:    2,
	RetryDelay: 5 * time.Second,
	Crawl:      DefaultCrawlConfig,
}

// CrawlStats is what a run did. The error breakdown uses the same buckets as
// RefetchStats, so the two fetch paths can be read side by side.
type CrawlStats struct {
	Fetched    int64 // pages fetched and archived
	Failed     int64 // pages given up on
	Retried    int64 // fetches put back in the queue
	Disallowed int64 // pages a robots.txt rule refused
	Discovered int64 // outlinks admitted to the frontier
	Bytes      int64 // response body bytes fetched

	// Unreachable is pages skipped because the host could not be asked, which is
	// a disallow under RFC 9309 section 2.3.1.4 but is not the site refusing us.
	// It is counted apart from Disallowed because the two mean different things:
	// one is a corpus that does not want us and the other is a network that is
	// not working, and a run that adds them together can tell you neither.
	Unreachable int64

	// Robots is what the robots cache did, so the extra request per host is
	// reported rather than guessed at.
	Robots RobotsStats

	ErrDNS     int64
	ErrTimeout int64
	ErrRefused int64
	ErrSkip    int64
	ErrOther   int64

	// OutFiles is what the run wrote, whichever format it wrote. It was called
	// WARCFiles when WARC was the only thing a run could write.
	OutFiles []string
}

// CrawlPage is one fetched page, emitted as the run goes.
type CrawlPage struct {
	URL         string `json:"url" table:"url"`
	FinalURL    string `json:"final_url,omitempty" table:"final_url"`
	Status      int    `json:"status" table:"status"`
	ContentType string `json:"content_type" table:"content_type"`
	Digest      string `json:"digest" table:"digest"`
	BodySize    int    `json:"body_size" table:"body_size"`
	LinkCount   int    `json:"link_count" table:"link_count"`
	Depth       int    `json:"depth" table:"depth"`
	FetchedAt   string `json:"fetched_at" table:"fetched_at"`
}

// Crawler drives a frontier through a crawl.
type Crawler struct {
	cfg RunConfig
	h   *HTTPClient
	f   *Frontier
	rc  *RobotsCache

	wmu sync.Mutex // one writer, so the WARC file is written by one goroutine
	w   *WARCWriter

	emu   sync.Mutex // the emit callback is not asked to be concurrency safe
	pages atomic.Int64
	stats crawlCounters

	hosts map[string]bool // seed hosts, for SameHost

	clock *hostClock // when each host may next be asked for something
}

// crawlCounters is CrawlStats while a run is happening, when every counter is
// being written by every worker.
type crawlCounters struct {
	fetched, failed, retried, disallowed, discovered, bytes atomic.Int64
	unreachable                                             atomic.Int64
	errDNS, errTimeout, errRefused, errSkip, errOther       atomic.Int64
}

// NewCrawler opens the frontier and the WARC writer for a run.
func NewCrawler(cfg RunConfig, h *HTTPClient) (*Crawler, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = DefaultRunConfig.Workers
	}
	// Delay is not re-defaulted the way Workers and RetryDelay are, because zero
	// is a value someone can mean. A caller that wants no spacing between two
	// requests to the same host has to be able to ask for it, for a benchmark
	// against a server they own or for a host whose robots.txt sets Crawl-delay
	// to nothing. Zero workers is not a request anybody can have meant, so that
	// one still gets the default. Build a RunConfig from DefaultRunConfig, as
	// every caller here does, and the polite one second arrives with it.
	if cfg.Delay < 0 {
		cfg.Delay = 0
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = DefaultRunConfig.RetryDelay
	}
	if cfg.Crawl.UserAgent == "" {
		cfg.Crawl = DefaultCrawlConfig
	}
	f, err := OpenFrontier(FrontierConfig{Path: cfg.StatePath, Delay: cfg.Delay})
	if err != nil {
		return nil, err
	}
	c := &Crawler{
		cfg:   cfg,
		h:     h,
		f:     f,
		rc:    NewRobotsCache(DefaultRobotsTTL, cfg.Crawl.UserAgent),
		hosts: make(map[string]bool),
		clock: newHostClock(),
	}
	if cfg.OutDir != "" {
		c.w = NewWARCWriter(cfg.OutDir, cfg.Prefix, cfg.WARCSize, cfg.Info)
	}
	return c, nil
}

// Frontier exposes the queue, for a caller that wants to read its stats.
func (c *Crawler) Frontier() *Frontier { return c.f }

// Seed admits one starting URL. Priority orders the queue, so passing the
// harmonic centrality from crawl seed crawls the important hosts first.
func (c *Crawler) Seed(rawURL string, priority float32) bool {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return false
	}
	c.hosts[u.Host] = true
	return c.f.Add(FrontierEntry{
		URL:      NormalizeURL(rawURL),
		Host:     u.Host,
		Priority: priority,
	})
}

// Close flushes the frontier and closes the WARC file.
func (c *Crawler) Close() error {
	var errs []error
	if c.w != nil {
		errs = append(errs, c.w.Close())
	}
	errs = append(errs, c.f.Close())
	return errors.Join(errs...)
}

// Run crawls until the frontier empties, the page limit is reached, or the
// context is cancelled. emit is called once per fetched page, from one
// goroutine at a time, and may be nil.
//
// Cancellation is not a failure: a run stopped by ctrl-c returns what it did
// along with the context error, and the frontier on disk is left in a state the
// next run resumes from.
func (c *Crawler) Run(ctx context.Context, emit func(CrawlPage) error) (CrawlStats, error) {
	if err := c.f.Flush(); err != nil {
		return c.snapshot(), err
	}

	// How long a worker with nothing to do waits before asking again. It tracks
	// the politeness delay, because that is what it is waiting for, with a floor
	// so a fast crawl of a few hosts does not turn into a spin.
	idle := min(max(c.cfg.Delay, 5*time.Millisecond), 50*time.Millisecond)

	var inflight atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, c.cfg.Workers)
	done := make(chan struct{})
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(func() { close(done) }) }

	for range c.cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-done:
					return
				default:
				}
				if c.cfg.MaxPages > 0 && c.pages.Load() >= c.cfg.MaxPages {
					stop()
					return
				}
				e, ok, err := c.f.Pop(time.Now().UnixMilli())
				if err != nil {
					errs <- err
					stop()
					return
				}
				if !ok {
					// Nothing eligible is not nothing left. Hosts inside their
					// politeness delay and URLs being fetched right now both look
					// like an empty queue from here, so the run ends only when the
					// frontier is empty and no worker is holding anything.
					if c.f.Len() == 0 && inflight.Load() == 0 {
						stop()
						return
					}
					select {
					case <-ctx.Done():
					case <-done:
					case <-time.After(idle):
					}
					continue
				}
				inflight.Add(1)
				if err := c.process(ctx, e, emit); err != nil {
					inflight.Add(-1)
					errs <- err
					stop()
					return
				}
				inflight.Add(-1)
			}
		}()
	}
	wg.Wait()
	close(errs)

	if err := c.f.Sync(); err != nil {
		return c.snapshot(), err
	}
	stats := c.snapshot()
	if c.w != nil {
		c.wmu.Lock()
		err := c.w.Close()
		c.w = nil
		c.wmu.Unlock()
		if err != nil {
			return stats, err
		}
	}
	if err := <-errs; err != nil {
		return stats, err
	}
	return stats, ctx.Err()
}

// process fetches one URL and does everything that follows from it.
func (c *Crawler) process(ctx context.Context, e FrontierEntry, emit func(CrawlPage) error) error {
	u, err := url.Parse(e.URL)
	if err != nil {
		c.stats.failed.Add(1)
		c.stats.errOther.Add(1)
		return c.f.Fail(e.URL)
	}

	// delay is what this host gets between requests: the run's own figure unless
	// robots.txt asks for more.
	delay := c.cfg.Delay
	if c.cfg.Robots {
		entry := c.rc.Fetch(ctx, c.h, u.Host, u.Scheme)
		if !entry.IsAllowed(u.RequestURI()) {
			if entry.Unreachable {
				c.stats.unreachable.Add(1)
			} else {
				c.stats.disallowed.Add(1)
			}
			return c.f.Done(e.URL)
		}
		// A Crawl-delay is a promise about the next request, so it is applied
		// before the fetch rather than after: Pop reserved the run's own delay
		// for this host, because at that point nobody had read its robots.txt,
		// and this is where the host's own number replaces it. A worker that
		// lost the host to another worker in the meantime puts its URL back.
		if entry.CrawlDelay > delay {
			delay = entry.CrawlDelay
			until := time.Now().Add(delay).UnixMilli()
			if !c.f.HoldClaim(e, until) {
				return c.f.Defer(e, until)
			}
		}
	}

	// The frontier spaces hand-outs, and the gap between being handed a URL and
	// getting the request onto the wire is not zero: under load a worker can be
	// forty milliseconds behind its own pop. Two requests to one host then land
	// closer together than the delay even though the queue did everything right.
	// This is the last gate before the fetch, and it is measured where it
	// matters, at the request rather than at the pop.
	if err := c.clock.wait(ctx, u.Host, delay); err != nil {
		// Cancelled while waiting: the URL was never fetched, so it goes back.
		return c.f.Retry(e, time.Now().UnixMilli())
	}

	c.pages.Add(1)
	// The wire time is what the host experiences, so it is what the next
	// request is spaced from. Without this the clock runs from the dispatch and
	// a request that had to open a connection arrives later than one that
	// reused it, which shows up at the host as a gap under the delay.
	crawlCfg := c.cfg.Crawl
	crawlCfg.OnRequestWritten = func(t time.Time) { c.clock.stamp(u.Host, t) }
	res, err := CrawlURL(ctx, e.URL, crawlCfg)
	if err != nil {
		if ctx.Err() != nil {
			// A cancelled fetch was never attempted as far as the queue is
			// concerned, so it goes back rather than counting as a failure.
			return c.f.Retry(e, time.Now().UnixMilli())
		}
		c.classify(err)
		if int(e.Retries) < c.cfg.Retries {
			c.stats.retried.Add(1)
			backoff := c.cfg.RetryDelay << e.Retries
			return c.f.Retry(e, time.Now().Add(backoff).UnixMilli())
		}
		c.stats.failed.Add(1)
		return c.f.Fail(e.URL)
	}

	if c.w != nil {
		c.wmu.Lock()
		err = c.w.Write(NewWARCCapture(res))
		c.wmu.Unlock()
		if err != nil {
			return fmt.Errorf("write WARC for %s: %w", e.URL, err)
		}
	}
	c.stats.fetched.Add(1)
	c.stats.bytes.Add(int64(len(res.Body)))

	if int(e.Depth) < c.cfg.MaxDepth {
		c.expand(res, e)
	}

	if emit != nil {
		c.emu.Lock()
		err = emit(CrawlPage{
			URL:         e.URL,
			FinalURL:    res.FinalURL,
			Status:      res.Status,
			ContentType: res.ContentType,
			Digest:      res.Digest,
			BodySize:    len(res.Body),
			LinkCount:   len(res.Links),
			Depth:       int(e.Depth),
			FetchedAt:   res.FetchedAt.Format(time.RFC3339),
		})
		c.emu.Unlock()
		if err != nil {
			return err
		}
	}
	return c.f.Done(e.URL)
}

// expand admits the outlinks of a fetched page at the next depth.
func (c *Crawler) expand(res *CrawlResult, from FrontierEntry) {
	for _, link := range res.Links {
		norm := NormalizeURL(link)
		u, err := url.Parse(norm)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			continue
		}
		host := u.Host
		if c.cfg.SameHost && !c.hosts[host] {
			continue
		}
		// Priority is inherited and stepped down, so a page three hops from a
		// central seed still sorts ahead of a seed nobody links to, and the
		// frontier drains breadth first within a level without a second queue.
		if c.f.Add(FrontierEntry{
			URL:      norm,
			Host:     host,
			Priority: from.Priority / 2,
			Depth:    from.Depth + 1,
		}) {
			c.stats.discovered.Add(1)
		}
	}
}

// classify buckets a fetch error the way refetch does.
func (c *Crawler) classify(err error) { classifyCrawlErr(err, &c.stats) }

// errClass is which bucket a fetch error falls in.
//
// It is a value rather than a counter write because two loops need the same
// answer and do not share a counter set. The page fetches keep theirs on the
// run and the robots cache keeps its own, and a breakdown that means one thing
// in one place and something else in the other is not a breakdown.
type errClass int

const (
	errClassOther errClass = iota
	errClassDNS
	errClassTimeout
	errClassRefused
	errClassSkip
)

// crawlErrClass reads a fetch error and says which bucket it belongs in. It
// matches on the message because the errors come from four packages and only
// some of them are the kind you can compare against.
func crawlErrClass(err error) errClass {
	e := strings.ToLower(err.Error())
	switch {
	case strings.Contains(e, "no such host"),
		strings.Contains(e, "no addresses found"),
		strings.Contains(e, "server misbehaving"),
		strings.Contains(e, "name resolution"):
		return errClassDNS
	case strings.Contains(e, "timeout"),
		strings.Contains(e, "deadline exceeded"),
		strings.Contains(e, "timed out"):
		return errClassTimeout
	case strings.Contains(e, "connection refused"),
		strings.Contains(e, "connection reset"),
		strings.Contains(e, "no route to host"),
		strings.Contains(e, "network is unreachable"):
		return errClassRefused
	case strings.Contains(e, "skip"), strings.Contains(e, "congested"):
		return errClassSkip
	}
	return errClassOther
}

// classifyCrawlErr puts a fetch error in one of the buckets a run reports, so
// a low yield run can be diagnosed without a second one. It is shared by the
// crawl and the recrawl, because a failure breakdown that means two different
// things depending on which loop produced it is not a breakdown.
func classifyCrawlErr(err error, stats *crawlCounters) {
	switch crawlErrClass(err) {
	case errClassDNS:
		stats.errDNS.Add(1)
	case errClassTimeout:
		stats.errTimeout.Add(1)
	case errClassRefused:
		stats.errRefused.Add(1)
	case errClassSkip:
		stats.errSkip.Add(1)
	case errClassOther:
		stats.errOther.Add(1)
	}
}

func (c *Crawler) snapshot() CrawlStats {
	s := CrawlStats{
		Fetched:     c.stats.fetched.Load(),
		Failed:      c.stats.failed.Load(),
		Retried:     c.stats.retried.Load(),
		Disallowed:  c.stats.disallowed.Load(),
		Unreachable: c.stats.unreachable.Load(),
		Discovered:  c.stats.discovered.Load(),
		Bytes:       c.stats.bytes.Load(),
		ErrDNS:      c.stats.errDNS.Load(),
		ErrTimeout:  c.stats.errTimeout.Load(),
		ErrRefused:  c.stats.errRefused.Load(),
		ErrSkip:     c.stats.errSkip.Load(),
		ErrOther:    c.stats.errOther.Load(),
	}
	if c.rc != nil {
		s.Robots = c.rc.Stats()
	}
	c.wmu.Lock()
	if c.w != nil {
		s.OutFiles = c.w.Files()
	}
	c.wmu.Unlock()
	return s
}

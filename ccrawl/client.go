package ccrawl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// HTTPClient is a polite, retrying HTTP client for Common Crawl. It rate-limits
// requests, retries on 403/429/5xx with exponential backoff (honoring a
// Retry-After header when the CDN sends one), and supports byte-range requests
// for single-record retrieval.
type HTTPClient struct {
	c          *http.Client
	download   *http.Client // no timeout, for large file bodies
	retries    int
	delay      time.Duration
	backoff    time.Duration // base wait before the first retry
	backoffMax time.Duration // ceiling for a single retry wait
	userAgent  string
	source     Source // which base bulk data paths are fetched from
	creds      awsCreds
	// global is the host-wide budget for Common Crawl requests, shared with every
	// other ccrawl process on the machine. Nil when it is switched off.
	global *globalLimiter

	mu   sync.Mutex
	next time.Time // earliest time the next request may start

	// cdxBytes is what the URL index has cost this run, so --explain can say
	// what pushing a filter to the server saved.
	cdxBytes atomic.Int64
}

// NewHTTPClient builds an HTTPClient from cfg.
func NewHTTPClient(cfg Config) *HTTPClient {
	ua := cfg.UserAgent
	if ua == "" {
		ua = UserAgent
	}
	backoff := cfg.Backoff
	if backoff <= 0 {
		backoff = DefaultBackoff
	}
	backoffMax := cfg.BackoffMax
	if backoffMax <= 0 {
		backoffMax = DefaultBackoffMax
	}
	src := cfg.Source
	if src == "" {
		src = SourceHTTPS
	}
	tr := pooledTransport()
	return &HTTPClient{
		c:          &http.Client{Timeout: cfg.Timeout, Transport: tr},
		download:   &http.Client{Transport: tr},
		retries:    max(cfg.Retries, 0),
		delay:      cfg.Delay,
		backoff:    backoff,
		backoffMax: backoffMax,
		userAgent:  ua,
		source:     src,
		creds:      resolveAWSCreds(),
		global:     newGlobalLimiter(cfg.DataDir, cfg.GlobalRate),
	}
}

// GlobalRate describes the host-wide request budget this client draws on, for
// the startup line that tells an operator what rate is actually in force.
func (h *HTTPClient) GlobalRate() string { return h.global.Describe() }

// NewCrawlClient builds the client a live crawl uses to fetch robots.txt from
// arbitrary sites. It is deliberately not the Common Crawl client.
//
// The ordinary client spaces every request by cfg.Delay, 200ms by default, and
// draws on a host-wide budget shared with every other ccrawl process. Both exist
// to be a good citizen towards data.commoncrawl.org, which is one host serving
// bulk files to everyone. Neither has anything to say about fetching robots.txt
// from a million unrelated sites, and applying them there is not politeness, it
// is a queue.
//
// It is also the difference between a crawl that runs and one that does not. The
// delay is per process, not per host, so robots.txt is fetched at five hosts a
// second no matter how many workers there are. Measured against 200 local hosts,
// a 2000 page crawl took 40.3 seconds with robots on and finished in under a
// second with robots off, and 200 hosts times 200ms is exactly the 40 seconds.
// Scaled to the 121 million domains in the recrawl corpus that is 280 days of
// waiting to read robots.txt before counting a single page.
//
// A crawl is polite per host, in the frontier and in waitForHost, which is where
// politeness belongs when the hosts are unrelated to each other.
func NewCrawlClient(cfg Config) *HTTPClient {
	cfg.Delay = 0
	cfg.GlobalRate = 0
	h := NewHTTPClient(cfg)
	tr := crawlTransport()
	h.c = &http.Client{Timeout: cfg.Timeout, Transport: tr}
	h.download = &http.Client{Transport: tr}
	return h
}

// crawlTransport returns a transport shaped for talking to many hosts once each,
// which is the opposite of what pooledTransport is for.
//
// pooledTransport keeps 64 idle connections per host because every bulk command
// talks to one host and wants them all kept warm. A crawl visits a host once, so
// per-host pooling buys nothing and the connections it holds open are pure cost:
// at 64 per host across thousands of hosts a run would sit on more sockets than
// the machine has file descriptors. The total goes up, the per-host figure comes
// down, and idle connections are reaped sooner because a host we have finished
// with is not coming back.
func crawlTransport() *http.Transport {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 4096
	tr.MaxIdleConnsPerHost = 4
	tr.IdleConnTimeout = 30 * time.Second
	return tr
}

// pooledTransport returns a transport that keeps enough idle connections for a
// worker pool. Go's default is two per host, which is fine for a browser and
// wrong here: every command that fans out talks to one host, so past two
// workers the rest pay a fresh TLS handshake on every request.
func pooledTransport() *http.Transport {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 256
	tr.MaxIdleConnsPerHost = 64
	tr.IdleConnTimeout = 90 * time.Second
	return tr
}

// WithoutDelay returns a copy of h that skips the inter-request delay, sharing
// the same connection pool, credentials and retry policy.
//
// The delay is priced for requests that mean something: a CDX page, a WARC
// range, a gigabyte file. A columnar scan is thousands of few-kilobyte reads of
// footers and column indexes, and spacing those two hundred milliseconds apart
// turns a thirty second query into two minutes. duckdb, which is the default
// engine for the same queries, applies no delay at all, so throttling the
// built-in engine only makes the dependency-free path the slow one.
//
// This drops the host-wide limiter as well, for the same reason and with the
// same consequence: a columnar scan does not draw on the shared budget, so
// --global-rate bounds everything except columnar scans. That is the behaviour
// that already shipped, and pushing thousands of footer reads through a five per
// second budget would turn a thirty second query into an hour.
func (h *HTTPClient) WithoutDelay() *HTTPClient {
	// Field by field rather than a struct copy, because the mutex must not be
	// carried over, and because global is deliberately left nil.
	return &HTTPClient{
		c:          h.c,
		download:   h.download,
		retries:    h.retries,
		backoff:    h.backoff,
		backoffMax: h.backoffMax,
		userAgent:  h.userAgent,
		source:     h.source,
		creds:      h.creds,
	}
}

// throttle blocks until this client may issue a request for url.
//
// Requests to Common Crawl go through the host-wide limiter first, so that N
// ccrawl processes running at once still add up to one polite client. A request
// to anything else, which means a live crawl fetching arbitrary sites, pays only
// the per process delay, because it is not spending Common Crawl's bandwidth.
func (h *HTTPClient) throttle(ctx context.Context, url string) error {
	if h.global != nil && isCommonCrawlURL(url) {
		granted, err := h.global.wait(ctx)
		if err != nil {
			return err
		}
		// The shared limiter already spaced this request against every other
		// process, this one included, so applying the local delay on top would
		// double the gap. The local delay stays as the fallback for a host where
		// the lock file is unusable.
		if granted {
			return nil
		}
	}
	if h.delay <= 0 {
		return nil
	}
	h.mu.Lock()
	now := time.Now()
	wait := time.Until(h.next)
	if h.next.Before(now) {
		h.next = now.Add(h.delay)
	} else {
		h.next = h.next.Add(h.delay)
	}
	h.mu.Unlock()
	if wait <= 0 {
		return nil
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// CDXBytesRead reports the bytes this client has read from the URL index so
// far, after transport decompression. Only the index is counted: WARC ranges,
// columnar reads and file downloads are not, because the question it answers is
// how much of the index a query had to move to find its rows.
func (h *HTTPClient) CDXBytesRead() int64 { return h.cdxBytes.Load() }

// countCDX wraps an index response body so what is read through it lands in the
// run's index byte total.
func (h *HTTPClient) countCDX(r io.Reader) io.Reader { return &cdxCounter{r: r, h: h} }

type cdxCounter struct {
	r io.Reader
	h *HTTPClient
}

func (c *cdxCounter) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.h.cdxBytes.Add(int64(n))
	return n, err
}

// Source reports which base this client fetches bulk data from.
func (h *HTTPClient) Source() Source { return h.source }

// DataURL renders a Common Crawl relative path as a URL for this client's
// configured source. Anything that reads bulk data should go through it rather
// than naming a base itself, which is how --source s3 reaches the fetch paths
// and not only the paths we print.
func (h *HTTPClient) DataURL(path string) string { return FileURL(path, h.source) }

// Get fetches url with retries.
func (h *HTTPClient) Get(ctx context.Context, url string) (*http.Response, error) {
	return h.doWith(ctx, h.c, url, "", false)
}

// getStatus fetches url and hands back whatever the server finally said, where
// Get would report a retryable status as an error. Callers that have to tell a
// 403 from a dead host need the difference: to robots.txt a 403 is "no rules
// here" and a failed connection is "assume everything is off limits", and one
// error value cannot say which happened.
func (h *HTTPClient) getStatus(ctx context.Context, url string) (*http.Response, error) {
	return h.doWith(ctx, h.c, url, "", true)
}

// GetRange fetches the [offset, offset+length) byte span of url.
func (h *HTTPClient) GetRange(ctx context.Context, url string, offset, length int64) (*http.Response, error) {
	rangeHdr := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	return h.doWith(ctx, h.c, url, rangeHdr, false)
}

// GetDownload fetches url with no client timeout (relies on ctx cancellation),
// for large archive bodies.
func (h *HTTPClient) GetDownload(ctx context.Context, url string) (*http.Response, error) {
	return h.doWith(ctx, h.download, url, "", false)
}

// GetDownloadFrom fetches url from offset to the end of the file, with no client
// timeout. It is GetDownload for a stream that died partway.
//
// A gigabyte WARC read over a long connection does not always survive to the end,
// and starting the gigabyte again over a dropped byte is most of the cost of a
// CC-NEWS index for none of the progress. A WARC is a sequence of independent
// gzip members, so a reader that knows where its last complete record ended can
// ask for the rest of the file and carry on from that boundary.
//
// offset 0 is a plain GetDownload, so a caller that has read nothing yet does not
// send a pointless range header.
func (h *HTTPClient) GetDownloadFrom(ctx context.Context, url string, offset int64) (*http.Response, error) {
	if offset <= 0 {
		return h.GetDownload(ctx, url)
	}
	return h.doWith(ctx, h.download, url, fmt.Sprintf("bytes=%d-", offset), false)
}

// ContentLength returns the total size of url. It sends a one-byte range request
// and reads the total out of the Content-Range header, which every Common Crawl
// data host answers, falling back to a full-response Content-Length when the host
// ignores the range. It is how a remote Parquet reader learns the file size before
// opening the footer.
func (h *HTTPClient) ContentLength(ctx context.Context, url string) (int64, error) {
	resp, err := h.GetRange(ctx, url, 0, 1)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		if i := strings.LastIndex(cr, "/"); i >= 0 && i+1 < len(cr) {
			if total, perr := strconv.ParseInt(strings.TrimSpace(cr[i+1:]), 10, 64); perr == nil && total > 0 {
				return total, nil
			}
		}
	}
	if resp.StatusCode == http.StatusOK && resp.ContentLength > 0 {
		return resp.ContentLength, nil
	}
	// Wrapped rather than formatted flat, so a caller can ask which status came
	// back. A recrawl walking a dataset part by part has to tell "this part is
	// not published, which is the end of the list" from "this host is having a
	// bad afternoon", and those two arrive here as a 404 and a 500.
	return 0, fmt.Errorf("cannot determine size of %s: %w", url, &httpStatusError{URL: url, Status: resp.StatusCode})
}

// httpStatusError is a response the caller cannot use. It carries the status
// because some callers have to tell "the bytes are wrong" apart from "I am not
// allowed to read them", and both arrive as a failed fetch.
type httpStatusError struct {
	URL    string
	Status int
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("HTTP %d from %s", e.Status, e.URL)
}

// ErrTransport marks a fetch that never got an answer: the dial failed, the
// connection went away, the handshake did not complete, or the deadline passed
// with nothing on the wire. It rides along with errors.Is so a caller can tell
// it apart from a server that answered with a status it did not like, which is
// the difference between "run it again later" and "this request is wrong".
//
// A retryable status that survives every attempt is not this. The server was
// there and said no, and saying otherwise would send a supervisor into a loop
// against a source that is up and refusing.
var ErrTransport = errors.New("transport failure, the bytes did not arrive")

// transportErr reports whether an error from the retry loop came from the
// transport rather than from a status the server sent back.
func transportErr(err error) bool {
	if err == nil {
		return false
	}
	var status *httpStatusError
	return !errors.As(err, &status)
}

// FetchBytes fetches url and returns the whole body.
func (h *HTTPClient) FetchBytes(ctx context.Context, url string) ([]byte, error) {
	resp, err := h.Get(ctx, url)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, &httpStatusError{URL: url, Status: resp.StatusCode}
	}
	return io.ReadAll(resp.Body)
}

// doWith runs the request with the retry, throttle and backoff policy. When
// keepStatus is set, a retryable status that survives the last attempt is
// returned as a response instead of an error.
func (h *HTTPClient) doWith(ctx context.Context, client *http.Client, url, rangeHdr string, keepStatus bool) (*http.Response, error) {
	var last error
	var retryAfter time.Duration // server-advised wait carried from the prior attempt
	var serverWait bool          // whether the prior attempt sent a Retry-After
	for attempt := 0; attempt <= h.retries; attempt++ {
		if attempt > 0 {
			if err := h.sleepBackoff(ctx, attempt, retryAfter, serverWait); err != nil {
				return nil, err
			}
			retryAfter, serverWait = 0, false
		}
		if err := h.throttle(ctx, url); err != nil {
			return nil, err
		}
		// s3://bucket/key is not something net/http can dial. The bucket is
		// public, so the REST endpoint for it is a plain anonymous GET, and every
		// other line in this loop, the throttle, the retries, the backoff, applies
		// to it unchanged.
		fetch, isS3 := s3Endpoint(url, s3BucketRegion)
		if !isS3 {
			fetch = url
		}
		if isS3 && !h.creds.valid() {
			// Not retryable: no amount of backing off produces a credential.
			return nil, fmt.Errorf("get %s: %w", url, ErrNoAWSCredentials)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fetch, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", h.userAgent)
		// We deliberately do not set Accept-Encoding. Letting the transport add
		// and strip gzip transparently keeps text endpoints (collinfo, the CDX
		// API, path manifests) decoded for us. The archive files are gzip as a
		// resource, not a transfer encoding, so they still arrive raw, and range
		// requests disable transparent decoding regardless.
		if rangeHdr != "" {
			req.Header.Set("Range", rangeHdr)
		}
		if isS3 {
			// Signed last, because SigV4 covers every header on the request.
			signS3(req, h.creds, s3BucketRegion, time.Now())
			// Checked on the first S3 read rather than at startup, so a run that
			// never touches bulk data never pays for the probe.
			warnIfS3Egress(ctx)
		}
		resp, err := client.Do(req)
		if err != nil {
			last = err
			continue
		}
		if isS3 {
			if authErr := s3AuthError(url, resp); authErr != nil {
				_ = resp.Body.Close()
				return nil, authErr
			}
		}
		if retryableStatus(resp.StatusCode) {
			if keepStatus && attempt == h.retries {
				return resp, nil
			}
			// A 503/429 commonly carries Retry-After; honor it on the next loop.
			retryAfter, serverWait = parseRetryAfter(resp.Header.Get("Retry-After"))
			_ = resp.Body.Close()
			last = &httpStatusError{URL: url, Status: resp.StatusCode}
			continue
		}
		return resp, nil
	}
	if transportErr(last) {
		return nil, fmt.Errorf("all %d attempts failed for %s: %w: %w", h.retries+1, url, ErrTransport, last)
	}
	return nil, fmt.Errorf("all %d attempts failed for %s: %w", h.retries+1, url, last)
}

// retryableStatus reports whether an HTTP status is worth retrying. 403 is
// included because Common Crawl's S3/CloudFront fronting returns it as a
// transient throttle or availability signal under load, not only as a hard
// "forbidden", so a genuinely forbidden URL burns all attempts before failing.
// That is the right trade for a polite bulk client and matches cdx_toolkit.
func retryableStatus(code int) bool {
	return code == http.StatusForbidden ||
		code == http.StatusTooManyRequests ||
		code >= 500
}

// sleepBackoff waits before the given retry attempt (1-based), returning early
// if the context is cancelled.
func (h *HTTPClient) sleepBackoff(ctx context.Context, attempt int, retryAfter time.Duration, serverWait bool) error {
	wait := h.backoffDelay(attempt, retryAfter, serverWait)
	if wait <= 0 {
		return nil
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// backoffDelay computes the wait before a retry. A server Retry-After wins when
// present (serverWait), clamped to [0, backoffMax] so the server can ask us to
// retry immediately but a huge value cannot stall a run. Otherwise it is
// exponential: the base doubles each attempt up to backoffMax, then equal
// jitter splits the window into a fixed half (a guaranteed minimum wait) plus a
// random half, so many concurrent workers do not retry in lockstep.
func (h *HTTPClient) backoffDelay(attempt int, retryAfter time.Duration, serverWait bool) time.Duration {
	if serverWait {
		return min(max(retryAfter, 0), h.backoffMax)
	}
	window := h.backoff
	for i := 1; i < attempt && window < h.backoffMax; i++ {
		window *= 2
	}
	window = min(window, h.backoffMax)
	half := window / 2
	return half + time.Duration(rand.Int63n(int64(half)+1))
}

// parseRetryAfter parses an HTTP Retry-After header, which is either an integer
// number of seconds or an HTTP-date. It returns the delay and whether the header
// was present and valid.
func parseRetryAfter(v string) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0, false
		}
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(time.Until(t), 0), true
	}
	return 0, false
}

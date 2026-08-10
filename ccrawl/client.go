package ccrawl

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
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

	mu   sync.Mutex
	next time.Time // earliest time the next request may start
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
	}
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
func (h *HTTPClient) WithoutDelay() *HTTPClient {
	// Field by field rather than a struct copy, because the mutex must not be
	// carried over.
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

// throttle blocks until the configured minimum inter-request delay has elapsed.
func (h *HTTPClient) throttle(ctx context.Context) error {
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
	return 0, fmt.Errorf("cannot determine size of %s (status %d)", url, resp.StatusCode)
}

// FetchBytes fetches url and returns the whole body.
func (h *HTTPClient) FetchBytes(ctx context.Context, url string) ([]byte, error) {
	resp, err := h.Get(ctx, url)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
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
		if err := h.throttle(ctx); err != nil {
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
			return nil, fmt.Errorf("GET %s: %w", url, ErrNoAWSCredentials)
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
			last = fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
			continue
		}
		return resp, nil
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

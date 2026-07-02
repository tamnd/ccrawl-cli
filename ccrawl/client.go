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
	return &HTTPClient{
		c:          &http.Client{Timeout: cfg.Timeout},
		download:   &http.Client{},
		retries:    max(cfg.Retries, 0),
		delay:      cfg.Delay,
		backoff:    backoff,
		backoffMax: backoffMax,
		userAgent:  ua,
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

// Get fetches url with retries.
func (h *HTTPClient) Get(ctx context.Context, url string) (*http.Response, error) {
	return h.doWith(ctx, h.c, url, "")
}

// GetRange fetches the [offset, offset+length) byte span of url.
func (h *HTTPClient) GetRange(ctx context.Context, url string, offset, length int64) (*http.Response, error) {
	rangeHdr := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	return h.doWith(ctx, h.c, url, rangeHdr)
}

// GetDownload fetches url with no client timeout (relies on ctx cancellation),
// for large archive bodies.
func (h *HTTPClient) GetDownload(ctx context.Context, url string) (*http.Response, error) {
	return h.doWith(ctx, h.download, url, "")
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

func (h *HTTPClient) doWith(ctx context.Context, client *http.Client, url, rangeHdr string) (*http.Response, error) {
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
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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
		resp, err := client.Do(req)
		if err != nil {
			last = err
			continue
		}
		if retryableStatus(resp.StatusCode) {
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

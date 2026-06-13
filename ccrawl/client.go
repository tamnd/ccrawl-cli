package ccrawl

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// HTTPClient is a polite, retrying HTTP client for Common Crawl. It rate-limits
// requests, retries on 429/5xx with linear backoff, and supports byte-range
// requests for single-record retrieval.
type HTTPClient struct {
	c         *http.Client
	download  *http.Client // no timeout, for large file bodies
	retries   int
	delay     time.Duration
	userAgent string

	mu   sync.Mutex
	next time.Time // earliest time the next request may start
}

// NewHTTPClient builds an HTTPClient from cfg.
func NewHTTPClient(cfg Config) *HTTPClient {
	ua := cfg.UserAgent
	if ua == "" {
		ua = UserAgent
	}
	retries := max(cfg.Retries, 0)
	return &HTTPClient{
		c:         &http.Client{Timeout: cfg.Timeout},
		download:  &http.Client{},
		retries:   retries,
		delay:     cfg.Delay,
		userAgent: ua,
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
	for i := 0; i <= h.retries; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(h.delay * time.Duration(i*i+1)):
			}
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
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			_ = resp.Body.Close()
			last = fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
			continue
		}
		return resp, nil
	}
	return nil, fmt.Errorf("all %d attempts failed for %s: %w", h.retries+1, url, last)
}

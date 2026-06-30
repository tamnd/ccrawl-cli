package ccrawl

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Common Crawl's fronting returns 403 as a transient throttle, so the client
// retries it like 429 and 5xx and succeeds once the server recovers.
func TestRetryRecoversFrom403(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&hits, 1) < 3 {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	h := NewHTTPClient(Config{Retries: 5, Backoff: time.Millisecond, BackoffMax: 5 * time.Millisecond})
	resp, err := h.Get(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("Get after transient 403s: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&hits); got != 3 {
		t.Fatalf("hits = %d, want 3 (two 403s then a 200)", got)
	}
}

// A persistent 403 is exhausted across retries+1 attempts and then fails.
func TestRetryExhaustsOnPersistent403(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	h := NewHTTPClient(Config{Retries: 2, Backoff: time.Millisecond, BackoffMax: 5 * time.Millisecond})
	if _, err := h.Get(context.Background(), srv.URL); err == nil {
		t.Fatal("expected error after exhausting retries on persistent 403")
	}
	if got := atomic.LoadInt32(&hits); got != 3 {
		t.Fatalf("hits = %d, want 3 (retries+1 attempts)", got)
	}
}

func TestRetryableStatus(t *testing.T) {
	for _, c := range []int{403, 429, 500, 502, 503, 504} {
		if !retryableStatus(c) {
			t.Errorf("status %d should be retryable", c)
		}
	}
	for _, c := range []int{200, 206, 301, 400, 404, 410} {
		if retryableStatus(c) {
			t.Errorf("status %d should not be retryable", c)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	if d, ok := parseRetryAfter("5"); !ok || d != 5*time.Second {
		t.Errorf("seconds: got %v %v", d, ok)
	}
	if _, ok := parseRetryAfter(""); ok {
		t.Error("empty header should report absent")
	}
	if _, ok := parseRetryAfter("-1"); ok {
		t.Error("negative seconds should be rejected")
	}
	if _, ok := parseRetryAfter("garbage"); ok {
		t.Error("unparseable header should report absent")
	}
	future := time.Now().Add(time.Minute).UTC().Format(http.TimeFormat)
	if d, ok := parseRetryAfter(future); !ok || d <= 0 || d > time.Minute+time.Second {
		t.Errorf("http-date: got %v %v", d, ok)
	}
	past := time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat)
	if d, ok := parseRetryAfter(past); !ok || d != 0 {
		t.Errorf("past date should clamp to 0: got %v %v", d, ok)
	}
}

func TestBackoffDelayHonorsRetryAfter(t *testing.T) {
	h := &HTTPClient{backoff: time.Second, backoffMax: 30 * time.Second}
	if got := h.backoffDelay(1, 3*time.Second, true); got != 3*time.Second {
		t.Errorf("Retry-After should win: got %v", got)
	}
	if got := h.backoffDelay(1, time.Hour, true); got != h.backoffMax {
		t.Errorf("Retry-After above the ceiling should clamp to backoffMax: got %v", got)
	}
	// Retry-After: 0 means "retry now" and must not fall back to exponential.
	if got := h.backoffDelay(1, 0, true); got != 0 {
		t.Errorf("Retry-After: 0 should yield no wait: got %v", got)
	}
}

func TestBackoffDelayExponentialBounds(t *testing.T) {
	h := &HTTPClient{backoff: time.Second, backoffMax: 30 * time.Second}
	// The window doubles per attempt (1s, 2s, 4s, ...) and equal jitter keeps
	// each wait within [window/2, window].
	for attempt, window := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second} {
		for range 200 {
			d := h.backoffDelay(attempt+1, 0, false)
			if d < window/2 || d > window {
				t.Fatalf("attempt %d: delay %v outside [%v, %v]", attempt+1, d, window/2, window)
			}
		}
	}
}

func TestBackoffDelayCappedAtMax(t *testing.T) {
	h := &HTTPClient{backoff: time.Second, backoffMax: 5 * time.Second}
	for range 200 {
		if d := h.backoffDelay(20, 0, false); d > h.backoffMax {
			t.Fatalf("delay %v exceeds backoffMax %v", d, h.backoffMax)
		}
	}
}

// A 503 with a Retry-After is retried after honoring the header, then succeeds.
func TestRetryHonorsRetryAfterHeader(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&hits, 1) < 2 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := NewHTTPClient(Config{Retries: 3, Backoff: time.Hour, BackoffMax: time.Hour})
	// Backoff is an hour, so finishing quickly proves Retry-After: 0 was honored.
	resp, err := h.Get(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("Get with Retry-After: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

// A retry still in its backoff wait returns the context error when cancelled.
func TestRetryCancelledDuringBackoff(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	h := NewHTTPClient(Config{Retries: 5, Backoff: time.Hour, BackoffMax: time.Hour})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := h.Get(ctx, srv.URL); err == nil {
		t.Fatal("expected context cancellation error")
	}
}

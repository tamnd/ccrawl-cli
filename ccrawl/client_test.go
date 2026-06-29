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

	h := NewHTTPClient(Config{Retries: 5, Delay: time.Millisecond})
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

	h := NewHTTPClient(Config{Retries: 2, Delay: time.Millisecond})
	if _, err := h.Get(context.Background(), srv.URL); err == nil {
		t.Fatal("expected error after exhausting retries on persistent 403")
	}
	if got := atomic.LoadInt32(&hits); got != 3 {
		t.Fatalf("hits = %d, want 3 (retries+1 attempts)", got)
	}
}

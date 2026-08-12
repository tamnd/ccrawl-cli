package ccrawl

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// PathsExist is what every publish command asks before it uploads anything, so
// a wrong answer here is not a wrong answer, it is a repeated upload of a
// dataset that was already there, or a resume that skips work it never did.
// These tests cover the three answers that matter: what is there, the empty
// repo, and a rate limit that must not be read as "nothing exists".

// hfTestServer points the hub at a local handler for the duration of the test
// and shortens the retry wait, which is five seconds against the real hub.
func hfTestServer(t *testing.T, h http.HandlerFunc) *HFClient {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	prevEndpoint, prevBackoff := hfEndpoint, hfRetryBase
	hfEndpoint, hfRetryBase = srv.URL, time.Millisecond
	t.Cleanup(func() { hfEndpoint, hfRetryBase = prevEndpoint, prevBackoff })

	return NewHFClient("test-token")
}

// pathsInfoHandler answers the paths-info endpoint with whichever of the posted
// paths are in present, the way the hub does.
func pathsInfoHandler(present map[string]int64, before func(w http.ResponseWriter) bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if before != nil && !before(w) {
			return
		}
		var req struct {
			Paths []string `json:"paths"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		var out []pathInfoEntry
		for _, p := range req.Paths {
			if size, ok := present[p]; ok {
				out = append(out, pathInfoEntry{Path: p, Size: size})
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}
}

func TestPathsExistReportsOnlyWhatIsThere(t *testing.T) {
	c := hfTestServer(t, pathsInfoHandler(map[string]int64{
		"data/part-0.parquet": 100,
		"data/part-2.parquet": 300,
	}, nil))

	got, err := c.PathsExist(context.Background(), "org/repo", []string{
		"data/part-0.parquet", "data/part-1.parquet", "data/part-2.parquet",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !got["data/part-0.parquet"] || !got["data/part-2.parquet"] {
		t.Fatalf("PathsExist reported %v", got)
	}
	if got["data/part-1.parquet"] {
		t.Fatal("PathsExist claims a path exists that the hub does not have")
	}

	// PathsInfo reads the same response and has to keep the sizes.
	sizes, err := c.PathsInfo(context.Background(), "org/repo", []string{"data/part-2.parquet"})
	if err != nil {
		t.Fatal(err)
	}
	if sizes["data/part-2.parquet"] != 300 {
		t.Fatalf("PathsInfo reported %v", sizes)
	}
}

// The hub answers 404 for a repo with nothing in it. That is an empty result,
// not a failure: the first publish into a new repo depends on it.
func TestPathsExistOnAnEmptyRepo(t *testing.T) {
	c := hfTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "Repository not found", http.StatusNotFound)
	})
	got, err := c.PathsExist(context.Background(), "org/new", []string{"data/part-0.parquet"})
	if err != nil {
		t.Fatalf("a 404 should be an empty answer, got %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("PathsExist reported %v for an empty repo", got)
	}
}

// A 429 must be retried, because the alternative is a publish that believes the
// repo is empty and uploads everything again. Retry-After is honoured, which is
// why the wait is shortened above rather than the header left off.
func TestPathsExistRetriesARateLimit(t *testing.T) {
	var calls atomic.Int32
	c := hfTestServer(t, pathsInfoHandler(
		map[string]int64{"data/part-0.parquet": 42},
		func(w http.ResponseWriter) bool {
			if calls.Add(1) > 1 {
				return true
			}
			w.Header().Set("Retry-After", "0")
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return false
		}))

	got, err := c.PathsExist(context.Background(), "org/repo", []string{"data/part-0.parquet"})
	if err != nil {
		t.Fatal(err)
	}
	if !got["data/part-0.parquet"] {
		t.Fatalf("the retry lost the answer: %v", got)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("the endpoint was called %d times, want 2 (one 429 then the answer)", n)
	}
}

// A rate limit that never lets up has to surface as an error. Returning an empty
// map here is the dangerous failure: the caller would re-upload the lot.
func TestPathsExistGivesUpOnAPersistentRateLimit(t *testing.T) {
	var calls atomic.Int32
	c := hfTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
	})

	got, err := c.PathsExist(context.Background(), "org/repo", []string{"data/part-0.parquet"})
	if err == nil {
		t.Fatalf("a persistent 429 returned %v and no error", got)
	}
	if !strings.Contains(err.Error(), "rate limited") {
		t.Fatalf("the error does not say what happened: %v", err)
	}
	if n := calls.Load(); n != 5 {
		t.Fatalf("the endpoint was called %d times, want the full 5 attempts", n)
	}
}

// A 403 is a token problem and no amount of retrying fixes it, so it comes back
// at once rather than after five waits.
func TestPathsExistDoesNotRetryAuthFailures(t *testing.T) {
	var calls atomic.Int32
	c := hfTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "Forbidden", http.StatusForbidden)
	})

	if _, err := c.PathsExist(context.Background(), "org/repo", []string{"a.parquet"}); err == nil {
		t.Fatal("a 403 has to be an error")
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("a 403 was retried %d times", n)
	}
}

// Paths go up in batches of a hundred, and the batching is the kind of loop that
// is off by one for years without anyone noticing, because the first batch
// answers and the caller sees plausible results.
func TestPathsExistBatchesInHundreds(t *testing.T) {
	const n = 250
	present := make(map[string]int64, n)
	var paths []string
	for i := range n {
		p := fmt.Sprintf("data/part-%03d.parquet", i)
		paths = append(paths, p)
		present[p] = int64(i)
	}
	var batches []int
	c := hfTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Paths []string `json:"paths"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		batches = append(batches, len(req.Paths))
		var out []pathInfoEntry
		for _, p := range req.Paths {
			if size, ok := present[p]; ok {
				out = append(out, pathInfoEntry{Path: p, Size: size})
			}
		}
		_ = json.NewEncoder(w).Encode(out)
	})

	got, err := c.PathsExist(context.Background(), "org/repo", paths)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("PathsExist returned %d of %d paths", len(got), n)
	}
	if want := []int{100, 100, 50}; !equalInts(batches, want) {
		t.Fatalf("batches were %v, want %v", batches, want)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

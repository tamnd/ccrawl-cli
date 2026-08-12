package ccrawl

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

// primedCache returns a cache pre-loaded with a collinfo manifest so ListCrawls
// (and ResolveCrawls through it) resolves offline, newest first.
func primedCache(t *testing.T, ids ...string) *Cache {
	t.Helper()
	c := NewCache(t.TempDir(), true)
	var b []byte
	b = append(b, '[')
	for i, id := range ids {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, []byte(`{"id":"`+id+`","name":"`+id+`"}`)...)
	}
	b = append(b, ']')
	c.Put("collinfo", b)
	return c
}

func TestResolveCrawls(t *testing.T) {
	ids := []string{
		"CC-MAIN-2024-10", "CC-MAIN-2023-50", "CC-MAIN-2023-23", "CC-MAIN-2022-05",
	}
	cache := primedCache(t, ids...)
	ctx := context.Background()

	cases := []struct {
		ref  string
		want []string
	}{
		{"latest", []string{"CC-MAIN-2024-10"}},
		{"", []string{"CC-MAIN-2024-10"}},
		{"all", ids},
		{"2", []string{"CC-MAIN-2024-10", "CC-MAIN-2023-50"}},
		{"99", ids}, // clamped to what exists
		{"2023", []string{"CC-MAIN-2023-50", "CC-MAIN-2023-23"}},
		{"2024-10", []string{"CC-MAIN-2024-10"}},
		{"CC-MAIN-2022-05", []string{"CC-MAIN-2022-05"}},
		{"CC-MAIN-2024-10,CC-MAIN-2022-05", []string{"CC-MAIN-2024-10", "CC-MAIN-2022-05"}},
		{"2023,2024-10", []string{"CC-MAIN-2023-50", "CC-MAIN-2023-23", "CC-MAIN-2024-10"}},
		// duplicates collapse, order of first appearance is kept
		{"latest,CC-MAIN-2024-10,2", []string{"CC-MAIN-2024-10", "CC-MAIN-2023-50"}},
	}
	for _, c := range cases {
		got, err := ResolveCrawls(ctx, nil, cache, c.ref)
		if err != nil {
			t.Errorf("ResolveCrawls(%q): %v", c.ref, err)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("ResolveCrawls(%q) = %v, want %v", c.ref, got, c.want)
		}
	}
}

func TestResolveCrawlsErrors(t *testing.T) {
	cache := primedCache(t, "CC-MAIN-2024-10")
	ctx := context.Background()
	for _, ref := range []string{"2019", "0", "nonsense"} {
		if _, err := ResolveCrawls(ctx, nil, cache, ref); err == nil {
			t.Errorf("ResolveCrawls(%q): expected error", ref)
		}
	}
}

// age backdates a cache entry so it is past whatever TTL a caller asked for.
func age(t *testing.T, c *Cache, key string, d time.Duration) {
	t.Helper()
	p := c.pathFor(key)
	when := time.Now().Add(-d)
	if err := os.Chtimes(p, when, when); err != nil {
		t.Fatal(err)
	}
}

// deadEndpoint points CollInfo at a server that is not there, the way
// index.commoncrawl.org looks during an outage, and restores it afterwards.
func deadEndpoint(t *testing.T) *HTTPClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL + "/collinfo.json"
	srv.Close()

	old := Endpoints.CollInfo
	Endpoints.CollInfo = url
	t.Cleanup(func() { Endpoints.CollInfo = old })

	return NewHTTPClient(Config{Retries: 1, Backoff: time.Millisecond, BackoffMax: 2 * time.Millisecond})
}

// captureStderr runs fn with os.Stderr replaced and returns what it wrote. The
// warning is the whole point of the fallback, so a test that did not read it
// would be blessing a command that silently answers from a day-old list.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stderr
	os.Stderr = w
	done := make(chan string, 1)
	go func() {
		var b strings.Builder
		_, _ = io.Copy(&b, r)
		done <- b.String()
	}()
	fn()
	os.Stderr = old
	_ = w.Close()
	out := <-done
	_ = r.Close()
	return out
}

// TestStaleCrawlListBeatsNoAnswer is the outage case. The index server went away
// for three days while this was written, and every command that only needed the
// word "latest" turned into a crawl ID failed with it, including the ones whose
// data lives on data.commoncrawl.org and was up the whole time.
func TestStaleCrawlListBeatsNoAnswer(t *testing.T) {
	cache := primedCache(t, "CC-MAIN-2026-30", "CC-MAIN-2026-25")
	age(t, cache, "collinfo", 30*time.Hour)
	h := deadEndpoint(t)

	var id string
	var err error
	warning := captureStderr(t, func() {
		id, err = ResolveCrawl(context.Background(), h, cache, "latest")
	})
	if err != nil {
		t.Fatalf("ResolveCrawl with a stale cache and a dead server: %v", err)
	}
	if id != "CC-MAIN-2026-30" {
		t.Errorf("resolved %q, want CC-MAIN-2026-30", id)
	}
	if !strings.Contains(warning, "unreachable") || !strings.Contains(warning, "30h") {
		t.Errorf("the warning does not say the list is unreachable and 30h old:\n%s", warning)
	}
}

// TestNoCachedListStillFails keeps the fallback from swallowing a real outage.
// Answering from nothing is worse than exit 8: a script that reads an empty
// crawl list as "Common Crawl has no crawls" writes that into a dataset.
func TestNoCachedListStillFails(t *testing.T) {
	h := deadEndpoint(t)
	for _, c := range []*Cache{nil, NewCache(t.TempDir(), true)} {
		if _, err := ListCrawls(context.Background(), h, c); err == nil {
			t.Error("ListCrawls with no cached copy and a dead server should fail")
		}
	}
}

// TestFreshFetchStillWins checks the fallback does not shadow the network. A
// cached list that is merely past its TTL must be refetched when the server is
// there, or a new crawl would take a cache clear to show up.
func TestFreshFetchStillWins(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"id":"CC-MAIN-2026-35","name":"new"}]`))
	}))
	defer srv.Close()
	old := Endpoints.CollInfo
	Endpoints.CollInfo = srv.URL
	defer func() { Endpoints.CollInfo = old }()

	cache := primedCache(t, "CC-MAIN-2026-30")
	age(t, cache, "collinfo", 30*time.Hour)

	h := NewHTTPClient(Config{Retries: 1, Backoff: time.Millisecond, BackoffMax: 2 * time.Millisecond})
	crawls, err := ListCrawls(context.Background(), h, cache)
	if err != nil {
		t.Fatal(err)
	}
	if crawls[0].ID != "CC-MAIN-2026-35" {
		t.Errorf("got %q, want the crawl the server sent, CC-MAIN-2026-35", crawls[0].ID)
	}
}

func TestRoundAge(t *testing.T) {
	for _, c := range []struct {
		in   time.Duration
		want string
	}{
		{30 * time.Second, "less than a minute"},
		{90 * time.Second, "2m0s"},
		{30 * time.Hour, "30h0m0s"},
		{72 * time.Hour, "3 days"},
	} {
		if got := roundAge(c.in); got != c.want {
			t.Errorf("roundAge(%s) = %q, want %q", c.in, got, c.want)
		}
	}
}

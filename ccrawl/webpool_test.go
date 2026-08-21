package ccrawl

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestWebPoolDefaultsScaleWithWorkers checks the derivation, because it is the
// only thing standing between an operator who says --workers 2000 and a resolver
// asked two thousand questions at once.
func TestWebPoolDefaultsScaleWithWorkers(t *testing.T) {
	for _, tc := range []struct{ workers, shards, lookups int }{
		{workers: 32, shards: 8, lookups: 16},
		{workers: 256, shards: 16, lookups: 32},
		{workers: 2000, shards: 64, lookups: 128},
	} {
		got := WebPoolConfig{Workers: tc.workers}.withDefaults()
		if got.Shards != tc.shards {
			t.Errorf("%d workers derived %d shards, want %d", tc.workers, got.Shards, tc.shards)
		}
		if got.DNSLookups != tc.lookups {
			t.Errorf("%d workers derived %d lookups, want %d", tc.workers, got.DNSLookups, tc.lookups)
		}
	}
}

// TestWebPoolKeepsAHostOnOneShard is the property the connection reuse rests on.
// robots.txt and the page are two requests to the same host, and if the hash
// sent them to different transports neither would find the other's connection.
func TestWebPoolKeepsAHostOnOneShard(t *testing.T) {
	p := newWebPool(WebPoolConfig{Workers: 256})
	for _, host := range []string{"example.com", "www.example.com", "a.co", ""} {
		first := p.shardFor(host)
		for range 20 {
			if p.shardFor(host) != first {
				t.Fatalf("%q landed on two different shards", host)
			}
		}
	}
	// And it does spread, or the sharding is a single pool with extra steps.
	seen := map[*http.Transport]bool{}
	for i := range 500 {
		seen[p.shardFor(hostName(i))] = true
	}
	if len(seen) < 8 {
		t.Fatalf("500 hosts landed on %d of %d shards", len(seen), len(p.trs))
	}
}

// TestWebPoolReusesTheConnection is the measured claim in a test: two requests to
// the same host over the pool open one connection, where the recrawl used to open
// two because robots.txt and the page went through different transports.
func TestWebPoolReusesTheConnection(t *testing.T) {
	var mu sync.Mutex
	conns := map[string]bool{}
	// Unstarted, because the ConnState hook has to be in place before the
	// listener takes anything and NewServer starts it for you.
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	srv.Config.ConnState = func(c net.Conn, state http.ConnState) {
		if state != http.StateNew {
			return
		}
		mu.Lock()
		conns[c.RemoteAddr().String()] = true
		mu.Unlock()
	}
	srv.Start()
	defer srv.Close()

	p := newWebPool(WebPoolConfig{Workers: 8})
	client := &http.Client{Transport: p, Timeout: 5 * time.Second}
	for _, path := range []string{"/robots.txt", "/"} {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		// Read to the end before closing. A connection only goes back in the pool
		// if its response was drained, which is the same reason FetchRobots now
		// drains the branches it does not parse.
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	mu.Lock()
	n := len(conns)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("two requests to one host opened %d connections", n)
	}
	p.CloseIdleConnections()
}

// TestWebPoolReportsAnUnresolvableHostAsDNS checks the dial path does not fall
// through to the system resolver when our own pool came back empty, which would
// put the lookup back outside the bound.
func TestWebPoolReportsAnUnresolvableHostAsDNS(t *testing.T) {
	p := newWebPool(WebPoolConfig{Workers: 8})
	client := &http.Client{Transport: p, Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		"http://this-name-does-not-exist-ccrawl-test.invalid/", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		t.Skip("something answered for a .invalid name, so this network is not one to test resolution failure on")
	}
	if s := p.stats(); s.Lookups != 1 || s.Failed != 1 {
		t.Fatalf("one unresolvable host cost %d lookups and %d failures", s.Lookups, s.Failed)
	}
}

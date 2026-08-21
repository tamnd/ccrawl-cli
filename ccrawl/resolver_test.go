package ccrawl

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// blockingResolver stands in for a stub under load: it records how many lookups
// are open at once and then fails, so a test can assert the bound without
// depending on anything answering.
func blockingResolver(open, peak *atomic.Int64, hold time.Duration) *net.Resolver {
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			n := open.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			select {
			case <-time.After(hold):
			case <-ctx.Done():
			}
			open.Add(-1)
			return nil, errors.New("no dns for you")
		},
	}
}

// TestResolverBoundsConcurrentLookups is the whole point of the file. Unbounded,
// a 256 worker run asks the resolver 256 questions at once and it answers about
// two thirds of them, and the crawl reads the rest as hosts that will not talk
// to us.
func TestResolverBoundsConcurrentLookups(t *testing.T) {
	var open, peak atomic.Int64
	r := newHostResolver(200*time.Millisecond, 4)
	r.pool = []*net.Resolver{blockingResolver(&open, &peak, 50*time.Millisecond)}

	done := make(chan struct{})
	for i := range 64 {
		go func(i int) {
			defer func() { done <- struct{}{} }()
			r.lookup(context.Background(), hostName(i))
		}(i)
	}
	for range 64 {
		<-done
	}
	if got := r.stats().Peak; got > 4 {
		t.Fatalf("%d lookups were open at once against a bound of 4", got)
	}
	if peak.Load() == 0 {
		t.Fatal("no lookup ever reached the resolver, so the bound is not what was measured")
	}
	// The sockets are not bounded by the same number and are not meant to be: a
	// single lookup asks for A and AAAA at once, so each one dials twice. What is
	// bounded is the questions, and the sockets follow from it by a small
	// constant rather than from the worker count by a large one.
	if peak.Load() > 4*4 {
		t.Fatalf("a bound of 4 lookups opened %d sockets at once", peak.Load())
	}
}

func hostName(i int) string {
	return string(rune('a'+i%26)) + string(rune('a'+i/26)) + ".example.invalid"
}

// TestResolverCachesAnswers checks the thing that makes robots.txt and the page
// one lookup rather than two.
func TestResolverCachesAnswers(t *testing.T) {
	r := newHostResolver(time.Second, 4)
	r.pool = nil // no resolver at all, so only the cache can answer
	want := []net.IP{net.ParseIP("192.0.2.7")}
	r.cache["example.test"] = want

	for range 3 {
		got, ok := r.lookup(context.Background(), "example.test")
		if !ok || len(got) != 1 || !got[0].Equal(want[0]) {
			t.Fatalf("cache returned %v, %v", got, ok)
		}
	}
	s := r.stats()
	if s.Hits != 3 || s.Lookups != 0 {
		t.Fatalf("three cached lookups counted as %d hits and %d lookups", s.Hits, s.Lookups)
	}
}

// TestResolverDoesNotAskAboutALiteralAddress keeps the bound for the names that
// need it. A work list of IPs would otherwise spend the whole budget asking the
// resolver about answers it was handed.
func TestResolverDoesNotAskAboutALiteralAddress(t *testing.T) {
	r := newHostResolver(time.Second, 1)
	r.pool = nil
	got, ok := r.lookup(context.Background(), "203.0.113.9")
	if !ok || len(got) != 1 || got[0].String() != "203.0.113.9" {
		t.Fatalf("lookup of a literal returned %v, %v", got, ok)
	}
	if s := r.stats(); s.Lookups != 0 || s.Hits != 0 {
		t.Fatalf("a literal cost %d lookups and %d hits", s.Lookups, s.Hits)
	}
}

// TestResolverOnlyRemembersANameEveryoneAgreesIsGone is the guard against the
// bug this file exists to fix wearing a different hat. Negative caching a lookup
// that was merely dropped turns a moment of load into a host the run has written
// off until it restarts.
func TestResolverOnlyRemembersANameEveryoneAgreesIsGone(t *testing.T) {
	var open, peak atomic.Int64
	r := newHostResolver(100*time.Millisecond, 4)
	// A resolver that fails without saying the name does not exist, which is what
	// a dropped or timed out lookup looks like.
	r.pool = []*net.Resolver{blockingResolver(&open, &peak, time.Millisecond)}

	if _, ok := r.lookup(context.Background(), "transient.example.invalid"); ok {
		t.Fatal("a failing resolver returned an answer")
	}
	r.mu.RLock()
	_, dead := r.dead["transient.example.invalid"]
	r.mu.RUnlock()
	if dead {
		t.Fatal("a dropped lookup was remembered as a name that does not exist")
	}
	if s := r.stats(); s.Failed != 1 || s.NXDomain != 0 {
		t.Fatalf("counted %d failed and %d nxdomain, want 1 and 0", s.Failed, s.NXDomain)
	}
}

// TestResolverRemembersARealNXDomain is the other side of it: a name every
// resolver in the pool says is gone is asked about once and not again.
func TestResolverRemembersARealNXDomain(t *testing.T) {
	var asked atomic.Int64
	r := newHostResolver(2*time.Second, 4)
	r.pool = []*net.Resolver{{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			asked.Add(1)
			d := net.Dialer{Timeout: 2 * time.Second}
			return d.DialContext(ctx, "udp", "8.8.8.8:53")
		},
	}}

	const name = "this-name-does-not-exist-ccrawl-test.invalid"
	for range 3 {
		if _, ok := r.lookup(context.Background(), name); ok {
			t.Skip("something answered for a .invalid name, so this network is not one to test negative caching on")
		}
	}
	s := r.stats()
	if s.Lookups != 1 {
		t.Fatalf("a name that does not exist was looked up %d times", s.Lookups)
	}
	if s.NXDomain != 1 {
		t.Fatalf("counted %d nxdomain, want 1", s.NXDomain)
	}
}

// TestOrderIPsPutsV4First checks the ordering that keeps a broken v6 path from
// costing the dial timeout on every host behind it.
func TestOrderIPsPutsV4First(t *testing.T) {
	in := []net.IPAddr{
		{IP: net.ParseIP("2001:db8::1")},
		{IP: net.ParseIP("198.51.100.4")},
		{IP: net.ParseIP("2001:db8::2")},
		{IP: net.ParseIP("198.51.100.5")},
	}
	got := orderIPs(in)
	if len(got) != 4 {
		t.Fatalf("orderIPs returned %d of 4 addresses", len(got))
	}
	if got[0].To4() == nil || got[1].To4() == nil {
		t.Fatalf("v6 came first: %v", got)
	}
	if got[2].To4() != nil || got[3].To4() != nil {
		t.Fatalf("v4 came last: %v", got)
	}
}

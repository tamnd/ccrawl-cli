package ccrawl

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Resolving a name without knocking the resolver over.
//
// A crawl asks about a different host on every row, so DNS is not a detail of
// the fetch, it is a per page cost that scales with the pool. Handing that
// straight to the system resolver is what a small pool gets away with and a
// large one does not, and the failure is quiet: the stub sheds load, the lookup
// comes back empty rather than wrong, and the crawl reads an empty answer as a
// host that will not talk to us.
//
// Measured on 400 hostnames taken from the successful captures of a live run, so
// every one of them is known to resolve:
//
//	system   conc   16 lookups    0: 400 resolved,   0 failed in 2.652s
//	system   conc  256 lookups    0: 269 resolved, 131 failed in 5.030s
//	system   conc  256 lookups   32: 399 resolved,   1 failed in 5.511s
//	race     conc  256 lookups    0: 278 resolved, 122 failed in 5.284s
//	race     conc  256 lookups   32: 400 resolved,   0 failed in 2.605s
//
// None of those failures was an NXDOMAIN. So the fix is the bound rather than
// the racing: asking 256 questions at once loses a third of them no matter who
// is answering. Racing on top of the bound is what makes it fast, because the
// local stub is then one answer among four rather than the only one.
//
// The reasoning and the shape are ami's, from fetch/dns.go, where the same thing
// showed up against systemd-resolved. It is written out again here rather than
// imported because ami keeps it unexported, and a copy with the numbers from our
// own runs in it is easier to argue with than a dependency.

// hostResolver caches DNS answers for the life of a run and bounds how many
// lookups are open at once.
//
// The cache is unbounded on purpose. An entry is a hostname and a couple of IPs,
// so a hundred million hosts is gigabytes and a run that reaches a hundred
// million hosts is a run that has been going for weeks. A recrawl visits a host
// once, which sounds like a cache that never hits, and it is not: robots.txt and
// the page are two lookups of the same name a second apart, so the hit rate
// floor is half.
type hostResolver struct {
	timeout time.Duration
	// sem bounds lookups that go to the network. A hit never touches it, so the
	// bound applies to the misses, which are the ones that open a socket.
	sem  chan struct{}
	pool []*net.Resolver

	mu    sync.RWMutex
	cache map[string][]net.IP
	dead  map[string]struct{}

	hits     atomic.Int64
	lookups  atomic.Int64
	failed   atomic.Int64
	nx       atomic.Int64
	inflight atomic.Int64
	peak     atomic.Int64
}

// ResolverStats is what the resolver did over a run, for the summary line. It
// exists because the bug it was written to fix was invisible from the outside:
// a dropped lookup and a host that is genuinely gone look identical by the time
// they reach the crawl.
type ResolverStats struct {
	Hits     int64 // answered from the cache, no socket opened
	Lookups  int64 // went to the network
	Failed   int64 // came back with nothing
	NXDomain int64 // came back with nothing and every resolver agreed the name is gone
	// Peak is the most lookups that were ever open at once.
	//
	// It used to say here that a peak sitting on the bound for a whole run means
	// workers are queueing for a lookup slot and the bound is the thing to raise.
	// That was measured on the live domain list and it is not true. The peak pegs
	// at whatever the bound is set to, because on a corpus where every row is a
	// different host there is always another lookup wanting a slot, so pegging
	// says there is demand and nothing about whether the queue costs anything.
	// Going from 32 to 128 moved the page rate by half a percent, from 19.2 to
	// 19.3, with the peak pegged in both cases.
	//
	// What the peak is good for is the other direction. A peak well under the
	// bound means DNS is not the constraint and raising it will do nothing, which
	// is worth knowing before anybody spends a run finding out.
	Peak int64
}

// newHostResolver builds the resolver. maxLookups is the bound on concurrent
// network lookups, and zero leaves it unbounded, which is what the measurement
// above says not to do.
func newHostResolver(timeout time.Duration, maxLookups int) *hostResolver {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	var sem chan struct{}
	if maxLookups > 0 {
		sem = make(chan struct{}, maxLookups)
	}
	return &hostResolver{
		timeout: timeout,
		sem:     sem,
		pool: []*net.Resolver{
			// The system stub is in the pool rather than in front of it. It is
			// the only one that knows about split horizon names and a machine's
			// own /etc/hosts, so it has to be asked, and it is also the one under
			// the most pressure, so it cannot be the only one asked.
			{PreferGo: true},
			udpResolver("8.8.8.8:53"),
			udpResolver("1.1.1.1:53"),
			udpResolver("9.9.9.9:53"),
		},
		cache: make(map[string][]net.IP),
		dead:  make(map[string]struct{}),
	}
}

// udpResolver returns a pure Go resolver that always talks to one server.
//
// PreferGo matters as much as the server does. The cgo resolver takes an OS
// thread per lookup, and a pool of two thousand workers all resolving at once is
// how a crawl runs out of threads rather than out of bandwidth.
func udpResolver(server string) *net.Resolver {
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			d := net.Dialer{Timeout: 3 * time.Second}
			return d.DialContext(ctx, "udp", server)
		},
	}
}

// lookup returns the addresses for host, IPv4 first. The second return is false
// when the name could not be resolved.
func (r *hostResolver) lookup(ctx context.Context, host string) ([]net.IP, bool) {
	// A literal address is not a question. Sending it to a resolver works and
	// costs a round trip through the stub for an answer we are holding, which on
	// a crawl of a work list full of them would be the whole bound spent on
	// nothing.
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}, true
	}

	r.mu.RLock()
	if _, dead := r.dead[host]; dead {
		r.mu.RUnlock()
		r.hits.Add(1)
		return nil, false
	}
	if ips, ok := r.cache[host]; ok {
		r.mu.RUnlock()
		r.hits.Add(1)
		return ips, true
	}
	r.mu.RUnlock()

	if r.sem != nil {
		select {
		case r.sem <- struct{}{}:
			defer func() { <-r.sem }()
		case <-ctx.Done():
			return nil, false
		}
	}
	// Checked again under the bound, because the wait for a slot is exactly long
	// enough for another worker to have answered the same question. On a work
	// list that arrives host clustered this is most of the hits.
	r.mu.RLock()
	ips, cached := r.cache[host]
	_, dead := r.dead[host]
	r.mu.RUnlock()
	if dead {
		r.hits.Add(1)
		return nil, false
	}
	if cached {
		r.hits.Add(1)
		return ips, true
	}

	r.lookups.Add(1)
	n := r.inflight.Add(1)
	for {
		p := r.peak.Load()
		if n <= p || r.peak.CompareAndSwap(p, n) {
			break
		}
	}
	ips, nxdomain := r.race(ctx, host)
	r.inflight.Add(-1)
	if len(ips) > 0 {
		r.mu.Lock()
		r.cache[host] = ips
		r.mu.Unlock()
		return ips, true
	}
	r.failed.Add(1)
	if nxdomain {
		// Only a name every resolver agrees is gone is remembered as gone. A
		// lookup that timed out or was dropped is not evidence about the name,
		// and negative caching one would turn a moment of load into a host this
		// run has written off for good, which is the bug this file exists to fix
		// wearing a different hat.
		r.nx.Add(1)
		r.mu.Lock()
		r.dead[host] = struct{}{}
		r.mu.Unlock()
	}
	return nil, false
}

// race asks every resolver at once and takes the first usable answer. The second
// return says whether all of them agreed the name does not exist, which is the
// only answer worth remembering.
func (r *hostResolver) race(ctx context.Context, host string) ([]net.IP, bool) {
	cctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	type answer struct {
		ips []net.IP
		nx  bool
	}
	ch := make(chan answer, len(r.pool))
	for _, res := range r.pool {
		go func(res *net.Resolver) {
			addrs, err := res.LookupIPAddr(cctx, host)
			if err == nil && len(addrs) > 0 {
				ch <- answer{ips: orderIPs(addrs)}
				return
			}
			var de *net.DNSError
			ch <- answer{nx: errors.As(err, &de) && de.IsNotFound}
		}(res)
	}

	allNX := true
	for range r.pool {
		a := <-ch
		if len(a.ips) > 0 {
			return a.ips, false
		}
		if !a.nx {
			allNX = false
		}
	}
	return nil, allNX
}

// stats reads the counters.
func (r *hostResolver) stats() ResolverStats {
	return ResolverStats{
		Hits:     r.hits.Load(),
		Lookups:  r.lookups.Load(),
		Failed:   r.failed.Load(),
		NXDomain: r.nx.Load(),
		Peak:     r.peak.Load(),
	}
}

// orderIPs returns the addresses IPv4 first. A crawl target is far more likely
// to have a working v4 path than a working v6 one, and a broken v6 path fails by
// hanging rather than by refusing, so trying it first costs the dial timeout on
// every host behind one.
func orderIPs(addrs []net.IPAddr) []net.IP {
	v4 := make([]net.IP, 0, len(addrs))
	v6 := make([]net.IP, 0, len(addrs))
	for _, a := range addrs {
		if a.IP.To4() != nil {
			v4 = append(v4, a.IP)
		} else {
			v6 = append(v6, a.IP)
		}
	}
	return append(v4, v6...)
}

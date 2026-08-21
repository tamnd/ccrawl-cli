package ccrawl

import (
	"context"
	"fmt"
	"hash/fnv"
	"net"
	"net/http"
	"time"
)

// One connection layer for the whole recrawl.
//
// The recrawl used to fetch through two unrelated transports. robots.txt went
// through the open web client's transport and the page went through a package
// level transport inside CrawlURL, so two requests to the same host a second
// apart opened two connections, and the second one paid a fresh TCP handshake
// and a fresh TLS handshake to a host we had just finished talking to. On the
// domain list, where every row is a host visited exactly once, that is half the
// connection setup in the run spent for nothing.
//
// The transport inside CrawlURL was also a single pool with MaxIdleConns 200.
// Above two hundred idle connections Go closes them as they are returned, so at
// 256 workers the pool was not a pool, it was a connection being closed on every
// release. ami answers this by sharding: several transports, a host pinned to
// one of them by hash, each keeping its own idle pool. The sharding is not about
// contention on a mutex, it is about the idle budget, which is per transport, so
// N transports is N times the budget without any one host's connections being
// spread across pools where they cannot be reused.
//
// The deadlines are the other half. A dead host on the open web mostly does not
// refuse the connection, it accepts it and then says nothing, and against a
// request budget alone that costs the whole budget. A response header timeout
// prices it at a few seconds instead, and it cannot hurt a slow but live host,
// whose headers have already arrived by the time its body is still coming.

// WebPoolConfig shapes the connection layer. Every zero value takes a default
// derived from Workers, so a caller that only knows how many workers it has is
// not required to have an opinion about transports.
type WebPoolConfig struct {
	Workers int
	// Shards is how many transports the hosts are spread across.
	Shards int
	// DNSLookups bounds how many DNS lookups may be open at once. This is the
	// number that mattered most, see resolver.go for the measurement.
	DNSLookups int
	DNSTimeout time.Duration
	// DialTimeout bounds the TCP connect and TLSTimeout the handshake, so a host
	// that is merely unreachable is written off before a worker is committed to
	// it for the whole request budget.
	DialTimeout time.Duration
	TLSTimeout  time.Duration
	// HeaderTimeout bounds the wait for response headers after the request has
	// gone out. Zero leaves it off and falls back to the request deadline.
	HeaderTimeout time.Duration
	// IdleTimeout is how long a connection to a host we are finished with is
	// held open. It is short, because on this work list we are finished with
	// almost every host after one page.
	IdleTimeout time.Duration
	// ConnsPerHost caps idle connections kept per host. The recrawl asks a host
	// for robots.txt and then one page, so two is generous and sixty four, which
	// is what the bulk transport keeps, would be a file descriptor leak wearing a
	// pool's clothes.
	ConnsPerHost int
}

// withDefaults fills in the zero values.
func (c WebPoolConfig) withDefaults() WebPoolConfig {
	if c.Workers <= 0 {
		c.Workers = DefaultRecrawlConfig.Workers
	}
	if c.Shards <= 0 {
		// One shard per sixteen workers, between eight and sixty four. Sixty four
		// is where ami's default sits and there is no evidence more helps.
		c.Shards = min(max(c.Workers/16, 8), 64)
	}
	if c.DNSLookups <= 0 {
		// One lookup slot per eight workers, at least sixteen and at most a
		// hundred and twenty eight. At 256 workers that is 32, which is the bound
		// measured to resolve 400 of 400 where unbounded resolved 269.
		c.DNSLookups = min(max(c.Workers/8, 16), 128)
	}
	if c.DNSTimeout <= 0 {
		c.DNSTimeout = 5 * time.Second
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.TLSTimeout <= 0 {
		c.TLSTimeout = 5 * time.Second
	}
	if c.HeaderTimeout <= 0 {
		c.HeaderTimeout = 10 * time.Second
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = 30 * time.Second
	}
	if c.ConnsPerHost <= 0 {
		c.ConnsPerHost = 2
	}
	return c
}

// webPool is a set of transports over one resolver, with a host pinned to one of
// them. It is an http.RoundTripper so that everything the run fetches goes
// through it, which is the point: robots.txt and the page have to land on the
// same transport or the connection is not reused.
type webPool struct {
	res *hostResolver
	trs []*http.Transport
}

// newWebPool builds the connection layer.
func newWebPool(cfg WebPoolConfig) *webPool {
	cfg = cfg.withDefaults()
	p := &webPool{
		res: newHostResolver(cfg.DNSTimeout, cfg.DNSLookups),
		trs: make([]*http.Transport, cfg.Shards),
	}
	for i := range p.trs {
		p.trs[i] = p.newTransport(cfg)
	}
	return p
}

// newTransport builds one shard.
func (p *webPool) newTransport(cfg WebPoolConfig) *http.Transport {
	dialer := &net.Dialer{Timeout: cfg.DialTimeout, KeepAlive: 30 * time.Second}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return dialer.DialContext(ctx, network, addr)
		}
		ips, ok := p.res.lookup(ctx, host)
		if !ok {
			// Deliberately not falling through to the dialer's own lookup. The
			// dialer would ask the system resolver again, outside the bound, and
			// the bound is the whole point. A name our pool could not resolve is
			// reported as unresolved rather than asked about a fifth time.
			return nil, &net.DNSError{Err: "no addresses found", Name: host, IsNotFound: true}
		}
		var last error
		for _, ip := range ips {
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if err == nil {
				return conn, nil
			}
			last = err
		}
		if last == nil {
			last = fmt.Errorf("no address for %s", host)
		}
		return nil, last
	}
	// MaxIdleConns 0 is unlimited, and it is right here only because
	// MaxIdleConnsPerHost is two and the idle timeout is thirty seconds. What
	// bounds the sockets is the per host cap and the reaping, not a global
	// number that closes a connection we are about to reuse.
	tr.MaxIdleConns = 0
	tr.MaxIdleConnsPerHost = cfg.ConnsPerHost
	tr.IdleConnTimeout = cfg.IdleTimeout
	tr.TLSHandshakeTimeout = cfg.TLSTimeout
	tr.ResponseHeaderTimeout = cfg.HeaderTimeout
	tr.ExpectContinueTimeout = time.Second
	// Compression stays on, unlike ami, which asks for identity. ami is sizing
	// for a link it saturates and wants the bytes it stores to be the bytes on
	// the wire. This run stores a decoded body either way and is nowhere near
	// saturating anything, so gzip is a third of the bandwidth for free.
	tr.DisableCompression = false
	return tr
}

// RoundTrip sends the request through the shard its host is pinned to.
func (p *webPool) RoundTrip(req *http.Request) (*http.Response, error) {
	return p.shardFor(req.URL.Hostname()).RoundTrip(req)
}

// shardFor picks the transport for a host. It has to be stable for the life of
// the run, or robots.txt and the page land on different pools and the whole
// exercise is pointless.
func (p *webPool) shardFor(host string) *http.Transport {
	h := fnv.New32a()
	_, _ = h.Write([]byte(host))
	return p.trs[int(h.Sum32()%uint32(len(p.trs)))]
}

// CloseIdleConnections closes every shard's idle connections. http.Client looks
// for this on its transport, so without it a client that is finished leaves the
// sockets open.
func (p *webPool) CloseIdleConnections() {
	for _, tr := range p.trs {
		tr.CloseIdleConnections()
	}
}

// stats reports what the resolver underneath did.
func (p *webPool) stats() ResolverStats { return p.res.stats() }

package ccrawl

import (
	"fmt"
	"hash/fnv"
	"net/url"
	"strings"

	"golang.org/x/net/publicsuffix"
)

// Shard splits one work list across several machines that never talk to each
// other. Each process is told which partition it owns and skips everything else,
// so three servers given the same list of URLs crawl three disjoint thirds of it
// with no coordination at all.
//
// The partition key is the registered domain. That is the whole design and it is
// worth saying why, because hashing the URL is the obvious thing and it is
// wrong here.
//
// A crawler keeps one politeness clock per host, and that clock only means
// anything if the host belongs to one process. Split a host's URLs across three
// servers and each one waits its own second while the site sees three requests a
// second, and every server believes it is behaving. Politeness that only holds
// per process is not politeness. Keying on the registered domain keeps a site and
// its clock on one machine, and it keeps a.example.co.uk with b.example.co.uk,
// which matter because they are usually the same server behind the same budget.
//
// ami shards on a hash of the URL, which is right for ami: it does not read
// robots.txt and keeps no per host clock, so splitting a host across processes
// costs it nothing. It costs us the only guarantee we make.
type Shard struct {
	Index int // this process's partition, 0-based
	Count int // total partitions, 1 means take everything
}

// Validate reports whether the pair describes a partition that exists.
func (s Shard) Validate() error {
	if s.Count < 1 {
		return fmt.Errorf("shards is %d, and a work list has at least one partition", s.Count)
	}
	if s.Index < 0 || s.Index >= s.Count {
		return fmt.Errorf("shard %d does not exist in %d shards, which are numbered 0 to %d", s.Index, s.Count, s.Count-1)
	}
	return nil
}

// Single reports whether this shard is the whole work list, which is the case
// worth skipping the hash for.
func (s Shard) Single() bool { return s.Count <= 1 }

// Owns reports whether rawURL belongs to this partition.
//
// A URL that cannot be parsed is owned by shard 0, so a malformed line in a
// work list is crawled once and refused once rather than silently dropped by
// every server in the fleet. Losing a URL to a parse bug is worse than one
// server spending a microsecond rejecting it.
func (s Shard) Owns(rawURL string) bool {
	if s.Single() {
		return true
	}
	key := ShardKey(rawURL)
	if key == "" {
		return s.Index == 0
	}
	return s.OwnsKey(key)
}

// OwnsKey reports whether an already-extracted partition key belongs to this
// shard. A caller reading a column of registered domains out of Parquet has the
// key already and should not pay to parse a URL again for it.
func (s Shard) OwnsKey(key string) bool {
	if s.Single() {
		return true
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum64()%uint64(s.Count)) == s.Index
}

// ShardKey is the registered domain a URL partitions on, lowercased, or empty
// when the URL has no host to speak of.
//
// It falls back to the host when the public suffix list has nothing to say,
// which covers an IP address, a name with no dot in it, and a suffix newer than
// the list we were built against. The fallback keeps the host whole rather than
// dropping the URL, so the guarantee that one host lives on one machine survives
// a name the list has never heard of.
func ShardKey(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return ""
	}
	if d, err := publicsuffix.EffectiveTLDPlusOne(host); err == nil && d != "" {
		return d
	}
	return host
}

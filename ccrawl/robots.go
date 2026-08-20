package ccrawl

import (
	"bufio"
	"context"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// robots.txt, to RFC 9309.
//
// What was here before matched literal path prefixes, recognised exactly two
// user agent names, and read a 5xx on /robots.txt as permission to crawl. That
// last one is the one that matters. A site whose robots endpoint is failing is
// a site that cannot tell us to stop, and crawling it anyway is how a crawler
// ends up in a block list and a mailbox full of abuse reports. RFC 9309 section
// 2.3.1.4 says an unreachable robots.txt means the whole site is disallowed,
// and that is what this does.

const (
	// robotsMaxBytes is the parse limit from RFC 9309 section 2.5, which asks
	// crawlers to handle at least 500 kibibytes. Anything past it is discarded
	// rather than treated as an error, so a file that is too long still yields
	// the rules it managed to state.
	robotsMaxBytes = 500 << 10

	// robotsErrorTTL is how long an unreachable robots.txt is remembered. The
	// spec allows caching a failure for up to 30 days. Five minutes is the
	// friendlier reading of the same rule: the host stays disallowed while it is
	// down and gets crawled again as soon as it recovers.
	robotsErrorTTL = 5 * time.Minute

	// robotsMinTTL is the floor under a lifetime a host asks for. A host that
	// says max-age=0 is asking us to refetch robots.txt before every page, which
	// at fleet speed is a request per page on a host that has just told us to be
	// careful with it.
	robotsMinTTL = time.Minute
)

// RobotsRule is one allow or disallow rule. The pattern is a path prefix in
// which * stands for any run of characters and a trailing $ anchors the match
// to the end of the path.
type RobotsRule struct {
	Allow   bool
	Pattern string
}

// RobotsEntry is a parsed robots.txt for one host, reduced to the group that
// applies to us.
type RobotsEntry struct {
	Rules      []RobotsRule
	CrawlDelay time.Duration
	Sitemaps   []string
	ExpiresAt  int64 // Unix timestamp, set by RobotsCache.Put

	// TTL, when set, overrides the cache TTL for this entry. It is how a
	// disallow that came from a failed fetch gets retried in minutes while a
	// robots.txt we actually read is kept for a day.
	TTL time.Duration

	// Unreachable marks an entry that came from a host that could not be asked
	// rather than from a robots.txt that was read. The rules are the same either
	// way, a complete disallow, but the two are not the same event and a run
	// that reports them as one number cannot tell a corpus that does not want us
	// from a network that is not working.
	Unreachable bool
}

// size estimates what the entry costs to hold, which is what the cache bounds
// itself by. Counting entries alone is not enough: a robots.txt is allowed to be
// 500 kibibytes and some of them are, so a fixed number of entries is a memory
// limit that varies by three orders of magnitude depending on who we crawl.
func (e *RobotsEntry) size() int64 {
	if e == nil {
		return 0
	}
	// The fixed term is the map entry, the node, the pointers and the struct
	// header. It is the part that dominates when the rule list is empty, which is
	// the common case and so the one worth being about right on.
	n := int64(96)
	for _, r := range e.Rules {
		n += int64(len(r.Pattern)) + 24
	}
	for _, s := range e.Sitemaps {
		n += int64(len(s)) + 16
	}
	return n
}

// IsAllowed reports whether the crawler may fetch a path. The longest matching
// rule wins and an allow beats a disallow of the same length, per RFC 9309
// section 2.2.2. A path no rule matches is allowed.
func (e *RobotsEntry) IsAllowed(path string) bool {
	if e == nil {
		return true
	}
	if path == "" {
		path = "/"
	}
	path = robotsNormalize(path)
	best, allow := -1, true
	for _, r := range e.Rules {
		if !robotsMatch(r.Pattern, path) {
			continue
		}
		if n := len(r.Pattern); n > best || (n == best && r.Allow) {
			best, allow = n, r.Allow
		}
	}
	return allow
}

// robotsMatch reports whether a rule pattern matches a path. The pattern is
// anchored at the start of the path and free at the end unless it ends in $.
func robotsMatch(pattern, path string) bool {
	anchored := strings.HasSuffix(pattern, "$")
	if anchored {
		pattern = pattern[:len(pattern)-1]
	}
	parts := strings.Split(pattern, "*")
	if !strings.HasPrefix(path, parts[0]) {
		return false
	}
	pos := len(parts[0])
	for i, part := range parts[1:] {
		if anchored && i == len(parts)-2 {
			// The last literal has to sit at the very end of the path, and there
			// has to be room for it after everything matched so far.
			return len(path)-len(part) >= pos && strings.HasSuffix(path, part)
		}
		// Leftmost match, which is what makes the search linear: a later literal
		// never needs an earlier one to give ground, because the pattern between
		// them is a * that absorbs anything.
		at := strings.Index(path[pos:], part)
		if at < 0 {
			return false
		}
		pos += at + len(part)
	}
	if anchored {
		return pos == len(path)
	}
	return true
}

// robotsNormalize puts a path and a rule pattern into the same encoding, since
// RFC 9309 section 2.2.2 compares them octet by octet. Existing escapes are
// upper cased rather than decoded and anything outside printable US-ASCII is
// escaped, so a URL and the rule written for it agree however the site spelled
// them.
func robotsNormalize(s string) string {
	if !robotsNeedsEscape(s) {
		return s
	}
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s) + 8)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '%' && i+2 < len(s) && isHexDigit(s[i+1]) && isHexDigit(s[i+2]):
			b.WriteByte('%')
			b.WriteByte(upperHexDigit(s[i+1]))
			b.WriteByte(upperHexDigit(s[i+2]))
			i += 2
		case c <= 0x20 || c >= 0x7f || c == '%':
			b.WriteByte('%')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&0x0f])
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

func robotsNeedsEscape(s string) bool {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c <= 0x20 || c >= 0x7f || c == '%' {
			return true
		}
	}
	return false
}

func isHexDigit(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

func upperHexDigit(c byte) byte {
	if c >= 'a' && c <= 'f' {
		return c - 'a' + 'A'
	}
	return c
}

// ── parsing ───────────────────────────────────────────────────────────────────

// robotsGroup is one group of records: the user agent lines that open it and
// the rules that follow until the next user agent line.
type robotsGroup struct {
	agents   []string
	rules    []RobotsRule
	delay    time.Duration
	hasDelay bool
}

// parseRobots parses a robots.txt body and returns the rules that apply to
// userAgent. Records for other user agents are read and discarded, so the
// result is a plain rule list the caller can match paths against without
// knowing which group it came from.
func parseRobots(r io.Reader, userAgent string) *RobotsEntry {
	var (
		groups   []*robotsGroup
		cur      *robotsGroup
		sitemaps []string
		opening  bool // consecutive user agent lines open one group, not several
	)
	sc := bufio.NewScanner(io.LimitReader(r, robotsMaxBytes))
	// Sitemap lines can be long and a single overlong line should not stop the
	// parse, so the buffer is generous rather than the 64 KB default.
	sc.Buffer(make([]byte, 0, 8<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		key, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		val = strings.TrimSpace(val)
		switch key {
		case "user-agent", "useragent":
			if !opening {
				cur = &robotsGroup{}
				groups = append(groups, cur)
				opening = true
			}
			// A group still opens on a nameless User-agent line, so the rules
			// under it stay out of the previous group, but it names nobody. The
			// empty string is a prefix of every product token, so keeping it
			// would let one malformed line beat the wildcard group for every
			// crawler that reads the file.
			if val != "" {
				cur.agents = append(cur.agents, strings.ToLower(val))
			}
		case "allow", "disallow":
			opening = false
			// Rules before any user agent line belong to no group, and RFC 9309
			// section 2.2.1 says to ignore them. An empty Disallow is the
			// documented way to say "nothing is disallowed", which is the absence
			// of a rule rather than a rule matching everything.
			if cur == nil || val == "" {
				continue
			}
			cur.rules = append(cur.rules, RobotsRule{
				Allow:   key == "allow",
				Pattern: robotsNormalize(val),
			})
		case "crawl-delay":
			opening = false
			if cur == nil {
				continue
			}
			if d, err := strconv.ParseFloat(val, 64); err == nil && d > 0 {
				cur.delay, cur.hasDelay = time.Duration(d*float64(time.Second)), true
			}
		case "sitemap":
			// Not a group record. It applies to the whole file wherever it
			// appears, and it does not interrupt the group being read.
			if val != "" {
				sitemaps = append(sitemaps, val)
			}
		}
	}
	entry := matchGroups(groups, robotsToken(userAgent))
	entry.Sitemaps = sitemaps
	return entry
}

// matchGroups reduces the parsed groups to the one that applies to token.
//
// RFC 9309 section 2.2.1: the most specific matching product token wins, the
// wildcard group is the fallback and never merges with a specific one, and
// groups that name the same token are followed as one.
func matchGroups(groups []*robotsGroup, token string) *RobotsEntry {
	best := -1
	for _, g := range groups {
		for _, a := range g.agents {
			if a == "*" || !strings.HasPrefix(token, a) {
				continue
			}
			if len(a) > best {
				best = len(a)
			}
		}
	}
	entry := &RobotsEntry{}
	for _, g := range groups {
		if !groupApplies(g, token, best) {
			continue
		}
		entry.Rules = append(entry.Rules, g.rules...)
		// The longest delay across merged groups, because the point of the field
		// is a floor on how hard we hit the host.
		if g.hasDelay && g.delay > entry.CrawlDelay {
			entry.CrawlDelay = g.delay
		}
	}
	return entry
}

// groupApplies reports whether a group is part of the set we follow, given the
// specificity of the best matching product token in the file. A best of -1
// means nothing named us and the wildcard group is what is left.
func groupApplies(g *robotsGroup, token string, best int) bool {
	for _, a := range g.agents {
		if best < 0 {
			if a == "*" {
				return true
			}
			continue
		}
		if a != "*" && len(a) == best && strings.HasPrefix(token, a) {
			return true
		}
	}
	return false
}

// robotsToken reduces a User-Agent header to the product token robots.txt is
// written against, so that "CCrawl/2.0 (+https://example.com/bot)" is matched
// by a group naming ccrawl. Matching is case insensitive, and a group naming a
// prefix of our token applies to us, which is how a site writes one rule for a
// crawler and all of its variants.
func robotsToken(userAgent string) string {
	token := strings.TrimSpace(userAgent)
	if i := strings.IndexAny(token, "/ \t"); i >= 0 {
		token = token[:i]
	}
	return strings.ToLower(token)
}

// ── fetching and caching ──────────────────────────────────────────────────────

// The cache, and why it is the whole of the robots story at fleet speed.
//
// A threefold recrawl of the domain corpus is 363M homepages across 121M
// registered domains. Fetched naively that is 121M extra requests, one for every
// three pages, which is a third of the fleet's capacity spent on a file that
// rarely changes and a third more load on every site we visit. So robots is
// fetched once per host and held.
//
// Held where, though. A map that never forgets is fine over a few thousand hosts
// and fatal over a hundred million, and the servers this fleet runs on have 5,
// 11 and 23 GB of RAM. So the cache is bounded, by entries and by bytes, and
// evicts least recently used. That policy is not a guess: the work list is
// published sorted, so a shard walks through hosts in bursts and leaves them
// behind, which is the access pattern LRU is for.
//
// The bound is on both because either one alone is a bad limit. Entries alone
// leaves memory at the mercy of how long the robots.txt files happen to be, and
// RFC 9309 lets them be 500 kibibytes. Bytes alone lets a corpus of tiny files
// fill the map with millions of entries whose real cost is the map, not the
// contents.

const (
	// DefaultRobotsTTL is how long a fetched robots.txt is believed when it does
	// not say. A day is what every crawl in this repo has always used and what
	// RFC 9309 section 2.4 suggests as standard; it is named so the two run loops
	// cannot drift apart on it.
	DefaultRobotsTTL = 24 * time.Hour

	// DefaultRobotsMaxEntries and DefaultRobotsMaxBytes bound the cache.
	//
	// 200 000 hosts at 64 MB is sized for the fleet: it is far more than the
	// hosts a shard is working through at any moment, small enough to be a
	// rounding error against the smallest server's 5 GB, and it holds a working
	// set across the bursts a sorted work list arrives in.
	DefaultRobotsMaxEntries = 200_000
	DefaultRobotsMaxBytes   = 64 << 20
)

// RobotsLimits bounds what the cache holds. A non-positive field means that
// bound is not applied, which is for a test or a small run and wrong for a
// fleet.
type RobotsLimits struct {
	MaxEntries int
	MaxBytes   int64
}

// DefaultRobotsLimits is what a run gets when it does not ask for anything else.
func DefaultRobotsLimits() RobotsLimits {
	return RobotsLimits{MaxEntries: DefaultRobotsMaxEntries, MaxBytes: DefaultRobotsMaxBytes}
}

// RobotsStats is what the cache did, so the extra request per host is reported
// rather than guessed at.
type RobotsStats struct {
	Fetches     int64 // robots.txt requests actually sent
	Hits        int64 // lookups a cached entry answered
	Evictions   int64 // entries dropped to stay inside the limits
	Expired     int64 // entries found past their lifetime and refetched
	Unreachable int64 // fetches that failed, every one of them a disallow
	Entries     int   // entries held right now
	Bytes       int64 // what they are estimated to cost
}

// robotsNode is a cache entry and its place in the recency list.
type robotsNode struct {
	host       string
	entry      *RobotsEntry
	size       int64
	prev, next *robotsNode
}

// RobotsCache caches parsed robots.txt per host, bounded and least recently
// used.
type RobotsCache struct {
	mu    sync.Mutex
	nodes map[string]*robotsNode
	// head is the most recently used and tail is the next to go. The list is
	// intrusive and doubly linked so a hit is a constant time unlink and relink
	// rather than a scan.
	head, tail *robotsNode
	bytes      int64
	lim        RobotsLimits
	// inflight holds one channel per host being fetched, closed when the fetch
	// lands. Without it, sixty four workers arriving at a new host together ask
	// that host for robots.txt sixty four times, which is a rude way to open a
	// conversation about politeness.
	inflight map[string]chan struct{}
	ttl      time.Duration
	ua       string // the crawler's user agent, matched against the groups

	fetches, hits, evictions, expired, unreachable atomic.Int64
}

// NewRobotsCache creates a cache with the given TTL and user agent string,
// bounded at the fleet defaults.
func NewRobotsCache(ttl time.Duration, userAgent string) *RobotsCache {
	return NewRobotsCacheWithLimits(ttl, userAgent, DefaultRobotsLimits())
}

// NewRobotsCacheWithLimits creates a cache bounded as asked.
func NewRobotsCacheWithLimits(ttl time.Duration, userAgent string, lim RobotsLimits) *RobotsCache {
	return &RobotsCache{
		nodes:    make(map[string]*robotsNode),
		inflight: make(map[string]chan struct{}),
		ttl:      ttl,
		ua:       userAgent,
		lim:      lim,
	}
}

// Stats reports what the cache has done and what it is holding.
func (rc *RobotsCache) Stats() RobotsStats {
	rc.mu.Lock()
	entries, bytes := len(rc.nodes), rc.bytes
	rc.mu.Unlock()
	return RobotsStats{
		Fetches:     rc.fetches.Load(),
		Hits:        rc.hits.Load(),
		Evictions:   rc.evictions.Load(),
		Expired:     rc.expired.Load(),
		Unreachable: rc.unreachable.Load(),
		Entries:     entries,
		Bytes:       bytes,
	}
}

// Get returns the cached robots entry for host, or nil if it is missing or
// expired. A hit is also a use, which is what moves the host to the front of the
// recency list.
func (rc *RobotsCache) Get(host string) *RobotsEntry {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	n, ok := rc.nodes[host]
	if !ok {
		return nil
	}
	if time.Now().Unix() >= n.entry.ExpiresAt {
		// Dropped rather than left to be evicted later, since an expired entry is
		// dead weight against both bounds and the caller is about to replace it.
		rc.remove(n)
		rc.expired.Add(1)
		return nil
	}
	rc.touch(n)
	rc.hits.Add(1)
	return n.entry
}

// Put stores a robots entry for host. An entry carrying its own TTL keeps it,
// which is how a disallow from a failed fetch expires in minutes instead of
// sticking for the cache's usual day.
func (rc *RobotsCache) Put(host string, e *RobotsEntry) {
	ttl := rc.ttl
	if e.TTL > 0 {
		ttl = e.TTL
	}
	e.ExpiresAt = time.Now().Add(ttl).Unix()

	rc.mu.Lock()
	defer rc.mu.Unlock()
	if old, ok := rc.nodes[host]; ok {
		rc.remove(old)
	}
	n := &robotsNode{host: host, entry: e, size: e.size() + int64(len(host))}
	rc.nodes[host] = n
	rc.bytes += n.size
	rc.pushFront(n)
	rc.evictLocked()
}

// evictLocked drops least recently used entries until the cache is inside both
// bounds. The entry just added is never the one dropped, which matters when a
// single robots.txt is larger than the whole byte budget: the caller is about to
// use it, and dropping it would turn every page on that host into a refetch.
func (rc *RobotsCache) evictLocked() {
	for rc.tail != nil && rc.tail != rc.head {
		overEntries := rc.lim.MaxEntries > 0 && len(rc.nodes) > rc.lim.MaxEntries
		overBytes := rc.lim.MaxBytes > 0 && rc.bytes > rc.lim.MaxBytes
		if !overEntries && !overBytes {
			return
		}
		rc.remove(rc.tail)
		rc.evictions.Add(1)
	}
}

// pushFront puts a node at the most recently used end.
func (rc *RobotsCache) pushFront(n *robotsNode) {
	n.prev, n.next = nil, rc.head
	if rc.head != nil {
		rc.head.prev = n
	}
	rc.head = n
	if rc.tail == nil {
		rc.tail = n
	}
}

// touch moves a node to the most recently used end.
func (rc *RobotsCache) touch(n *robotsNode) {
	if rc.head == n {
		return
	}
	rc.unlink(n)
	rc.pushFront(n)
}

// unlink takes a node out of the recency list without forgetting it.
func (rc *RobotsCache) unlink(n *robotsNode) {
	if n.prev != nil {
		n.prev.next = n.next
	} else if rc.head == n {
		rc.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else if rc.tail == n {
		rc.tail = n.prev
	}
	n.prev, n.next = nil, nil
}

// remove forgets a node entirely.
func (rc *RobotsCache) remove(n *robotsNode) {
	rc.unlink(n)
	delete(rc.nodes, n.host)
	rc.bytes -= n.size
}

// Fetch returns the robots entry for a host, fetching and caching it on a miss.
// Concurrent misses on the same host share one fetch: the first caller goes to
// the network and the rest wait for it.
func (rc *RobotsCache) Fetch(ctx context.Context, h *HTTPClient, host, scheme string) *RobotsEntry {
	for {
		if e := rc.Get(host); e != nil {
			return e
		}
		rc.mu.Lock()
		if wait, ok := rc.inflight[host]; ok {
			rc.mu.Unlock()
			select {
			case <-wait:
			case <-ctx.Done():
				return robotsUnreachable()
			}
			continue
		}
		done := make(chan struct{})
		rc.inflight[host] = done
		rc.mu.Unlock()

		rc.fetches.Add(1)
		e := FetchRobots(ctx, h, host, scheme, rc.ua)
		if e.Unreachable {
			rc.unreachable.Add(1)
		}
		rc.Put(host, e)
		rc.mu.Lock()
		delete(rc.inflight, host)
		rc.mu.Unlock()
		close(done)
		return e
	}
}

// FetchRobots fetches and parses robots.txt for one host.
//
// The status handling is RFC 9309 section 2.3.1 and it is not symmetric. A 2xx
// is parsed. A 4xx means there is no robots.txt, which the spec reads as the
// whole site being open. A 3xx that outlived the client's redirect following is
// unresolvable and section 2.3.1.2 treats it the same way. A 5xx or a network
// failure means the site could not tell us anything, and section 2.3.1.4 is
// explicit that this is a complete disallow rather than a shrug.
// The lifetime comes from the response where the host states one, per section
// 2.4, which asks crawlers to use standard cache control. A host that wants to
// be asked again in an hour gets asked again in an hour, and one that asks for a
// year gets a day, because a day is as long as the spec lets anybody hold this.
func FetchRobots(ctx context.Context, h *HTTPClient, host, scheme, userAgent string) *RobotsEntry {
	resp, err := h.getStatus(ctx, scheme+"://"+host+"/robots.txt")
	if err != nil {
		return robotsUnreachable()
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		e := parseRobots(resp.Body, userAgent)
		e.TTL = robotsStatedTTL(resp.Header.Get("Cache-Control"))
		return e
	case resp.StatusCode >= 500:
		return robotsUnreachable()
	default:
		return &RobotsEntry{}
	}
}

// robotsStatedTTL reads max-age out of a Cache-Control header, returning zero
// when the host said nothing usable and the cache's own TTL should stand.
//
// no-store and no-cache are honoured as the shortest lifetime we are willing to
// keep rather than as no caching at all. Refetching robots.txt before every page
// would be one request per page on a host that asked us to be careful, which is
// the opposite of what it asked for.
func robotsStatedTTL(header string) time.Duration {
	if header == "" {
		return 0
	}
	for _, part := range strings.Split(header, ",") {
		part = strings.ToLower(strings.TrimSpace(part))
		switch {
		case part == "no-store", part == "no-cache":
			return robotsMinTTL
		case strings.HasPrefix(part, "max-age="):
			secs, err := strconv.Atoi(strings.TrimSpace(part[len("max-age="):]))
			if err != nil || secs < 0 {
				return 0
			}
			d := time.Duration(secs) * time.Second
			return min(max(d, robotsMinTTL), DefaultRobotsTTL)
		}
	}
	return 0
}

// robotsUnreachable is the entry for a host that could not be asked: everything
// disallowed, and remembered only briefly so the host is retried soon.
func robotsUnreachable() *RobotsEntry {
	return &RobotsEntry{
		Rules:       []RobotsRule{{Pattern: "/"}},
		TTL:         robotsErrorTTL,
		Unreachable: true,
	}
}

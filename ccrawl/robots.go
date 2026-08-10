package ccrawl

import (
	"bufio"
	"context"
	"io"
	"strconv"
	"strings"
	"sync"
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

// RobotsCache caches parsed robots.txt per host with a TTL.
type RobotsCache struct {
	mu      sync.RWMutex
	entries map[string]*RobotsEntry
	ttl     time.Duration
	ua      string // the crawler's user agent, matched against the groups
}

// NewRobotsCache creates a cache with the given TTL and user agent string.
func NewRobotsCache(ttl time.Duration, userAgent string) *RobotsCache {
	return &RobotsCache{
		entries: make(map[string]*RobotsEntry),
		ttl:     ttl,
		ua:      userAgent,
	}
}

// Get returns the cached robots entry for host, or nil if it is missing or
// expired.
func (rc *RobotsCache) Get(host string) *RobotsEntry {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	e, ok := rc.entries[host]
	if !ok || time.Now().Unix() >= e.ExpiresAt {
		return nil
	}
	return e
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
	rc.entries[host] = e
	rc.mu.Unlock()
}

// Fetch returns the robots entry for a host, fetching and caching it on a miss.
func (rc *RobotsCache) Fetch(ctx context.Context, h *HTTPClient, host, scheme string) *RobotsEntry {
	if e := rc.Get(host); e != nil {
		return e
	}
	e := FetchRobots(ctx, h, host, scheme, rc.ua)
	rc.Put(host, e)
	return e
}

// FetchRobots fetches and parses robots.txt for one host.
//
// The status handling is RFC 9309 section 2.3.1 and it is not symmetric. A 2xx
// is parsed. A 4xx means there is no robots.txt, which the spec reads as the
// whole site being open. A 3xx that outlived the client's redirect following is
// unresolvable and section 2.3.1.2 treats it the same way. A 5xx or a network
// failure means the site could not tell us anything, and section 2.3.1.4 is
// explicit that this is a complete disallow rather than a shrug.
func FetchRobots(ctx context.Context, h *HTTPClient, host, scheme, userAgent string) *RobotsEntry {
	resp, err := h.getStatus(ctx, scheme+"://"+host+"/robots.txt")
	if err != nil {
		return robotsUnreachable()
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return parseRobots(resp.Body, userAgent)
	case resp.StatusCode >= 500:
		return robotsUnreachable()
	default:
		return &RobotsEntry{}
	}
}

// robotsUnreachable is the entry for a host that could not be asked: everything
// disallowed, and remembered only briefly so the host is retried soon.
func robotsUnreachable() *RobotsEntry {
	return &RobotsEntry{
		Rules: []RobotsRule{{Pattern: "/"}},
		TTL:   robotsErrorTTL,
	}
}

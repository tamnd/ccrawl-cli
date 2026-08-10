package ccrawl

// seenCache is a memory bounded exact set of URL hashes.
//
// It answers "this URL was definitely admitted already" without a disk read,
// which is the answer that matters at crawl scale: a crawl rediscovers the same
// URLs over and over from outlinks, and a lookup per rediscovery is what turns
// a frontier into the slowest part of the crawler.
//
// The obvious structure here is a Bloom filter, and it is the wrong one. A
// Bloom filter's error is a false positive, which in front of a seen check
// means reporting a brand new URL as already known, and a frontier that does
// that drops the URL. At the usual one percent that is one page in a hundred
// gone from the crawl, silently, with nothing downstream able to tell. Trading
// coverage for memory is a decision a crawl operator might make on purpose, but
// it is not one a data structure should make on their behalf.
//
// So this is exact for what it holds and simply forgets the coldest entries
// instead of lying about them. A forgotten entry costs one wasted row in the
// next insert batch, where the primary key drops it, and nothing else.
//
// Eviction is two generations rather than an LRU list, because an LRU needs a
// list node per entry and the node costs more than the key. When the hot
// generation fills it becomes the cold one and the previous cold one is
// dropped, so the set holds between limit and two times limit entries and the
// bookkeeping is a map assignment.
type seenCache struct {
	hot, cold map[uint64]struct{}
	limit     int
}

func newSeenCache(limit int) *seenCache {
	if limit < 1 {
		limit = 1
	}
	return &seenCache{hot: make(map[uint64]struct{}, limit/8), limit: limit}
}

// add records a key and reports whether it was already known. A true return is
// exact. A false return means the cache does not know, which is either a new
// URL or one evicted long enough ago to be cold.
func (c *seenCache) add(key uint64) bool {
	if _, ok := c.hot[key]; ok {
		return true
	}
	if _, ok := c.cold[key]; ok {
		// Promote, so a URL that keeps being rediscovered stays resident rather
		// than aging out on a fixed schedule.
		c.hot[key] = struct{}{}
		return true
	}
	if len(c.hot) >= c.limit {
		c.cold, c.hot = c.hot, make(map[uint64]struct{}, c.limit/8)
	}
	c.hot[key] = struct{}{}
	return false
}

// len reports how many keys are resident, for tests and stats.
func (c *seenCache) len() int { return len(c.hot) + len(c.cold) }

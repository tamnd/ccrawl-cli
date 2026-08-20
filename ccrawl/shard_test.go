package ccrawl

import (
	"fmt"
	"testing"
)

// TestShardVectorsAreFixed pins the partition each of these URLs lands in.
//
// The whole fleet design rests on three machines agreeing about who owns what
// without ever asking each other, so the assignment has to be the same on every
// build of every binary on every host. That is a property no property test can
// check, because a hash function swapped for another one is still a function.
// Fixed vectors catch it: change the hash, the seed, the modulus or the key and
// this test says so.
//
// The expected values were produced by the implementation and read back by
// hand, which is the only way to write a vector table. They are correct in the
// only sense that matters, which is that they must never move.
func TestShardVectorsAreFixed(t *testing.T) {
	cases := []struct {
		url   string
		shard int
	}{
		{"https://example.com/", 2},
		{"https://example.com/deep/page.html?q=1", 2},
		{"http://www.example.com/", 2},
		{"https://news.ycombinator.com/", 0},
		{"https://en.wikipedia.org/wiki/Main_Page", 2},
		{"https://bbc.co.uk/news", 0},
		{"https://www.bbc.co.uk/news", 0},
		{"https://golang.org/", 0},
		{"https://commoncrawl.org/", 1},
	}
	for _, c := range cases {
		var got int
		for i := 0; i < 3; i++ {
			if (Shard{Index: i, Count: 3}).Owns(c.url) {
				got = i
			}
		}
		if got != c.shard {
			t.Errorf("%s lands in shard %d of 3, want %d; the partition assignment moved and the fleet no longer agrees with itself", c.url, got, c.shard)
		}
	}
}

// TestShardKeepsARegisteredDomainWhole is the guarantee the crawler's politeness
// depends on. Two hosts under one registered domain are usually one server, and
// if they land on two machines then each waits its own delay and the site sees
// twice the traffic while both machines believe they are behaving.
func TestShardKeepsARegisteredDomainWhole(t *testing.T) {
	groups := [][]string{
		{"https://a.example.co.uk/", "https://b.example.co.uk/", "https://example.co.uk/x"},
		{"https://www.example.com/", "https://shop.example.com/", "http://EXAMPLE.COM:8080/y"},
		{"https://deep.sub.domain.example.org/1", "https://example.org/2"},
	}
	for _, g := range groups {
		want := ownerOf(t, g[0], 7)
		for _, u := range g[1:] {
			if got := ownerOf(t, u, 7); got != want {
				t.Errorf("%s went to shard %d and %s went to shard %d, so one registered domain is split across two machines and neither can keep its politeness clock", g[0], want, u, got)
			}
		}
	}
}

// TestShardSeparatesDistinctRegisteredDomains is the other half. A public suffix
// is not a registered domain, so two unrelated sites under .co.uk or under a
// hosting suffix must be free to land on different machines, or the partition
// collapses towards one bucket.
func TestShardSeparatesDistinctRegisteredDomains(t *testing.T) {
	if ShardKey("https://a.co.uk/") == ShardKey("https://b.co.uk/") {
		t.Fatal("a.co.uk and b.co.uk share a partition key, so the public suffix list is not being consulted and every .co.uk site would crowd onto one machine")
	}
	if got := ShardKey("https://a.example.co.uk/"); got != "example.co.uk" {
		t.Errorf("ShardKey gave %q, want example.co.uk", got)
	}
	if got := ShardKey("http://192.168.0.1:9000/page"); got != "192.168.0.1" {
		t.Errorf("ShardKey gave %q for an IP, want the address itself", got)
	}
	if got := ShardKey("http://localhost:8080/page"); got != "localhost" {
		t.Errorf("ShardKey gave %q for a name with no suffix, want the host kept whole", got)
	}
}

// TestShardOneTakesEverything covers the case a single machine runs, which has
// to behave exactly as it did before shards existed.
func TestShardOneTakesEverything(t *testing.T) {
	for _, s := range []Shard{{Index: 0, Count: 1}, {Count: 0}} {
		for _, u := range []string{"https://example.com/", "not a url at all", ""} {
			if !s.Owns(u) {
				t.Errorf("shard %+v dropped %q, and a single shard is the whole work list", s, u)
			}
		}
	}
}

// TestShardCoversEveryURLExactlyOnce is the property that makes a fleet a fleet.
// A URL owned by nobody is never crawled and a URL owned by two machines is
// crawled twice, and both are silent failures at fleet scale.
func TestShardCoversEveryURLExactlyOnce(t *testing.T) {
	for _, count := range []int{2, 3, 5, 8} {
		for i := 0; i < 20000; i++ {
			u := fmt.Sprintf("https://site%d.example%d.com/page%d", i%977, i, i)
			owners := 0
			for s := 0; s < count; s++ {
				if (Shard{Index: s, Count: count}).Owns(u) {
					owners++
				}
			}
			if owners != 1 {
				t.Fatalf("%s is owned by %d of %d shards, want exactly 1", u, owners, count)
			}
		}
	}
}

// TestShardMalformedURLsGoToShardZero says where the awkward cases go. Dropping
// them from every shard would lose them without a word, which is the one outcome
// worth ruling out.
func TestShardMalformedURLsGoToShardZero(t *testing.T) {
	for _, u := range []string{"", "://nonsense", "https://", "mailto:someone@example.com"} {
		owners := 0
		for s := 0; s < 3; s++ {
			if (Shard{Index: s, Count: 3}).Owns(u) {
				owners++
			}
		}
		if owners != 1 {
			t.Errorf("%q is owned by %d shards, want exactly 1 so it is crawled once rather than dropped by all three", u, owners)
		}
		if !(Shard{Index: 0, Count: 3}).Owns(u) {
			t.Errorf("%q did not land in shard 0, which is where the unparseable ones are meant to go", u)
		}
	}
}

func TestShardValidate(t *testing.T) {
	for _, s := range []Shard{{Index: 0, Count: 1}, {Index: 2, Count: 3}} {
		if err := s.Validate(); err != nil {
			t.Errorf("shard %+v is valid, got %v", s, err)
		}
	}
	for _, s := range []Shard{{Index: 3, Count: 3}, {Index: -1, Count: 3}, {Index: 0, Count: 0}} {
		if err := s.Validate(); err == nil {
			t.Errorf("shard %+v does not exist and Validate accepted it", s)
		}
	}
}

// ownerOf returns the shard that claims a URL, failing if that is not exactly
// one shard.
func ownerOf(t *testing.T, rawURL string, count int) int {
	t.Helper()
	owner := -1
	for s := 0; s < count; s++ {
		if (Shard{Index: s, Count: count}).Owns(rawURL) {
			if owner >= 0 {
				t.Fatalf("%s is claimed by shard %d and shard %d", rawURL, owner, s)
			}
			owner = s
		}
	}
	if owner < 0 {
		t.Fatalf("%s is claimed by no shard out of %d", rawURL, count)
	}
	return owner
}

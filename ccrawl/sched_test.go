package ccrawl

import (
	"strings"
	"testing"
)

func TestCrawlTier(t *testing.T) {
	cases := []struct {
		pos        int64
		changeRate float64
		wantTier   int
	}{
		{50_000, 0.9, 1},       // top 100K, high change → tier 1
		{500_000, 0.6, 2},      // top 1M, moderate change → tier 2
		{2_000_000, 0.3, 3},    // top 5M, low change → tier 3
		{5_000_000, 0.1, 4},    // top 10M → tier 4
		{50_000_000, 0.9, 5},   // long tail → tier 5
		{100_001, 0.9, 2},      // just outside tier-1 rank but top 1M with high change → tier 2
	}
	for _, c := range cases {
		got := CrawlTier(c.pos, c.changeRate)
		if got != c.wantTier {
			t.Errorf("CrawlTier(%d, %.1f) = %d, want %d", c.pos, c.changeRate, got, c.wantTier)
		}
	}
}

func TestTierInterval(t *testing.T) {
	if TierInterval(1) != 24 {
		t.Errorf("tier 1 should be 24h")
	}
	if TierInterval(5) != 0 {
		t.Errorf("tier 5 should be 0 (on-demand)")
	}
}

func TestCrawlScore(t *testing.T) {
	// higher harmonic val → higher score (given same change rate)
	s1 := CrawlScore(1e7, 0.5)
	s2 := CrawlScore(1e6, 0.5)
	if s1 <= s2 {
		t.Errorf("higher harmonic val should give higher score")
	}
	// zero change rate → zero score
	if CrawlScore(1e7, 0) != 0 {
		t.Error("zero change rate should give zero score")
	}
}

func TestHostScheduleFrom(t *testing.T) {
	r := Rank{Key: "github.com", HarmonicPos: 16, HarmonicVal: 23e6, PageRankPos: 23, PageRankVal: 0.001}
	hs := HostScheduleFrom(r, 0.7)
	if hs.Host != "github.com" {
		t.Errorf("Host = %q", hs.Host)
	}
	if hs.Tier < 1 || hs.Tier > 5 {
		t.Errorf("Tier out of range: %d", hs.Tier)
	}
	if hs.IntervalH == 0 && hs.Tier < 5 {
		t.Error("non-tier-5 hosts should have a non-zero interval")
	}
	if hs.Score <= 0 {
		t.Errorf("Score should be positive, got %f", hs.Score)
	}
}

func TestDiffCDXSQL(t *testing.T) {
	urlsA := []string{"https://example.com/partA.parquet"}
	urlsB := []string{"https://example.com/partB.parquet"}
	sql := DiffCDXSQL(urlsA, urlsB, "CC-MAIN-2026-17", "CC-MAIN-2026-21")
	if sql == "" {
		t.Fatal("empty SQL")
	}
	for _, want := range []string{
		"url_host_name", "url_digest", "change_rate",
		"CC-MAIN-2026-17", "CC-MAIN-2026-21",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("SQL missing %q", want)
		}
	}
}

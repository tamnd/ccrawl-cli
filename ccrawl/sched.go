package ccrawl

import (
	"context"
	"math"
)

// ── recrawl scheduling ────────────────────────────────────────────────────────

// CrawlTier assigns a 1–5 crawl tier to a host based on its harmonic rank
// position and estimated change rate. Lower tier = more frequent crawling.
//
//	Tier 1: > 0.8 change rate + top 100 K rank  → 24 h interval
//	Tier 2: 0.5–0.8 + top 1 M                  → 3 days
//	Tier 3: 0.2–0.5 + top 5 M                  → 7 days
//	Tier 4: < 0.2  + top 10 M                  → 30 days
//	Tier 5: everything else                     → on-demand
func CrawlTier(harmonicPos int64, changeRate float64) int {
	switch {
	case harmonicPos <= 100_000 && changeRate > 0.8:
		return 1
	case harmonicPos <= 1_000_000 && changeRate >= 0.5:
		return 2
	case harmonicPos <= 5_000_000 && changeRate >= 0.2:
		return 3
	case harmonicPos <= 10_000_000:
		return 4
	default:
		return 5
	}
}

// CrawlScore computes a composite score combining link-graph importance and
// empirical change rate. Higher = higher crawl priority.
func CrawlScore(harmonicVal, changeRate float64) float64 {
	return changeRate * math.Log1p(harmonicVal)
}

// TierInterval returns the recommended re-crawl interval in hours for a tier.
func TierInterval(tier int) int {
	switch tier {
	case 1:
		return 24
	case 2:
		return 72
	case 3:
		return 168
	case 4:
		return 720
	default:
		return 0 // on-demand only
	}
}

// ── differential CDX analysis ─────────────────────────────────────────────────

// HostDiffEntry is one row of the per-host change report between two CDX
// snapshots. It shows how many URLs were seen in both crawls, how many had
// their content digest change, and the derived change rate.
type HostDiffEntry struct {
	Host        string  `json:"host" table:"host"`
	TotalURLs   int64   `json:"total_urls" table:"total_urls"`
	ChangedURLs int64   `json:"changed_urls" table:"changed_urls"`
	ChangeRate  float64 `json:"change_rate" table:"change_rate"`
	Tier        int     `json:"tier" table:"tier"`
}

// DiffCDXSQL returns the DuckDB SQL that computes per-host change rates between
// two crawl snapshots by joining on url and comparing content digests.
func DiffCDXSQL(urlsA, urlsB []string, crawlA, crawlB string) string {
	srcA := ParquetListLiteral(urlsA)
	srcB := ParquetListLiteral(urlsB)
	return `SELECT
    a.url_host_name AS host,
    COUNT(*) AS total_urls,
    SUM(CASE WHEN a.content_digest != b.content_digest THEN 1 ELSE 0 END) AS changed_urls,
    ROUND(SUM(CASE WHEN a.content_digest != b.content_digest THEN 1 ELSE 0 END)::double / NULLIF(COUNT(*), 0), 4) AS change_rate
FROM read_parquet(` + srcA + `, hive_partitioning=1) a
JOIN read_parquet(` + srcB + `, hive_partitioning=1) b
    ON a.url = b.url
WHERE a.crawl = '` + sqlEscape(crawlA) + `'
  AND b.crawl = '` + sqlEscape(crawlB) + `'
GROUP BY a.url_host_name
ORDER BY change_rate DESC`
}

// DiffCDX runs the differential CDX analysis via DuckDB and calls fn for each
// HostDiffEntry. Requires DuckDB on PATH.
func DiffCDX(ctx context.Context, urlsA, urlsB []string, crawlA, crawlB string, fn func(HostDiffEntry) error) error {
	sql := DiffCDXSQL(urlsA, urlsB, crawlA, crawlB)
	return RunDuckDBJSON(ctx, "", sql, func(row map[string]any) error {
		e := HostDiffEntry{
			Host:        stringVal(row, "host"),
			TotalURLs:   int64Val(row, "total_urls"),
			ChangedURLs: int64Val(row, "changed_urls"),
			ChangeRate:  float64Val(row, "change_rate"),
		}
		// use harmonic rank position 0 (unknown) → tier based only on change rate
		e.Tier = CrawlTier(0, e.ChangeRate)
		return fn(e)
	})
}

// HostSchedule holds the crawl schedule for one host: its tier and derived
// priority score.
type HostSchedule struct {
	Host        string  `json:"host" table:"host"`
	HarmonicPos int64   `json:"harmonic_pos" table:"harmonic_pos"`
	HarmonicVal float64 `json:"harmonic_val" table:"harmonic_val"`
	ChangeRate  float64 `json:"change_rate" table:"change_rate"`
	Tier        int     `json:"tier" table:"tier"`
	IntervalH   int     `json:"interval_h" table:"interval_h"`
	Score       float64 `json:"score" table:"score"`
}

// HostScheduleFrom builds a HostSchedule from a Rank entry and a change rate.
func HostScheduleFrom(r Rank, changeRate float64) HostSchedule {
	tier := CrawlTier(r.HarmonicPos, changeRate)
	return HostSchedule{
		Host:        r.Key,
		HarmonicPos: r.HarmonicPos,
		HarmonicVal: r.HarmonicVal,
		ChangeRate:  changeRate,
		Tier:        tier,
		IntervalH:   TierInterval(tier),
		Score:       CrawlScore(r.HarmonicVal, changeRate),
	}
}

// float64Val extracts a float64 from a DuckDB JSON row.
func float64Val(row map[string]any, key string) float64 {
	if v, ok := row[key]; ok && v != nil {
		switch n := v.(type) {
		case float64:
			return n
		case int64:
			return float64(n)
		case int:
			return float64(n)
		}
	}
	return 0
}

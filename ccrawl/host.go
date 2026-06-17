package ccrawl

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// HostRecord is the fully enriched profile for one host, combining signals from
// the web-graph rank table, graph topology (in/out-degree), and the CDX URL index.
type HostRecord struct {
	// Identity
	Host             string `json:"host" kit:"id" table:"host"`
	HostRev          string `json:"host_rev,omitempty" table:"-"`
	TLD              string `json:"tld" table:"tld"`
	RegisteredDomain string `json:"registered_domain,omitempty" table:"registered_domain"`

	// Rank signals (from rank table)
	HarmonicPos int64   `json:"harmonic_pos" table:"harmonic_pos"`
	HarmonicVal float64 `json:"harmonic_val" table:"harmonic_val"`
	PageRankPos int64   `json:"pagerank_pos" table:"pagerank_pos"`
	PageRankVal float64 `json:"pagerank_val" table:"pagerank_val"`

	// Graph topology (from edge files)
	InDegree  int64 `json:"in_degree" table:"in_degree"`
	OutDegree int64 `json:"out_degree" table:"out_degree"`

	// CDX statistics (from columnar Parquet index)
	URLCount   int64  `json:"url_count" table:"url_count"`
	Status2xx  int64  `json:"status_2xx" table:"status_2xx"`
	Status3xx  int64  `json:"status_3xx" table:"status_3xx"`
	Status4xx  int64  `json:"status_4xx" table:"status_4xx"`
	Status5xx  int64  `json:"status_5xx" table:"status_5xx"`
	TopMIME    string `json:"top_mime,omitempty" table:"top_mime"`
	Language   string `json:"language,omitempty" table:"language"`
	FirstSeen  string `json:"first_seen,omitempty" table:"first_seen"`
	LastSeen   string `json:"last_seen,omitempty" table:"last_seen"`
	TotalBytes int64  `json:"total_bytes" table:"total_bytes"`
}

// HostFromRank builds a minimal HostRecord from a Rank entry.
func HostFromRank(r Rank) HostRecord {
	return HostRecord{
		Host:        r.Key,
		HostRev:     reverseHost(r.Key),
		TLD:         hostTLD(r.Key),
		HarmonicPos: r.HarmonicPos,
		HarmonicVal: r.HarmonicVal,
		PageRankPos: r.PageRankPos,
		PageRankVal: r.PageRankVal,
	}
}

// HostCDXStats is the CDX aggregation result for one host, produced by a DuckDB
// GROUP BY query over the columnar Parquet index.
type HostCDXStats struct {
	Host             string `json:"url_host_name"`
	RegisteredDomain string `json:"registered_domain"`
	URLCount         int64  `json:"url_count"`
	Status2xx        int64  `json:"status_2xx"`
	Status3xx        int64  `json:"status_3xx"`
	Status4xx        int64  `json:"status_4xx"`
	Status5xx        int64  `json:"status_5xx"`
	TopMIME          string `json:"top_mime"`
	Language         string `json:"language"`
	FirstSeen        string `json:"first_seen"`
	LastSeen         string `json:"last_seen"`
	TotalBytes       int64  `json:"total_bytes"`
}

// HostCDXAggSQL returns the DuckDB SQL to aggregate per-host CDX statistics from
// the columnar Parquet index for a given crawl. If host is non-empty the query
// filters to that host only; otherwise it aggregates all hosts (full table scan).
func HostCDXAggSQL(parquetURLs []string, crawlID, host string) string {
	src := ParquetListLiteral(parquetURLs)
	var hostFilter string
	if host != "" {
		hostFilter = "\n  AND url_host_name = '" + sqlEscape(host) + "'"
	}
	return fmt.Sprintf(`
SELECT
    url_host_name,
    ANY_VALUE(url_host_registered_domain) AS registered_domain,
    COUNT(*) AS url_count,
    SUM(CASE WHEN fetch_status >= 200 AND fetch_status < 300 THEN 1 ELSE 0 END) AS status_2xx,
    SUM(CASE WHEN fetch_status >= 300 AND fetch_status < 400 THEN 1 ELSE 0 END) AS status_3xx,
    SUM(CASE WHEN fetch_status >= 400 AND fetch_status < 500 THEN 1 ELSE 0 END) AS status_4xx,
    SUM(CASE WHEN fetch_status >= 500 AND fetch_status < 600 THEN 1 ELSE 0 END) AS status_5xx,
    MODE(content_mime_detected) AS top_mime,
    MODE(content_languages) AS language,
    MIN(CAST(fetch_time AS VARCHAR)) AS first_seen,
    MAX(CAST(fetch_time AS VARCHAR)) AS last_seen,
    SUM(COALESCE(warc_record_length, 0)) AS total_bytes
FROM read_parquet(%s, hive_partitioning=1)
WHERE crawl = '%s'%s
GROUP BY url_host_name
ORDER BY url_count DESC`, src, sqlEscape(crawlID), hostFilter)
}

// HostCDXSingleSQL returns the DuckDB SQL to aggregate CDX stats for one host.
// This is faster than the full aggregation when only one host is needed.
func HostCDXSingleSQL(parquetURLs []string, crawlID, host string) string {
	return HostCDXAggSQL(parquetURLs, crawlID, host)
}

// HostCDXAgg runs the CDX aggregation for one or all hosts via DuckDB and calls
// fn for each result row. If host is non-empty, only that host is aggregated.
func HostCDXAgg(ctx context.Context, parquetURLs []string, crawlID, host string, fn func(HostCDXStats) error) error {
	sql := HostCDXAggSQL(parquetURLs, crawlID, host)
	return RunDuckDBJSON(ctx, "", sql, func(row map[string]any) error {
		s := HostCDXStats{
			Host:             stringVal(row, "url_host_name"),
			RegisteredDomain: stringVal(row, "registered_domain"),
			URLCount:         int64Val(row, "url_count"),
			Status2xx:        int64Val(row, "status_2xx"),
			Status3xx:        int64Val(row, "status_3xx"),
			Status4xx:        int64Val(row, "status_4xx"),
			Status5xx:        int64Val(row, "status_5xx"),
			TopMIME:          stringVal(row, "top_mime"),
			Language:         stringVal(row, "language"),
			FirstSeen:        stringVal(row, "first_seen"),
			LastSeen:         stringVal(row, "last_seen"),
			TotalBytes:       int64Val(row, "total_bytes"),
		}
		if s.Host == "" {
			return nil
		}
		return fn(s)
	})
}

// HostLookupRank returns the rank-only HostRecord for a host by streaming the
// rank table. This is O(rank_table_size) — for a single-host lookup it is best
// to use HostLookupCDX for CDX stats and join separately.
func HostLookupRank(ctx context.Context, h *HTTPClient, rankURL, host string) (HostRecord, error) {
	r, err := RankLookup(ctx, h, rankURL, host)
	if err != nil {
		return HostRecord{}, err
	}
	return HostFromRank(r), nil
}

// hostTLD returns the top-level domain of a host.
func hostTLD(host string) string {
	parts := strings.Split(host, ".")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

// registeredDomain returns the registered domain (last two labels).
func registeredDomain(host string) string {
	parts := strings.Split(host, ".")
	if len(parts) <= 2 {
		return host
	}
	return strings.Join(parts[len(parts)-2:], ".")
}

// stringVal extracts a string from a DuckDB JSON row.
func stringVal(row map[string]any, key string) string {
	if v, ok := row[key]; ok && v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// int64Val extracts an int64 from a DuckDB JSON row. DuckDB emits int128 (and
// large sums) as JSON strings, so we handle both numeric and string forms.
func int64Val(row map[string]any, key string) int64 {
	if v, ok := row[key]; ok && v != nil {
		switch n := v.(type) {
		case float64:
			return int64(n)
		case int64:
			return n
		case int:
			return int64(n)
		case string:
			// int128 and large aggregates are returned as strings
			parsed, _ := strconv.ParseInt(n, 10, 64)
			return parsed
		}
	}
	return 0
}

package cli

import (
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
)

// errStop is a sentinel returned from callbacks to halt streaming early.
var errStop = errors.New("stop")

func itoa(n int) string { return strconv.Itoa(n) }

// limitFrom returns the global --limit value.
func limitFrom(app *App) int { return app.Limit }

// normalizePath strips a full Common Crawl URL down to its relative path so the
// downloader treats stdin URLs and manifest paths the same way.
func normalizePath(p string) string {
	p = strings.TrimSpace(p)
	for _, prefix := range []string{"https://data.commoncrawl.org/", "http://data.commoncrawl.org/", "s3://commoncrawl/"} {
		if rest, ok := strings.CutPrefix(p, prefix); ok {
			return rest
		}
	}
	return p
}

// filterPaths applies the segment filter, optional sampling, and limit.
func filterPaths(paths []string, segment string, sample float64, limit int) []string {
	var out []string
	for _, p := range paths {
		if segment != "" && !strings.Contains(p, segment) {
			continue
		}
		if sample > 0 && sample < 1 && sampleHash(p) >= sample {
			continue
		}
		out = append(out, p)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

// sampleHash maps a path deterministically into [0,1) so sampling is stable
// across runs (the same path is always kept or always dropped).
func sampleHash(s string) float64 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return float64(h.Sum32()) / float64(1<<32)
}

func itoa64(n int64) string { return strconv.FormatInt(n, 10) }

// str renders a DuckDB JSON value as a string.
func str(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case float64:
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

func toInt64(v any) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case string:
		n, _ := strconv.ParseInt(t, 10, 64)
		return n
	default:
		return 0
	}
}

// mapRow builds an output Row from a DuckDB result map, in the given column order.
func mapRow(row map[string]any, cols ...string) Row {
	vals := make([]string, len(cols))
	for i, c := range cols {
		vals[i] = str(row[c])
	}
	return Row{Cols: cols, Vals: vals, Value: row}
}

// genericRow builds a Row from an arbitrary result map with sorted columns.
func genericRow(row map[string]any) Row {
	cols := make([]string, 0, len(row))
	for k := range row {
		cols = append(cols, k)
	}
	sortStrings(cols)
	return mapRow(row, cols...)
}

func replaceCCIndex(sql, src string) string {
	return strings.ReplaceAll(sql, "ccindex", fmt.Sprintf("read_parquet('%s', hive_partitioning=1)", src))
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

// humanBytes renders a byte count in a compact human form.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}

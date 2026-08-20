package cli

import (
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"

	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// errStop is a sentinel returned from callbacks to halt streaming early.
var errStop = errors.New("stop")

func itoa(n int) string { return strconv.Itoa(n) }

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

// str renders a result value as a string. Numbers arrive as float64 from
// duckdb, which goes through encoding/json, and as int64 from the native
// engine, which reads the Parquet column directly.
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

// toInt64 reads an integer out of a result value. duckdb's JSON gives every
// number as a float64, but the native engine hands back the Parquet column's
// own type, so a warc offset arrives as an int64 and used to fall through to
// zero here. A location with a zero offset and length is a record nobody can
// fetch, and nothing about it looks wrong until the fetch comes back empty.
func toInt64(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int32:
		return int64(t)
	case int:
		return int64(t)
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

// plural renders a count with its noun, so a message reads "1 artifact" and
// "3 artifacts" without the caller doing it at every call site.
func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
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

// robotsLine reports what robots.txt cost the run and what it bought.
//
// The cost is the extra request per host, which on a domain corpus is one
// request for every three pages and is therefore worth seeing rather than
// guessing at. The saved figure is the requests the cache did not have to make,
// which is the whole reason for holding one. Unreachable is kept apart from
// refused because a host that could not be asked and a host that said no are the
// same outcome for the page and two very different things to read in a log.
func robotsLine(stats ccrawl.CrawlStats) string {
	r := stats.Robots
	line := fmt.Sprintf("robots: %s fetched, %d saved by the cache, %d refused, %d unreachable",
		plural(int(r.Fetches), "host"), r.Hits, stats.Disallowed, stats.Unreachable)
	if r.Evictions > 0 {
		line += fmt.Sprintf(", %d evicted from %s held", r.Evictions, humanBytes(r.Bytes))
	}
	return line
}

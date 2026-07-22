package ccrawl

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
)

// URLCrawlStat is one row of the ccrawl-urls stats.csv ledger: the rollup for one
// crawl. It is uploaded to the hub and drives the dataset card.
type URLCrawlStat struct {
	Crawl          string
	Shards         int
	TotalShards    int
	Rows           int64
	ParquetBytes   int64
	Complete       bool
	FirstCommitted string
	LastCommitted  string
}

var urlStatsHeader = []string{
	"crawl", "shards", "total_shards", "rows", "parquet_bytes",
	"complete", "first_committed", "last_committed",
}

// DomainGraphStat is one row of the ccrawl-domains stats.csv ledger: the rollup
// for one web-graph release.
type DomainGraphStat struct {
	Graph        string
	Shards       int
	Domains      int64
	ParquetBytes int64
	SourceBytes  int64
	ShardRows    int
	CommittedAt  string
}

var domainStatsHeader = []string{
	"graph", "shards", "domains", "parquet_bytes", "source_bytes",
	"shard_rows", "committed_at",
}

// ReadURLStats reads the ccrawl-urls stats.csv ledger. A missing file is an empty
// ledger, not an error.
func ReadURLStats(path string) ([]URLCrawlStat, error) {
	recs, err := readCSV(path)
	if err != nil {
		return nil, err
	}
	var rows []URLCrawlStat
	for _, r := range recs {
		if len(r) < len(urlStatsHeader) {
			continue
		}
		rows = append(rows, URLCrawlStat{
			Crawl:          r[0],
			Shards:         atoi(r[1]),
			TotalShards:    atoi(r[2]),
			Rows:           atoi64(r[3]),
			ParquetBytes:   atoi64(r[4]),
			Complete:       r[5] == "true",
			FirstCommitted: r[6],
			LastCommitted:  r[7],
		})
	}
	return rows, nil
}

// WriteURLStats writes the ledger sorted by crawl id descending (newest first),
// atomically via a temp file and rename.
func WriteURLStats(path string, rows []URLCrawlStat) error {
	sort.Slice(rows, func(i, j int) bool { return rows[i].Crawl > rows[j].Crawl })
	recs := [][]string{urlStatsHeader}
	for _, r := range rows {
		recs = append(recs, []string{
			r.Crawl,
			strconv.Itoa(r.Shards),
			strconv.Itoa(r.TotalShards),
			strconv.FormatInt(r.Rows, 10),
			strconv.FormatInt(r.ParquetBytes, 10),
			strconv.FormatBool(r.Complete),
			r.FirstCommitted,
			r.LastCommitted,
		})
	}
	return writeCSV(path, recs)
}

// UpsertURLStat replaces the row for a crawl, or appends it if new.
func UpsertURLStat(rows []URLCrawlStat, s URLCrawlStat) []URLCrawlStat {
	for i := range rows {
		if rows[i].Crawl == s.Crawl {
			rows[i] = s
			return rows
		}
	}
	return append(rows, s)
}

// ReadDomainStats reads the ccrawl-domains stats.csv ledger.
func ReadDomainStats(path string) ([]DomainGraphStat, error) {
	recs, err := readCSV(path)
	if err != nil {
		return nil, err
	}
	var rows []DomainGraphStat
	for _, r := range recs {
		if len(r) < len(domainStatsHeader) {
			continue
		}
		rows = append(rows, DomainGraphStat{
			Graph:        r[0],
			Shards:       atoi(r[1]),
			Domains:      atoi64(r[2]),
			ParquetBytes: atoi64(r[3]),
			SourceBytes:  atoi64(r[4]),
			ShardRows:    atoi(r[5]),
			CommittedAt:  r[6],
		})
	}
	return rows, nil
}

// WriteDomainStats writes the domain ledger sorted by graph id descending.
func WriteDomainStats(path string, rows []DomainGraphStat) error {
	sort.Slice(rows, func(i, j int) bool { return rows[i].Graph > rows[j].Graph })
	recs := [][]string{domainStatsHeader}
	for _, r := range rows {
		recs = append(recs, []string{
			r.Graph,
			strconv.Itoa(r.Shards),
			strconv.FormatInt(r.Domains, 10),
			strconv.FormatInt(r.ParquetBytes, 10),
			strconv.FormatInt(r.SourceBytes, 10),
			strconv.Itoa(r.ShardRows),
			r.CommittedAt,
		})
	}
	return writeCSV(path, recs)
}

// UpsertDomainStat replaces the row for a graph release, or appends it if new.
func UpsertDomainStat(rows []DomainGraphStat, s DomainGraphStat) []DomainGraphStat {
	for i := range rows {
		if rows[i].Graph == s.Graph {
			rows[i] = s
			return rows
		}
	}
	return append(rows, s)
}

// ProgressEntry is the fine local shard-level progress for one in-flight unit.
type ProgressEntry struct {
	Shards int   `json:"shards"`
	Rows   int64 `json:"rows"`
	Bytes  int64 `json:"bytes"`
}

// ReadProgress reads publish-progress.json. A missing file is an empty map.
func ReadProgress(path string) (map[string]ProgressEntry, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return map[string]ProgressEntry{}, nil
	}
	if err != nil {
		return nil, err
	}
	m := map[string]ProgressEntry{}
	if len(data) == 0 {
		return m, nil
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return m, nil
}

// WriteProgress writes publish-progress.json atomically.
func WriteProgress(path string, m map[string]ProgressEntry) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data)
}

func readCSV(path string) ([][]string, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	recs, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(recs) <= 1 {
		return nil, nil
	}
	return recs[1:], nil // drop header
}

func writeCSV(path string, recs [][]string) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	if err := w.WriteAll(recs); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

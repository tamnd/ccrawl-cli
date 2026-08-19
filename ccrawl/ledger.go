package ccrawl

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
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
	// Complete is true only once the release has been streamed to its end. The
	// domain source is a single object with no shard count known ahead of time,
	// so a partial run cannot be told from a finished one by shard presence alone.
	// The resume path trusts this flag, not the mere existence of the last shard.
	Complete bool
	// FirstCommitted is the timestamp of the release's first shard commit, kept so
	// the card can report elapsed publish wall-clock (CommittedAt minus this). It
	// survives resumes: a partial run seeds it once and later runs preserve it.
	FirstCommitted string
}

// complete and first_committed are appended after committed_at so a stats.csv
// written before either column existed still parses: its rows lack the columns
// and read as not complete with an empty first-committed stamp.
var domainStatsHeader = []string{
	"graph", "shards", "domains", "parquet_bytes", "source_bytes",
	"shard_rows", "committed_at", "complete", "first_committed",
}

// NewsMonthStat is one row of the ccrawl-news stats.csv ledger: the rollup for
// one CC-NEWS month.
//
// SourceBytes is the compressed WARC bytes the month's committed files added up
// to. It is worth carrying because it is the cost of the dataset rather than its
// size: CC-NEWS has no index to mirror, so every row here was paid for by
// streaming an archive, and the ratio between the two numbers is the whole
// argument for publishing this at all.
type NewsMonthStat struct {
	Month        string // YYYY-MM
	Files        int    // WARC files indexed and committed
	TotalFiles   int    // WARC files the month publishes
	Rows         int64
	ParquetBytes int64
	SourceBytes  int64
	Rows2xx      int64
	RowsHTML     int64
	Complete     bool

	FirstCommitted string
	LastCommitted  string
}

var newsStatsHeader = []string{
	"month", "files", "total_files", "rows", "parquet_bytes", "source_bytes",
	"rows_2xx", "rows_html", "complete", "first_committed", "last_committed",
}

// ReadNewsStats reads the ccrawl-news stats.csv ledger. A missing file is an
// empty ledger, not an error.
func ReadNewsStats(path string) ([]NewsMonthStat, error) {
	recs, err := readCSV(path)
	if err != nil {
		return nil, err
	}
	return newsStatsFrom(recs), nil
}

// DecodeNewsStats parses a stats.csv that was fetched rather than read off disk.
// It is how a search finds out which months the published index covers without
// downloading a single shard.
func DecodeNewsStats(data []byte) ([]NewsMonthStat, error) {
	recs, err := decodeCSV(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return newsStatsFrom(recs), nil
}

func newsStatsFrom(recs [][]string) []NewsMonthStat {
	var rows []NewsMonthStat
	for _, r := range recs {
		if len(r) < len(newsStatsHeader) {
			continue
		}
		rows = append(rows, NewsMonthStat{
			Month:          r[0],
			Files:          atoi(r[1]),
			TotalFiles:     atoi(r[2]),
			Rows:           atoi64(r[3]),
			ParquetBytes:   atoi64(r[4]),
			SourceBytes:    atoi64(r[5]),
			Rows2xx:        atoi64(r[6]),
			RowsHTML:       atoi64(r[7]),
			Complete:       r[8] == "true",
			FirstCommitted: r[9],
			LastCommitted:  r[10],
		})
	}
	return rows
}

// WriteNewsStats writes the ledger sorted by month descending (newest first).
func WriteNewsStats(path string, rows []NewsMonthStat) error {
	sort.Slice(rows, func(i, j int) bool { return rows[i].Month > rows[j].Month })
	recs := [][]string{newsStatsHeader}
	for _, r := range rows {
		recs = append(recs, []string{
			r.Month,
			strconv.Itoa(r.Files),
			strconv.Itoa(r.TotalFiles),
			strconv.FormatInt(r.Rows, 10),
			strconv.FormatInt(r.ParquetBytes, 10),
			strconv.FormatInt(r.SourceBytes, 10),
			strconv.FormatInt(r.Rows2xx, 10),
			strconv.FormatInt(r.RowsHTML, 10),
			strconv.FormatBool(r.Complete),
			r.FirstCommitted,
			r.LastCommitted,
		})
	}
	return writeCSV(path, recs)
}

// UpsertNewsStat replaces the row for a month, or appends it if new.
func UpsertNewsStat(rows []NewsMonthStat, s NewsMonthStat) []NewsMonthStat {
	for i := range rows {
		if rows[i].Month == s.Month {
			rows[i] = s
			return rows
		}
	}
	return append(rows, s)
}

// NewsLangStat is one row of the ccrawl-news languages.csv ledger: how many
// articles of one detected language a month holds.
//
// It is kept in its own file rather than folded into the month row because it is
// the one breakdown that cannot be recovered from the shards without reading the
// whole dataset back, and because the number of languages in a month is not
// known ahead of time. The counts are accumulated as shards commit, which is
// safe across restarts for the same reason the row counts are: a file that is
// already on the hub is skipped, so nothing is counted twice.
type NewsLangStat struct {
	Month string
	Lang  string
	Rows  int64
}

var newsLangHeader = []string{"month", "language", "rows"}

// ReadNewsLangs reads the ccrawl-news languages.csv ledger.
func ReadNewsLangs(path string) ([]NewsLangStat, error) {
	recs, err := readCSV(path)
	if err != nil {
		return nil, err
	}
	var rows []NewsLangStat
	for _, r := range recs {
		if len(r) < len(newsLangHeader) {
			continue
		}
		rows = append(rows, NewsLangStat{Month: r[0], Lang: r[1], Rows: atoi64(r[2])})
	}
	return rows, nil
}

// WriteNewsLangs writes the language ledger, newest month first and the biggest
// language first within a month.
func WriteNewsLangs(path string, rows []NewsLangStat) error {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Month != rows[j].Month {
			return rows[i].Month > rows[j].Month
		}
		if rows[i].Rows != rows[j].Rows {
			return rows[i].Rows > rows[j].Rows
		}
		return rows[i].Lang < rows[j].Lang
	})
	recs := [][]string{newsLangHeader}
	for _, r := range rows {
		recs = append(recs, []string{r.Month, r.Lang, strconv.FormatInt(r.Rows, 10)})
	}
	return writeCSV(path, recs)
}

// MergeNewsLangs adds a month's per-language counts into the ledger, replacing
// that month's rows and leaving every other month alone.
func MergeNewsLangs(rows []NewsLangStat, month string, counts map[string]int64) []NewsLangStat {
	out := make([]NewsLangStat, 0, len(rows)+len(counts))
	for _, r := range rows {
		if r.Month != month {
			out = append(out, r)
		}
	}
	for lang, n := range counts {
		out = append(out, NewsLangStat{Month: month, Lang: lang, Rows: n})
	}
	return out
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
		// Seven fields is the pre-complete layout; the eighth is optional.
		if len(r) < 7 {
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
			Complete:     len(r) >= 8 && r[7] == "true",
			FirstCommitted: func() string {
				if len(r) >= 9 {
					return r[8]
				}
				return ""
			}(),
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
			strconv.FormatBool(r.Complete),
			r.FirstCommitted,
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
	return decodeCSV(f)
}

// decodeCSV reads a ledger from anywhere, which is how a fetched stats.csv is
// parsed without being written to disk first.
func decodeCSV(src io.Reader) ([][]string, error) {
	r := csv.NewReader(src)
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

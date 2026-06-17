package ccrawl

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// HostDatasetRow is the Parquet schema for one row of the public cc-host-dataset.
// It combines rank signals, graph topology, and CDX statistics into a flat record.
type HostDatasetRow struct {
	Host             string  `parquet:"host"`
	HostRev          string  `parquet:"host_rev,dict"`
	TLD              string  `parquet:"tld,dict"`
	RegisteredDomain string  `parquet:"registered_domain,dict"`
	CrawlID          string  `parquet:"crawl_id,dict"`
	GraphID          string  `parquet:"graph_id,dict"`
	HarmonicPos      int64   `parquet:"harmonic_pos"`
	HarmonicVal      float64 `parquet:"harmonic_val"`
	PageRankPos      int64   `parquet:"pagerank_pos"`
	PageRankVal      float64 `parquet:"pagerank_val"`
	InDegree         int32   `parquet:"in_degree"`
	OutDegree        int32   `parquet:"out_degree"`
	URLCount         int64   `parquet:"url_count"`
	Status2xx        int64   `parquet:"status_2xx"`
	Status3xx        int64   `parquet:"status_3xx"`
	Status4xx        int64   `parquet:"status_4xx"`
	Status5xx        int64   `parquet:"status_5xx"`
	TopMIME          string  `parquet:"top_mime,dict"`
	Language         string  `parquet:"language,dict"`
	FirstSeen        string  `parquet:"first_seen,dict"`
	LastSeen         string  `parquet:"last_seen,dict"`
	TotalBytes       int64   `parquet:"total_bytes"`
}

// DatasetPrefixes is the ordered set of shard prefix keys.
// Letters a-z, "0" for digit-initial hosts, "misc" for everything else.
var DatasetPrefixes = func() []string {
	out := make([]string, 0, 28)
	for c := 'a'; c <= 'z'; c++ {
		out = append(out, string(c))
	}
	out = append(out, "0", "misc")
	return out
}()

// datasetPrefix returns the shard key for a hostname.
func datasetPrefix(host string) string {
	if host == "" {
		return "misc"
	}
	c := strings.ToLower(string([]rune(host)[:1]))
	if c >= "a" && c <= "z" {
		return c
	}
	if c >= "0" && c <= "9" {
		return "0"
	}
	return "misc"
}

// prefixWriters manages a set of gzipped JSONL or TSV writers keyed by prefix.
type prefixWriters struct {
	dir    string
	stem   string
	ext    string
	files  map[string]*os.File
	writers map[string]*gzip.Writer
}

func newPrefixWriters(dir, stem, ext string) *prefixWriters {
	return &prefixWriters{
		dir:    dir,
		stem:   stem,
		ext:    ext,
		files:  make(map[string]*os.File),
		writers: make(map[string]*gzip.Writer),
	}
}

func (pw *prefixWriters) get(prefix string) (*gzip.Writer, error) {
	if w, ok := pw.writers[prefix]; ok {
		return w, nil
	}
	path := fmt.Sprintf("%s/%s-%s%s", pw.dir, pw.stem, prefix, pw.ext)
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	pw.files[prefix] = f
	gz := gzip.NewWriter(f)
	pw.writers[prefix] = gz
	return gz, nil
}

func (pw *prefixWriters) close() {
	for _, gz := range pw.writers {
		_ = gz.Close()
	}
	for _, f := range pw.files {
		_ = f.Close()
	}
}

// HostCDXPrefixSQL returns the CDX aggregation SQL filtered to a single prefix
// letter. On low-RAM machines this is the safe approach: one DuckDB run per
// prefix means the GROUP BY hash table covers only ~1/26 of all hosts (~2-4M
// unique entries, ~600 MB) instead of all 100M+ (>15 GB).
//
// tempDir, if non-empty, configures DuckDB to spill the hash aggregate to disk
// when it exceeds the memory limit (essential on machines with < 4 GB free RAM).
func HostCDXPrefixSQL(parquetURLs []string, crawlID, prefix, tempDir string) string {
	src := ParquetListLiteral(parquetURLs)
	var hostFilter string
	switch {
	case prefix >= "a" && prefix <= "z":
		hostFilter = fmt.Sprintf("AND LOWER(SUBSTR(url_host_name, 1, 1)) = '%s'", sqlEscape(prefix))
	case prefix == "0":
		hostFilter = "AND LOWER(SUBSTR(url_host_name, 1, 1)) BETWEEN '0' AND '9'"
	default:
		hostFilter = "AND LOWER(SUBSTR(url_host_name, 1, 1)) NOT BETWEEN 'a' AND 'z' AND LOWER(SUBSTR(url_host_name, 1, 1)) NOT BETWEEN '0' AND '9'"
	}
	preamble := "SET threads=2;"
	if tempDir != "" {
		preamble += fmt.Sprintf(
			" SET memory_limit='2GB'; SET temp_directory='%s'; SET max_temp_directory_size='40GB';",
			sqlEscape(tempDir),
		)
	}
	return preamble + fmt.Sprintf(`
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
WHERE crawl = '%s'
  %s
GROUP BY url_host_name`, src, sqlEscape(crawlID), hostFilter)
}

// SaveCDXSplitByPrefix runs one DuckDB aggregation per prefix letter and writes
// cdx-{prefix}.jsonl.gz files into workDir. Running one prefix at a time keeps the
// DuckDB GROUP BY hash table to ~600 MB (2-4M unique hosts) instead of >15 GB for
// all 100M+ hosts at once — this makes it safe on machines with 5.8 GB RAM.
// prefixes is the list of prefix keys to process; pass DatasetPrefixes for all.
// progress is called after each prefix completes with the running total and current prefix.
func SaveCDXSplitByPrefix(ctx context.Context, parquetURLs []string, crawlID, workDir string, prefixes []string, progress func(prefix string, n int64)) (map[string]int64, error) {
	counts := make(map[string]int64)
	var total int64

	for _, prefix := range prefixes {
		outPath := fmt.Sprintf("%s/cdx-%s.jsonl.gz", workDir, prefix)
		// Skip if already written (resumable)
		if _, err := os.Stat(outPath); err == nil {
			// Count approximate rows by re-reading? For now just skip.
			continue
		}

		f, err := os.Create(outPath)
		if err != nil {
			return nil, fmt.Errorf("create cdx file for prefix %q: %w", prefix, err)
		}
		gz := gzip.NewWriter(f)
		enc := json.NewEncoder(gz)
		var n int64

		tempDir := workDir + "/duck-tmp"
		if err := os.MkdirAll(tempDir, 0o755); err != nil {
			_ = os.Remove(outPath)
			return nil, fmt.Errorf("create DuckDB temp dir: %w", err)
		}
		sql := HostCDXPrefixSQL(parquetURLs, crawlID, prefix, tempDir)
		runErr := RunDuckDBJSON(ctx, "", sql, func(row map[string]any) error {
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
			if err := enc.Encode(s); err != nil {
				return err
			}
			n++
			return nil
		})
		_ = gz.Close()
		_ = f.Close()

		if runErr != nil {
			_ = os.Remove(outPath) // don't leave a partial file
			return counts, fmt.Errorf("CDX query for prefix %q: %w", prefix, runErr)
		}
		counts[prefix] = n
		total += n
		if progress != nil {
			progress(prefix, total)
		}
	}
	return counts, nil
}

// DownloadRankTable downloads the full rank table (gzipped TSV, 3-8 GB) to
// rankCachePath in workDir using curl with HTTP range resume (--continue-at -).
// If the file is already complete (i.e. curl exits 0 without needing to
// download anything), this is a no-op beyond the curl HEAD check.
// Uses curl so that interrupted downloads can be resumed without re-reading
// from the beginning — essential for multi-GB files over unstable connections.
func DownloadRankTable(ctx context.Context, rankURL, localPath string) error {
	args := []string{
		"-L",           // follow redirects
		"-C", "-",      // resume from byte offset already downloaded
		"-o", localPath,
		"--retry", "5",
		"--retry-delay", "30",
		rankURL,
	}
	cmd := exec.CommandContext(ctx, "curl", args...)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run()
}

// SplitRankFromFile reads the rank table from a local gzipped TSV file and
// writes one per-prefix gzipped TSV into workDir. Suitable for use after
// DownloadRankTable completes. Call signature identical to SplitRankByURL.
func SplitRankFromFile(ctx context.Context, localPath, workDir string, progress func(total int64)) (map[string]int64, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("open rank table: %w", err)
	}
	defer func() { _ = f.Close() }()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("gunzip rank table: %w", err)
	}
	defer func() { _ = gz.Close() }()

	return splitRankStream(ctx, gz, workDir, progress)
}

// SplitRankByPrefix downloads the rank table once via HTTP and writes one
// gzipped TSV file per prefix into workDir (rank-a.tsv.gz ... rank-misc.tsv.gz).
// Each line: harmonic_pos\tharmonic_val\tpagerank_pos\tpagerank_val\thost
// For large rank tables that may fail mid-stream, prefer DownloadRankTable +
// SplitRankFromFile which supports HTTP range resume via curl.
func SplitRankByPrefix(ctx context.Context, h *HTTPClient, rankURL, workDir string, progress func(total int64)) (map[string]int64, error) {
	resp, err := h.GetDownload(ctx, rankURL)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("rank table HTTP %d (%s)", resp.StatusCode, rankURL)
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return nil, err
	}
	defer func() { _ = gz.Close() }()

	return splitRankStream(ctx, gz, workDir, progress)
}

// splitRankStream is the shared rank-split logic: reads gzipped TSV lines from
// r and fans each row out to the appropriate per-prefix gzipped TSV in workDir.
func splitRankStream(ctx context.Context, r io.Reader, workDir string, progress func(total int64)) (map[string]int64, error) {
	pw := newPrefixWriters(workDir, "rank", ".tsv.gz")
	counts := make(map[string]int64)
	var total int64

	err := streamRankTSV(ctx, r, func(rank Rank) error {
		prefix := datasetPrefix(rank.Key)
		w, err := pw.get(prefix)
		if err != nil {
			return err
		}
		line := fmt.Sprintf("%d\t%g\t%d\t%g\t%s\n", rank.HarmonicPos, rank.HarmonicVal, rank.PageRankPos, rank.PageRankVal, rank.Key)
		if _, err := io.WriteString(w, line); err != nil {
			return err
		}
		counts[prefix]++
		total++
		if progress != nil && total%1_000_000 == 0 {
			progress(total)
		}
		return nil
	})
	pw.close()
	if err != nil {
		return nil, err
	}
	return counts, nil
}

// streamRankTSV parses the CC harmonic-rank gzipped TSV from r and calls fn
// for each rank entry. The TSV format is documented in parseRank.
func streamRankTSV(ctx context.Context, r io.Reader, fn func(Rank) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	for sc.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := sc.Text()
		if rankComment(line) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) <= hostRevField {
			continue
		}
		if err := fn(parseRank(fields)); err != nil {
			return err
		}
	}
	return sc.Err()
}

// LoadCDXPrefix reads the CDX prefix file for the given prefix from workDir
// and calls fn for each HostCDXStats row.
func LoadCDXPrefix(workDir, prefix string, fn func(HostCDXStats) error) error {
	path := fmt.Sprintf("%s/cdx-%s.jsonl.gz", workDir, prefix)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // no CDX data for this prefix
		}
		return err
	}
	defer func() { _ = f.Close() }()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer func() { _ = gz.Close() }()

	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 1<<20), 4<<20)
	for sc.Scan() {
		var s HostCDXStats
		if err := json.Unmarshal(sc.Bytes(), &s); err != nil {
			continue
		}
		if err := fn(s); err != nil {
			return err
		}
	}
	return sc.Err()
}

// BuildDatasetShard reads the rank prefix file and CDX prefix file from workDir,
// joins them, and writes a zstd-compressed Parquet file to outPath.
// Returns the number of rows written.
func BuildDatasetShard(ctx context.Context, prefix, workDir, crawlID, graphID, outPath string, progress func(n int64)) (int64, error) {
	// Load CDX map for this prefix into RAM
	cdxMap := make(map[string]HostCDXStats)
	if err := LoadCDXPrefix(workDir, prefix, func(s HostCDXStats) error {
		cdxMap[s.Host] = s
		return nil
	}); err != nil {
		return 0, fmt.Errorf("load CDX cache for prefix %q: %w", prefix, err)
	}

	// Open Parquet writer
	out, err := NewParquetWriter[HostDatasetRow](outPath)
	if err != nil {
		return 0, err
	}

	// Stream rank prefix file
	rankPath := fmt.Sprintf("%s/rank-%s.tsv.gz", workDir, prefix)
	rf, err := os.Open(rankPath)
	if err != nil {
		_ = out.Close()
		return 0, fmt.Errorf("open rank file: %w", err)
	}
	defer func() { _ = rf.Close() }()
	gz, err := gzip.NewReader(rf)
	if err != nil {
		_ = out.Close()
		return 0, err
	}
	defer func() { _ = gz.Close() }()

	var n int64
	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 1<<20), 4<<20)
	for sc.Scan() {
		if err := ctx.Err(); err != nil {
			_ = out.Close()
			return n, err
		}
		fields := strings.SplitN(sc.Text(), "\t", 5)
		if len(fields) < 5 {
			continue
		}
		hp, _ := strconv.ParseInt(fields[0], 10, 64)
		hv, _ := strconv.ParseFloat(fields[1], 64)
		pp, _ := strconv.ParseInt(fields[2], 10, 64)
		pv, _ := strconv.ParseFloat(fields[3], 64)
		host := fields[4]

		row := HostDatasetRow{
			Host:             host,
			HostRev:          reverseHost(host),
			TLD:              hostTLD(host),
			RegisteredDomain: registeredDomain(host),
			CrawlID:          crawlID,
			GraphID:          graphID,
			HarmonicPos:      hp,
			HarmonicVal:      hv,
			PageRankPos:      pp,
			PageRankVal:      pv,
		}
		if s, ok := cdxMap[host]; ok {
			if s.RegisteredDomain != "" {
				row.RegisteredDomain = s.RegisteredDomain
			}
			row.URLCount = s.URLCount
			row.Status2xx = s.Status2xx
			row.Status3xx = s.Status3xx
			row.Status4xx = s.Status4xx
			row.Status5xx = s.Status5xx
			row.TopMIME = s.TopMIME
			row.Language = s.Language
			row.FirstSeen = s.FirstSeen
			row.LastSeen = s.LastSeen
			row.TotalBytes = s.TotalBytes
		}

		if err := out.Write(row); err != nil {
			_ = out.Close()
			return n, err
		}
		n++
		if progress != nil && n%500_000 == 0 {
			progress(n)
		}
	}
	if err := sc.Err(); err != nil {
		_ = out.Close()
		return n, err
	}
	return n, out.Close()
}

// DatasetWorkFiles returns the paths of all intermediate files for a prefix.
func DatasetWorkFiles(workDir, prefix string) (cdxFile, rankFile string) {
	cdxFile = fmt.Sprintf("%s/cdx-%s.jsonl.gz", workDir, prefix)
	rankFile = fmt.Sprintf("%s/rank-%s.tsv.gz", workDir, prefix)
	return
}

// DatasetPrefixDone reports whether a prefix has been split (both cdx and rank
// files present for workDir).
func DatasetPrefixReady(workDir, prefix string) bool {
	cdx, rank := DatasetWorkFiles(workDir, prefix)
	_, e1 := os.Stat(cdx)
	_, e2 := os.Stat(rank)
	return e1 == nil && e2 == nil
}

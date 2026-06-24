package ccrawl

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/tamnd/ami/config"
	"github.com/tamnd/ami/fetch"
	amirun "github.com/tamnd/ami/run"
)

// RefetchRow is the parquet schema for a re-fetched page. It combines CC
// provenance (crawl_id, crawl_date, warc_record_id) with live fetch metadata
// (status, TTFB, timing, headers) and the converted markdown body.
type RefetchRow struct {
	DocID          string `parquet:"doc_id"`
	URL            string `parquet:"url"`
	FinalURL       string `parquet:"final_url"`
	Host           string `parquet:"host"`
	IPAddress      string `parquet:"ip_address"`
	CrawlID        string `parquet:"crawl_id"`
	CrawlDate      string `parquet:"crawl_date"`
	WARCRecordID   string `parquet:"warc_record_id"`
	FetchedAt      int64  `parquet:"fetched_at"`
	Status         int32  `parquet:"status"`
	ContentType    string `parquet:"content_type"`
	FetchDurMS     int64  `parquet:"fetch_duration_ms"`
	TTFBMS         int64  `parquet:"ttfb_ms"`
	ETag           string `parquet:"etag"`
	LastModified   string `parquet:"last_modified"`
	RespHeaders    string `parquet:"resp_headers"`
	BodyLength     int64  `parquet:"body_length"`
	Digest         string `parquet:"digest"`
	HTMLLength     int64  `parquet:"html_length"`
	MarkdownLength int64  `parquet:"markdown_length"`
	Markdown       string `parquet:"markdown"`
	Error          string `parquet:"error"`
}

// RefetchStats summarises one shard's refetch run with per-phase breakdown.
type RefetchStats struct {
	ShardIdx int

	// Phase 1: WARC URL extraction.
	URLsFound  int64
	DurExtract time.Duration
	WARCBytes  int64

	// Phase 2: ami refetch.
	Fetched    int64
	Failed     int64
	Redirected int64
	FetchBytes int64
	DurFetch   time.Duration

	// Failure breakdown, so a low-yield shard can be diagnosed without a second
	// run. These count Result.Err by class and sum to Failed.
	ErrDNS     int64 // name does not resolve (no such host / NXDOMAIN)
	ErrTimeout int64 // deadline exceeded before a response
	ErrRefused int64 // connection refused / reset by peer
	ErrSkip    int64 // engine skipped the URL (dead-domain breaker, non-HTTP)
	ErrOther   int64 // TLS, protocol, and everything else

	// Phase 3: HTML to Markdown convert.
	Rows       int64
	HTMLBytes  int64
	MDBytes    int64
	DurConvert time.Duration

	// Phase 4: parquet write.
	ParquetBytes int64
	DurExport    time.Duration

	MemRSSBytes int64
}

// WARCURLRecord is one URL extracted from a WARC shard during phase 1.
type WARCURLRecord struct {
	URL      string
	Date     string // YYYY-MM-DD
	RecordID string
}

// RefetchPackConfig controls one shard refetch run.
type RefetchPackConfig struct {
	CrawlID    string
	ShardIdx   int
	WARCPath   string
	OutPath    string
	FetchCfg   config.Config
	ConvertSem chan struct{}
	Progress   func(RefetchStats)

	// CacheDir, when set, is where the downloaded WARC is cached so a re-run of
	// the same shard skips the multi-second download. The download streams to a
	// .part file beside the final name and is renamed into place only once it is
	// complete, so an interrupted run never leaves a truncated file that a later
	// run would mistake for a good cache. Empty disables caching.
	CacheDir string
}

// PackRefetchShard runs the four-phase refetch pipeline for one WARC shard:
//  1. Extract URLs from WARC headers (stream the WARC, keep only response+HTML records)
//  2. Re-fetch all URLs live with ami's FetchBatch
//  3. Convert HTML bodies to Markdown
//  4. Write a parquet file at cfg.OutPath
func PackRefetchShard(ctx context.Context, h *HTTPClient, cfg RefetchPackConfig) (RefetchStats, error) {
	stats := RefetchStats{ShardIdx: cfg.ShardIdx}

	// Phase 1: open the WARC (cache hit or live download) and extract URLs from
	// record headers.
	t0 := time.Now()
	src, cached, nbytes, err := openWARCShard(ctx, h, cfg)
	if err != nil {
		return stats, err
	}
	defer func() { _ = src.Close() }()
	if nbytes > 0 {
		stats.WARCBytes = nbytes
	}

	urlRecords, err := extractWARCURLs(ctx, src)
	if err != nil {
		// On a failed extract the streamed-to-cache .part (if any) is incomplete;
		// drop it so the next run re-downloads cleanly rather than reusing a
		// truncated file.
		src.Discard()
		return stats, fmt.Errorf("shard %d: extract urls: %w", cfg.ShardIdx, err)
	}
	// Promote the freshly downloaded .part into the cache now that the whole
	// stream read cleanly. A cache hit has nothing to commit.
	if !cached {
		if cerr := src.Commit(); cerr != nil {
			fmt.Fprintf(os.Stderr, "refetch: shard %d: cache write failed: %v\n", cfg.ShardIdx, cerr)
		}
	}
	stats.URLsFound = int64(len(urlRecords))
	stats.DurExtract = time.Since(t0)

	// Build the URL lookup for CC provenance (WARCURLRecord by URL).
	provenance := make(map[string]WARCURLRecord, len(urlRecords))
	for _, r := range urlRecords {
		provenance[r.URL] = r
	}
	urls := make([]string, len(urlRecords))
	for i, r := range urlRecords {
		urls[i] = r.URL
	}
	// CC WARC shards arrive in SURT order, so a shard is heavily host-clustered:
	// hundreds of consecutive URLs share one host. Fed to the fetch engine in
	// that order, the worker pool stalls on the per-host connection cap while a
	// handful of hosts monopolize the in-flight slots, and effective concurrency
	// collapses far below the worker count. ami's own engine has a reorder buffer
	// for exactly this, but the FetchBatch entry point does not apply it, so we
	// spread the URLs across hosts here before handing them over. This keeps a
	// wide host set in flight regardless of the input ordering.
	urls = spreadByHost(urls)

	// Phase 2: ami refetch.
	t1 := time.Now()
	fetchCfg := cfg.FetchCfg
	if fetchCfg.Workers <= 0 {
		fetchCfg.Workers = 400
	}

	results := make(chan fetch.Result, fetchCfg.Workers)

	// fetchEndTime is written by the goroutine when FetchBatch returns. Safe to
	// read after rows is drained (which implies all workers exited, which implies
	// FetchBatch returned and the goroutine completed its write).
	var fetchEndTime time.Time
	go func() {
		_ = amirun.FetchBatch(ctx, fetchCfg, urls, results)
		fetchEndTime = time.Now()
		close(results)
	}()

	// Phase 3+4: convert and write parquet concurrently with phase 2.
	if err := os.MkdirAll(filepath.Dir(cfg.OutPath), 0o755); err != nil {
		return stats, err
	}
	pw, err := newRefetchParquetWriter(cfg.OutPath)
	if err != nil {
		return stats, err
	}

	convertWorkers := runtime.NumCPU()
	rows := make(chan RefetchRow, convertWorkers*4)

	var wg sync.WaitGroup
	wg.Add(convertWorkers)
	t2 := time.Now()
	for range convertWorkers {
		go func() {
			defer wg.Done()
			for res := range results {
				prov := provenance[res.URL]
				row := RefetchRow{
					DocID:        MarkdownDocID(res.URL),
					URL:          res.URL,
					FinalURL:     res.FinalURL,
					Host:         urlHostname(res.URL),
					IPAddress:    res.IP,
					CrawlID:      cfg.CrawlID,
					CrawlDate:    prov.Date,
					WARCRecordID: prov.RecordID,
					FetchedAt:    res.FetchedAt.UnixMilli(),
					Status:       int32(res.Status),
					ETag:         res.ETag,
					LastModified: res.LastModified,
					FetchDurMS:   res.FetchDuration.Milliseconds(),
					TTFBMS:       res.TTFB.Milliseconds(),
					BodyLength:   int64(len(res.Body)),
					Digest:       res.Digest,
				}
				if res.Header != nil {
					row.ContentType = res.Header.Get("Content-Type")
					row.RespHeaders = buildRespHeaders(res.Status, res.Header)
				}
				if res.Err != nil {
					row.Error = res.Err.Error()
					rows <- row
					continue
				}
				if isHTMLMIME(row.ContentType) && len(res.Body) > 0 {
					md := convertGated(cfg.ConvertSem, res.Body, res.URL)
					row.HTMLLength = int64(len(res.Body))
					row.MarkdownLength = int64(len(md))
					row.Markdown = md
				}
				rows <- row
			}
		}()
	}
	go func() {
		wg.Wait()
		close(rows)
	}()

	for row := range rows {
		if row.Error != "" {
			stats.Failed++
			classifyFetchErr(row.Error, &stats)
		} else {
			stats.Fetched++
			stats.FetchBytes += row.BodyLength
			stats.HTMLBytes += row.HTMLLength
			stats.MDBytes += row.MarkdownLength
			if row.FinalURL != "" && row.FinalURL != row.URL {
				stats.Redirected++
			}
			if row.Markdown != "" {
				stats.Rows++
			}
		}
		if werr := pw.Write(row); werr != nil {
			go func() {
				for range rows {
				}
			}()
			_ = pw.Close()
			return stats, fmt.Errorf("shard %d: write parquet: %w", cfg.ShardIdx, werr)
		}
		if cfg.Progress != nil {
			cfg.Progress(stats)
		}
	}

	// By here, rows is drained → workers exited → results was closed by the
	// FetchBatch goroutine → fetchEndTime has been written.
	stats.DurFetch = fetchEndTime.Sub(t1)
	stats.DurConvert = time.Since(t2)

	tExport := time.Now()
	if err := pw.Close(); err != nil {
		return stats, fmt.Errorf("shard %d: finalize parquet: %w", cfg.ShardIdx, err)
	}
	stats.DurExport = time.Since(tExport)

	if fi, serr := os.Stat(cfg.OutPath); serr == nil {
		stats.ParquetBytes = fi.Size()
	}
	stats.MemRSSBytes = currentRSSBytes()
	return stats, nil
}

// spreadByHost reorders a host-clustered URL list so consecutive entries land
// on different hosts. It groups the input by host (preserving each host's
// internal order so a server still sees its pages in a sensible sequence), then
// emits one URL per host round-robin until every group is drained. The result
// holds exactly the same URLs as the input, only interleaved, so the fetch
// engine keeps as many distinct hosts in flight as it has workers instead of
// piling a single host's pages onto one capped connection pool.
func spreadByHost(urls []string) []string {
	if len(urls) < 2 {
		return urls
	}
	groups := make(map[string][]string)
	order := make([]string, 0, len(urls)) // first-seen host order, for determinism
	for _, u := range urls {
		host := urlHostname(u)
		if _, ok := groups[host]; !ok {
			order = append(order, host)
		}
		groups[host] = append(groups[host], u)
	}
	if len(order) == 1 {
		return urls // single host: nothing to spread
	}
	out := make([]string, 0, len(urls))
	for len(out) < len(urls) {
		for _, host := range order {
			g := groups[host]
			if len(g) == 0 {
				continue
			}
			out = append(out, g[0])
			groups[host] = g[1:]
		}
	}
	return out
}

// classifyFetchErr buckets a fetch error string into the RefetchStats failure
// counters. It reads the rendered error text rather than the typed error
// because the row carries only the string; the substrings below are stable
// across the Go net stack and the ami engine's sentinels.
func classifyFetchErr(errText string, stats *RefetchStats) {
	e := strings.ToLower(errText)
	switch {
	case strings.Contains(e, "no such host"),
		strings.Contains(e, "server misbehaving"),
		strings.Contains(e, "name resolution"):
		stats.ErrDNS++
	case strings.Contains(e, "timeout"),
		strings.Contains(e, "deadline exceeded"),
		strings.Contains(e, "timed out"):
		stats.ErrTimeout++
	case strings.Contains(e, "connection refused"),
		strings.Contains(e, "connection reset"),
		strings.Contains(e, "no route to host"),
		strings.Contains(e, "network is unreachable"):
		stats.ErrRefused++
	case strings.Contains(e, "skip"), strings.Contains(e, "congested"):
		stats.ErrSkip++
	default:
		stats.ErrOther++
	}
}

// extractWARCURLs iterates a WARC gzip stream and collects all HTTP 200 HTML
// response records. Only record headers are kept in memory; the full bodies are
// discarded to keep peak memory proportional to the URL count, not the WARC size.
func extractWARCURLs(ctx context.Context, r io.Reader) ([]WARCURLRecord, error) {
	var out []WARCURLRecord
	err := IterateWARC(r, func(rec WARCRecord) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if rec.Header.Type != "response" || rec.Header.HTTPStatus != 200 {
			return nil
		}
		if !isHTMLMIME(rec.Header.HTTPMIME) {
			return nil
		}
		if rec.Header.TargetURI == "" {
			return nil
		}
		out = append(out, WARCURLRecord{
			URL:      rec.Header.TargetURI,
			Date:     rec.Header.Date.Format("2006-01-02"),
			RecordID: rec.Header.RecordID,
		})
		return nil
	})
	return out, err
}

// buildRespHeaders reconstructs the HTTP response head as a single string.
func buildRespHeaders(status int, h http.Header) string {
	var b strings.Builder
	fmt.Fprintf(&b, "HTTP/1.1 %d %s\r\n", status, http.StatusText(status))
	for k, vv := range h {
		for _, v := range vv {
			fmt.Fprintf(&b, "%s: %s\r\n", k, v)
		}
	}
	b.WriteString("\r\n")
	return b.String()
}

// newRefetchParquetWriter builds a zstd-compressed parquet writer for RefetchRow.
func newRefetchParquetWriter(path string) (*ParquetWriter[RefetchRow], error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	codec := &zstd.Codec{Level: zstd.SpeedBetterCompression, Concurrency: 4}
	w := parquet.NewGenericWriter[RefetchRow](f, parquet.Compression(codec))
	return &ParquetWriter[RefetchRow]{f: f, w: w}, nil
}

// HFRefetchPath returns the HF repo path for one refetch parquet shard.
func HFRefetchPath(crawlID string, shardIdx int) string {
	return fmt.Sprintf("data/crawl=%s/%06d.parquet", crawlID, shardIdx)
}

// currentRSSBytes reads the current process RSS from /proc/self/status.
// Returns 0 on any error or non-Linux platform.
func currentRSSBytes() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, err := strconv.ParseInt(fields[1], 10, 64)
				if err == nil {
					return kb * 1024
				}
			}
		}
	}
	return 0
}

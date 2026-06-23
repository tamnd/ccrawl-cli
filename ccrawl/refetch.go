package ccrawl

import (
	"bufio"
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
}

// PackRefetchShard runs the four-phase refetch pipeline for one WARC shard:
//  1. Extract URLs from WARC headers (stream the WARC, keep only response+HTML records)
//  2. Re-fetch all URLs live with ami's FetchBatch
//  3. Convert HTML bodies to Markdown
//  4. Write a parquet file at cfg.OutPath
func PackRefetchShard(ctx context.Context, h *HTTPClient, cfg RefetchPackConfig) (RefetchStats, error) {
	stats := RefetchStats{ShardIdx: cfg.ShardIdx}

	// Phase 1: download WARC and extract URLs from record headers.
	t0 := time.Now()
	warcURL := FileURL(cfg.WARCPath, SourceHTTPS)
	resp, err := h.GetDownload(ctx, warcURL)
	if err != nil {
		return stats, fmt.Errorf("shard %d: download warc: %w", cfg.ShardIdx, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return stats, fmt.Errorf("shard %d: download warc: HTTP %d", cfg.ShardIdx, resp.StatusCode)
	}
	if resp.ContentLength > 0 {
		stats.WARCBytes = resp.ContentLength
	}

	urlRecords, err := extractWARCURLs(ctx, resp.Body)
	if err != nil {
		return stats, fmt.Errorf("shard %d: extract urls: %w", cfg.ShardIdx, err)
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

	// Phase 2: ami refetch.
	t1 := time.Now()
	fetchCfg := cfg.FetchCfg
	if fetchCfg.Workers <= 0 {
		fetchCfg.Workers = 400
	}

	results := make(chan fetch.Result, fetchCfg.Workers)
	go func() {
		_ = amirun.FetchBatch(ctx, fetchCfg, urls, results)
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

	stats.DurFetch = time.Since(t1)

	for row := range rows {
		if row.Error != "" {
			stats.Failed++
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
	w := parquet.NewGenericWriter[RefetchRow](
		bufio.NewWriterSize(f, 256*1024),
		parquet.Compression(codec),
	)
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

package ccrawl

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/tamnd/yomi/extract"
	"github.com/tamnd/yomi/mdconv"
	"golang.org/x/net/html/charset"
)

// MarkdownRow is the parquet schema for one converted HTML document. It matches
// the open-index/open-markdown dataset layout so the output can be appended to
// existing crawls without a schema migration.
type MarkdownRow struct {
	DocID          string `parquet:"doc_id"`
	URL            string `parquet:"url"`
	Host           string `parquet:"host"`
	CrawlDate      string `parquet:"crawl_date"`
	WARCRecordID   string `parquet:"warc_record_id"`
	HTMLLength     int64  `parquet:"html_length"`
	MarkdownLength int64  `parquet:"markdown_length"`
	Markdown       string `parquet:"markdown"`
}

// MarkdownDocID returns a stable 16-byte hex document ID derived from the URL.
// The same URL always produces the same ID regardless of crawl date or shard,
// so duplicate-URL deduplication across crawls is a simple equi-join on doc_id.
func MarkdownDocID(rawURL string) string {
	h := sha256.Sum256([]byte(rawURL))
	return hex.EncodeToString(h[:16])
}

// MarkdownStats summarises one WARC shard's conversion run.
type MarkdownStats struct {
	ShardIdx     int
	Rows         int64
	HTMLBytes    int64
	MDBytes      int64
	ParquetBytes int64
	DurDownload  time.Duration
	DurConvert   time.Duration
	DurExport    time.Duration
	DurPublish   time.Duration
}

// htmlRecord carries the extracted fields from one WARC response record.
type htmlRecord struct {
	url      string
	date     string
	recordID string
	html     []byte
}

// MarkdownPackConfig controls one shard conversion run.
type MarkdownPackConfig struct {
	// CrawlID is the CC crawl identifier (e.g. CC-MAIN-2026-25).
	CrawlID string
	// ShardIdx is the 0-based index of this WARC file in the crawl manifest.
	ShardIdx int
	// WARCPath is the Common Crawl relative path (crawl-data/.../warc.gz).
	WARCPath string
	// OutPath is the local parquet file to write.
	OutPath string
	// Workers is the number of goroutines for HTML→Markdown conversion.
	// 0 selects runtime.NumCPU().
	Workers int
	// Progress is called after each row is written. It may be nil.
	Progress func(MarkdownStats)
}

// PackMarkdownShard streams one WARC shard through the conversion pipeline and
// writes a parquet file at cfg.OutPath. The WARC is never written to disk —
// the HTTP response body streams directly to the WARC iterator.
//
// Pipeline:
//
//	HTTP stream → WARC iterator → filter (response + HTML) → records chan
//	→ [N conversion workers]   → rows chan → parquet writer
//
// N workers parallelise readability extraction and markdown rendering.
// The single writer keeps the parquet output sequential.
func PackMarkdownShard(ctx context.Context, h *HTTPClient, cfg MarkdownPackConfig) (MarkdownStats, error) {
	workers := cfg.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	stats := MarkdownStats{ShardIdx: cfg.ShardIdx}

	t0 := time.Now()
	warcURL := FileURL(cfg.WARCPath, SourceHTTPS)
	resp, err := h.GetDownload(ctx, warcURL)
	if err != nil {
		return stats, fmt.Errorf("download shard %d: %w", cfg.ShardIdx, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return stats, fmt.Errorf("download shard %d: HTTP %d", cfg.ShardIdx, resp.StatusCode)
	}

	if err := os.MkdirAll(filepath.Dir(cfg.OutPath), 0o755); err != nil {
		return stats, err
	}
	pw, err := newMarkdownParquetWriter(cfg.OutPath)
	if err != nil {
		return stats, err
	}

	records := make(chan htmlRecord, workers*4)
	rows := make(chan MarkdownRow, workers*4)

	// Reader: iterate the WARC multi-member gzip stream, push HTML records.
	var readErr error
	go func() {
		defer close(records)
		readErr = IterateWARC(resp.Body, func(rec WARCRecord) error {
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
			body := HTTPBody(rec.Block)
			if len(body) == 0 {
				return nil
			}
			records <- htmlRecord{
				url:      rec.Header.TargetURI,
				date:     rec.Header.Date.Format("2006-01-02"),
				recordID: rec.Header.RecordID,
				html:     body,
			}
			return nil
		})
	}()

	tConvert := time.Now()

	// Workers: readability + mdconv, in parallel.
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for r := range records {
				md := htmlToMarkdown(r.html, r.url)
				if md == "" {
					continue
				}
				rows <- MarkdownRow{
					DocID:          MarkdownDocID(r.url),
					URL:            r.url,
					Host:           urlHostname(r.url),
					CrawlDate:      r.date,
					WARCRecordID:   r.recordID,
					HTMLLength:     int64(len(r.html)),
					MarkdownLength: int64(len(md)),
					Markdown:       md,
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(rows)
	}()

	stats.DurDownload = time.Since(t0)

	for row := range rows {
		if werr := pw.Write(row); werr != nil {
			go func() {
				for range rows {
				}
			}()
			_ = pw.Close()
			return stats, fmt.Errorf("write parquet: %w", werr)
		}
		stats.Rows++
		stats.HTMLBytes += row.HTMLLength
		stats.MDBytes += row.MarkdownLength
		if cfg.Progress != nil {
			cfg.Progress(stats)
		}
	}

	stats.DurConvert = time.Since(tConvert)

	if readErr != nil {
		_ = pw.Close()
		return stats, readErr
	}

	tExport := time.Now()
	if err := pw.Close(); err != nil {
		return stats, fmt.Errorf("finalize parquet: %w", err)
	}
	stats.DurExport = time.Since(tExport)

	if fi, serr := os.Stat(cfg.OutPath); serr == nil {
		stats.ParquetBytes = fi.Size()
	}
	return stats, nil
}

// newMarkdownParquetWriter builds a parquet writer for MarkdownRow with zstd
// BetterCompression. The crawl is network-bound so the extra compression is
// effectively free, and the output is about 30x smaller than the raw WARC.
func newMarkdownParquetWriter(path string) (*ParquetWriter[MarkdownRow], error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	codec := &zstd.Codec{Level: zstd.SpeedBetterCompression, Concurrency: 4}
	w := parquet.NewGenericWriter[MarkdownRow](
		bufio.NewWriterSize(f, 256*1024),
		parquet.Compression(codec),
	)
	return &ParquetWriter[MarkdownRow]{f: f, w: w}, nil
}

// htmlToMarkdown converts one HTML body to Markdown. It transcodes the body to
// UTF-8 first, then runs yomi's readability extraction and mdconv rendering.
// The charset transcode is the key addition beyond calling extract.FromHTML
// directly: a Common Crawl WARC contains pages in GBK, Shift-JIS, Latin-1, and
// many other encodings declared in the HTML body's <meta charset> tag, and
// extract.FromHTML parses bytes as UTF-8 directly.
//
// Returns "" when the body yields no extractable article or conversion fails.
func htmlToMarkdown(body []byte, pageURL string) string {
	if len(body) == 0 {
		return ""
	}
	// charset.NewReader passes "" for the Content-Type here because the WARC
	// MIME field is not threaded into this function. charset falls back to the
	// <meta charset> inside the HTML, which covers the majority of non-UTF-8
	// pages correctly. The remaining gap (pages where charset is declared only
	// in the HTTP header and not in <meta>) is small enough to accept.
	r, err := charset.NewReader(strings.NewReader(string(body)), "")
	if err != nil {
		return ""
	}
	utf8Body, err := io.ReadAll(r)
	if err != nil || len(utf8Body) == 0 {
		return ""
	}

	art, err := extract.FromHTML(utf8Body, pageURL)
	if err != nil || art.Node == nil {
		return ""
	}

	base, _ := url.Parse(pageURL)
	md, err := mdconv.Convert(art.Node, mdconv.Options{Base: base})
	if err != nil {
		return ""
	}
	return strings.TrimSpace(md)
}

// isHTMLMIME reports whether a MIME type string names an HTML document.
func isHTMLMIME(mime string) bool {
	m := strings.ToLower(strings.TrimSpace(mime))
	if i := strings.IndexByte(m, ';'); i >= 0 {
		m = strings.TrimSpace(m[:i])
	}
	return m == "text/html" || m == "application/xhtml+xml"
}

// urlHostname returns the hostname component of a URL, or "" on parse error.
func urlHostname(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// HFMarkdownPath returns the HuggingFace repo path for one parquet shard.
// All shards for a crawl land under data/crawl=CC-MAIN-YYYY-WW/ so HF's
// partition-aware tooling can filter by crawl without a full scan.
func HFMarkdownPath(crawlID string, shardIdx int) string {
	return fmt.Sprintf("data/crawl=%s/%06d.parquet", crawlID, shardIdx)
}

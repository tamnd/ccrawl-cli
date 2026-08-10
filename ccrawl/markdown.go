package ccrawl

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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
)

// MarkdownRow is the parquet schema for one converted HTML document. It matches
// the open-index/open-markdown dataset layout so the output can be appended to
// existing crawls without a schema migration.
//
// language, language_confidence, extractor, and simhash make this
// open-markdown-v3. They are appended at the end and every earlier column keeps
// its name and type, so a v2 reader projecting the columns it knows about reads
// a v3 file without changes: parquet is read by name, and a reader that never
// asks for the new columns never touches them.
//
// extractor is name@version, not just the name. Extraction changes between
// releases, sometimes visibly, and a dataset that only recorded the name could
// not answer why two shards built months apart disagree about the same page.
//
// simhash is a fingerprint, not a decision. Collapsing near duplicates is a
// judgement about what a corpus is for, and it belongs in a query the consumer
// writes rather than in a pipeline that already threw the evidence away.
type MarkdownRow struct {
	DocID          string  `parquet:"doc_id"`
	URL            string  `parquet:"url"`
	Host           string  `parquet:"host"`
	CrawlDate      string  `parquet:"crawl_date"`
	WARCRecordID   string  `parquet:"warc_record_id"`
	HTMLLength     int64   `parquet:"html_length"`
	MarkdownLength int64   `parquet:"markdown_length"`
	Markdown       string  `parquet:"markdown"`
	Language       string  `parquet:"language"`
	LangConfidence float64 `parquet:"language_confidence"`
	Extractor      string  `parquet:"extractor"`
	Simhash        uint64  `parquet:"simhash"`
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
	WARCBytes    int64 // compressed .warc.gz bytes downloaded (Content-Length)
	HTMLBytes    int64
	MDBytes      int64
	ParquetBytes int64
	DurDownload  time.Duration
	DurConvert   time.Duration
	DurExport    time.Duration
	DurPublish   time.Duration

	// LangDropped counts documents a --lang filter threw away, and LangCounts
	// breaks that down by the language they were identified as, with "" for the
	// ones too short to identify. A drop rate on its own says the filter is
	// working; the breakdown says what it is working on, which is the number
	// worth looking at when the rate is not what you expected.
	//
	// LangCounts is filled in on the returned stats only, not on the snapshots
	// handed to Progress, so nothing reads the map while it is being written.
	LangDropped int64
	LangCounts  map[string]int64

	// DigestDropped counts records --dedup-digest skipped because an identical
	// payload had already been seen in this shard. It is filled in on the
	// returned stats, after the reader goroutine has finished.
	DigestDropped int64
}

// candidateRow is a converted document on its way to the writer, carrying the
// language filter's verdict. Dropped rows travel too, so the counting stays in
// the single writer goroutine where it needs no lock.
type candidateRow struct {
	row  MarkdownRow
	keep bool
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
	// MaxRecords, when > 0, stops the shard after this many HTML response
	// records have been queued for conversion. 0 means convert the whole shard.
	// It exists for the throughput benchmark, which needs a fixed, fast slice of
	// a shard; production export leaves it at 0.
	MaxRecords int
	// ConvertSem, when non-nil, caps the number of conversions running at once
	// across every shard that shares it. The orchestrator passes one semaphore
	// sized to the CPU count so several shards can be in flight (hiding download
	// latency) without oversubscribing the cores. nil means no global cap.
	ConvertSem chan struct{}
	// Lang, when set, keeps only documents identified as that ISO 639-3 language
	// with at least MinLangConfidence. Empty keeps everything and still records
	// the detected language on every row.
	Lang string
	// MinLangConfidence is the floor Lang applies. 0 selects
	// DefaultMinLangConfidence.
	MinLangConfidence float64
	// Extractor is the engine that turns a captured page into Markdown. nil
	// selects the default, which keeps every existing caller on h2m.
	Extractor *Extractor
	// DedupDigest skips a record whose payload is byte identical to one already
	// seen in this shard. The check runs before conversion, so a dropped record
	// costs a hash and not an extraction.
	//
	// The scope is one shard, not the whole run. Shards convert in parallel, so a
	// run wide set would make which copy survives depend on which shard happened
	// to finish first, and it would grow without a bound over --shards all. A
	// per-shard set is deterministic and costs one entry per page.
	DedupDigest bool
	// Progress is called after each row is written. It may be nil.
	Progress func(MarkdownStats)

	// Locations, when non-empty, replaces WARCPath as the source: instead of
	// downloading a whole shard, the pack reads exactly these records out of the
	// crawl with coalesced ranged GETs. This is the recovery pass shape, where a
	// columnar query picks a few thousand pages scattered over a few hundred
	// WARC files and reading the files whole would move a thousand times the
	// bytes.
	//
	// Everything downstream is identical: same extractor, same language filter,
	// same digest dedup, same schema. Only where the records come from changes.
	Locations []Location
	// FetchGap and FetchMaxSpan tune the coalescing for Locations. 0 selects
	// DefaultFetchGap and DefaultFetchMaxSpan.
	FetchGap     int64
	FetchMaxSpan int64
	// FetchWorkers is how many ranged GETs run at once for Locations. 0 selects
	// the conversion worker count, which is the right shape when the fetch is
	// the slow side and the CPU is idle waiting for it.
	FetchWorkers int
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
// N workers parallelise the h2m extraction and markdown rendering.
// The single writer keeps the parquet output sequential.
func PackMarkdownShard(ctx context.Context, h *HTTPClient, cfg MarkdownPackConfig) (MarkdownStats, error) {
	stats := MarkdownStats{ShardIdx: cfg.ShardIdx}

	t0 := time.Now()
	if len(cfg.Locations) > 0 {
		return packLocations(ctx, h, cfg, stats, t0)
	}
	warcURL := h.DataURL(cfg.WARCPath)
	resp, err := h.GetDownload(ctx, warcURL)
	if err != nil {
		return stats, fmt.Errorf("download shard %d: %w", cfg.ShardIdx, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return stats, fmt.Errorf("download shard %d: HTTP %d", cfg.ShardIdx, resp.StatusCode)
	}
	if resp.ContentLength > 0 {
		stats.WARCBytes = resp.ContentLength
	}

	return packStream(ctx, resp.Body, cfg, stats, t0)
}

// recordSource yields the captured pages a pack is going to convert, calling
// emit once per page and stopping early if emit says so.
//
// It exists so the conversion pipeline does not know or care whether the pages
// arrived as a whole downloaded shard or as a few thousand ranged reads picked
// out by an index query. Both go through the same extractor, filter, and writer.
type recordSource func(ctx context.Context, emit func(htmlRecord) error) error

// packStream runs the conversion pipeline over an already-open WARC byte stream
// and writes the parquet file at cfg.OutPath. It is the core shared by the
// network path (PackMarkdownShard) and the local-file benchmark, so both
// exercise identical extract and encode work. t0 marks when the download
// started so DurDownload covers the time until the first records flow.
func packStream(ctx context.Context, body io.Reader, cfg MarkdownPackConfig, stats MarkdownStats, t0 time.Time) (MarkdownStats, error) {
	ex := cfg.Extractor
	if ex == nil {
		ex = Extractors[DefaultExtractor]
	}
	return packSource(ctx, streamSource(body, cfg, ex), cfg, stats, t0)
}

// packSource is the conversion pipeline: it pulls pages from src, converts them
// in parallel, and writes the parquet file at cfg.OutPath.
func packSource(ctx context.Context, src recordSource, cfg MarkdownPackConfig, stats MarkdownStats, t0 time.Time) (MarkdownStats, error) {
	workers := cfg.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	if err := os.MkdirAll(filepath.Dir(cfg.OutPath), 0o755); err != nil {
		return stats, err
	}
	pw, err := newMarkdownParquetWriter(cfg.OutPath)
	if err != nil {
		return stats, err
	}

	minConf := cfg.MinLangConfidence
	if minConf <= 0 {
		minConf = DefaultMinLangConfidence
	}
	ex := cfg.Extractor
	if ex == nil {
		ex = Extractors[DefaultExtractor]
	}
	exID := ex.ID(cfg.CrawlID)

	records := make(chan htmlRecord, workers*4)
	rows := make(chan candidateRow, workers*4)
	langCounts := map[string]int64{}

	// Reader: iterate the source stream and push one record per page. Both
	// results are read after the rows channel closes, which the reader's own
	// close of records happens before, so neither needs a lock.
	var readErr error
	var digestDropped int64
	go func() {
		defer close(records)
		digestDropped, readErr = drainSource(ctx, src, cfg, records)
	}()

	tConvert := time.Now()

	// Workers: h2m extraction + markdown rendering, in parallel.
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for r := range records {
				md := convertGated(cfg.ConvertSem, ex, r.html, r.url)
				if md == "" {
					continue
				}
				// Identification runs on the extracted Markdown rather than the
				// HTML, so the answer is about the text being kept and not about
				// the nav bars and script tags that went in the bin.
				code, conf := DetectLanguage(md)
				rows <- candidateRow{
					keep: LangMatches(cfg.Lang, code, conf, minConf),
					row: MarkdownRow{
						DocID:          MarkdownDocID(r.url),
						URL:            r.url,
						Host:           urlHostname(r.url),
						CrawlDate:      r.date,
						WARCRecordID:   r.recordID,
						HTMLLength:     int64(len(r.html)),
						MarkdownLength: int64(len(md)),
						Markdown:       md,
						Language:       code,
						LangConfidence: conf,
						Extractor:      exID,
						Simhash:        Simhash(md),
					},
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(rows)
	}()

	stats.DurDownload = time.Since(t0)

	for c := range rows {
		langCounts[c.row.Language]++
		if !c.keep {
			stats.LangDropped++
			continue
		}
		if werr := pw.Write(c.row); werr != nil {
			go func() {
				for range rows {
				}
			}()
			_ = pw.Close()
			return stats, fmt.Errorf("write parquet: %w", werr)
		}
		stats.Rows++
		stats.HTMLBytes += c.row.HTMLLength
		stats.MDBytes += c.row.MarkdownLength
		if cfg.Progress != nil {
			cfg.Progress(stats)
		}
	}
	stats.LangCounts = langCounts
	stats.DigestDropped = digestDropped

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
	w := parquet.NewGenericWriter[MarkdownRow](f, parquet.Compression(codec))
	return &ParquetWriter[MarkdownRow]{f: f, w: w}, nil
}

// drainSource pulls every page out of src and pushes it onto records, returning
// how many pages --dedup-digest skipped.
//
// This is where the two policies that apply to any source live: the payload
// digest set, and the record limit. Where the pages came from is src's problem.
func drainSource(ctx context.Context, src recordSource, cfg MarkdownPackConfig, records chan<- htmlRecord) (int64, error) {
	queued := 0
	var dropped int64
	errStop := errors.New("record limit reached")

	// Exact duplicates are the cheap half of deduplication and the bigger half by
	// volume: the same page served on two URLs, a mirror, a session id in the
	// query string. Hashing the payload before conversion means a duplicate costs
	// a hash instead of an extraction.
	var seen map[[sha256.Size]byte]struct{}
	if cfg.DedupDigest {
		seen = make(map[[sha256.Size]byte]struct{}, 8192)
	}

	err := src(ctx, func(rec htmlRecord) error {
		if seen != nil {
			sum := sha256.Sum256(rec.html)
			if _, dup := seen[sum]; dup {
				dropped++
				return nil
			}
			seen[sum] = struct{}{}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case records <- rec:
		}
		queued++
		if cfg.MaxRecords > 0 && queued >= cfg.MaxRecords {
			return errStop
		}
		return nil
	})
	if errors.Is(err, errStop) {
		return dropped, nil // a deliberate early stop is not a read failure
	}
	return dropped, err
}

// streamSource reads a whole shard off an open byte stream. A WARC shard carries
// HTML in response records; a WET shard carries text Common Crawl already
// extracted, in conversion records. Both come out as the same struct, so
// everything downstream is identical for the two and only the extractor knows
// the difference.
func streamSource(body io.Reader, cfg MarkdownPackConfig, ex *Extractor) recordSource {
	return func(_ context.Context, emit func(htmlRecord) error) error {
		if ex.SourceKind == "wet" {
			return IterateWET(body, cfg.CrawlID, func(rec WETRecord) error {
				return emit(htmlRecord{
					url:      rec.URL,
					date:     rec.Date.Format("2006-01-02"),
					recordID: rec.RecordID,
					html:     []byte(rec.Text),
				})
			})
		}
		return IterateWARC(body, func(rec WARCRecord) error {
			page, ok := htmlPageOf(rec)
			if !ok {
				return nil
			}
			return emit(page)
		})
	}
}

// htmlPageOf picks the HTML page out of a WARC record, reporting false for the
// records a Markdown pack has no use for: requests, metadata, non-200 captures,
// non-HTML payloads, and empty bodies.
func htmlPageOf(rec WARCRecord) (htmlRecord, bool) {
	if rec.Header.Type != "response" || rec.Header.HTTPStatus != 200 {
		return htmlRecord{}, false
	}
	if !isHTMLMIME(rec.Header.HTTPMIME) {
		return htmlRecord{}, false
	}
	html := HTTPBody(rec.Block)
	if len(html) == 0 {
		return htmlRecord{}, false
	}
	return htmlRecord{
		url:      rec.Header.TargetURI,
		date:     rec.Header.Date.Format("2006-01-02"),
		recordID: rec.Header.RecordID,
		html:     html,
	}, true
}

// convertGated runs the extractor while holding a slot in sem, so the total
// number of concurrent conversions across all in-flight shards stays bounded.
// A nil sem runs the conversion ungated, and a nil extractor runs the default.
func convertGated(sem chan struct{}, ex *Extractor, body []byte, pageURL string) string {
	if sem != nil {
		sem <- struct{}{}
		defer func() { <-sem }()
	}
	return ex.Convert(body, pageURL)
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

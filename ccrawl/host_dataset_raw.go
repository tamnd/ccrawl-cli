package ccrawl

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"github.com/parquet-go/parquet-go"
)

// CDXRawRow is the projected schema for CC CDX Parquet files.
// parquet-go reads only these 8 columns, skipping the other ~22 in the file.
// FetchTime is a string to avoid timestamp conversion overhead; parquet-go
// encodes the INT64 microsecond timestamp directly into its string representation.
type CDXRawRow struct {
	URLHostName             string `parquet:"url_host_name"`
	URLHostRegisteredDomain string `parquet:"url_host_registered_domain"`
	FetchStatus             int32  `parquet:"fetch_status"`
	ContentMIMEDetected     string `parquet:"content_mime_detected"`
	ContentLanguages        string `parquet:"content_languages"`
	FetchTime               string `parquet:"fetch_time"`
	WARCRecordLength        int64  `parquet:"warc_record_length"`
}

// CDXRawOutputRow is written to cdx-raw-{prefix}.jsonl.gz.
// Short field names reduce storage.
type CDXRawOutputRow struct {
	Host  string `json:"host"`
	RD    string `json:"rd,omitempty"`
	ST    int32  `json:"st"`
	MIME  string `json:"mime,omitempty"`
	Lang  string `json:"lang,omitempty"`
	TS    string `json:"ts,omitempty"`
	Bytes int64  `json:"bytes,omitempty"`
}

// ExtractCDXRaw downloads each of parquetURLs with up to workers goroutines,
// reads each file with parquet-go (column-projected to CDXRawRow), and fans
// every row to the appropriate per-prefix cdx-raw-{prefix}.jsonl.gz file.
//
// No aggregation is performed. Each URL capture becomes one output row.
// Prefixes whose cdx-raw-{prefix}.done marker already exists are skipped.
// limit, if > 0, stops after that many files (useful for benchmarking).
//
// progress is called after each source file is fully processed.
func ExtractCDXRaw(ctx context.Context, h *HTTPClient, parquetURLs []string, workDir string, workers, limit int, progress func(fileN int, totalRows int64)) error {
	if workers <= 0 {
		workers = 8
	}

	prefixes := DatasetPrefixes

	// Determine which prefixes still need work.
	needed := make(map[string]bool, len(prefixes))
	for _, p := range prefixes {
		doneMarker := fmt.Sprintf("%s/cdx-raw-%s.done", workDir, p)
		if _, err := os.Stat(doneMarker); os.IsNotExist(err) {
			needed[p] = true
		}
	}
	if len(needed) == 0 {
		return nil
	}

	// Open per-prefix gzip+JSON writers (only for prefixes that need work).
	writers := make(map[string]*cdxPrefixWriter, len(needed))
	for p := range needed {
		tmp := fmt.Sprintf("%s/cdx-raw-%s.jsonl.gz.tmp", workDir, p)
		f, err := os.Create(tmp)
		if err != nil {
			for _, pw := range writers {
				_ = pw.gz.Close(); _ = pw.f.Close(); _ = os.Remove(pw.tmp)
			}
			return fmt.Errorf("create raw CDX file for prefix %q: %w", p, err)
		}
		gz, _ := gzip.NewWriterLevel(f, gzip.BestSpeed)
		writers[p] = &cdxPrefixWriter{f: f, gz: gz, enc: json.NewEncoder(gz), tmp: tmp}
	}

	var totalRows int64
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	for i, url := range parquetURLs {
		if ctx.Err() != nil {
			break
		}
		if limit > 0 && i >= limit {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(fileIdx int, fileURL string) {
			defer func() { <-sem; wg.Done() }()

			n, err := extractOneParquetFile(ctx, h, fileURL, fileIdx, workDir, writers)
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("file %d (%s): %w", fileIdx, fileURL, err)
				}
				errMu.Unlock()
				return
			}
			atomic.AddInt64(&totalRows, n)
			if progress != nil {
				progress(fileIdx+1, atomic.LoadInt64(&totalRows))
			}
		}(i, url)
	}
	wg.Wait()

	// Close all writers and rename .tmp → final (or delete on error).
	var closeErr error
	for p, pw := range writers {
		if err := pw.gz.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		if err := pw.f.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		if firstErr == nil && closeErr == nil {
			final := fmt.Sprintf("%s/cdx-raw-%s.jsonl.gz", workDir, p)
			if err := os.Rename(pw.tmp, final); err != nil && closeErr == nil {
				closeErr = err
			}
		} else {
			_ = os.Remove(pw.tmp)
		}
	}

	if firstErr != nil {
		return firstErr
	}
	if closeErr != nil {
		return closeErr
	}

	// Write per-prefix done markers.
	for p := range needed {
		marker := fmt.Sprintf("%s/cdx-raw-%s.done", workDir, p)
		_ = os.WriteFile(marker, []byte(fmt.Sprintf("total_rows=%d\n", totalRows)), 0o644)
	}
	return nil
}

// extractOneParquetFile downloads fileURL to a temp file, reads it with
// parquet-go, and fans each row to the appropriate prefix writer.
func extractOneParquetFile(ctx context.Context, h *HTTPClient, fileURL string, fileIdx int, workDir string, writers map[string]*cdxPrefixWriter) (int64, error) {
	tmpPath := fmt.Sprintf("%s/.cdx-dl-%04d.parquet", workDir, fileIdx)
	// Retry download up to 3 times — CC returns HTTP/2 INTERNAL_ERROR intermittently.
	var dlErr error
	for attempt := range 3 {
		_ = os.Remove(tmpPath)
		dlErr = DownloadToFile(ctx, h, fileURL, tmpPath)
		if dlErr == nil {
			break
		}
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		_ = attempt
	}
	if dlErr != nil {
		return 0, fmt.Errorf("download: %w", dlErr)
	}
	defer func() { _ = os.Remove(tmpPath) }()

	var n int64
	if err := streamParquetCDX(tmpPath, func(row CDXRawRow) error {
		p := datasetPrefix(row.URLHostName)
		pw, ok := writers[p]
		if !ok {
			return nil // prefix already done
		}
		out := CDXRawOutputRow{
			Host:  row.URLHostName,
			RD:    row.URLHostRegisteredDomain,
			ST:    row.FetchStatus,
			MIME:  row.ContentMIMEDetected,
			Lang:  row.ContentLanguages,
			Bytes: row.WARCRecordLength,
		}
		out.TS = row.FetchTime
		pw.mu.Lock()
		err := pw.enc.Encode(out)
		if err == nil {
			pw.n++
			n++
		}
		pw.mu.Unlock()
		return err
	}); err != nil {
		return n, fmt.Errorf("stream parquet: %w", err)
	}
	return n, nil
}

type cdxPrefixWriter struct {
	f   *os.File
	gz  *gzip.Writer
	enc *json.Encoder
	mu  sync.Mutex
	n   int64
	tmp string
}

// streamParquetCDX opens a local Parquet file and calls emit for each row,
// projecting only the columns declared in CDXRawRow.
func streamParquetCDX(path string, emit func(CDXRawRow) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		return fmt.Errorf("open parquet: %w", err)
	}

	reader := parquet.NewGenericReader[CDXRawRow](pf)
	defer func() { _ = reader.Close() }()

	buf := make([]CDXRawRow, 4096)
	for {
		n, err := reader.Read(buf)
		for i := range buf[:n] {
			if row := buf[i]; row.URLHostName != "" {
				if cerr := emit(row); cerr != nil {
					return cerr
				}
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// AggregateCDXRaw reads cdx-raw-{prefix}.jsonl.gz for each prefix and writes
// cdx-agg-{prefix}.jsonl.gz with one row per unique host.
// parallel controls how many prefixes are aggregated concurrently.
// Skips prefixes whose cdx-agg-{prefix}.done marker already exists.
func AggregateCDXRaw(ctx context.Context, workDir string, prefixes []string, parallel int, progress func(prefix string, hosts int64)) error {
	if parallel <= 0 {
		parallel = 1
	}

	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	for _, p := range prefixes {
		p := p
		if ctx.Err() != nil {
			break
		}

		doneMarker := fmt.Sprintf("%s/cdx-agg-%s.done", workDir, p)
		if _, err := os.Stat(doneMarker); err == nil {
			continue
		}

		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			hosts, err := aggregateCDXPrefix(ctx, workDir, p)
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("aggregate prefix %q: %w", p, err)
				}
				errMu.Unlock()
				return
			}
			if progress != nil {
				progress(p, hosts)
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// aggregateCDXPrefix reads cdx-raw-{prefix}.jsonl.gz, groups rows by host
// using hostSeqAccum, and writes cdx-agg-{prefix}.jsonl.gz.
func aggregateCDXPrefix(ctx context.Context, workDir, prefix string) (int64, error) {
	rawPath := fmt.Sprintf("%s/cdx-raw-%s.jsonl.gz", workDir, prefix)
	rf, err := os.Open(rawPath)
	if err != nil {
		return 0, fmt.Errorf("open raw CDX: %w", err)
	}
	defer func() { _ = rf.Close() }()
	rgz, err := gzip.NewReader(rf)
	if err != nil {
		return 0, fmt.Errorf("gunzip raw CDX: %w", err)
	}
	defer func() { _ = rgz.Close() }()

	hostMap := make(map[string]*hostSeqAccum, 8_000_000)
	dec := json.NewDecoder(rgz)
	for dec.More() {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		var row CDXRawOutputRow
		if err := dec.Decode(&row); err != nil {
			break
		}
		if row.Host == "" {
			continue
		}
		a := hostMap[row.Host]
		if a == nil {
			a = &hostSeqAccum{}
			hostMap[row.Host] = a
		}
		s := seqFileRow{
			Host:             row.Host,
			RegisteredDomain: row.RD,
			MIME:             row.MIME,
			Language:         row.Lang,
			URLCount:         1,
			FirstSeen:        row.TS,
			LastSeen:         row.TS,
			TotalBytes:       row.Bytes,
		}
		switch {
		case row.ST >= 200 && row.ST < 300:
			s.Status2xx = 1
		case row.ST >= 300 && row.ST < 400:
			s.Status3xx = 1
		case row.ST >= 400 && row.ST < 500:
			s.Status4xx = 1
		case row.ST >= 500 && row.ST < 600:
			s.Status5xx = 1
		}
		a.merge(s)
	}

	// Write cdx-agg-{prefix}.jsonl.gz
	outPath := fmt.Sprintf("%s/cdx-agg-%s.jsonl.gz", workDir, prefix)
	tmpPath := outPath + ".tmp"
	of, err := os.Create(tmpPath)
	if err != nil {
		return 0, err
	}
	ogz := gzip.NewWriter(of)
	enc := json.NewEncoder(ogz)
	for host, a := range hostMap {
		s := HostCDXStats{
			Host:             host,
			RegisteredDomain: a.RegisteredDomain,
			URLCount:         a.URLCount,
			Status2xx:        a.Status2xx,
			Status3xx:        a.Status3xx,
			Status4xx:        a.Status4xx,
			Status5xx:        a.Status5xx,
			TopMIME:          a.TopMIME,
			Language:         a.Language,
			FirstSeen:        a.FirstSeen,
			LastSeen:         a.LastSeen,
			TotalBytes:       a.TotalBytes,
		}
		if err := enc.Encode(s); err != nil {
			_ = ogz.Close(); _ = of.Close(); _ = os.Remove(tmpPath)
			return 0, err
		}
	}
	if err := ogz.Close(); err != nil {
		_ = of.Close(); _ = os.Remove(tmpPath)
		return 0, err
	}
	if err := of.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return 0, err
	}
	if err := os.Rename(tmpPath, outPath); err != nil {
		return 0, err
	}

	hosts := int64(len(hostMap))
	doneMarker := fmt.Sprintf("%s/cdx-agg-%s.done", workDir, prefix)
	_ = os.WriteFile(doneMarker, []byte(fmt.Sprintf("hosts=%d\n", hosts)), 0o644)
	return hosts, nil
}

package ccrawl

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

// hostSeqAccum is the in-memory aggregate for one hostname built across
// sequential per-file DuckDB results. No intermediate files are written;
// the accumulator is held in memory until all files are processed.
type hostSeqAccum struct {
	RegisteredDomain string
	URLCount         int64
	Status2xx        int64
	Status3xx        int64
	Status4xx        int64
	Status5xx        int64
	TotalBytes       int64
	FirstSeen        string
	LastSeen         string
	TopMIME          string
	topMIMECount     int64 // count for the winning mime bucket
	Language         string
	langCount        int64
}

func (a *hostSeqAccum) merge(s seqFileRow) {
	if a.RegisteredDomain == "" {
		a.RegisteredDomain = s.RegisteredDomain
	}
	a.URLCount += s.URLCount
	a.Status2xx += s.Status2xx
	a.Status3xx += s.Status3xx
	a.Status4xx += s.Status4xx
	a.Status5xx += s.Status5xx
	a.TotalBytes += s.TotalBytes
	if a.FirstSeen == "" || s.FirstSeen < a.FirstSeen {
		a.FirstSeen = s.FirstSeen
	}
	if s.LastSeen > a.LastSeen {
		a.LastSeen = s.LastSeen
	}
	// Pick MIME from the (host,mime,lang) bucket with the highest single-file
	// count as the best approximation of global MODE.
	if s.URLCount > a.topMIMECount {
		a.TopMIME = s.MIME
		a.topMIMECount = s.URLCount
	}
	if s.URLCount > a.langCount {
		a.Language = s.Language
		a.langCount = s.URLCount
	}
}

// seqFileRow is one row from CDXSeqLocalSQL: a (host, mime, lang) triple
// with aggregated counts for a single downloaded Parquet file.
type seqFileRow struct {
	Host             string
	RegisteredDomain string
	MIME             string
	Language         string
	URLCount         int64
	Status2xx        int64
	Status3xx        int64
	Status4xx        int64
	Status5xx        int64
	TotalBytes       int64
	FirstSeen        string
	LastSeen         string
}

// CDXSeqLocalSQL returns DuckDB SQL for a LOCAL parquet file. It groups by
// (url_host_name, content_mime_detected, content_languages) so that the Go
// accumulator can track per-MIME counts for accurate top-MIME selection
// without any file-level merge step.
func CDXSeqLocalSQL(localPath, prefix string) string {
	var filter string
	switch {
	case prefix >= "a" && prefix <= "z":
		filter = fmt.Sprintf("AND LOWER(SUBSTR(url_host_name, 1, 1)) = '%s'", sqlEscape(prefix))
	case prefix == "0":
		filter = "AND LOWER(SUBSTR(url_host_name, 1, 1)) BETWEEN '0' AND '9'"
	default:
		filter = "AND LOWER(SUBSTR(url_host_name, 1, 1)) NOT BETWEEN 'a' AND 'z' AND LOWER(SUBSTR(url_host_name, 1, 1)) NOT BETWEEN '0' AND '9'"
	}
	return fmt.Sprintf(`SET threads=2;
SELECT
    url_host_name,
    ANY_VALUE(url_host_registered_domain) AS registered_domain,
    COALESCE(content_mime_detected, '') AS mime,
    COALESCE(content_languages, '') AS language,
    COUNT(*) AS url_count,
    SUM(CASE WHEN fetch_status >= 200 AND fetch_status < 300 THEN 1 ELSE 0 END) AS status_2xx,
    SUM(CASE WHEN fetch_status >= 300 AND fetch_status < 400 THEN 1 ELSE 0 END) AS status_3xx,
    SUM(CASE WHEN fetch_status >= 400 AND fetch_status < 500 THEN 1 ELSE 0 END) AS status_4xx,
    SUM(CASE WHEN fetch_status >= 500 AND fetch_status < 600 THEN 1 ELSE 0 END) AS status_5xx,
    SUM(COALESCE(warc_record_length, 0)) AS total_bytes,
    MIN(CAST(fetch_time AS VARCHAR)) AS first_seen,
    MAX(CAST(fetch_time AS VARCHAR)) AS last_seen
FROM read_parquet('%s', hive_partitioning=0)
WHERE url_host_name IS NOT NULL
  AND url_host_name != ''
  %s
GROUP BY url_host_name, content_mime_detected, content_languages`,
		sqlEscape(localPath), filter)
}

// DownloadToFile downloads url into localPath using the download HTTP client.
// The caller is responsible for deleting the file after use.
func DownloadToFile(ctx context.Context, h *HTTPClient, url, localPath string) error {
	resp, err := h.GetDownload(ctx, url)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}
	f, err := os.Create(localPath)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = f.Close()
		_ = os.Remove(localPath)
		return fmt.Errorf("download body: %w", err)
	}
	return f.Close()
}

// SaveCDXSeqByPrefix processes all parquetURLs for a single prefix by
// downloading each file sequentially, running DuckDB locally, and
// accumulating per-host stats in memory — no intermediate files, no merge.
//
// For each URL: download to tmpFile, run CDXSeqLocalSQL, accumulate into
// hostMap, delete tmpFile. After all URLs, write cdx-{prefix}.jsonl.gz.
//
// progress is called after each file with (fileIndex, uniqueHostsSoFar).
func SaveCDXSeqByPrefix(ctx context.Context, h *HTTPClient, parquetURLs []string, prefix, workDir string, progress func(fileN int, hosts int64)) error {
	outPath := fmt.Sprintf("%s/cdx-%s.jsonl.gz", workDir, prefix)
	if _, err := os.Stat(outPath); err == nil {
		return nil // already done, skip
	}

	tmpFile := fmt.Sprintf("%s/.cdx-dl-%s.parquet", workDir, prefix)
	hostMap := make(map[string]*hostSeqAccum, 4_000_000)

	for i, url := range parquetURLs {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := DownloadToFile(ctx, h, url, tmpFile); err != nil {
			return fmt.Errorf("file %d: download: %w", i, err)
		}

		sql := CDXSeqLocalSQL(tmpFile, prefix)
		runErr := RunDuckDBJSON(ctx, "", sql, func(row map[string]any) error {
			host := stringVal(row, "url_host_name")
			if host == "" {
				return nil
			}
			s := seqFileRow{
				Host:             host,
				RegisteredDomain: stringVal(row, "registered_domain"),
				MIME:             stringVal(row, "mime"),
				Language:         stringVal(row, "language"),
				URLCount:         int64Val(row, "url_count"),
				Status2xx:        int64Val(row, "status_2xx"),
				Status3xx:        int64Val(row, "status_3xx"),
				Status4xx:        int64Val(row, "status_4xx"),
				Status5xx:        int64Val(row, "status_5xx"),
				TotalBytes:       int64Val(row, "total_bytes"),
				FirstSeen:        stringVal(row, "first_seen"),
				LastSeen:         stringVal(row, "last_seen"),
			}
			a := hostMap[host]
			if a == nil {
				a = &hostSeqAccum{}
				hostMap[host] = a
			}
			a.merge(s)
			return nil
		})
		_ = os.Remove(tmpFile)
		if runErr != nil {
			return fmt.Errorf("file %d: duckdb: %w", i, runErr)
		}
		if progress != nil {
			progress(i+1, int64(len(hostMap)))
		}
	}

	// Write final aggregated output.
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	gz := gzip.NewWriter(f)
	enc := json.NewEncoder(gz)
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
			_ = gz.Close()
			_ = f.Close()
			_ = os.Remove(outPath)
			return err
		}
	}
	if err := gz.Close(); err != nil {
		_ = f.Close()
		_ = os.Remove(outPath)
		return err
	}
	return f.Close()
}

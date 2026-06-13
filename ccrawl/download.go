package ccrawl

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// DownloadResult is the outcome of fetching one file.
type DownloadResult struct {
	Path      string
	LocalPath string
	Bytes     int64
	Skipped   bool
	Err       error
}

// DownloadFiles fetches a list of Common Crawl relative paths into localDir,
// concurrently and resumably. progress is called once per file when non-nil.
func DownloadFiles(ctx context.Context, h *HTTPClient, src Source, paths []string, localDir string, workers int, flat bool, progress func(DownloadResult)) error {
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		return err
	}
	if workers < 1 {
		workers = 1
	}
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var failed int64

	for _, p := range paths {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			defer func() { <-sem }()
			res := downloadOne(ctx, h, src, p, localDir, flat)
			if res.Err != nil {
				atomic.AddInt64(&failed, 1)
			}
			if progress != nil {
				progress(res)
			}
		}(p)
	}
	wg.Wait()

	if n := atomic.LoadInt64(&failed); n > 0 {
		return fmt.Errorf("%d of %d files failed", n, len(paths))
	}
	return nil
}

func downloadOne(ctx context.Context, h *HTTPClient, src Source, ccPath, localDir string, flat bool) DownloadResult {
	local := filepath.Join(localDir, filepath.Base(ccPath))
	if !flat {
		local = filepath.Join(localDir, filepath.FromSlash(ccPath))
	}
	if info, err := os.Stat(local); err == nil && info.Size() > 0 {
		return DownloadResult{Path: ccPath, LocalPath: local, Bytes: info.Size(), Skipped: true}
	}
	if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
		return DownloadResult{Path: ccPath, Err: err}
	}

	// S3 source still downloads over HTTPS unless an s3 client is configured;
	// the CloudFront mirror serves the same bytes, so prefer it for downloads.
	url := FileURL(ccPath, SourceHTTPS)
	_ = src
	resp, err := h.GetDownload(ctx, url)
	if err != nil {
		return DownloadResult{Path: ccPath, Err: fmt.Errorf("GET %s: %w", url, err)}
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return DownloadResult{Path: ccPath, Err: fmt.Errorf("GET %s: HTTP %d", url, resp.StatusCode)}
	}

	tmp := local + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return DownloadResult{Path: ccPath, Err: err}
	}
	n, err := io.Copy(f, resp.Body)
	if cerr := f.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return DownloadResult{Path: ccPath, Err: fmt.Errorf("write %s: %w", local, err)}
	}
	if err := os.Rename(tmp, local); err != nil {
		_ = os.Remove(tmp)
		return DownloadResult{Path: ccPath, Err: err}
	}
	return DownloadResult{Path: ccPath, LocalPath: local, Bytes: n}
}

// FetchWARCRecord retrieves a single WARC record from the given file using a
// byte-range request. This is how a capture's content is pulled without
// downloading the whole multi-gigabyte WARC.
func FetchWARCRecord(ctx context.Context, h *HTTPClient, filename string, offset, length int64) (WARCRecord, error) {
	resp, err := h.GetRange(ctx, FileURL(filename, SourceHTTPS), offset, length)
	if err != nil {
		return WARCRecord{}, fmt.Errorf("range GET: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var out WARCRecord
	found := false
	err = IterateWARC(resp.Body, func(r WARCRecord) error {
		out = r
		found = true
		return errStop
	})
	if err != nil && err != errStop {
		return WARCRecord{}, err
	}
	if !found {
		return WARCRecord{}, fmt.Errorf("no WARC record in range %d+%d of %s", offset, length, filename)
	}
	return out, nil
}

package ccrawl

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// warcSource is the phase-1 WARC byte stream for one shard. It hides whether the
// bytes come from a local cache file or a live download, and (on a download)
// tees them into a .part file that Commit promotes into the cache. The caller
// reads it like any io.Reader, then calls exactly one of Commit (the stream read
// cleanly, keep the cache) or Discard (the read failed, drop the partial), and
// always Close.
type warcSource struct {
	r    io.Reader // what the caller reads
	body io.Closer // network response body, nil on a cache hit
	part *os.File  // open .part file being filled, nil on a cache hit or no cache
	file *os.File  // open cache file on a hit, nil otherwise

	finalPath string // cache path the .part is renamed to on Commit
	partPath  string // the .part path, removed on Discard
}

func (w *warcSource) Read(p []byte) (int, error) { return w.r.Read(p) }

// Close releases the network body and any open file handles. It does not delete
// or rename anything; Commit and Discard own the .part file's fate.
func (w *warcSource) Close() error {
	if w.body != nil {
		_ = w.body.Close()
	}
	if w.file != nil {
		_ = w.file.Close()
	}
	if w.part != nil {
		_ = w.part.Close()
	}
	return nil
}

// Commit flushes the .part file and renames it into the cache atomically, so a
// later run finds a complete file. It is a no-op when there is no cache write in
// flight (a cache hit, or caching disabled).
func (w *warcSource) Commit() error {
	if w.part == nil {
		return nil
	}
	if err := w.part.Sync(); err != nil {
		_ = w.part.Close()
		_ = os.Remove(w.partPath)
		w.part = nil
		return err
	}
	if err := w.part.Close(); err != nil {
		_ = os.Remove(w.partPath)
		w.part = nil
		return err
	}
	err := os.Rename(w.partPath, w.finalPath)
	w.part = nil
	if err != nil {
		_ = os.Remove(w.partPath)
	}
	return err
}

// Discard removes the partial .part file. It is the cleanup path for an
// interrupted or failed download, so a truncated file is never left where a
// later run would reuse it. A cache hit has nothing to discard.
func (w *warcSource) Discard() {
	if w.part != nil {
		_ = w.part.Close()
		_ = os.Remove(w.partPath)
		w.part = nil
	}
}

// openWARCShard returns the WARC byte stream for a shard, preferring a cached
// copy. On a miss it streams the live download while teeing the bytes into a
// .part file beside the cache path; the caller promotes it with Commit once the
// stream has been read in full. When cfg.CacheDir is empty it streams straight
// from the network with no disk copy.
//
// The returned bool reports whether the stream is a cache hit (Commit is then a
// no-op). nbytes is the cached file size on a hit, or the download's
// Content-Length on a miss (0 if the server did not send one).
func openWARCShard(ctx context.Context, h *HTTPClient, cfg RefetchPackConfig) (src *warcSource, cached bool, nbytes int64, err error) {
	var cachePath string
	if cfg.CacheDir != "" {
		dir := filepath.Join(cfg.CacheDir, sanitizePathSegment(cfg.CrawlID))
		cachePath = filepath.Join(dir, filepath.Base(cfg.WARCPath))

		if fi, statErr := os.Stat(cachePath); statErr == nil && fi.Size() > 0 {
			f, oerr := os.Open(cachePath)
			if oerr == nil {
				return &warcSource{r: f, file: f}, true, fi.Size(), nil
			}
			// Unreadable cache entry: fall through to a fresh download.
		}
		if mkErr := os.MkdirAll(dir, 0o755); mkErr != nil {
			// Caching is best-effort: log and download without it rather than fail.
			fmt.Fprintf(os.Stderr, "refetch: shard %d: cache dir %s: %v\n", cfg.ShardIdx, dir, mkErr)
			cachePath = ""
		}
	}

	warcURL := FileURL(cfg.WARCPath, SourceHTTPS)
	resp, derr := h.GetDownload(ctx, warcURL)
	if derr != nil {
		return nil, false, 0, fmt.Errorf("shard %d: download warc: %w", cfg.ShardIdx, derr)
	}
	if resp.StatusCode != 200 {
		_ = resp.Body.Close()
		return nil, false, 0, fmt.Errorf("shard %d: download warc: HTTP %d", cfg.ShardIdx, resp.StatusCode)
	}
	if resp.ContentLength > 0 {
		nbytes = resp.ContentLength
	}

	// No cache: stream straight from the network.
	if cachePath == "" {
		return &warcSource{r: resp.Body, body: resp.Body}, false, nbytes, nil
	}

	// Cache miss: tee the download into a .part file the caller commits on a
	// clean read. A failure to create the temp file degrades to an uncached
	// stream rather than aborting the shard.
	partPath := cachePath + ".part"
	part, perr := os.Create(partPath)
	if perr != nil {
		fmt.Fprintf(os.Stderr, "refetch: shard %d: cache temp %s: %v\n", cfg.ShardIdx, partPath, perr)
		return &warcSource{r: resp.Body, body: resp.Body}, false, nbytes, nil
	}
	return &warcSource{
		r:         io.TeeReader(resp.Body, part),
		body:      resp.Body,
		part:      part,
		finalPath: cachePath,
		partPath:  partPath,
	}, false, nbytes, nil
}

// sanitizePathSegment turns a crawl ID into a safe single path segment, so a
// value like "CC-MAIN-2026-25" becomes a tidy cache subdirectory and nothing
// can escape the cache root via separators.
func sanitizePathSegment(s string) string {
	if s == "" {
		return "unknown"
	}
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			out = append(out, r)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}

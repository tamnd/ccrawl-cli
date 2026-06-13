package ccrawl

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// PathKinds are the file manifests published per crawl.
var PathKinds = []string{"warc", "wat", "wet", "robotstxt", "non200responses", "cc-index", "cc-index-table", "segment"}

// ListCrawls fetches and parses collinfo.json. Results are cached when a cache
// is supplied (pass nil to skip).
func ListCrawls(ctx context.Context, h *HTTPClient, cache *Cache) ([]Crawl, error) {
	if cache != nil {
		if data, ok := cache.Get("collinfo", 6*time.Hour); ok {
			var crawls []Crawl
			if json.Unmarshal(data, &crawls) == nil && len(crawls) > 0 {
				return crawls, nil
			}
		}
	}
	data, err := h.FetchBytes(ctx, CollInfoURL)
	if err != nil {
		return nil, fmt.Errorf("fetch collinfo: %w", err)
	}
	var crawls []Crawl
	if err := json.Unmarshal(data, &crawls); err != nil {
		return nil, fmt.Errorf("parse collinfo: %w", err)
	}
	if cache != nil {
		cache.Put("collinfo", data)
	}
	return crawls, nil
}

var reCrawlYearWeek = regexp.MustCompile(`^(\d{4})-(\d{2})$`)

// ResolveCrawl turns a loose reference into a canonical crawl ID.
//
//	"latest"           -> newest crawl
//	"CC-MAIN-2024-51"  -> itself
//	"2024-51"          -> "CC-MAIN-2024-51"
//	"2024"             -> newest crawl whose ID starts with CC-MAIN-2024
func ResolveCrawl(ctx context.Context, h *HTTPClient, cache *Cache, ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" || strings.EqualFold(ref, "latest") {
		crawls, err := ListCrawls(ctx, h, cache)
		if err != nil {
			return "", err
		}
		if len(crawls) == 0 {
			return "", fmt.Errorf("no crawls available")
		}
		return crawls[0].ID, nil
	}
	if strings.HasPrefix(ref, "CC-MAIN-") || strings.HasPrefix(ref, "CC-NEWS") {
		return ref, nil
	}
	if reCrawlYearWeek.MatchString(ref) {
		return "CC-MAIN-" + ref, nil
	}
	// Year only: find the newest matching crawl.
	if regexp.MustCompile(`^\d{4}$`).MatchString(ref) {
		crawls, err := ListCrawls(ctx, h, cache)
		if err != nil {
			return "", err
		}
		prefix := "CC-MAIN-" + ref + "-"
		for _, c := range crawls {
			if strings.HasPrefix(c.ID, prefix) {
				return c.ID, nil
			}
		}
		return "", fmt.Errorf("no crawl found for year %s", ref)
	}
	return "", fmt.Errorf("unrecognized crawl reference %q", ref)
}

// FetchPaths downloads and decompresses a crawl's path manifest.
func FetchPaths(ctx context.Context, h *HTTPClient, cache *Cache, crawlID, kind string) ([]string, error) {
	cacheKey := "paths:" + crawlID + ":" + kind
	if cache != nil {
		if data, ok := cache.Get(cacheKey, 30*24*time.Hour); ok {
			return splitLines(string(data)), nil
		}
	}
	var paths []string
	if err := StreamPaths(ctx, h, crawlID, kind, func(p string) error {
		paths = append(paths, p)
		return nil
	}); err != nil {
		return nil, err
	}
	if cache != nil && len(paths) > 0 {
		cache.Put(cacheKey, []byte(strings.Join(paths, "\n")))
	}
	return paths, nil
}

// StreamPaths streams a crawl's path manifest one path at a time.
func StreamPaths(ctx context.Context, h *HTTPClient, crawlID, kind string, fn func(string) error) error {
	resp, err := h.Get(ctx, pathsURL(crawlID, kind))
	if err != nil {
		return fmt.Errorf("fetch %s paths: %w", kind, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("fetch %s.paths.gz: HTTP %d (does crawl %s have this kind?)", kind, resp.StatusCode, crawlID)
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return fmt.Errorf("decompress %s paths: %w", kind, err)
	}
	defer gz.Close()

	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	for sc.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if err := fn(line); err != nil {
			return err
		}
	}
	return sc.Err()
}

func splitLines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	return out
}

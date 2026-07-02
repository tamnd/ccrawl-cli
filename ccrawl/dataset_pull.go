package ccrawl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
)

// DatasetPullConfig drives RunDatasetPull: which HF dataset repo to restore, an
// optional crawl filter, and where to write the files. It is the inverse of
// RunDatasetPublish: it turns a published Parquet corpus back into a local
// directory an indexer can read, so a rebuild never has to redownload and
// reconvert the crawl.
type DatasetPullConfig struct {
	Repo    string // HF dataset repo, org/name
	CrawlID string // when set, pull only data/crawl=<id>/; empty pulls all data/
	OutDir  string // local directory to write into
	Workers int    // concurrent downloads; 0 means 4
	Flat    bool   // write basenames into OutDir instead of the repo path tree

	// Progress is called once per file when non-nil.
	Progress func(DatasetPullResult)
}

// DatasetPullResult is the outcome of fetching one file.
type DatasetPullResult struct {
	Path      string // repo-relative path
	LocalPath string
	Bytes     int64
	Skipped   bool // already present locally with the expected size
	Err       error
}

// hfTreeEntry is one node in a HuggingFace repo tree listing.
type hfTreeEntry struct {
	Type string `json:"type"` // "file" or "directory"
	Path string `json:"path"`
	Size int64  `json:"size"`
}

// linkNextRE pulls the next-page URL out of a Link header (rel="next").
var linkNextRE = regexp.MustCompile(`<([^>]+)>;\s*rel="next"`)

// RunDatasetPull lists the Parquet files in a HF dataset repo and downloads them
// concurrently into OutDir, skipping any file already present with the expected
// size so a killed pull resumes. It resolves each file over plain HTTPS from the
// repo's resolve endpoint, which needs no token for a public dataset and uses the
// client's token for a private one.
func RunDatasetPull(ctx context.Context, hf *HFClient, cfg DatasetPullConfig) (int, error) {
	workers := cfg.Workers
	if workers < 1 {
		workers = 4
	}

	prefix := "data/"
	if cfg.CrawlID != "" {
		prefix = "data/crawl=" + cfg.CrawlID + "/"
	}

	entries, err := hf.listTree(ctx, cfg.Repo, prefix)
	if err != nil {
		return 0, err
	}
	var files []hfTreeEntry
	for _, e := range entries {
		if e.Type == "file" && strings.HasSuffix(e.Path, ".parquet") {
			files = append(files, e)
		}
	}
	if len(files) == 0 {
		return 0, fmt.Errorf("no .parquet files under %s in %s", prefix, cfg.Repo)
	}

	if err := os.MkdirAll(cfg.OutDir, 0o755); err != nil {
		return 0, err
	}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var failed int64

	for _, f := range files {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(f hfTreeEntry) {
			defer wg.Done()
			defer func() { <-sem }()
			res := hf.pullOne(ctx, cfg, f)
			if res.Err != nil {
				atomic.AddInt64(&failed, 1)
			}
			if cfg.Progress != nil {
				cfg.Progress(res)
			}
		}(f)
	}
	wg.Wait()

	if n := atomic.LoadInt64(&failed); n > 0 {
		return len(files), fmt.Errorf("%d of %d files failed", n, len(files))
	}
	return len(files), ctx.Err()
}

// listTree walks a HF dataset repo tree under prefix, following the Link header
// to page through large listings, and returns every file and directory entry.
func (c *HFClient) listTree(ctx context.Context, repoID, prefix string) ([]hfTreeEntry, error) {
	url := fmt.Sprintf("https://huggingface.co/api/datasets/%s/tree/main/%s?recursive=true",
		repoID, strings.TrimSuffix(prefix, "/"))
	var all []hfTreeEntry
	for url != "" {
		req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
		if c.token != "" {
			req.Header.Set("Authorization", "Bearer "+c.token)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return nil, fmt.Errorf("list tree: %w", err)
		}
		if resp.StatusCode == 404 {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("repo %s or path %s not found", repoID, prefix)
		}
		if resp.StatusCode != 200 {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("list tree HTTP %d", resp.StatusCode)
		}
		var page []hfTreeEntry
		if derr := json.NewDecoder(resp.Body).Decode(&page); derr != nil {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("decode tree: %w", derr)
		}
		link := resp.Header.Get("Link")
		_ = resp.Body.Close()
		all = append(all, page...)

		url = ""
		if m := linkNextRE.FindStringSubmatch(link); m != nil {
			url = m[1]
		}
	}
	return all, nil
}

// pullOne downloads one repo file over the resolve endpoint into the configured
// output directory. A file already present with the expected size is skipped.
func (c *HFClient) pullOne(ctx context.Context, cfg DatasetPullConfig, f hfTreeEntry) DatasetPullResult {
	res := DatasetPullResult{Path: f.Path}

	rel := f.Path
	if cfg.Flat {
		rel = path.Base(f.Path)
	}
	local := filepath.Join(cfg.OutDir, filepath.FromSlash(rel))
	res.LocalPath = local

	if fi, err := os.Stat(local); err == nil && (f.Size == 0 || fi.Size() == f.Size) {
		res.Skipped = true
		res.Bytes = fi.Size()
		return res
	}
	if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
		res.Err = err
		return res
	}

	url := fmt.Sprintf("https://huggingface.co/datasets/%s/resolve/main/%s", cfg.Repo, f.Path)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		res.Err = fmt.Errorf("get %s: %w", f.Path, err)
		return res
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		res.Err = fmt.Errorf("get %s: HTTP %d", f.Path, resp.StatusCode)
		return res
	}

	// Write to a temp file next to the target and rename on success, so an
	// interrupted download never leaves a truncated file the resume logic would
	// mistake for complete.
	tmp := local + ".part"
	out, err := os.Create(tmp)
	if err != nil {
		res.Err = err
		return res
	}
	n, err := io.Copy(out, resp.Body)
	closeErr := out.Close()
	if err != nil {
		_ = os.Remove(tmp)
		res.Err = fmt.Errorf("write %s: %w", f.Path, err)
		return res
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		res.Err = closeErr
		return res
	}
	if err := os.Rename(tmp, local); err != nil {
		_ = os.Remove(tmp)
		res.Err = err
		return res
	}
	res.Bytes = n
	return res
}

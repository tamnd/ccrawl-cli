package ccrawl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// HFOperation describes a file to add to a HuggingFace commit.
type HFOperation struct {
	LocalPath  string
	PathInRepo string
}

// HFShardPath returns the canonical HF repo path for a prefix shard (flat layout).
//
//	data/crawl=CC-MAIN-2026-21/subset=urls/hosts-a.parquet
func HFShardPath(crawlID, subset, prefix string) string {
	return fmt.Sprintf("data/crawl=%s/subset=%s/hosts-%s.parquet", crawlID, subset, prefix)
}

// HFShardPathChunk returns the HF path for one prefix shard within a CDX batch.
// chunk is 1-based. DuckDB reads all chunks with:
//
//	read_parquet('.../subset=urls/**/*.parquet')
func HFShardPathChunk(crawlID, subset, prefix string, chunk int) string {
	return fmt.Sprintf("data/crawl=%s/subset=%s/chunk=%03d/hosts-%s.parquet", crawlID, subset, chunk, prefix)
}

// RateLimitError is returned when HuggingFace responds 429 Too Many Requests.
type RateLimitError struct {
	RetryAfter time.Duration
	Msg        string
}

func (e *RateLimitError) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("HF rate limited (retry after %s): %s", e.RetryAfter.Round(time.Second), e.Msg)
	}
	return "HF rate limited: " + e.Msg
}

// Unwrap makes errors.Is(err, ErrHFRateLimited) work alongside the existing
// errors.As call sites that reach for RetryAfter.
func (e *RateLimitError) Unwrap() error { return ErrHFRateLimited }

// HFClient is a HuggingFace Hub client. It speaks the hub's commit protocol
// directly, including LFS multipart uploads, so nothing outside this binary is
// involved in publishing a dataset.
type HFClient struct {
	token string
	http  *http.Client
}

// NewHFClient creates an HFClient. If token is empty, HF_TOKEN is used
// (falling back to HUGGINGFACE_TOKEN for compatibility).
func NewHFClient(token string) *HFClient {
	if token == "" {
		token = os.Getenv("HF_TOKEN")
	}
	if token == "" {
		token = os.Getenv("HUGGINGFACE_TOKEN")
	}
	// The commit path opens many concurrent PUTs to the same object store host.
	// The default transport keeps only two idle connections per host, so most of
	// those would pay for a fresh TLS handshake; keeping a connection per
	// concurrent upload means the bandwidth goes to bytes instead.
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 100
	tr.MaxIdleConnsPerHost = hfUploadConcurrency
	tr.IdleConnTimeout = 90 * time.Second

	return &HFClient{
		token: token,
		http:  &http.Client{Timeout: 30 * time.Minute, Transport: tr},
	}
}

// Valid returns true if the client has a non-empty token.
func (c *HFClient) Valid() bool { return c.token != "" }

// CreateDatasetRepo creates a dataset repo if it does not exist.
// Returns nil for both 200/201 (created) and 409 (already exists).
func (c *HFClient) CreateDatasetRepo(ctx context.Context, repoID string, private bool) error {
	parts := strings.SplitN(repoID, "/", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid HF repo ID %q (must be org/name)", repoID)
	}
	body, _ := json.Marshal(map[string]interface{}{
		"type": "dataset", "name": parts[1], "organization": parts[0], "private": private,
	})
	req, _ := http.NewRequestWithContext(ctx, "POST", hfEndpoint+"/api/repos/create", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("create repo: %w", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode == 200 || resp.StatusCode == 201 || resp.StatusCode == 409 {
		return nil
	}
	return fmt.Errorf("create dataset repo HTTP %d", resp.StatusCode)
}

// pathInfoEntry is one entry of a paths-info response. Size is zero for the
// existence-only caller, which ignores it.
type pathInfoEntry struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

// pathsInfoBatch POSTs one batch of up to 100 paths to the paths-info endpoint
// and returns the entries for those that exist. A 404 means the whole batch is
// absent (empty repo or none present), which is a normal empty answer. Transient
// failures (429 and 5xx and network errors) are retried with backoff so a passing
// rate-limit cannot make the caller believe nothing exists; a persistent or 4xx
// failure is returned so the run stops rather than silently re-uploading
// everything the resume should have skipped.
func (c *HFClient) pathsInfoBatch(ctx context.Context, repoID string, paths []string) ([]pathInfoEntry, error) {
	body, _ := json.Marshal(map[string]any{"paths": paths})
	url := fmt.Sprintf("%s/api/datasets/%s/paths-info/main", hfEndpoint, repoID)
	var lastErr error
	for attempt := range 5 {
		if attempt > 0 {
			backoff := time.Duration(attempt*attempt) * hfRetryBase
			var rl *RateLimitError
			if errors.As(lastErr, &rl) && rl.RetryAfter > backoff {
				backoff = rl.RetryAfter
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
		req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("ask the hub for paths-info: %w", err)
			continue
		}
		if resp.StatusCode == 404 {
			_ = resp.Body.Close()
			return nil, nil
		}
		if resp.StatusCode == 200 {
			var infos []pathInfoEntry
			derr := json.NewDecoder(resp.Body).Decode(&infos)
			_ = resp.Body.Close()
			if derr != nil {
				return nil, fmt.Errorf("decode the paths-info reply: %w", derr)
			}
			return infos, nil
		}
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		_ = resp.Body.Close()
		msg := strings.TrimSpace(string(snippet))
		switch {
		case resp.StatusCode == 429:
			lastErr = &RateLimitError{Msg: msg}
		case resp.StatusCode >= 500:
			lastErr = fmt.Errorf("the paths-info endpoint answered HTTP %d: %s", resp.StatusCode, msg)
		default:
			return nil, fmt.Errorf("the paths-info endpoint answered HTTP %d: %s", resp.StatusCode, msg)
		}
	}
	return nil, fmt.Errorf("gave up on paths-info after retries: %w", lastErr)
}

// pathsInfoAll walks paths in batches of 100 through pathsInfoBatch and returns
// every entry that exists.
func (c *HFClient) pathsInfoAll(ctx context.Context, repoID string, paths []string) ([]pathInfoEntry, error) {
	var all []pathInfoEntry
	for start := 0; start < len(paths); start += 100 {
		end := min(start+100, len(paths))
		infos, err := c.pathsInfoBatch(ctx, repoID, paths[start:end])
		if err != nil {
			return nil, err
		}
		all = append(all, infos...)
	}
	return all, nil
}

// PathsExist returns which of the given paths already exist in the repo at "main".
func (c *HFClient) PathsExist(ctx context.Context, repoID string, paths []string) (map[string]bool, error) {
	infos, err := c.pathsInfoAll(ctx, repoID, paths)
	if err != nil {
		return nil, err
	}
	existing := make(map[string]bool, len(infos))
	for _, info := range infos {
		existing[info.Path] = true
	}
	return existing, nil
}

// PathsInfo returns the byte size of each given path that exists in the repo at
// "main". Missing paths are simply absent from the map. It reads the same
// paths-info endpoint as PathsExist, keeping the exact on-hub size the API
// reports so a caller can total bytes without downloading anything.
func (c *HFClient) PathsInfo(ctx context.Context, repoID string, paths []string) (map[string]int64, error) {
	infos, err := c.pathsInfoAll(ctx, repoID, paths)
	if err != nil {
		return nil, err
	}
	sizes := make(map[string]int64, len(infos))
	for _, info := range infos {
		sizes[info.Path] = info.Size
	}
	return sizes, nil
}

// CreateCommit uploads files to HuggingFace and returns the commit URL. It
// speaks the hub's commit protocol directly (see hf_upload.go), so publishing
// needs nothing on the box but this binary.
func (c *HFClient) CreateCommit(ctx context.Context, repoID, message string, ops []HFOperation) (string, error) {
	return c.createCommitGo(ctx, repoID, message, ops)
}

// CommitWithRetry calls CreateCommit up to maxAttempts times with exponential backoff.
// Rate-limit errors respect the server's Retry-After duration.
func (c *HFClient) CommitWithRetry(ctx context.Context, repoID, message string, ops []HFOperation, maxAttempts int) (string, error) {
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt*attempt) * 15 * time.Second
			var rl *RateLimitError
			if errors.As(lastErr, &rl) && rl.RetryAfter > backoff {
				backoff = rl.RetryAfter
			}
			fmt.Fprintf(os.Stderr, "  HF commit retry %d/%d after %s\n", attempt+1, maxAttempts, backoff.Round(time.Second))
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(backoff):
			}
		}
		url, err := c.CreateCommit(ctx, repoID, message, ops)
		if err == nil {
			return url, nil
		}
		lastErr = err
		fmt.Fprintf(os.Stderr, "  HF commit attempt %d/%d failed: %v\n", attempt+1, maxAttempts, err)
	}
	return "", fmt.Errorf("commit to the hub after %d attempts: %w", maxAttempts, lastErr)
}

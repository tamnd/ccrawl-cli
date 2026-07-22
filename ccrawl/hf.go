package ccrawl

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

//go:embed embed/hf_commit.py
var hfCommitPy []byte

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

// HFClient is a HuggingFace Hub client. Large-file commits are delegated to an
// embedded Python helper (hf_commit.py) run via uv, which uses huggingface_hub
// + hf-xet for xet-aware uploads.
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
	return &HFClient{
		token: token,
		http:  &http.Client{Timeout: 30 * time.Minute},
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
	req, _ := http.NewRequestWithContext(ctx, "POST", "https://huggingface.co/api/repos/create", bytes.NewReader(body))
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

// PathsExist returns which of the given paths already exist in the repo at "main".
func (c *HFClient) PathsExist(ctx context.Context, repoID string, paths []string) (map[string]bool, error) {
	existing := make(map[string]bool)
	for start := 0; start < len(paths); start += 100 {
		end := start + 100
		if end > len(paths) {
			end = len(paths)
		}
		body, _ := json.Marshal(map[string]interface{}{"paths": paths[start:end]})
		url := fmt.Sprintf("https://huggingface.co/api/datasets/%s/paths-info/main", repoID)
		req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(req)
		if err != nil {
			return nil, fmt.Errorf("paths-info: %w", err)
		}
		if resp.StatusCode == 404 {
			_ = resp.Body.Close()
			continue
		}
		var infos []struct {
			Path string `json:"path"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&infos)
		_ = resp.Body.Close()
		for _, info := range infos {
			existing[info.Path] = true
		}
	}
	return existing, nil
}

// PathsInfo returns the byte size of each given path that exists in the repo at
// "main". Missing paths are simply absent from the map. It reads the same
// paths-info endpoint as PathsExist, keeping the exact on-hub size the API
// reports so a caller can total bytes without downloading anything.
func (c *HFClient) PathsInfo(ctx context.Context, repoID string, paths []string) (map[string]int64, error) {
	sizes := make(map[string]int64)
	for start := 0; start < len(paths); start += 100 {
		end := start + 100
		if end > len(paths) {
			end = len(paths)
		}
		body, _ := json.Marshal(map[string]interface{}{"paths": paths[start:end]})
		url := fmt.Sprintf("https://huggingface.co/api/datasets/%s/paths-info/main", repoID)
		req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(req)
		if err != nil {
			return nil, fmt.Errorf("paths-info: %w", err)
		}
		if resp.StatusCode == 404 {
			_ = resp.Body.Close()
			continue
		}
		var infos []struct {
			Path string `json:"path"`
			Size int64  `json:"size"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&infos)
		_ = resp.Body.Close()
		for _, info := range infos {
			sizes[info.Path] = info.Size
		}
	}
	return sizes, nil
}

// CreateCommit uploads files to HuggingFace via uv+Python and returns the commit URL.
// The Python helper (hf_commit.py) is extracted to ~/.cache/ccrawl/ on first use.
func (c *HFClient) CreateCommit(ctx context.Context, repoID, message string, ops []HFOperation) (string, error) {
	scriptPath, err := hfScriptPath()
	if err != nil {
		return "", fmt.Errorf("hf_commit.py: %w", err)
	}
	type opJSON struct {
		LocalPath  string `json:"local_path,omitempty"`
		PathInRepo string `json:"path_in_repo"`
	}
	opsJSON := make([]opJSON, len(ops))
	for i, op := range ops {
		opsJSON[i] = opJSON(op)
	}
	stdin, _ := json.Marshal(map[string]interface{}{
		"token": c.token, "repo_id": repoID,
		"message": message, "num_threads": 8, "ops": opsJSON,
	})
	uvBin := hfResolveUV()
	if uvBin == "" {
		return "", fmt.Errorf("uv not found — install with: curl -LsSf https://astral.sh/uv/install.sh | sh")
	}
	uploadCtx, cancel := context.WithTimeout(ctx, 45*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(uploadCtx, uvBin, "run", scriptPath)
	cmd.Stdin = bytes.NewReader(stdin)
	cmd.Stderr = os.Stderr
	cmd.Cancel = func() error { return cmd.Process.Kill() }
	cmd.WaitDelay = 10 * time.Second
	cmd.Env = append(os.Environ(),
		"HF_HUB_VERBOSITY=warning",
		"HF_XET_FIXED_UPLOAD_CONCURRENCY=4",
		"HF_XET_CLIENT_RETRY_MAX_ATTEMPTS=7",
		"HF_XET_CLIENT_RETRY_MAX_DURATION=600s",
		"HF_XET_CLIENT_READ_TIMEOUT=300s",
		"HF_XET_CLIENT_CONNECT_TIMEOUT=120s",
	)
	out, err := cmd.Output()
	if uploadCtx.Err() == context.DeadlineExceeded {
		return "", fmt.Errorf("HF commit timed out after 45 minutes")
	}
	if err != nil {
		return "", fmt.Errorf("HF commit failed: %w", err)
	}
	var result struct {
		CommitURL  string `json:"commit_url"`
		Error      string `json:"error"`
		RetryAfter int    `json:"retry_after"`
	}
	if jsonErr := json.Unmarshal(out, &result); jsonErr != nil {
		return "", fmt.Errorf("HF commit parse: %w", jsonErr)
	}
	if result.Error != "" {
		if result.RetryAfter > 0 {
			return "", &RateLimitError{
				RetryAfter: time.Duration(result.RetryAfter) * time.Second,
				Msg:        result.Error,
			}
		}
		return "", fmt.Errorf("HF commit: %s", result.Error)
	}
	return result.CommitURL, nil
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
	return "", fmt.Errorf("HF commit after %d attempts: %w", maxAttempts, lastErr)
}

func hfScriptPath() (string, error) {
	home, _ := os.UserHomeDir()
	dir := filepath.Join(home, ".cache", "ccrawl")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	p := filepath.Join(dir, "hf_commit.py")
	existing, _ := os.ReadFile(p)
	if string(existing) != string(hfCommitPy) {
		if err := os.WriteFile(p, hfCommitPy, 0o755); err != nil {
			return "", err
		}
	}
	return p, nil
}

func hfResolveUV() string {
	if p, err := exec.LookPath("uv"); err == nil {
		return p
	}
	home, _ := os.UserHomeDir()
	for _, candidate := range []string{
		filepath.Join(home, ".local", "bin", "uv"),
		filepath.Join(home, ".cargo", "bin", "uv"),
		"/usr/local/bin/uv",
	} {
		if fi, err := os.Stat(candidate); err == nil && !fi.IsDir() {
			return candidate
		}
	}
	return ""
}

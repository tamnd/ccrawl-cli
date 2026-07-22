package ccrawl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

// DeleteDatasetRepo deletes a dataset repo. A 404 (already gone) is treated as
// success so the call is idempotent. This is used only by the guarded
// delete-obsolete migration command.
func (c *HFClient) DeleteDatasetRepo(ctx context.Context, repoID string) error {
	parts := strings.SplitN(repoID, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("invalid HF repo ID %q (must be org/name)", repoID)
	}
	body := fmt.Sprintf(`{"type":"dataset","name":%q,"organization":%q}`, parts[1], parts[0])
	req, _ := http.NewRequestWithContext(ctx, "DELETE", "https://huggingface.co/api/repos/delete", bytes.NewReader([]byte(body)))
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("delete repo: %w", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode == 200 || resp.StatusCode == 204 || resp.StatusCode == 404 {
		return nil
	}
	return fmt.Errorf("delete dataset repo HTTP %d", resp.StatusCode)
}

// DownloadRepoFile fetches one file from a dataset repo's main branch into
// localPath. It returns false without error when the file does not exist (404),
// so a caller can seed a fresh local ledger from the hub when present and start
// empty when not. A private repo uses the client token.
func (c *HFClient) DownloadRepoFile(ctx context.Context, repoID, pathInRepo, localPath string) (bool, error) {
	url := fmt.Sprintf("https://huggingface.co/datasets/%s/resolve/main/%s", repoID, pathInRepo)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return false, fmt.Errorf("download %s: %w", pathInRepo, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == 404 {
		return false, nil
	}
	if resp.StatusCode != 200 {
		return false, fmt.Errorf("download %s HTTP %d", pathInRepo, resp.StatusCode)
	}
	f, err := os.Create(localPath)
	if err != nil {
		return false, err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = f.Close()
		return false, err
	}
	if err := f.Close(); err != nil {
		return false, err
	}
	return true, nil
}

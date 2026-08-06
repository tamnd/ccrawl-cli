package ccrawl

// Live tests against the real HuggingFace Hub. These are skipped unless you opt
// in, because they need a token, they push real bytes over the network, and they
// take minutes rather than milliseconds.
//
// Run them with real Common Crawl parquet:
//
//	ccrawl markdown export --shards 0-9 --push=false --keep-parquet --out /tmp/hf-real
//	CCRAWL_HF_LIVE_DIR=/tmp/hf-real \
//	CCRAWL_HF_LIVE_REPO=your-org/ccrawl-hf-live \
//	go test ./ccrawl/ -run TestLive -v -timeout 60m
//
// TestLiveCommitIntegrity proves the bytes arrive unchanged: every LFS oid the
// hub reports back is the sha256 we computed locally. TestLiveCommitThroughput
// reports what the upload actually sustains. Point them at a scratch repo, they
// write to it.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// liveShards returns the parquet files in CCRAWL_HF_LIVE_DIR, sorted by name,
// skipping the test if the opt-in is not set.
func liveShards(t *testing.T) []string {
	t.Helper()
	dir := os.Getenv("CCRAWL_HF_LIVE_DIR")
	if dir == "" {
		t.Skip("set CCRAWL_HF_LIVE_DIR to a directory of parquet shards to run live HF tests")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".parquet") {
			continue
		}
		// A zero-byte parquet is a shard an export is still writing, not
		// something to publish.
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() == 0 {
			t.Logf("skipping %s, still being written", e.Name())
			continue
		}
		out = append(out, filepath.Join(dir, e.Name()))
	}
	sort.Strings(out)
	if len(out) == 0 {
		t.Fatalf("no parquet files in %s", dir)
	}
	return out
}

func liveClient(t *testing.T) *HFClient {
	t.Helper()
	c := NewHFClient("")
	if !c.Valid() {
		t.Skip("set HF_TOKEN to run live HF tests")
	}
	return c
}

func liveRepo(t *testing.T, envVar string) string {
	t.Helper()
	repo := os.Getenv(envVar)
	if repo == "" {
		t.Skipf("set %s to run this test", envVar)
	}
	return repo
}

// hubFile is what the hub reports about a committed path. For an LFS file the
// oid is the sha256 of the content, so comparing it to the local hash proves the
// bytes made it across unchanged.
type hubFile struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
	LFS  *struct {
		OID  string `json:"oid"`
		Size int64  `json:"size"`
	} `json:"lfs"`
}

// hubPathsInfo asks the hub what it holds at the given paths, keeping the LFS
// oid that PathsInfo throws away.
func hubPathsInfo(ctx context.Context, c *HFClient, repoID string, paths []string) (map[string]hubFile, error) {
	body, _ := json.Marshal(map[string]any{"paths": paths})
	url := fmt.Sprintf("https://huggingface.co/api/datasets/%s/paths-info/main", repoID)
	req, _ := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("paths-info HTTP %d: %s", resp.StatusCode, snippet)
	}
	var files []hubFile
	if err := json.NewDecoder(resp.Body).Decode(&files); err != nil {
		return nil, err
	}
	out := make(map[string]hubFile, len(files))
	for _, f := range files {
		out[f.Path] = f
	}
	return out, nil
}

func localSHA256(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

func totalSize(t *testing.T, paths []string) int64 {
	t.Helper()
	var n int64
	for _, p := range paths {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		n += fi.Size()
	}
	return n
}

func opsFor(paths []string) []HFOperation {
	ops := make([]HFOperation, len(paths))
	for i, p := range paths {
		ops[i] = HFOperation{LocalPath: p, PathInRepo: "data/crawl=live/" + filepath.Base(p)}
	}
	return ops
}

// TestLiveCommitIntegrity commits real shards and checks the hub ended up
// holding what we sent: the same paths, the same sizes, and LFS oids equal to
// the local sha256.
func TestLiveCommitIntegrity(t *testing.T) {
	shards := liveShards(t)
	c := liveClient(t)
	repo := liveRepo(t, "CCRAWL_HF_LIVE_REPO")

	ops := opsFor(shards)
	paths := make([]string, len(ops))
	for i, op := range ops {
		paths[i] = op.PathInRepo
	}

	ctx := t.Context()
	if _, err := c.CreateCommit(ctx, repo, "integrity check", ops); err != nil {
		t.Fatalf("commit: %v", err)
	}

	files, err := hubPathsInfo(ctx, c, repo, paths)
	if err != nil {
		t.Fatal(err)
	}

	for i, path := range paths {
		want := localSHA256(t, shards[i])
		f, ok := files[path]
		if !ok {
			t.Fatalf("%s missing from %s", path, repo)
		}
		if f.LFS == nil {
			t.Fatalf("%s: expected an LFS file, got %+v", path, f)
		}
		if f.LFS.OID != want {
			t.Errorf("%s: hub oid %s, local sha256 %s", path, f.LFS.OID, want)
		}
		if want := fileSize(t, shards[i]); f.LFS.Size != want {
			t.Errorf("%s: hub size %d, local size %d", path, f.LFS.Size, want)
		}
		t.Logf("%s: hub stored oid %s (%d bytes)", path, f.LFS.OID, f.LFS.Size)
	}
}

// TestLiveCommitThroughput commits real parquet and reports wall clock and MB/s.
// The hub deduplicates, so a repeat run against the same repo with the same
// shards measures nothing. Use a fresh repo, or fresh shards, for a real number.
func TestLiveCommitThroughput(t *testing.T) {
	shards := liveShards(t)
	c := liveClient(t)
	repo := liveRepo(t, "CCRAWL_HF_LIVE_REPO")

	size := totalSize(t, shards)
	ctx := t.Context()
	start := time.Now()
	if _, err := c.CreateCommit(ctx, repo, "throughput check", opsFor(shards)); err != nil {
		t.Fatalf("commit: %v", err)
	}
	elapsed := time.Since(start)
	t.Logf("%d shards, %.1f MB in %s = %.1f MB/s", len(shards),
		float64(size)/(1<<20), elapsed.Round(time.Second),
		float64(size)/(1<<20)/elapsed.Seconds())
}

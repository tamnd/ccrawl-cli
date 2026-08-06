package ccrawl

// Live tests against the real HuggingFace Hub. These are skipped unless you opt
// in, because they need a token, they push real bytes over the network, and they
// take minutes rather than milliseconds.
//
// Run them with real Common Crawl parquet:
//
//	ccrawl markdown export --shards 0-9 --push=false --keep-parquet --out /tmp/e1-real
//	CCRAWL_HF_LIVE_DIR=/tmp/e1-real \
//	CCRAWL_HF_LIVE_GO_REPO=open-index/ccrawl-e1-bench-go \
//	CCRAWL_HF_LIVE_PY_REPO=open-index/ccrawl-e1-bench-py \
//	go test ./ccrawl/ -run TestLive -v -timeout 60m
//
// TestLiveCommitParity proves the Go path stores the same bytes as the Python
// path. TestLiveCommitThroughput measures both, using a different half of the
// shards for each so neither benefits from the other having already uploaded the
// same content.

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

// TestLiveCommitParity commits the same real shards through both paths into two
// repos and checks the hub ended up holding identical content: the same paths,
// the same sizes, and LFS oids equal to the local sha256 on both sides.
func TestLiveCommitParity(t *testing.T) {
	shards := liveShards(t)
	c := liveClient(t)
	goRepo := liveRepo(t, "CCRAWL_HF_LIVE_GO_PARITY_REPO")
	pyRepo := liveRepo(t, "CCRAWL_HF_LIVE_PY_PARITY_REPO")

	// Every shard in the directory is the fixture, so point CCRAWL_HF_LIVE_DIR at
	// a ten shard export to get the ten shard comparison issue #40 asks for.
	ops := opsFor(shards)
	paths := make([]string, len(ops))
	for i, op := range ops {
		paths[i] = op.PathInRepo
	}

	ctx := t.Context()
	if _, err := c.createCommitGo(ctx, goRepo, "parity check via go", ops); err != nil {
		t.Fatalf("go commit: %v", err)
	}
	if _, err := c.createCommitPython(ctx, pyRepo, "parity check via python", ops); err != nil {
		t.Fatalf("python commit: %v", err)
	}

	goFiles, err := hubPathsInfo(ctx, c, goRepo, paths)
	if err != nil {
		t.Fatal(err)
	}
	pyFiles, err := hubPathsInfo(ctx, c, pyRepo, paths)
	if err != nil {
		t.Fatal(err)
	}

	for i, path := range paths {
		want := localSHA256(t, shards[i])
		g, ok := goFiles[path]
		if !ok {
			t.Fatalf("%s missing from %s", path, goRepo)
		}
		p, ok := pyFiles[path]
		if !ok {
			t.Fatalf("%s missing from %s", path, pyRepo)
		}
		if g.LFS == nil || p.LFS == nil {
			t.Fatalf("%s: expected both sides to be LFS, go=%+v py=%+v", path, g.LFS, p.LFS)
		}
		if g.LFS.OID != want {
			t.Errorf("%s: go oid %s, local sha256 %s", path, g.LFS.OID, want)
		}
		if p.LFS.OID != want {
			t.Errorf("%s: python oid %s, local sha256 %s", path, p.LFS.OID, want)
		}
		if g.LFS.Size != p.LFS.Size {
			t.Errorf("%s: go size %d, python size %d", path, g.LFS.Size, p.LFS.Size)
		}
		t.Logf("%s: both paths stored oid %s (%d bytes)", path, want, g.LFS.Size)
	}
}

// TestLiveCommitThroughput commits real parquet through both paths and reports
// wall clock and MB/s. Each path gets its own half of the shards so neither is
// uploading content the other already put on the hub, which would turn the
// second run into a dedup no-op and make the number meaningless.
func TestLiveCommitThroughput(t *testing.T) {
	shards := liveShards(t)
	c := liveClient(t)
	goRepo := liveRepo(t, "CCRAWL_HF_LIVE_GO_REPO")
	pyRepo := liveRepo(t, "CCRAWL_HF_LIVE_PY_REPO")

	if len(shards) < 2 {
		t.Skip("need at least two shards to give each path its own set")
	}
	half := len(shards) / 2
	goShards, pyShards := shards[:half], shards[half:]

	ctx := t.Context()
	run := func(name, repo string, paths []string, commit func(context.Context, string, string, []HFOperation) (string, error)) float64 {
		bytes := totalSize(t, paths)
		start := time.Now()
		if _, err := commit(ctx, repo, "throughput check via "+name, opsFor(paths)); err != nil {
			t.Fatalf("%s commit: %v", name, err)
		}
		elapsed := time.Since(start)
		mbps := float64(bytes) / (1 << 20) / elapsed.Seconds()
		t.Logf("%s: %d shards, %.1f MB in %s = %.1f MB/s",
			name, len(paths), float64(bytes)/(1<<20), elapsed.Round(time.Second), mbps)
		return mbps
	}

	// Set CCRAWL_HF_LIVE_PY_FIRST to swap the order, which is how you check that
	// whichever path runs second is not being flattered by a warm connection.
	var goMBps, pyMBps float64
	if os.Getenv("CCRAWL_HF_LIVE_PY_FIRST") != "" {
		pyMBps = run("python", pyRepo, pyShards, c.createCommitPython)
		goMBps = run("go", goRepo, goShards, c.createCommitGo)
	} else {
		goMBps = run("go", goRepo, goShards, c.createCommitGo)
		pyMBps = run("python", pyRepo, pyShards, c.createCommitPython)
	}
	t.Logf("go is %.2fx the python throughput", goMBps/pyMBps)
}

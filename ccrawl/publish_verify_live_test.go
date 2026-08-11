package ccrawl

// Live verify tests against the real HuggingFace Hub and the real published
// datasets. They are skipped unless you opt in, because they need a token, they
// push and pull real bytes, and they take minutes rather than milliseconds.
//
//	CCRAWL_VERIFY_LIVE_REPO=open-index/ccrawl-urls \
//	go test ./ccrawl/ -run TestLiveVerifyPublishedCrawl -v -timeout 60m
//
//	CCRAWL_HF_LIVE_REPO=your-org/ccrawl-verify-scratch \
//	CCRAWL_VERIFY_LIVE_CRAWL=CC-MAIN-2026-25 \
//	go test ./ccrawl/ -run TestLiveVerifyRepairsATruncatedShard -v -timeout 120m
//
// The first is read only and measures what a full verify costs against the
// dataset it is checking. The second writes: it puts a deliberately truncated
// copy of a real shard on a scratch repo, checks that verify catches it, repairs
// it from the Common Crawl source part, and checks that the repaired shard
// passes and holds the rows the real one holds.

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

// publishedURLsRepo is the dataset the truncated fixture is cut from and the
// repaired shard is compared against. It is only ever read here.
const publishedURLsRepo = "open-index/ccrawl-urls"

func liveVerifyCrawl(t *testing.T) string {
	t.Helper()
	if c := os.Getenv("CCRAWL_VERIFY_LIVE_CRAWL"); c != "" {
		return c
	}
	t.Skip("set CCRAWL_VERIFY_LIVE_CRAWL to the published crawl to verify")
	return ""
}

// TestLiveVerifyPublishedCrawl checks the whole published crawl the way an
// operator would and reports what the check cost. The done-when for this feature
// is a number, so the test logs it: bytes read as a percentage of the bytes the
// dataset is holding.
func TestLiveVerifyPublishedCrawl(t *testing.T) {
	repo := liveRepo(t, "CCRAWL_VERIFY_LIVE_REPO")
	crawl := liveVerifyCrawl(t)
	hf := NewHFClient("")

	ctx := t.Context()
	h := NewHTTPClient(Config{})
	start := time.Now()
	rep, err := VerifyURLCrawl(ctx, h, NewCache(t.TempDir(), true), hf, URLPublishOptions{
		Repo:     repo,
		StageDir: t.TempDir(),
		Logf:     t.Logf,
	}, crawl, VerifyOptions{Workers: 16})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	pct := 100 * float64(rep.BytesRead) / float64(rep.Bytes)
	t.Logf("%s: %d shards, %d passed, %d failed, %d rows, %.1f GB held, %.1f MB read (%.3f%%) in %s",
		crawl, rep.Shards, rep.Passed, rep.Failed, rep.Rows,
		float64(rep.Bytes)/(1<<30), float64(rep.BytesRead)/(1<<20), pct, time.Since(start).Round(time.Second))
	for _, c := range rep.Failures() {
		t.Errorf("%s: %s, %s", c.Path, c.Status, c.Detail)
	}
	for _, n := range rep.Notes {
		t.Errorf("ledger: %s", n)
	}
	if rep.Shards == 0 {
		t.Fatal("nothing was checked")
	}
	if pct >= 1 {
		t.Errorf("read %.3f%% of the dataset, and a footer check should stay under 1%%", pct)
	}
}

// TestLiveVerifyRepairsATruncatedShard is the end to end proof for --repair. It
// works on a scratch repo, never on the published one.
func TestLiveVerifyRepairsATruncatedShard(t *testing.T) {
	hf := liveClient(t)
	scratch := liveRepo(t, "CCRAWL_HF_LIVE_REPO")
	crawl := liveVerifyCrawl(t)
	if scratch == publishedURLsRepo {
		t.Fatalf("%s is the published dataset; point CCRAWL_HF_LIVE_REPO at a scratch repo", scratch)
	}

	ctx := t.Context()
	h := NewHTTPClient(Config{})
	dir := t.TempDir()
	repoPath := fmt.Sprintf("data/%s/part-00000.parquet", crawl)

	// Take the front of a real published shard. Those are real Parquet bytes
	// that stop early, which is exactly what an upload that died leaves behind.
	src := hfResolveURL(publishedURLsRepo, repoPath)
	full, err := h.ContentLength(ctx, src)
	if err != nil {
		t.Fatalf("size of %s: %v", src, err)
	}
	cut := full * 2 / 5
	stub := filepath.Join(dir, "truncated.parquet")
	if err := fetchPrefix(ctx, h, src, cut, stub); err != nil {
		t.Fatalf("fetch the first %d bytes: %v", cut, err)
	}
	t.Logf("staged %.1f MB of the %.1f MB shard as a truncated upload",
		float64(cut)/(1<<20), float64(full)/(1<<20))

	// The scratch repo has to be public, because verify reads shards with plain
	// ranged GETs the way any dataset consumer would, and the published datasets
	// are public.
	if err := hf.CreateDatasetRepo(ctx, scratch, false); err != nil {
		t.Fatalf("scratch repo: %v", err)
	}
	if _, err := hf.CreateCommit(ctx, scratch, "stage a truncated shard for the verify test",
		[]HFOperation{{LocalPath: stub, PathInRepo: repoPath}}); err != nil {
		t.Fatalf("stage: %v", err)
	}

	store := HFShardStore{HF: hf, Repo: scratch}
	vo := VerifyOptions{Workers: 4, Schema: parquet.SchemaOf(URLRow{}), Logf: t.Logf}
	rep, err := VerifyShards(ctx, h, store, []string{repoPath}, vo)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	got := rep.Checks[0]
	if got.Status != VerifyUnreadable && got.Status != VerifyTruncated {
		t.Fatalf("a shard cut to %d of %d bytes came back %q, %s", cut, full, got.Status, got.Detail)
	}
	t.Logf("detected: %s, %s", got.Status, got.Detail)

	// Repair from the Common Crawl source part the shard is the projection of.
	urls, err := ColumnarParquetURLs(ctx, h, NewCache(dir, true), crawl, "warc", "")
	if err != nil {
		t.Fatalf("source parts: %v", err)
	}
	if len(urls) == 0 {
		t.Fatalf("no source parts for %s", crawl)
	}
	o := URLPublishOptions{Repo: scratch, StageDir: dir, DoCommit: true, Logf: t.Logf}
	job := urlPartJob{
		index:     0,
		sourceURL: urls[0],
		repoPath:  repoPath,
		tmpPath:   filepath.Join(dir, "part-00000.parquet.tmp"),
		outPath:   filepath.Join(dir, "part-00000.parquet"),
	}
	start := time.Now()
	if _, err := repairURLShards(ctx, h, hf, o, crawl, []urlPartJob{job}); err != nil {
		t.Fatalf("repair: %v", err)
	}
	t.Logf("repaired in %s", time.Since(start).Round(time.Second))

	rep, err = VerifyShards(ctx, h, store, []string{repoPath}, vo)
	if err != nil {
		t.Fatalf("verify after repair: %v", err)
	}
	got = rep.Checks[0]
	if !got.OK() {
		t.Fatalf("after repair: %s, %s", got.Status, got.Detail)
	}
	t.Logf("after repair: ok, %d rows, %.1f MB", got.Rows, float64(got.Bytes)/(1<<20))

	// The repaired shard has to hold what the published one holds, otherwise
	// repair swapped a truncated shard for a wrong one.
	want, err := VerifyShards(ctx, h, HFShardStore{HF: hf, Repo: publishedURLsRepo}, []string{repoPath}, vo)
	if err != nil {
		t.Fatalf("verify the published shard: %v", err)
	}
	if got.Rows != want.Checks[0].Rows {
		t.Errorf("repaired shard holds %d rows and the published one holds %d", got.Rows, want.Checks[0].Rows)
	}
}

// fetchPrefix writes the first n bytes of a URL to a file.
func fetchPrefix(ctx context.Context, h *HTTPClient, url string, n int64, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	ra := newHTTPReaderAt(ctx, h, url, n, 8<<20, 2)
	if _, err := io.Copy(f, io.NewSectionReader(ra, 0, n)); err != nil {
		return err
	}
	return f.Close()
}

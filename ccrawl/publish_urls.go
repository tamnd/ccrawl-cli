package ccrawl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
)

// URLPublishOptions configures a ccrawl-urls publish run.
type URLPublishOptions struct {
	Repo        string   // target dataset repo, org/name
	CrawlIDs    []string // resolved crawl ids, newest first
	Subset      string   // columnar subset, defaults to warc
	Source      Source   // https mirror or s3
	StageDir    string   // local staging root
	CommitEvery int      // shards per commit
	Workers     int      // download-and-convert workers
	Whole       bool     // download the whole part before reading (range-hostile fallback)
	Private     bool     // create the repo private
	Keep        bool     // keep local shards after commit (implies no delete)
	DoCommit    bool     // false is a dry run: stage and print, never touch the hub
	MinFreeGB   int      // free-disk floor gating new downloads
	MaxStall    time.Duration
	Logf        func(string, ...any)
}

// partIndexRE matches the zero-padded shard index in a columnar file name such
// as part-00042-<uuid>.c000.gz.parquet.
var partIndexRE = regexp.MustCompile(`part-(\d+)-`)

// partIndexFromURL extracts the shard index from a columnar parquet URL.
func partIndexFromURL(url string) (int, bool) {
	m := partIndexRE.FindStringSubmatch(url)
	if m == nil {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, false
	}
	return n, true
}

// urlPartJob is one part to project into an output shard.
type urlPartJob struct {
	index    int
	sourceURL string
	repoPath string // data/<crawl>/part-NNNNN.parquet
	tmpPath  string // local staged temp
	outPath  string // local staged final
}

// PublishURLs runs the ccrawl-urls pipeline: for each crawl it projects every
// original columnar part into a slim URLRow Parquet shard and commits it to the
// hub. It is idempotent from remote truth (parts already present are skipped)
// and keeps local disk flat by deleting each shard right after it commits.
func PublishURLs(ctx context.Context, h *HTTPClient, cache *Cache, hf *HFClient, o URLPublishOptions) error {
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
	if o.Subset == "" {
		o.Subset = "warc"
	}
	if o.CommitEvery <= 0 {
		o.CommitEvery = 16
	}
	if o.Workers <= 0 {
		o.Workers = budgetProcess(0)
	}
	if err := os.MkdirAll(o.StageDir, 0o755); err != nil {
		return err
	}
	sweepTemps(o.StageDir)

	statsPath := filepath.Join(o.StageDir, "stats.csv")
	progressPath := filepath.Join(o.StageDir, "publish-progress.json")

	if o.DoCommit {
		if !hf.Valid() {
			return errors.New("no HF token: set HF_TOKEN to publish")
		}
		if err := hf.CreateDatasetRepo(ctx, o.Repo, o.Private); err != nil {
			return err
		}
		// Seed the local ledger from the hub so rollups stay correct across
		// machines. A missing file just means a fresh dataset.
		if _, err := os.Stat(statsPath); os.IsNotExist(err) {
			if _, err := hf.DownloadRepoFile(ctx, o.Repo, "stats.csv", statsPath); err != nil {
				o.Logf("warning: could not seed stats.csv from hub: %v", err)
			}
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	clock := newStallClock(o.MaxStall, cancel)
	go clock.watch(runCtx)

	for _, crawl := range o.CrawlIDs {
		if err := runCtx.Err(); err != nil {
			break
		}
		if err := publishURLCrawl(runCtx, h, cache, hf, o, crawl, statsPath, progressPath, clock); err != nil {
			if clock.stalled() {
				return ErrCommitStall
			}
			return fmt.Errorf("crawl %s: %w", crawl, err)
		}
	}
	if clock.stalled() {
		return ErrCommitStall
	}
	return nil
}

func publishURLCrawl(ctx context.Context, h *HTTPClient, cache *Cache, hf *HFClient, o URLPublishOptions, crawl, statsPath, progressPath string, clock *stallClock) error {
	urls, err := ColumnarParquetURLs(ctx, h, cache, crawl, o.Subset, o.Source)
	if err != nil {
		return err
	}
	total := len(urls)

	jobs := make([]urlPartJob, 0, total)
	repoPaths := make([]string, 0, total)
	crawlDir := filepath.Join(o.StageDir, "data", crawl)
	if err := os.MkdirAll(crawlDir, 0o755); err != nil {
		return err
	}
	for _, u := range urls {
		idx, ok := partIndexFromURL(u)
		if !ok {
			return fmt.Errorf("cannot parse part index from %q", u)
		}
		repoPath := fmt.Sprintf("data/%s/part-%05d.parquet", crawl, idx)
		repoPaths = append(repoPaths, repoPath)
		jobs = append(jobs, urlPartJob{
			index:     idx,
			sourceURL: u,
			repoPath:  repoPath,
			tmpPath:   filepath.Join(crawlDir, fmt.Sprintf("part-%05d.parquet.tmp", idx)),
			outPath:   filepath.Join(crawlDir, fmt.Sprintf("part-%05d.parquet", idx)),
		})
	}

	done := map[string]bool{}
	if o.DoCommit {
		done, err = hf.PathsExist(ctx, o.Repo, repoPaths)
		if err != nil {
			return err
		}
	}
	work := make([]urlPartJob, 0, total)
	for _, j := range jobs {
		if !done[j.repoPath] {
			work = append(work, j)
		}
	}
	o.Logf("crawl %s: %d parts, %d already published, %d to do", crawl, total, total-len(work), len(work))

	// Seed the committer's rollup from the current ledger and hub truth so the
	// finalize stat is cumulative rather than per-run.
	base := findURLStat(statsPath, crawl)
	doneCount := max(len(done), base.Shards)
	c := &committer{
		hf:           hf,
		repo:         o.Repo,
		scope:        crawl,
		kind:         "url",
		width:        5,
		commitEvery:  o.CommitEvery,
		keep:         o.Keep,
		doCommit:     o.DoCommit,
		progressKey:  crawl,
		progressPath: progressPath,
		clock:        clock,
		logf:         o.Logf,
		committed:    doneCount,
		rows:         base.Rows,
		bytes:        base.ParquetBytes,
	}

	if len(work) > 0 {
		if err := runURLWorkers(ctx, h, o, work, c); err != nil {
			return err
		}
		if err := c.flush(ctx); err != nil {
			return err
		}
	}

	return finalizeURLCrawl(ctx, hf, o, crawl, total, c, statsPath, base)
}

// runURLWorkers projects the work list concurrently and feeds finished shards to
// the single committer running on this goroutine.
func runURLWorkers(ctx context.Context, h *HTTPClient, o URLPublishOptions, work []urlPartJob, c *committer) error {
	jobs := make(chan urlPartJob)
	shards := make(chan shard)

	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	fail := func(err error) {
		once.Do(func() { firstErr = err })
	}

	for i := 0; i < o.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if ctx.Err() != nil {
					return
				}
				if err := waitForDiskFloor(ctx, o.StageDir, o.MinFreeGB, c.clock); err != nil {
					fail(err)
					return
				}
				rows, bytes, err := projectURLPart(ctx, h, j, o.Whole)
				if err != nil {
					// A single bad part is not fatal: it is retried on the next
					// run because its target still does not exist on the hub.
					o.Logf("skip %s: %v", j.repoPath, err)
					_ = os.Remove(j.tmpPath)
					continue
				}
				select {
				case shards <- shard{Index: j.index, RepoPath: j.repoPath, Local: j.outPath, Rows: rows, Bytes: bytes}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, j := range work {
			select {
			case jobs <- j:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(shards)
	}()

	for s := range shards {
		if err := c.add(ctx, s); err != nil {
			fail(err)
			// Drain remaining shards so producers do not block on send.
			go func() {
				for range shards {
				}
			}()
			break
		}
	}
	return firstErr
}

// projectURLPart reads one columnar part, projecting only the URLRow columns,
// and writes a zstd Parquet shard. It returns the row count and output size.
func projectURLPart(ctx context.Context, h *HTTPClient, j urlPartJob, whole bool) (int64, int64, error) {
	w, err := NewParquetWriter[URLRow](j.tmpPath)
	if err != nil {
		return 0, 0, err
	}

	stream := func(pf *parquet.File) error {
		r := parquet.NewGenericReader[URLRow](pf)
		defer func() { _ = r.Close() }()
		buf := make([]URLRow, 4096)
		for {
			n, rerr := r.Read(buf)
			if n > 0 {
				if werr := w.WriteRows(buf[:n]); werr != nil {
					return werr
				}
			}
			if errors.Is(rerr, io.EOF) {
				return nil
			}
			if rerr != nil {
				return rerr
			}
		}
	}

	if whole {
		err = withWholeFile(ctx, h, j.sourceURL, j.tmpPath+".src", func(pf *parquet.File) error { return stream(pf) })
	} else {
		var size int64
		size, err = h.ContentLength(ctx, j.sourceURL)
		if err == nil {
			ra := newHTTPReaderAt(ctx, h, j.sourceURL, size, 8<<20, 8)
			var pf *parquet.File
			pf, err = parquet.OpenFile(ra, size)
			if err == nil {
				err = stream(pf)
			}
		}
	}
	if err != nil {
		_ = w.Close()
		_ = os.Remove(j.tmpPath)
		return 0, 0, err
	}
	if err := w.Close(); err != nil {
		_ = os.Remove(j.tmpPath)
		return 0, 0, err
	}
	if err := os.Rename(j.tmpPath, j.outPath); err != nil {
		return 0, 0, err
	}
	fi, err := os.Stat(j.outPath)
	if err != nil {
		return 0, 0, err
	}
	return w.Rows(), fi.Size(), nil
}

// withWholeFile downloads url to a local temp, opens it as Parquet, runs fn, and
// removes the temp. It is the fallback for servers that dislike ranged reads.
func withWholeFile(ctx context.Context, h *HTTPClient, url, tmp string, fn func(*parquet.File) error) error {
	resp, err := h.GetDownload(ctx, url)
	if err != nil {
		return err
	}
	f, err := os.Create(tmp)
	if err != nil {
		_ = resp.Body.Close()
		return err
	}
	_, err = io.Copy(f, resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	defer func() { _ = os.Remove(tmp) }()
	rf, err := os.Open(tmp)
	if err != nil {
		return err
	}
	defer func() { _ = rf.Close() }()
	fi, err := rf.Stat()
	if err != nil {
		return err
	}
	pf, err := parquet.OpenFile(rf, fi.Size())
	if err != nil {
		return err
	}
	return fn(pf)
}

// finalizeURLCrawl upserts the crawl's ledger row, regenerates the dataset card,
// and commits both. On a fresh crawl with nothing new it still refreshes the
// card so the hub reflects the current state.
func finalizeURLCrawl(ctx context.Context, hf *HFClient, o URLPublishOptions, crawl string, total int, c *committer, statsPath string, base URLCrawlStat) error {
	now := time.Now().UTC().Format(time.RFC3339)
	first := base.FirstCommitted
	if first == "" {
		first = now
	}
	stat := URLCrawlStat{
		Crawl:          crawl,
		Shards:         c.committed,
		TotalShards:    total,
		Rows:           c.rows,
		ParquetBytes:   c.bytes,
		Complete:       c.committed >= total && total > 0,
		FirstCommitted: first,
		LastCommitted:  now,
	}

	rows, err := ReadURLStats(statsPath)
	if err != nil {
		return err
	}
	rows = UpsertURLStat(rows, stat)
	if err := WriteURLStats(statsPath, rows); err != nil {
		return err
	}

	readmePath := filepath.Join(o.StageDir, "README.md")
	if err := os.WriteFile(readmePath, []byte(GenerateURLsREADME(o.Repo, rows)), 0o644); err != nil {
		return err
	}

	o.Logf("crawl %s: %d/%d shards, %s rows, %s", crawl, stat.Shards, stat.TotalShards, humanCountShort(stat.Rows), humanBytes(stat.ParquetBytes))

	if !o.DoCommit {
		o.Logf("[dry-run] would update ledger and card for %s", crawl)
		return nil
	}
	ops := []HFOperation{
		{LocalPath: statsPath, PathInRepo: "stats.csv"},
		{LocalPath: readmePath, PathInRepo: "README.md"},
	}
	if _, err := hf.CommitWithRetry(ctx, o.Repo, finalizeURLMessage(stat), ops, 5); err != nil {
		return err
	}
	c.clock.mark()
	return nil
}

// findURLStat returns the ledger row for a crawl, or a zero row when absent.
func findURLStat(statsPath, crawl string) URLCrawlStat {
	rows, err := ReadURLStats(statsPath)
	if err != nil {
		return URLCrawlStat{Crawl: crawl}
	}
	for _, r := range rows {
		if r.Crawl == crawl {
			return r
		}
	}
	return URLCrawlStat{Crawl: crawl}
}

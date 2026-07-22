package ccrawl

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/parquet-go/parquet-go"
)

// RecountURLs recomputes exact row and byte totals for each crawl straight from
// the shards already on the hub and commits a corrected ledger row and card. It
// repairs totals that a normal run could not seed, for example the first batches
// of a crawl that were published before any ledger lived on the hub, so the card
// counts never disagree with the shard coverage. Shard files are never touched;
// only stats.csv and README.md change.
func RecountURLs(ctx context.Context, h *HTTPClient, cache *Cache, hf *HFClient, o URLPublishOptions) error {
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
	if o.Subset == "" {
		o.Subset = "warc"
	}
	if !hf.Valid() {
		return errors.New("no HF token: set HF_TOKEN to recount")
	}
	if err := os.MkdirAll(o.StageDir, 0o755); err != nil {
		return err
	}
	statsPath := filepath.Join(o.StageDir, "stats.csv")
	// Seed the local ledger from the hub so FirstCommitted and other crawls'
	// rows survive the rewrite.
	if _, err := os.Stat(statsPath); os.IsNotExist(err) {
		if _, err := hf.DownloadRepoFile(ctx, o.Repo, "stats.csv", statsPath); err != nil {
			o.Logf("warning: could not seed stats.csv from hub: %v", err)
		}
	}

	for _, crawl := range o.CrawlIDs {
		if err := recountURLCrawl(ctx, h, cache, hf, o, crawl, statsPath); err != nil {
			return fmt.Errorf("crawl %s: %w", crawl, err)
		}
	}
	return nil
}

// recountURLCrawl reads the footer of every published shard for one crawl to sum
// exact rows and bytes, then commits the refreshed ledger and card.
func recountURLCrawl(ctx context.Context, h *HTTPClient, cache *Cache, hf *HFClient, o URLPublishOptions, crawl, statsPath string) error {
	urls, err := ColumnarParquetURLs(ctx, h, cache, crawl, o.Subset, o.Source)
	if err != nil {
		return err
	}
	total := len(urls)

	repoPaths := make([]string, 0, total)
	for _, u := range urls {
		idx, ok := partIndexFromURL(u)
		if !ok {
			return fmt.Errorf("cannot parse part index from %q", u)
		}
		repoPaths = append(repoPaths, fmt.Sprintf("data/%s/part-%05d.parquet", crawl, idx))
	}

	done, err := hf.PathsExist(ctx, o.Repo, repoPaths)
	if err != nil {
		return err
	}
	published := make([]string, 0, len(done))
	for _, p := range repoPaths {
		if done[p] {
			published = append(published, p)
		}
	}
	if len(published) == 0 {
		o.Logf("crawl %s: nothing published yet, nothing to recount", crawl)
		return nil
	}

	sizes, err := hf.PathsInfo(ctx, o.Repo, published)
	if err != nil {
		return err
	}

	rows, bytes, err := sumPublishedTotals(ctx, h, o.Repo, published, sizes, o.Workers)
	if err != nil {
		return err
	}

	base := findURLStat(statsPath, crawl)
	stat, ops, err := refreshURLCard(o, crawl, total, len(published), rows, bytes, base, statsPath)
	if err != nil {
		return err
	}
	o.Logf("crawl %s: recounted %d/%d shards, %s rows, %s", crawl, stat.Shards, stat.TotalShards, humanCountShort(stat.Rows), humanBytes(stat.ParquetBytes))

	if !o.DoCommit {
		o.Logf("[dry-run] would commit recounted ledger and card for %s", crawl)
		return nil
	}
	msg := fmt.Sprintf("Recount %s url totals (%d shards, %s rows, %s)", crawl, stat.Shards, humanCountShort(stat.Rows), humanBytes(stat.ParquetBytes))
	if _, err := hf.CommitWithRetry(ctx, o.Repo, msg, ops, 5); err != nil {
		return err
	}
	return nil
}

// RecountDomainGraph recomputes exact domain and byte totals for a web-graph
// release straight from the shards already on the hub and commits a corrected
// ledger row and card. Like RecountURLs it repairs totals a resuming run could
// not seed, for example the first batches published before any ledger lived on
// the hub. Shard files are never touched; only stats.csv and README.md change.
func RecountDomainGraph(ctx context.Context, h *HTTPClient, hf *HFClient, o DomainPublishOptions) error {
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
	if o.ShardRows <= 0 {
		o.ShardRows = DefaultShardRows
	}
	if !hf.Valid() {
		return errors.New("no HF token: set HF_TOKEN to recount")
	}
	if err := os.MkdirAll(o.StageDir, 0o755); err != nil {
		return err
	}
	statsPath := filepath.Join(o.StageDir, "stats.csv")
	if _, err := os.Stat(statsPath); os.IsNotExist(err) {
		if _, err := hf.DownloadRepoFile(ctx, o.Repo, "stats.csv", statsPath); err != nil {
			o.Logf("warning: could not seed stats.csv from hub: %v", err)
		}
	}

	graph := o.Graph.ID

	// The final shard count is not known from the source, so probe a generous
	// range of shard paths and keep the ones the hub actually has.
	probe := make([]string, 0, 512)
	for i := range 512 {
		probe = append(probe, fmt.Sprintf("data/%s/part-%03d.parquet", graph, i))
	}
	done, err := hf.PathsExist(ctx, o.Repo, probe)
	if err != nil {
		return err
	}
	published := make([]string, 0, len(done))
	for _, p := range probe {
		if done[p] {
			published = append(published, p)
		}
	}
	if len(published) == 0 {
		o.Logf("graph %s: nothing published yet, nothing to recount", graph)
		return nil
	}

	sizes, err := hf.PathsInfo(ctx, o.Repo, published)
	if err != nil {
		return err
	}
	domains, bytes, err := sumPublishedTotals(ctx, h, o.Repo, published, sizes, o.Workers)
	if err != nil {
		return err
	}

	// Source size is invariant across shards; reuse the ledger's if present, else
	// read it once from the source with a HEAD.
	base := findDomainStat(statsPath, graph)
	srcBytes := base.SourceBytes
	if srcBytes <= 0 {
		if n, cerr := h.ContentLength(ctx, o.Graph.DomainRankURL()); cerr == nil {
			srcBytes = n
		}
	}

	// Recount only re-totals the shards on the hub; it cannot tell whether the
	// release was streamed to its end, so it preserves the ledger's complete flag.
	stat, ops, err := refreshDomainCard(o, graph, len(published), domains, bytes, srcBytes, base.Complete, statsPath)
	if err != nil {
		return err
	}
	o.Logf("graph %s: recounted %d shards, %s domains, %s", graph, stat.Shards, humanCountShort(stat.Domains), humanBytes(stat.ParquetBytes))

	if !o.DoCommit {
		o.Logf("[dry-run] would commit recounted ledger and card for %s", graph)
		return nil
	}
	msg := fmt.Sprintf("Recount %s domain totals (%d shards, %s domains, %s)", graph, stat.Shards, humanCountShort(stat.Domains), humanBytes(stat.ParquetBytes))
	if _, err := hf.CommitWithRetry(ctx, o.Repo, msg, ops, 5); err != nil {
		return err
	}
	return nil
}

// sumPublishedTotals reads each shard's Parquet footer over HTTP to total its
// exact row count and adds up the on-hub byte sizes. Footers are read
// concurrently with a bounded pool; only the footer and its index are fetched,
// not the shard body, so the cost is a couple of small range requests per file.
func sumPublishedTotals(ctx context.Context, h *HTTPClient, repo string, paths []string, sizes map[string]int64, workers int) (int64, int64, error) {
	if workers <= 0 {
		workers = budgetProcess(0)
	}
	var (
		mu         sync.Mutex
		totalRows  int64
		totalBytes int64
		firstErr   error
	)
	jobs := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				if ctx.Err() != nil {
					return
				}
				size := sizes[p]
				if size <= 0 {
					var cerr error
					if size, cerr = h.ContentLength(ctx, hfResolveURL(repo, p)); cerr != nil {
						mu.Lock()
						if firstErr == nil {
							firstErr = fmt.Errorf("size %s: %w", p, cerr)
						}
						mu.Unlock()
						return
					}
				}
				n, err := parquetRowsAt(ctx, h, hfResolveURL(repo, p), size)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("rows %s: %w", p, err)
					}
					mu.Unlock()
					return
				}
				mu.Lock()
				totalRows += n
				totalBytes += size
				mu.Unlock()
			}
		}()
	}
	for _, p := range paths {
		select {
		case jobs <- p:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return 0, 0, ctx.Err()
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return 0, 0, firstErr
	}
	return totalRows, totalBytes, nil
}

// parquetRowsAt opens a remote Parquet file by its footer alone and returns its
// row count. It reuses the ranged reader that the projection path uses, so only
// the footer and metadata are fetched, not the column data.
func parquetRowsAt(ctx context.Context, h *HTTPClient, url string, size int64) (int64, error) {
	ra := newHTTPReaderAt(ctx, h, url, size, 8<<20, 4)
	pf, err := parquet.OpenFile(ra, size)
	if err != nil {
		return 0, err
	}
	return pf.NumRows(), nil
}

// hfResolveURL is the public download URL for one file on a dataset's main branch.
func hfResolveURL(repo, path string) string {
	return fmt.Sprintf("https://huggingface.co/datasets/%s/resolve/main/%s", repo, path)
}

package ccrawl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parquet-go/parquet-go"
	mg "github.com/tamnd/meguri"
	"github.com/tamnd/meguri/frontier"
	"github.com/tamnd/meguri/seed"
)

// urlOnlyRow projects just the url column of the columnar index. parquet-go decodes
// only the columns a reader's schema names, so this single-field struct makes each
// shard read pull the url column chunks and nothing else, the "fields that contain
// urls" selection that keeps the bulk pull to a fraction of the 530 MiB shard.
type urlOnlyRow struct {
	URL string `parquet:"url"`
}

// CCSeedOptions drives BuildCCSeed: which crawl and subset to pull, how many shards to
// route into, and the knobs that bound a run (a url limit, a file window, the worker
// count). The output is a sharded .seed directory that `meguri shard build` turns into
// a partitioned store with no reprocessing.
type CCSeedOptions struct {
	Crawl     string     // crawl ref, "latest" resolves to the newest
	Subset    string     // warc (default), crawldiagnostics, robotstxt
	Shards    int        // hostkey-range shards, rounded up to a power of two
	BlockSize int        // seed block size, 0 = seed.DefaultBlockSize
	Codec     seed.Codec // seed.CodecZstd shrinks the seed ~3-4x
	OutDir    string
	Limit     int64 // stop after this many urls routed (0 = all)
	SkipFiles int   // skip the first N index files (coarse resume)
	MaxFiles  int   // process at most this many index files (0 = all)
	Workers   int   // concurrent index files in flight
	Whole     bool  // download whole shard then read locally, instead of ranged column reads

	// Progress, if set, is called after each index file finishes with the running totals.
	Progress func(CCSeedProgress)
}

// CCSeedProgress is a running snapshot handed to the Progress callback.
type CCSeedProgress struct {
	FilesDone    int
	FilesTotal   int
	URLs         int64
	BytesFetched int64
	Elapsed      time.Duration
}

// CCSeedStats is the final outcome of a run.
type CCSeedStats struct {
	Crawl        string
	Files        int
	Scanned      int64
	Written      int64
	BytesFetched int64
	Shards       int
	SeedBytes    int64
	Elapsed      time.Duration
}

// BuildCCSeed pulls the columnar index of a crawl in parallel, projecting only the url
// column, routes every url into a sharded .seed under OutDir by its meguri hostkey, and
// writes the seed manifest. The hostkey and the path split use meguri's own frontier and
// hash, so a url keys the same here as it does when meguri ingests the seed: the seed this
// writes is byte-for-byte a seed `meguri seedpack` would write from the same urls.
//
// Concurrency is one worker per index file, up to opt.Workers. A url limit stops the run
// early by cancelling the remaining files once the count is reached, so a small ladder rung
// reads only the first few files. The seed's ShardSet Add is concurrency-safe, so the
// workers route into it directly with no per-worker merge.
func BuildCCSeed(ctx context.Context, h *HTTPClient, cache *Cache, src Source, opt CCSeedOptions) (CCSeedStats, error) {
	start := time.Now()
	crawlID, err := ResolveCrawl(ctx, h, cache, opt.Crawl)
	if err != nil {
		return CCSeedStats{}, err
	}
	urls, err := ColumnarParquetURLs(ctx, h, cache, crawlID, opt.Subset, src)
	if err != nil {
		return CCSeedStats{}, err
	}
	if opt.SkipFiles > 0 {
		if opt.SkipFiles >= len(urls) {
			return CCSeedStats{}, fmt.Errorf("skip-files %d >= %d available files", opt.SkipFiles, len(urls))
		}
		urls = urls[opt.SkipFiles:]
	}
	if opt.MaxFiles > 0 && opt.MaxFiles < len(urls) {
		urls = urls[:opt.MaxFiles]
	}
	if err := os.MkdirAll(opt.OutDir, 0o755); err != nil {
		return CCSeedStats{}, err
	}

	set, err := seed.NewShardSet(opt.OutDir, opt.Shards, opt.BlockSize, opt.Codec)
	if err != nil {
		return CCSeedStats{}, err
	}

	workers := opt.Workers
	if workers <= 0 {
		workers = 8
	}
	if workers > len(urls) {
		workers = len(urls)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		scanned   atomic.Int64
		written   atomic.Int64 // live global count, incremented per routed url
		fetched   atomic.Int64
		filesDone atomic.Int64
		firstErr  error
		errOnce   sync.Once
	)
	limitHit := func() bool { return opt.Limit > 0 && written.Load() >= opt.Limit }

	work := make(chan string)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for u := range work {
				if ctx.Err() != nil || limitHit() {
					continue
				}
				sc, bytes, ferr := ingestIndexFile(ctx, h, set, u, opt.Whole, opt.OutDir, &written, opt.Limit)
				scanned.Add(sc)
				fetched.Add(bytes)
				if ferr != nil && !errors.Is(ferr, context.Canceled) && !errors.Is(ferr, errSeedLimit) {
					errOnce.Do(func() { firstErr = fmt.Errorf("index file %s: %w", u, ferr) })
					cancel()
				}
				filesDone.Add(1)
				if opt.Progress != nil {
					opt.Progress(CCSeedProgress{
						FilesDone:    int(filesDone.Load()),
						FilesTotal:   len(urls),
						URLs:         written.Load(),
						BytesFetched: fetched.Load(),
						Elapsed:      time.Since(start),
					})
				}
				if limitHit() {
					cancel()
				}
			}
		})
	}
	for _, u := range urls {
		if ctx.Err() != nil || limitHit() {
			break
		}
		work <- u
	}
	close(work)
	wg.Wait()

	man, cerr := set.Close()
	if cerr != nil && firstErr == nil {
		firstErr = cerr
	}
	if firstErr != nil {
		return CCSeedStats{}, firstErr
	}

	var seedBytes int64
	if entries, derr := os.ReadDir(opt.OutDir); derr == nil {
		for _, e := range entries {
			if fi, ferr := e.Info(); ferr == nil {
				seedBytes += fi.Size()
			}
		}
	}
	return CCSeedStats{
		Crawl:        crawlID,
		Files:        int(filesDone.Load()),
		Scanned:      scanned.Load(),
		Written:      written.Load(),
		BytesFetched: fetched.Load(),
		Shards:       len(man.Shards),
		SeedBytes:    seedBytes,
		Elapsed:      time.Since(start),
	}, nil
}

// errSeedLimit unwinds a file's read loop once the global url limit is reached.
var errSeedLimit = errors.New("cc-seed url limit reached")

// ingestIndexFile reads one columnar index file's url column and routes every url into the
// shard set, incrementing the shared written counter per url so the limit is enforced live
// across all workers. It returns the rows scanned and the bytes pulled over the network. In
// ranged mode it opens the remote parquet through an httpReaderAt so only the url column
// chunks download; in whole mode it downloads the shard to a temp file first.
func ingestIndexFile(ctx context.Context, h *HTTPClient, set *seed.ShardSet, url string, whole bool, tmpDir string, written *atomic.Int64, limit int64) (scanned, bytes int64, err error) {
	route := func(u string) error {
		host := frontier.HostOf(u)
		if host == "" {
			return nil
		}
		if aerr := set.Add(mg.HostKeyOf(host), u); aerr != nil {
			return aerr
		}
		if n := written.Add(1); limit > 0 && n >= limit {
			return errSeedLimit
		}
		return nil
	}

	if whole {
		tmp, terr := os.CreateTemp(tmpDir, "cc-index-*.parquet")
		if terr != nil {
			return 0, 0, terr
		}
		tmpPath := tmp.Name()
		_ = tmp.Close()
		defer func() { _ = os.Remove(tmpPath) }()
		if derr := DownloadToFile(ctx, h, url, tmpPath); derr != nil {
			return 0, 0, derr
		}
		if fi, serr := os.Stat(tmpPath); serr == nil {
			bytes = fi.Size()
		}
		scanned, err = streamURLColumnLocal(tmpPath, route)
		return scanned, bytes, err
	}

	size, serr := h.ContentLength(ctx, url)
	if serr != nil {
		return 0, 0, serr
	}
	rat := newHTTPReaderAt(ctx, h, url, size, 0, 0)
	pf, perr := parquet.OpenFile(rat, size)
	if perr != nil {
		return 0, rat.BytesFetched(), fmt.Errorf("open parquet: %w", perr)
	}
	scanned, err = streamURLColumn(pf, route)
	return scanned, rat.BytesFetched(), err
}

// streamURLColumnLocal opens a local parquet file and streams its url column.
func streamURLColumnLocal(path string, route func(string) error) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		return 0, fmt.Errorf("open parquet: %w", err)
	}
	return streamURLColumn(pf, route)
}

// streamURLColumn reads the url column of an open parquet file in batches and routes each
// non-empty url. It returns the number of rows scanned.
func streamURLColumn(pf *parquet.File, route func(string) error) (int64, error) {
	reader := parquet.NewGenericReader[urlOnlyRow](pf)
	defer func() { _ = reader.Close() }()
	buf := make([]urlOnlyRow, 4096)
	var scanned int64
	for {
		n, err := reader.Read(buf)
		for i := range buf[:n] {
			scanned++
			if buf[i].URL == "" {
				continue
			}
			if rerr := route(buf[i].URL); rerr != nil {
				return scanned, rerr
			}
		}
		if errors.Is(err, io.EOF) {
			return scanned, nil
		}
		if err != nil {
			return scanned, err
		}
	}
}

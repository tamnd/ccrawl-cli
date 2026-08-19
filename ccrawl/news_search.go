package ccrawl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"

	"github.com/parquet-go/parquet-go"
)

// NewsIndexCoverage is what the published index says about one month.
//
// Found is false when the month has no row in the dataset's stats.csv, which is
// the honest answer to "is this month indexed" and the signal to fall back to a
// full scan of the archives. Files against TotalFiles is how far along a month
// that is indexed but still building has got: searching it answers from a real
// index, but from an index of part of the month, and a caller that reports a
// count needs to be able to say so.
type NewsIndexCoverage struct {
	Repo       string
	Month      string
	Found      bool
	Complete   bool
	Files      int
	TotalFiles int
	Rows       int64
}

// NewsIndexCoverageFor asks the published dataset what it holds for a month. It
// reads one small CSV over plain HTTPS, so it costs a single request and works
// without a HuggingFace token against a public dataset.
func NewsIndexCoverageFor(ctx context.Context, h *HTTPClient, repo string, year, month int) (NewsIndexCoverage, error) {
	label := newsMonthLabel(year, month)
	cov := NewsIndexCoverage{Repo: repo, Month: label}
	data, err := h.FetchBytes(ctx, hfResolveURL(repo, "stats.csv"))
	if err != nil {
		// No ledger is not an error worth failing on: it means there is nothing
		// published to search, and the caller has a slower way to get an answer.
		return cov, nil
	}
	rows, err := DecodeNewsStats(data)
	if err != nil {
		return cov, nil
	}
	for _, r := range rows {
		if r.Month != label {
			continue
		}
		cov.Found = r.Files > 0
		cov.Complete = r.Complete
		cov.Files = r.Files
		cov.TotalFiles = r.TotalFiles
		cov.Rows = r.Rows
		break
	}
	return cov, nil
}

// NewsSearchOptions configures a search of the published CC-NEWS index.
type NewsSearchOptions struct {
	Repo    string
	Year    int
	Month   int
	Host    string // matched as a substring of url_host_name, lower case
	Limit   int    // stop after this many hits, 0 for no limit
	Workers int
}

// errNewsSearchDone unwinds the worker pool once the limit is reached.
var errNewsSearchDone = errors.New("news search: limit reached")

// SearchNewsIndex answers a host query out of the published index instead of by
// reading the archives.
//
// The whole reason the index exists is the difference in cost. A month of
// CC-NEWS is several hundred gigabytes of WARC and there is no index published
// with it, so the only way to find one publisher's articles is to decompress all
// of it. The same question against these shards reads one small column out of
// each and then only opens the shards that had a match.
//
// Shards are named for the WARC they index, so the month's own warc.paths
// manifest names every shard that could exist. Which of them do exist comes from
// one batched question to the hub rather than from letting each worker discover
// a 404, because a month that is half published is 176 pointless round trips and
// a month that is fully published still pays a size lookup per shard. The answer
// carries the sizes too, which is what a Parquet reader needs before it can open
// a footer, so the shards that do exist start reading immediately.
//
// emit is called from one goroutine at a time.
func SearchNewsIndex(ctx context.Context, h *HTTPClient, o NewsSearchOptions, emit func(NewsRow) error) (int64, error) {
	files, err := ListNewsFiles(ctx, h, o.Year, o.Month)
	if err != nil {
		return 0, err
	}
	if len(files) == 0 {
		return 0, fmt.Errorf("no CC-NEWS files published for %04d/%02d", o.Year, o.Month)
	}
	candidates := make([]string, 0, len(files))
	for _, f := range files {
		candidates = append(candidates, newsShardPath(f.Path))
	}
	// A public dataset answers this without a token, so a search never asks for
	// one. It still picks up an ambient HF_TOKEN, which is what lets someone
	// search an index they published privately.
	sizes, err := NewHFClient("").PathsInfo(ctx, o.Repo, candidates)
	if err != nil {
		return 0, fmt.Errorf("list the published shards for %04d/%02d: %w", o.Year, o.Month, err)
	}
	shards := make([]string, 0, len(sizes))
	for _, p := range candidates {
		if sizes[p] > 0 {
			shards = append(shards, p)
		}
	}
	if len(shards) == 0 {
		return 0, nil
	}
	workers := o.Workers
	if workers <= 0 {
		workers = budgetProcess(0)
	}
	host := strings.ToLower(o.Host)

	var (
		mu      sync.Mutex
		hits    int64
		emitErr error
	)
	// keep is called with each matching row and owns the shared output and the
	// limit, so workers never need to agree about either.
	keep := func(row NewsRow) error {
		mu.Lock()
		defer mu.Unlock()
		if o.Limit > 0 && hits >= int64(o.Limit) {
			return errNewsSearchDone
		}
		if err := emit(row); err != nil {
			emitErr = err
			return err
		}
		hits++
		if o.Limit > 0 && hits >= int64(o.Limit) {
			return errNewsSearchDone
		}
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
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
				if err := searchNewsShard(ctx, h, o.Repo, p, sizes[p], host, keep); err != nil {
					if errors.Is(err, errNewsSearchDone) || emitErr != nil {
						cancel()
						return
					}
					// A shard that is missing or unreadable is not a failed
					// search. The month is only as complete as stats.csv said it
					// was, and the caller has already been told that.
					continue
				}
			}
		}()
	}
	for _, p := range shards {
		select {
		case jobs <- p:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
	}
	close(jobs)
	wg.Wait()

	if emitErr != nil {
		return hits, emitErr
	}
	return hits, nil
}

// newsShardPath is the repo path of the shard indexing one source WARC.
func newsShardPath(warcPath string) string {
	base := strings.TrimSuffix(path.Base(warcPath), ".warc.gz")
	// crawl-data/CC-NEWS/2026/07/CC-NEWS-...warc.gz -> data/2026/07/CC-NEWS-...parquet
	dir := path.Dir(warcPath)
	mon := path.Base(dir)
	year := path.Base(path.Dir(dir))
	return fmt.Sprintf("data/%s/%s/%s.parquet", year, mon, base)
}

// newsHostProbe is the projection used to test a shard. Reading one column out
// of a Parquet file reads that column's pages and nothing else, so a shard with
// no matching host costs a footer and a few hundred kilobytes rather than the
// whole file.
type newsHostProbe struct {
	URLHostName string `parquet:"url_host_name"`
}

// searchNewsShard reads one published shard and passes matching rows to keep.
// size is what the hub reported for the shard; a caller that does not know it
// passes zero and pays a round trip to ask.
func searchNewsShard(ctx context.Context, h *HTTPClient, repo, repoPath string, size int64, host string, keep func(NewsRow) error) error {
	url := hfResolveURL(repo, repoPath)
	if size <= 0 {
		var err error
		if size, err = h.ContentLength(ctx, url); err != nil {
			return err
		}
	}
	ra := newHTTPReaderAt(ctx, h, url, size, 4<<20, 4)
	pf, err := parquet.OpenFile(ra, size)
	if err != nil {
		return err
	}

	matched, err := newsShardHasHost(pf, host)
	if err != nil || !matched {
		return err
	}

	r := parquet.NewGenericReader[NewsRow](pf)
	defer func() { _ = r.Close() }()
	buf := make([]NewsRow, 512)
	for {
		n, rerr := r.Read(buf)
		for i := 0; i < n; i++ {
			if !strings.Contains(strings.ToLower(buf[i].URLHostName), host) {
				continue
			}
			if err := keep(buf[i]); err != nil {
				return err
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

// newsShardHasHost reports whether any row in the shard could match, reading the
// host column alone. Most shards in a month hold nothing for a given publisher,
// so this is the check that makes an index search cheap.
func newsShardHasHost(pf *parquet.File, host string) (bool, error) {
	r := parquet.NewGenericReader[newsHostProbe](pf)
	defer func() { _ = r.Close() }()
	buf := make([]newsHostProbe, 4096)
	for {
		n, rerr := r.Read(buf)
		for i := 0; i < n; i++ {
			if strings.Contains(strings.ToLower(buf[i].URLHostName), host) {
				return true, nil
			}
		}
		if errors.Is(rerr, io.EOF) {
			return false, nil
		}
		if rerr != nil {
			return false, rerr
		}
	}
}

package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/any-cli/kit/errs"
	"github.com/tamnd/ccrawl-cli/ccrawl"
	"golang.org/x/sync/errgroup"
)

// newsEscapeHatches returns the news verbs that do not emit a record stream
// (a bulk download and a streamed scan), so they attach under the news parent
// next to the list operation. The list verb is a kit operation (registerNewsList).
func newsEscapeHatches() []kit.Command {
	return []kit.Command{newNewsDownloadCmd(), newNewsSearchCmd(), newNewsPublishCmd()}
}

// defaultNewsRepo is the target dataset for the CC-NEWS index, and the index a
// search reads before falling back to a scan. CCRAWL_NEWS_REPO overrides it.
const defaultNewsRepo = "open-index/ccrawl-news"

// newsDownloadCmd holds the flags for the news download command.
type newsDownloadCmd struct {
	year, month int
	outDir      string
}

func newNewsDownloadCmd() kit.Command {
	n := &newsDownloadCmd{}
	return kit.Command{
		Use:   "download",
		Short: "Download CC-NEWS WARC files",
		Flags: n.flags,
		Run:   n.run,
	}
}

func (n *newsDownloadCmd) flags(f *kit.FlagSet) {
	f.IntVar(&n.year, "year", 0, "year")
	f.IntVar(&n.month, "month", 0, "month")
	f.StringVar(&n.outDir, "out", "", "output directory")
}

func (n *newsDownloadCmd) run(ctx context.Context, _ []string) error {
	app := appFromCtx(ctx)
	files, err := ccrawl.ListNewsFiles(ctx, app.HTTP, n.year, n.month)
	if err != nil {
		return err
	}
	var paths []string
	for _, f := range files {
		paths = append(paths, f.Path)
	}
	paths = filterPaths(paths, "", 0, app.Limit)
	if len(paths) == 0 {
		return noResults("no CC-NEWS files to download")
	}
	outDir := n.outDir
	if outDir == "" {
		outDir = app.Cfg.RawDir() + "/news"
	}
	var done int64
	progress := func(r ccrawl.DownloadResult) {
		i := atomic.AddInt64(&done, 1)
		if r.Err != nil {
			_, _ = fmt.Fprintf(cmdErr, "[%d/%d] FAIL %s: %v\n", i, len(paths), r.Path, r.Err)
			return
		}
		_, _ = fmt.Fprintf(cmdErr, "[%d/%d] %s (%s)\n", i, len(paths), r.LocalPath, humanBytes(r.Bytes))
	}
	return ccrawl.DownloadFiles(ctx, app.HTTP, app.Cfg.Source, paths, outDir, app.Workers, true, progress)
}

// newsSearchCmd holds the flags for the news search command.
type newsSearchCmd struct {
	year, month int
	repo        string
	noIndex     bool
}

func newNewsSearchCmd() kit.Command {
	n := &newsSearchCmd{}
	return kit.Command{
		Use:   "search <host>",
		Short: "Find CC-NEWS articles for a host, from the published index where there is one",
		Long: `Common Crawl publishes no index for CC-NEWS, so the only way to find a
publisher's articles used to be to stream every WARC of the month, which is a few
hundred gigabytes for one question.

This looks for a published index for the month first and answers from it when
there is one, which takes seconds. When there is no index for that month it falls
back to streaming the archives, which takes hours. Both paths emit the same rows,
so either answer pipes into "ccrawl fetch -" to pull the articles themselves.

  ccrawl news search bbc.co.uk --year 2026 --month 7
  ccrawl news search elpais.com --year 2026 --month 7 -o jsonl | ccrawl fetch - --text
  ccrawl news search bbc.co.uk --year 2026 --month 7 --no-index   # scan, ignore the index`,
		Args:  kit.ExactArgs(1),
		Flags: n.flags,
		Run:   n.run,
	}
}

func (n *newsSearchCmd) flags(f *kit.FlagSet) {
	f.IntVar(&n.year, "year", 0, "year")
	f.IntVar(&n.month, "month", 0, "month")
	f.StringVar(&n.repo, "repo", setting("news_repo", defaultNewsRepo), "published index to search (org/name on HuggingFace)")
	f.BoolVar(&n.noIndex, "no-index", false, "ignore the published index and scan the archives")
}

func (n *newsSearchCmd) run(ctx context.Context, args []string) error {
	return runNewsSearch(ctx, appFromCtx(ctx), args[0], n.year, n.month, n.repo, n.noIndex)
}

func runNewsSearch(ctx context.Context, app *App, host string, year, month int, repo string, noIndex bool) error {
	host = strings.ToLower(host)

	// The index only covers one month at a time, since that is how the dataset is
	// partitioned and how the ledger reports coverage. A query that has not named
	// a month cannot be answered from it.
	if !noIndex && repo != "" && year > 0 && month > 0 {
		cov, err := ccrawl.NewsIndexCoverageFor(ctx, app.HTTP, repo, year, month)
		if err != nil {
			return err
		}
		if cov.Found {
			if !cov.Complete {
				_, _ = fmt.Fprintf(cmdErr, "warning: %s is %d of %d files indexed, so this answer covers part of the month\n", cov.Month, cov.Files, cov.TotalFiles)
			}
			return searchNewsIndexed(ctx, app, host, year, month, repo)
		}
		_, _ = fmt.Fprintf(cmdErr, "no published index for %04d-%02d in %s, scanning the archives instead\n", year, month, repo)
	}
	return scanNewsArchives(ctx, app, host, year, month)
}

// searchNewsIndexed answers the query out of the published Parquet index.
func searchNewsIndexed(ctx context.Context, app *App, host string, year, month int, repo string) error {
	hits, err := ccrawl.SearchNewsIndex(ctx, app.HTTP, ccrawl.NewsSearchOptions{
		Repo:    repo,
		Year:    year,
		Month:   month,
		Host:    host,
		Limit:   app.Limit,
		Workers: app.Workers,
	}, func(r ccrawl.NewsRow) error { return app.Out.Emit(newsRow(r)) })
	if ferr := app.Out.Flush(); ferr != nil && err == nil {
		err = ferr
	}
	if err != nil {
		return err
	}
	if hits == 0 {
		return noResults("no matching news records")
	}
	return nil
}

// scanNewsArchives is the fallback for a month with no published index: stream
// every WARC and keep the responses whose host matches.
//
// It records each record's byte span as it goes, so its rows carry the same
// location triple the index does and pipe into fetch the same way. That costs
// nothing here, because the offsets fall out of the reading the scan is doing
// anyway.
func scanNewsArchives(ctx context.Context, app *App, host string, year, month int) error {
	files, err := ccrawl.ListNewsFiles(ctx, app.HTTP, year, month)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return noResults("no CC-NEWS files for that period")
	}

	var mu sync.Mutex
	var hits int64
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(app.Workers)
	for _, f := range files {
		g.Go(func() error {
			resp, err := app.HTTP.GetDownload(ctx, app.HTTP.DataURL(f.Path))
			if err != nil {
				return nil
			}
			defer func() { _ = resp.Body.Close() }()
			return ccrawl.IterateWARCFrom(resp.Body, 0, func(rec ccrawl.WARCRecord) error {
				if rec.Header.Type != "response" {
					return nil
				}
				if !strings.Contains(strings.ToLower(ccrawl.HostOf(rec.Header.TargetURI)), host) {
					return nil
				}
				row, ok := ccrawl.NewsIndexRow(f.Path, rec)
				if !ok {
					return nil
				}
				mu.Lock()
				err := app.Out.Emit(newsRow(row))
				mu.Unlock()
				atomic.AddInt64(&hits, 1)
				if app.Limit > 0 && atomic.LoadInt64(&hits) >= int64(app.Limit) {
					return errStopNews
				}
				return err
			})
		})
	}
	gerr := g.Wait()
	if ferr := app.Out.Flush(); ferr != nil && gerr == nil {
		gerr = ferr
	}
	if gerr != nil && gerr != errStopNews {
		return gerr
	}
	if atomic.LoadInt64(&hits) == 0 {
		return noResults("no matching news records")
	}
	return nil
}

var errStopNews = fmt.Errorf("stop")

// newsPublishCmd holds the flags for the news publish command.
type newsPublishCmd struct {
	repo        string
	months      string
	files       int
	commitEvery int
	workers     int
	private     bool
	keep        bool
	minFreeGB   int
	maxStall    time.Duration
	noPush      bool
}

func newNewsPublishCmd() kit.Command {
	n := &newsPublishCmd{}
	return kit.Command{
		Use:   "publish",
		Short: "Build the CC-NEWS index and publish it to a HuggingFace dataset",
		Long: `Common Crawl publishes no index for CC-NEWS. This builds one: it streams every
WARC file of the selected months, records the byte span of each stored response,
and writes one Parquet shard per source file with the URL, host, fetch time,
status, content type, and identified language of every article.

The archives are never written to disk. They are decompressed, indexed, and
dropped as they stream, so a run holds one output shard per worker and nothing
else. A stream that dies partway resumes at the last complete record rather than
restarting the file.

Reading a month is the cost of this dataset: around 350 files of roughly a
gigabyte each, so a few hundred gigabytes of transfer per month, once. Use
--files to index the first N files of a month when proving a setup.

The run is idempotent from remote truth. Shards already on the hub are skipped,
so a killed run resumes cleanly. HF_TOKEN (or HUGGINGFACE_TOKEN) must be set to
push. Examples:

  ccrawl news publish --months 2026/07
  ccrawl news publish --months 2026/07,2026/06 --commit-every 16
  ccrawl news publish --months 2026/07 --files 4 --no-push   # index a slice, upload nothing`,
		Args:  kit.NoArgs,
		Flags: n.flags,
		Run:   n.run,
	}
}

func (n *newsPublishCmd) flags(f *kit.FlagSet) {
	f.StringVar(&n.repo, "repo", setting("news_repo", defaultNewsRepo), "dataset repo on HuggingFace (org/name)")
	f.StringVar(&n.months, "months", "", "months to index, YYYY/MM, comma separated")
	f.IntVar(&n.files, "files", 0, "index only the first N WARC files of each month (0 indexes the month)")
	f.IntVar(&n.commitEvery, "commit-every", 8, "shards per HuggingFace commit")
	f.IntVar(&n.workers, "workers", 0, "workers streaming and indexing (0 picks a default from CPU count)")
	f.BoolVar(&n.private, "private", false, "create the dataset repo private")
	f.BoolVar(&n.keep, "keep", false, "keep local shards after commit instead of deleting them")
	f.IntVar(&n.minFreeGB, "min-free-gb", ccrawl.DefaultMinFreeGB, "pause new downloads when free disk is under this many GB")
	f.DurationVar(&n.maxStall, "max-stall", ccrawl.DefaultMaxStall, "restart the run (exit 75) after this long with no progress")
	f.BoolVar(&n.noPush, "no-push", false, "index and stage but skip the upload")
}

func (n *newsPublishCmd) run(ctx context.Context, args []string) error {
	app := appFromCtx(ctx)
	if n.repo == "" {
		return usageErr("name the dataset with --repo, or set CCRAWL_NEWS_REPO")
	}
	months := splitList(n.months)
	if len(months) == 0 {
		return usageErr("name the months to index with --months, for example --months 2026/07")
	}

	push := !n.noPush && !app.dryRun
	hf := ccrawl.NewHFClient("")
	if push && !hf.Valid() {
		return errs.New(errs.KindNeedAuth, "no HuggingFace token; set HF_TOKEN (or HUGGINGFACE_TOKEN), or pass --no-push")
	}

	stageDir := filepath.Join(app.Cfg.DataDir, "publish", "news")
	if push {
		_, _ = fmt.Fprintf(cmdErr, "publishing the CC-NEWS index for %d month(s) to https://huggingface.co/datasets/%s\n", len(months), n.repo)
	} else {
		_, _ = fmt.Fprintf(cmdErr, "staging the CC-NEWS index for %d month(s) under %s (no push)\n", len(months), stageDir)
	}

	err := ccrawl.PublishNews(ctx, app.HTTP, hf, ccrawl.NewsPublishOptions{
		Repo:        n.repo,
		Months:      months,
		StageDir:    stageDir,
		CommitEvery: n.commitEvery,
		Workers:     n.workers,
		Files:       n.files,
		Private:     n.private,
		Keep:        n.keep,
		DoCommit:    push,
		MinFreeGB:   n.minFreeGB,
		MaxStall:    n.maxStall,
		Logf:        func(f string, a ...any) { _, _ = fmt.Fprintf(cmdErr, f+"\n", a...) },
	})
	if errors.Is(err, ccrawl.ErrCommitStall) || errors.Is(err, ccrawl.ErrIncomplete) {
		// The kit framework owns exit codes 0 to 8, so signal a temp-fail restart
		// to the supervisor directly. A stall and a still-incomplete month both
		// want the same remote-truth resume on the next run.
		_, _ = fmt.Fprintln(cmdErr, "exiting 75 for supervised restart")
		os.Exit(75)
	}
	if err != nil {
		return err
	}
	if push {
		_, _ = fmt.Fprintf(cmdErr, "dataset: https://huggingface.co/datasets/%s\n", n.repo)
	}
	return nil
}

// splitList reads a comma separated flag value, dropping empty entries so a
// trailing comma or a stray space is not a mysterious failure later on.
func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

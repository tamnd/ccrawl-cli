package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/any-cli/kit/errs"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// defaultURLsRepo is the target dataset for the URL index. CCRAWL_URLS_REPO
// overrides it.
const defaultURLsRepo = "open-index/ccrawl-urls"

// registerURLs attaches the `urls` command group.
func registerURLs(app *kit.App) {
	app.CommandGroup("urls", "Publish the Common Crawl URL index to HuggingFace")
	app.AddCommandUnder("urls", newURLsPublishCmd())
	app.AddCommandUnder("urls", newURLsRecountCmd())
}

type urlsPublishCmd struct {
	repo        string
	commitEvery int
	workers     int
	whole       bool
	private     bool
	keep        bool
	minFreeGB   int
	maxStall    time.Duration
	noPush      bool
}

func newURLsPublishCmd() kit.Command {
	v := &urlsPublishCmd{}
	return kit.Command{
		Use:   "publish",
		Short: "Mirror the Common Crawl URL index to a HuggingFace dataset, shard for shard",
		Long: `Download the Common Crawl columnar URL index and republish it to a HuggingFace
dataset, one output Parquet shard per original source part. Nothing is
aggregated, deduplicated, or filtered: the rows and their order match the
source, projected down to the URL-level columns.

The run is idempotent from remote truth. Shards already on the hub are skipped,
so a killed run resumes cleanly, and each local shard is deleted right after it
commits to keep disk flat. Pick the crawls with the global -c flag: a single id,
a year, the newest N, "all", or a comma-separated list.

HF_TOKEN (or HUGGINGFACE_TOKEN) must be set to push. Examples:

  ccrawl urls publish -c CC-MAIN-2026-25
  ccrawl urls publish -c 2 --commit-every 32
  ccrawl urls publish -c CC-MAIN-2026-25 --no-push   # scan and report, upload nothing`,
		Args:  kit.NoArgs,
		Flags: v.flags,
		Run:   v.run,
	}
}

func (v *urlsPublishCmd) flags(f *kit.FlagSet) {
	f.StringVar(&v.repo, "repo", envOr("CCRAWL_URLS_REPO", defaultURLsRepo), "HuggingFace dataset repo (org/name)")
	f.IntVar(&v.commitEvery, "commit-every", 16, "shards per HuggingFace commit")
	f.IntVar(&v.workers, "workers", 0, "download-and-convert workers (0 picks a default from CPU count)")
	f.BoolVar(&v.whole, "whole", false, "download each part whole before reading (fallback for range-hostile mirrors)")
	f.BoolVar(&v.private, "private", false, "create the dataset repo private")
	f.BoolVar(&v.keep, "keep", false, "keep local shards after commit instead of deleting them")
	f.IntVar(&v.minFreeGB, "min-free-gb", ccrawl.DefaultMinFreeGB, "pause new downloads when free disk is under this many GB")
	f.DurationVar(&v.maxStall, "max-stall", ccrawl.DefaultMaxStall, "restart the run (exit 75) after this long with no progress")
	f.BoolVar(&v.noPush, "no-push", false, "scan and stage but skip the upload")
}

func (v *urlsPublishCmd) run(ctx context.Context, args []string) error {
	app := appFromCtx(ctx)
	if v.repo == "" {
		return usageErr("--repo is required (or set CCRAWL_URLS_REPO)")
	}

	crawls, err := app.AllCrawls(ctx)
	if err != nil {
		return err
	}
	if len(crawls) == 0 {
		return noResults("no crawls resolved from -c")
	}

	push := !v.noPush && !app.dryRun
	hf := ccrawl.NewHFClient("")
	if push && !hf.Valid() {
		return errs.New(errs.KindNeedAuth, "HF_TOKEN (or HUGGINGFACE_TOKEN) is not set; set it or pass --no-push")
	}

	stageDir := filepath.Join(app.Cfg.DataDir, "publish", "urls")
	if push {
		fmt.Fprintf(os.Stderr, "publishing URL index for %d crawl(s) to https://huggingface.co/datasets/%s\n", len(crawls), v.repo)
	} else {
		fmt.Fprintf(os.Stderr, "staging URL index for %d crawl(s) under %s (no push)\n", len(crawls), stageDir)
	}

	err = ccrawl.PublishURLs(ctx, app.HTTP, app.Cache, hf, ccrawl.URLPublishOptions{
		Repo:        v.repo,
		CrawlIDs:    crawls,
		Source:      app.Cfg.Source,
		StageDir:    stageDir,
		CommitEvery: v.commitEvery,
		Workers:     v.workers,
		Whole:       v.whole,
		Private:     v.private,
		Keep:        v.keep,
		DoCommit:    push,
		MinFreeGB:   v.minFreeGB,
		MaxStall:    v.maxStall,
		Logf:        func(f string, a ...any) { fmt.Fprintf(os.Stderr, f+"\n", a...) },
	})
	if errors.Is(err, ccrawl.ErrCommitStall) || errors.Is(err, ccrawl.ErrIncomplete) {
		// The kit framework owns exit codes 0 to 8, so signal a temp-fail
		// restart to the supervisor directly. A stall or a still-incomplete crawl
		// both want the same remote-truth resume on the next run.
		fmt.Fprintln(os.Stderr, "exiting 75 for supervised restart")
		os.Exit(75)
	}
	if err != nil {
		return err
	}
	if push {
		fmt.Fprintf(os.Stderr, "dataset: https://huggingface.co/datasets/%s\n", v.repo)
	}
	return nil
}

type urlsRecountCmd struct {
	repo    string
	workers int
	noPush  bool
}

func newURLsRecountCmd() kit.Command {
	v := &urlsRecountCmd{}
	return kit.Command{
		Use:   "recount",
		Short: "Recompute a crawl's URL and byte totals from the shards already on the hub",
		Long: `Recount reads the footer of every published shard for the selected crawls and
rewrites the ledger row and dataset card with exact URL and byte totals. It is a
repair tool: a normal run keeps the totals current as it publishes, but totals
can drift when shards were committed before any ledger existed on the hub, for
example the first batches of a crawl. Only stats.csv and README.md are rewritten;
the shard files are never touched.

Footers are fetched with small range requests, so this is cheap even over a full
crawl. Pick the crawls with the global -c flag.

  ccrawl urls recount -c CC-MAIN-2026-25
  ccrawl urls recount -c CC-MAIN-2026-25 --no-push   # report the totals, commit nothing`,
		Args: kit.NoArgs,
		Flags: func(f *kit.FlagSet) {
			f.StringVar(&v.repo, "repo", envOr("CCRAWL_URLS_REPO", defaultURLsRepo), "HuggingFace dataset repo (org/name)")
			f.IntVar(&v.workers, "workers", 0, "footer-read workers (0 picks a default from CPU count)")
			f.BoolVar(&v.noPush, "no-push", false, "read and report totals but skip the commit")
		},
		Run: v.run,
	}
}

func (v *urlsRecountCmd) run(ctx context.Context, args []string) error {
	app := appFromCtx(ctx)
	if v.repo == "" {
		return usageErr("--repo is required (or set CCRAWL_URLS_REPO)")
	}

	crawls, err := app.AllCrawls(ctx)
	if err != nil {
		return err
	}
	if len(crawls) == 0 {
		return noResults("no crawls resolved from -c")
	}

	push := !v.noPush && !app.dryRun
	hf := ccrawl.NewHFClient("")
	if push && !hf.Valid() {
		return errs.New(errs.KindNeedAuth, "HF_TOKEN (or HUGGINGFACE_TOKEN) is not set; set it or pass --no-push")
	}

	stageDir := filepath.Join(app.Cfg.DataDir, "publish", "urls")
	return ccrawl.RecountURLs(ctx, app.HTTP, app.Cache, hf, ccrawl.URLPublishOptions{
		Repo:     v.repo,
		CrawlIDs: crawls,
		Source:   app.Cfg.Source,
		StageDir: stageDir,
		Workers:  v.workers,
		DoCommit: push,
		Logf:     func(f string, a ...any) { fmt.Fprintf(os.Stderr, f+"\n", a...) },
	})
}

// envOr returns the environment value for key, or def when it is unset or empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

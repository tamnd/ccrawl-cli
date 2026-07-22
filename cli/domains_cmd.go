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

// defaultDomainsRepo is the target dataset for the domain ranks. CCRAWL_DOMAINS_REPO
// overrides it.
const defaultDomainsRepo = "open-index/ccrawl-domains"

// registerDomains attaches the `domains` command group.
func registerDomains(app *kit.App) {
	app.CommandGroup("domains", "Publish the Common Crawl domain ranks to HuggingFace")
	app.AddCommandUnder("domains", newDomainsPublishCmd())
}

type domainsPublishCmd struct {
	repo        string
	graph       string
	shardRows   int
	commitEvery int
	private     bool
	keep        bool
	minFreeGB   int
	maxStall    time.Duration
	noPush      bool
}

func newDomainsPublishCmd() kit.Command {
	v := &domainsPublishCmd{}
	return kit.Command{
		Use:   "publish",
		Short: "Mirror the Common Crawl domain ranks to a HuggingFace dataset",
		Long: `Download the Common Crawl domain-level web-graph ranks and republish them to a
HuggingFace dataset. The source is one gzipped table per release, pre-sorted by
harmonic centrality, so it streams top to bottom into fixed-size Parquet shards
with the rank order preserved: part-000 holds the top-ranked domains. The only
change to the data is un-reversing the source's host key into a plain domain.

The run is idempotent: a release already on the hub is left alone, and each
local shard is deleted right after it commits. By default it publishes the
latest release; pass --graph to pick one.

HF_TOKEN (or HUGGINGFACE_TOKEN) must be set to push. Examples:

  ccrawl domains publish
  ccrawl domains publish --graph cc-main-2026-mar-apr-may --shard-rows 2000000
  ccrawl domains publish --no-push   # scan and report, upload nothing`,
		Args:  kit.NoArgs,
		Flags: v.flags,
		Run:   v.run,
	}
}

func (v *domainsPublishCmd) flags(f *kit.FlagSet) {
	f.StringVar(&v.repo, "repo", envOr("CCRAWL_DOMAINS_REPO", defaultDomainsRepo), "HuggingFace dataset repo (org/name)")
	f.StringVar(&v.graph, "graph", "", "web-graph release id (default: the latest)")
	f.IntVar(&v.shardRows, "shard-rows", ccrawl.DefaultShardRows, "rows per output shard")
	f.IntVar(&v.commitEvery, "commit-every", 4, "shards per HuggingFace commit")
	f.BoolVar(&v.private, "private", false, "create the dataset repo private")
	f.BoolVar(&v.keep, "keep", false, "keep local shards after commit instead of deleting them")
	f.IntVar(&v.minFreeGB, "min-free-gb", ccrawl.DefaultMinFreeGB, "pause new downloads when free disk is under this many GB")
	f.DurationVar(&v.maxStall, "max-stall", ccrawl.DefaultMaxStall, "restart the run (exit 75) after this long with no progress")
	f.BoolVar(&v.noPush, "no-push", false, "scan and stage but skip the upload")
}

func (v *domainsPublishCmd) run(ctx context.Context, args []string) error {
	app := appFromCtx(ctx)
	if v.repo == "" {
		return usageErr("--repo is required (or set CCRAWL_DOMAINS_REPO)")
	}

	var graph ccrawl.WebGraph
	if v.graph != "" {
		graph = ccrawl.WebGraph{ID: v.graph, BaseURL: ccrawl.WebGraphBaseURL(v.graph)}
	} else {
		g, err := ccrawl.LatestWebGraph(ctx, app.HTTP, app.Cache)
		if err != nil {
			return fmt.Errorf("resolve latest web graph (pass --graph): %w", err)
		}
		graph = g
	}

	push := !v.noPush && !app.dryRun
	hf := ccrawl.NewHFClient("")
	if push && !hf.Valid() {
		return errs.New(errs.KindNeedAuth, "HF_TOKEN (or HUGGINGFACE_TOKEN) is not set; set it or pass --no-push")
	}

	stageDir := filepath.Join(app.Cfg.DataDir, "publish", "domains")
	if push {
		fmt.Fprintf(os.Stderr, "publishing domain ranks for %s to https://huggingface.co/datasets/%s\n", graph.ID, v.repo)
	} else {
		fmt.Fprintf(os.Stderr, "staging domain ranks for %s under %s (no push)\n", graph.ID, stageDir)
	}

	err := ccrawl.PublishDomains(ctx, app.HTTP, hf, ccrawl.DomainPublishOptions{
		Repo:        v.repo,
		Graph:       graph,
		ShardRows:   v.shardRows,
		StageDir:    stageDir,
		CommitEvery: v.commitEvery,
		Private:     v.private,
		Keep:        v.keep,
		DoCommit:    push,
		MinFreeGB:   v.minFreeGB,
		MaxStall:    v.maxStall,
		Logf:        func(f string, a ...any) { fmt.Fprintf(os.Stderr, f+"\n", a...) },
	})
	if errors.Is(err, ccrawl.ErrCommitStall) {
		fmt.Fprintln(os.Stderr, "commit stall: exiting 75 for supervised restart")
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

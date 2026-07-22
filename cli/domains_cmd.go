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
	app.AddCommandUnder("domains", newDomainsRecountCmd())
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

type domainsRecountCmd struct {
	repo    string
	graph   string
	workers int
	noPush  bool
}

func newDomainsRecountCmd() kit.Command {
	v := &domainsRecountCmd{}
	return kit.Command{
		Use:   "recount",
		Short: "Recompute a release's domain and byte totals from the shards already on the hub",
		Long: `Recount reads the footer of every published shard for a web-graph release and
rewrites the ledger row and dataset card with exact domain and byte totals. It is
a repair tool: a normal run keeps the totals current as it publishes, but totals
can drift when shards were committed before any ledger existed on the hub, for
example the first batches of a release. Only stats.csv and README.md are
rewritten; the shard files are never touched.

Footers are fetched with small range requests, so this is cheap even over a whole
release. By default it recounts the latest release; pass --graph to pick one.

  ccrawl domains recount
  ccrawl domains recount --graph cc-main-2026-mar-apr-may --no-push   # report only`,
		Args: kit.NoArgs,
		Flags: func(f *kit.FlagSet) {
			f.StringVar(&v.repo, "repo", envOr("CCRAWL_DOMAINS_REPO", defaultDomainsRepo), "HuggingFace dataset repo (org/name)")
			f.StringVar(&v.graph, "graph", "", "web-graph release id (default: the latest)")
			f.IntVar(&v.workers, "workers", 0, "footer-read workers (0 picks a default from CPU count)")
			f.BoolVar(&v.noPush, "no-push", false, "read and report totals but skip the commit")
		},
		Run: v.run,
	}
}

func (v *domainsRecountCmd) run(ctx context.Context, args []string) error {
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
	return ccrawl.RecountDomainGraph(ctx, app.HTTP, hf, ccrawl.DomainPublishOptions{
		Repo:     v.repo,
		Graph:    graph,
		StageDir: stageDir,
		Workers:  v.workers,
		DoCommit: push,
		Logf:     func(f string, a ...any) { fmt.Fprintf(os.Stderr, f+"\n", a...) },
	})
}

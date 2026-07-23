package cli

import (
	"bufio"
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
	app.AddCommandUnder("domains", newDomainsDiffCmd())
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
local shard is deleted right after it commits. By default it discovers every
published web-graph release and publishes the most recent one that actually has
a domain-ranks table, since a release is listed before its domain ranks land;
pass --graph to pick one.

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
	f.StringVar(&v.graph, "graph", "", "web-graph release id (default: the latest with a domain-ranks table)")
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
		g, err := ccrawl.LatestDomainWebGraph(ctx, app.HTTP, app.Cache)
		if err != nil {
			return fmt.Errorf("resolve latest web graph with domain ranks (pass --graph): %w", err)
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
release. By default it recounts the latest release that has a domain-ranks
table; pass --graph to pick one.

  ccrawl domains recount
  ccrawl domains recount --graph cc-main-2026-mar-apr-may --no-push   # report only`,
		Args: kit.NoArgs,
		Flags: func(f *kit.FlagSet) {
			f.StringVar(&v.repo, "repo", envOr("CCRAWL_DOMAINS_REPO", defaultDomainsRepo), "HuggingFace dataset repo (org/name)")
			f.StringVar(&v.graph, "graph", "", "web-graph release id (default: the latest with a domain-ranks table)")
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
		g, err := ccrawl.LatestDomainWebGraph(ctx, app.HTTP, app.Cache)
		if err != nil {
			return fmt.Errorf("resolve latest web graph with domain ranks (pass --graph): %w", err)
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

type domainsDiffCmd struct {
	repo     string
	from     string
	to       string
	workers  int
	addedOut string
}

func newDomainsDiffCmd() kit.Command {
	v := &domainsDiffCmd{}
	return kit.Command{
		Use:   "diff",
		Short: "Count domains added, removed, and shared between two published releases",
		Long: `Diff compares two web-graph domain releases already published to the dataset
and reports how many domains are new in the later release, how many dropped out
of the earlier one, and how many the two share. It reads only the domain column
of each shard straight from the hub, so it never downloads the rank fields.

With no ids it diffs the two most recent complete releases in the dataset, older
against newer. Pass --from and --to to pick releases explicitly. Use --added-out
to also write the domains that are new in the later release, one per line.

  ccrawl domains diff
  ccrawl domains diff --from cc-main-2026-mar-apr-may --to cc-main-2026-apr-may-jun
  ccrawl domains diff --added-out new-domains.txt`,
		Args:  kit.NoArgs,
		Flags: v.flags,
		Run:   v.run,
	}
}

func (v *domainsDiffCmd) flags(f *kit.FlagSet) {
	f.StringVar(&v.repo, "repo", envOr("CCRAWL_DOMAINS_REPO", defaultDomainsRepo), "HuggingFace dataset repo (org/name)")
	f.StringVar(&v.from, "from", "", "older web-graph release id (default: second-newest published)")
	f.StringVar(&v.to, "to", "", "newer web-graph release id (default: newest published)")
	f.IntVar(&v.workers, "workers", 0, "concurrent shard readers (0 picks a default from CPU count)")
	f.StringVar(&v.addedOut, "added-out", "", "write domains new in the later release to this file, one per line")
}

func (v *domainsDiffCmd) run(ctx context.Context, args []string) error {
	app := appFromCtx(ctx)
	if v.repo == "" {
		return usageErr("--repo is required (or set CCRAWL_DOMAINS_REPO)")
	}
	hf := ccrawl.NewHFClient("")

	from, to := v.from, v.to
	if from == "" || to == "" {
		rf, rt, err := resolveDiffReleases(ctx, hf, v.repo)
		if err != nil {
			return err
		}
		if from == "" {
			from = rf
		}
		if to == "" {
			to = rt
		}
	}
	if from == to {
		return usageErr("--from and --to are the same release; nothing to diff")
	}

	// Optional sink for the added domains. It is written to a caller-named path,
	// never a publish stage dir, so a concurrent publish run is untouched.
	var collect func(string)
	var flush func() error
	if v.addedOut != "" {
		f, err := os.Create(v.addedOut)
		if err != nil {
			return err
		}
		bw := bufio.NewWriter(f)
		collect = func(d string) { _, _ = bw.WriteString(d); _ = bw.WriteByte('\n') }
		flush = func() error {
			if err := bw.Flush(); err != nil {
				_ = f.Close()
				return err
			}
			return f.Close()
		}
	}

	d, err := ccrawl.DiffDomainReleases(ctx, app.HTTP, hf, ccrawl.DomainDiffOptions{
		Repo:    v.repo,
		From:    from,
		To:      to,
		Workers: v.workers,
		Logf:    func(f string, a ...any) { fmt.Fprintf(os.Stderr, f+"\n", a...) },
		Collect: collect,
	})
	if err != nil {
		return err
	}
	if flush != nil {
		if err := flush(); err != nil {
			return err
		}
	}

	fmt.Print(d.Summary())
	if v.addedOut != "" {
		fmt.Fprintf(os.Stderr, "wrote %s new domains to %s\n", commaCount(d.Added), v.addedOut)
	}
	return nil
}

// resolveDiffReleases downloads the domains ledger to a temp file and returns the
// two most recent complete releases, older then newer. It writes only to a temp
// path so a concurrent publish's stage dir is never touched.
func resolveDiffReleases(ctx context.Context, hf *ccrawl.HFClient, repo string) (from, to string, err error) {
	tmp, err := os.CreateTemp("", "ccrawl-domains-stats-*.csv")
	if err != nil {
		return "", "", err
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	defer func() { _ = os.Remove(tmpPath) }()

	if _, err := hf.DownloadRepoFile(ctx, repo, "stats.csv", tmpPath); err != nil {
		return "", "", fmt.Errorf("read %s ledger to pick releases (pass --from/--to): %w", repo, err)
	}
	ledger, err := ccrawl.ReadDomainStats(tmpPath)
	if err != nil {
		return "", "", err
	}
	return ccrawl.TwoNewestDomainReleases(ledger)
}

// commaCount groups an integer with thousands separators for a one-line report.
func commaCount(n int64) string {
	s := fmt.Sprintf("%d", n)
	var out []byte
	for i := 0; i < len(s); i++ {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, s[i])
	}
	return string(out)
}

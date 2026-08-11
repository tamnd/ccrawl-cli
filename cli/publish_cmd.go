package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/any-cli/kit/errs"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// obsoleteRepos are the first-generation dataset repos superseded by
// open-index/ccrawl-urls and open-index/ccrawl-domains. delete-obsolete removes
// them so the account does not carry stale, fragmented datasets.
var obsoleteRepos = []string{
	"open-index/cc-host-dataset",
	"open-index/commoncrawl-urls",
}

// registerPublish attaches the `publish` command group for cross-dataset
// maintenance that does not belong to a single dataset.
func registerPublish(app *kit.App) {
	app.CommandGroup("publish", "Maintenance for the published Common Crawl datasets")
	app.AddCommandUnder("publish", newPublishVerifyCmd())
	app.AddCommandUnder("publish", newDeleteObsoleteCmd())
}

type publishVerifyCmd struct {
	repo    string
	graph   string
	sample  int
	workers int
	repair  bool
	noPush  bool
	asJSON  bool
}

func newPublishVerifyCmd() kit.Command {
	v := &publishVerifyCmd{}
	return kit.Command{
		Use:   "verify",
		Short: "Check that the published shards are readable, complete, and the schema they claim",
		Long: `Read every published shard's Parquet footer and check it against the dataset it
belongs to. Publishing only ever asks whether a path exists, so an upload that
was cut off part way through leaves an object the resume path will never look at
again and nothing notices until somebody reads the dataset.

Each shard has to parse, carry the dataset's columns, hold row groups that add up
to the row count its footer claims, and keep every column chunk inside the bytes
the hub is holding. The totals are then reconciled against the ledger row that
the dataset card is built from. Only footers are fetched, a few hundred KB per
shard, so a whole crawl costs a fraction of a percent of the dataset.

--sample decodes rows out of each shard's last row group as well, which is the
only way to catch a page whose bytes are wrong rather than missing. It reads a
page of every column instead of a footer, so it costs more.

--repair rebuilds the shards that failed and commits them over what the hub has.
It works on the URL dataset, where a shard is the projection of exactly one
source part and can be rebuilt on its own. A domain shard is a cut of one
sequential stream, so verify reports those and leaves the rebuild to
ccrawl domains publish.

Pick the crawls with the global -c flag, or pass --graph for a web-graph release.

  ccrawl publish verify -c CC-MAIN-2026-25
  ccrawl publish verify -c CC-MAIN-2026-25 --sample 64
  ccrawl publish verify -c CC-MAIN-2026-25 --repair
  ccrawl publish verify --graph cc-main-2026-mar-apr-may --json`,
		Args:  kit.NoArgs,
		Flags: v.flags,
		Run:   v.run,
	}
}

func (v *publishVerifyCmd) flags(f *kit.FlagSet) {
	f.StringVar(&v.repo, "repo", "", "HuggingFace dataset repo (org/name), defaults to the dataset the unit belongs to")
	f.StringVar(&v.graph, "graph", "", "verify a web-graph release of the domains dataset instead of URL crawls")
	f.IntVar(&v.sample, "sample", 0, "rows to decode from each shard's last row group (0 reads the footer alone)")
	f.IntVar(&v.workers, "workers", 0, "shards checked at once (0 picks a default from CPU count)")
	f.BoolVar(&v.repair, "repair", false, "rebuild and re-upload the shards that fail")
	f.BoolVar(&v.noPush, "no-push", false, "with --repair, rebuild locally but skip the upload")
	f.BoolVar(&v.asJSON, "json", false, "print the report as JSON")
}

func (v *publishVerifyCmd) run(ctx context.Context, args []string) error {
	app := appFromCtx(ctx)
	if v.sample < 0 {
		return usageErr("--sample cannot be negative")
	}
	push := !v.noPush && !app.dryRun
	hf := ccrawl.NewHFClient("")
	if v.repair && push && !hf.Valid() {
		return errs.New(errs.KindNeedAuth, "HF_TOKEN (or HUGGINGFACE_TOKEN) is not set; set it or pass --no-push")
	}
	vo := ccrawl.VerifyOptions{
		Workers: v.workers,
		Sample:  v.sample,
		Repair:  v.repair,
		Logf:    func(f string, a ...any) { fmt.Fprintf(os.Stderr, f+"\n", a...) },
	}

	var reports []*ccrawl.VerifyReport
	if v.graph != "" {
		if v.repair {
			return usageErr("--repair does not apply to a graph: a domain shard is a cut of one stream, so re-run ccrawl domains publish to rebuild it")
		}
		repo := v.repo
		if repo == "" {
			repo = envOr("CCRAWL_DOMAINS_REPO", defaultDomainsRepo)
		}
		rep, err := ccrawl.VerifyDomainGraph(ctx, app.HTTP, hf, ccrawl.DomainPublishOptions{
			Repo:     repo,
			Graph:    ccrawl.WebGraph{ID: v.graph, BaseURL: ccrawl.WebGraphBaseURL(v.graph)},
			StageDir: filepath.Join(app.Cfg.DataDir, "publish", "domains"),
			Workers:  v.workers,
			Logf:     vo.Logf,
		}, vo)
		if err != nil {
			return err
		}
		reports = append(reports, rep)
	} else {
		crawls, err := app.AllCrawls(ctx)
		if err != nil {
			return err
		}
		if len(crawls) == 0 {
			return noResults("no crawls resolved from -c")
		}
		repo := v.repo
		if repo == "" {
			repo = envOr("CCRAWL_URLS_REPO", defaultURLsRepo)
		}
		for _, crawl := range crawls {
			rep, err := ccrawl.VerifyURLCrawl(ctx, app.HTTP, app.Cache, hf, ccrawl.URLPublishOptions{
				Repo:     repo,
				Source:   app.Cfg.Source,
				StageDir: filepath.Join(app.Cfg.DataDir, "publish", "urls"),
				Workers:  v.workers,
				DoCommit: push,
				Logf:     vo.Logf,
			}, crawl, vo)
			if err != nil {
				return err
			}
			reports = append(reports, rep)
		}
	}

	if v.asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		for _, rep := range reports {
			if err := enc.Encode(rep); err != nil {
				return err
			}
		}
	} else {
		for _, rep := range reports {
			printVerifyReport(rep)
		}
	}

	bad := 0
	for _, rep := range reports {
		bad += rep.Failed
	}
	if bad > 0 && !v.repair {
		return fmt.Errorf("%d shard(s) did not pass; re-run with --repair to rebuild them", bad)
	}
	return nil
}

// printVerifyReport writes the human readable summary: the totals, then every
// shard that did not pass, then what the ledger disagrees about.
func printVerifyReport(rep *ccrawl.VerifyReport) {
	fmt.Printf("%s in %s: %d shards, %d passed, %d failed\n",
		rep.Scope, rep.Repo, rep.Shards, rep.Passed, rep.Failed)
	fmt.Printf("  rows %d, size %s, read %s to check it",
		rep.Rows, humanBytes(rep.Bytes), humanBytes(rep.BytesRead))
	if rep.Bytes > 0 {
		fmt.Printf(" (%.3f%% of the dataset)", 100*float64(rep.BytesRead)/float64(rep.Bytes))
	}
	fmt.Println()
	for _, c := range rep.Failures() {
		fmt.Printf("  %s: %s, %s", c.Path, c.Status, c.Detail)
		if c.Repaired {
			fmt.Print(" [repaired]")
		}
		fmt.Println()
	}
	for _, n := range rep.Notes {
		fmt.Printf("  ledger: %s\n", n)
	}
}

type deleteObsoleteCmd struct {
	yes bool
}

func newDeleteObsoleteCmd() kit.Command {
	v := &deleteObsoleteCmd{}
	return kit.Command{
		Use:   "delete-obsolete",
		Short: "Delete the superseded first-generation dataset repos",
		Long: `Delete the obsolete dataset repos that the ccrawl-urls and ccrawl-domains
datasets replace:

  open-index/cc-host-dataset
  open-index/commoncrawl-urls

This is irreversible and removes the repos and all their data on HuggingFace.
It requires --yes to run. HF_TOKEN (or HUGGINGFACE_TOKEN) must be set.`,
		Args:  kit.NoArgs,
		Flags: v.flags,
		Run:   v.run,
	}
}

func (v *deleteObsoleteCmd) flags(f *kit.FlagSet) {
	f.BoolVar(&v.yes, "yes", false, "confirm the irreversible deletion")
}

func (v *deleteObsoleteCmd) run(ctx context.Context, args []string) error {
	if !v.yes {
		return usageErr("this deletes repos permanently; pass --yes to confirm")
	}
	hf := ccrawl.NewHFClient("")
	if !hf.Valid() {
		return fmt.Errorf("HF_TOKEN (or HUGGINGFACE_TOKEN) is not set")
	}
	for _, repo := range obsoleteRepos {
		if err := hf.DeleteDatasetRepo(ctx, repo); err != nil {
			return fmt.Errorf("delete %s: %w", repo, err)
		}
		fmt.Fprintf(os.Stderr, "deleted https://huggingface.co/datasets/%s\n", repo)
	}
	return nil
}

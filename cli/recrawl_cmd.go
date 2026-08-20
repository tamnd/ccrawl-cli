package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func registerRecrawl(app *kit.App) {
	app.CommandGroup("recrawl", "Recrawl a published work list, streamed and resumable")
	registerRecrawlRun(app)
	registerRecrawlPublish(app)
}

// ── recrawl run ───────────────────────────────────────────────────────────────

type recrawlRunIn struct {
	App       *App          `kit:"inject"`
	From      string        `kit:"flag" help:"published dataset to recrawl: domains, urls, or a repo ID"`
	Dir       string        `kit:"flag" help:"directory of parts inside the dataset, e.g. data/cc-main-2026-apr-may-jun"`
	Column    string        `kit:"flag" help:"string column holding the work: domain or url"`
	Out       string        `kit:"flag" help:"directory to write WARC files into (empty fetches without archiving)"`
	State     string        `kit:"flag" help:"checkpoint file, so a killed run resumes where it stopped"`
	Delay     time.Duration `kit:"flag" default:"1s" help:"minimum spacing between two requests to the same host (0 for none)"`
	MaxPages  int64         `kit:"flag,name=max-pages" help:"stop after this many fetches (0 = no limit)"`
	NoRobots  bool          `kit:"flag,name=no-robots" help:"do not check robots.txt, which you had better have a reason for"`
	Format    string        `kit:"flag" default:"parquet" help:"output format: parquet rows with the body inline, or warc"`
	ShardSize int64         `kit:"flag,name=shard-size" help:"rotate to a new output shard past this much payload"`
	Prefix    string        `kit:"flag" help:"file name prefix for the output files"`
	Batch     int           `kit:"flag" help:"work items fetched between checkpoints (default 2000)"`
	Shard     int           `kit:"flag" help:"which partition of the work list this process takes, 0-based"`
	Shards    int           `kit:"flag" default:"1" help:"how many machines are splitting the work list"`
}

// datasetShorthand maps the two names anybody running the fleet will type onto
// the repos they mean, along with the column that holds the work.
var datasetShorthand = map[string]ccrawl.WorkSource{
	"domains": {Repo: "open-index/ccrawl-domains", Column: "domain"},
	"urls":    {Repo: "open-index/ccrawl-urls", Column: "url"},
}

func registerRecrawlRun(app *kit.App) {
	handle(app, kit.OpMeta{
		Name:    "run",
		Parent:  "recrawl",
		Summary: "Recrawl a published work list, streaming it rather than queueing it",
		Long: `Walk a published dataset and fetch every URL in it, streaming the work list out
of Parquet instead of loading it into a frontier. robots.txt is fetched once per
host and enforced, each host gets one request per --delay, and every fetch is
written out as a Parquet row with the body inline, or to WARC with --format warc.

The run keeps its place in --state, which holds a part number and a row offset
and nothing else. It is a few hundred bytes whether the work list has a thousand
rows or a billion. A killed run resumes from the last checkpoint, refetching at
most --batch pages and skipping none.

Use --shard and --shards to split the work list across machines. The partition
key is the registered domain, so a site and its politeness clock stay on one
machine.

Examples:
  ccrawl recrawl run --from domains --out captures/ --state recrawl.json
  ccrawl recrawl run --from urls --dir data/CC-MAIN-2026-25 --out captures/ --state recrawl.json --shard 0 --shards 3
  ccrawl recrawl run --from open-index/ccrawl-domains --column domain --max-pages 1000`,
	}, func(ctx context.Context, in recrawlRunIn, emit func(ccrawl.CrawlPage) error) error {
		src, err := resolveWorkSource(in.From, in.Dir, in.Column)
		if err != nil {
			return err
		}
		if src.Dir == "" {
			// Nobody should have to know that the domain ranks are published
			// under a web graph release name and the URL index under a crawl ID.
			// Ask the dataset and take the newest.
			dirs, err := ccrawl.DatasetDirs(ctx, in.App.HTTP, src.Repo)
			if err != nil {
				return err
			}
			if len(dirs) == 0 {
				return fmt.Errorf("the dataset %s publishes no parquet parts, so there is nothing to recrawl", src.Repo)
			}
			src.Dir = dirs[0]
			fmt.Fprintf(os.Stderr, "recrawl run: reading %s, the newest of %d releases in %s\n", src.Dir, len(dirs), src.Repo)
		}
		shard := ccrawl.Shard{Index: in.Shard, Count: in.Shards}
		if err := shard.Validate(); err != nil {
			return usageErr(err.Error())
		}

		cfg := ccrawl.DefaultRecrawlConfig
		cfg.Source = src
		cfg.Shard = shard
		cfg.StatePath = in.State
		cfg.OutDir = in.Out
		cfg.Workers = in.App.Workers
		cfg.Robots = !in.NoRobots
		cfg.MaxPages = in.MaxPages
		cfg.Crawl = ccrawl.DefaultCrawlConfig
		cfg.Info = crawlWARCInfo()
		cfg.Format = ccrawl.CaptureFormat(strings.ToLower(strings.TrimSpace(in.Format)))
		if err := cfg.Format.Validate(); err != nil {
			return usageErr(err.Error())
		}
		// Taken as given, including zero, the same way crawl run takes it.
		cfg.Delay = in.Delay
		if in.ShardSize > 0 {
			cfg.ShardSize = in.ShardSize
		}
		if in.Prefix != "" {
			cfg.Prefix = in.Prefix
		}
		if in.Batch > 0 {
			cfg.Batch = in.Batch
		}

		// The work list is read from HuggingFace, which is a bulk host like
		// data.commoncrawl.org, so the ordinary client and its budget are right
		// for it. robots.txt and the pages are the open web and get the crawl
		// client instead.
		r, err := ccrawl.NewRecrawler(cfg, in.App.HTTP, ccrawl.NewCrawlClient(in.App.Cfg))
		if errors.Is(err, ccrawl.ErrRecrawlDone) {
			fmt.Fprintf(os.Stderr, "recrawl run: %s says the work list is finished\n", in.State)
			return nil
		}
		if err != nil {
			return err
		}
		defer func() { _ = r.Close() }()

		rep, stopRun, err := in.App.StartRun("recrawl run", "")
		if err != nil {
			return err
		}
		defer stopRun()
		sp := ccrawl.StartStreamProgress(rep, "pages", int(cfg.MaxPages), 0)
		defer sp.Stop()

		stats, runErr := r.Run(ctx, func(p ccrawl.CrawlPage) error {
			sp.Add(1, 1, int64(p.BodySize))
			return emit(p)
		})
		ck := r.Checkpoint()
		fmt.Fprintf(os.Stderr,
			"recrawl run: %d fetched, %d failed, %d disallowed, %s, %d %s files, at part %d row %d\n",
			stats.Fetched, stats.Failed, stats.Disallowed,
			humanBytes(stats.Bytes), len(stats.OutFiles), cfg.Format, ck.Part, ck.Row)
		if stats.Failed > 0 {
			fmt.Fprintf(os.Stderr, "recrawl run: failures by class: dns %d, timeout %d, refused %d, skipped %d, other %d\n",
				stats.ErrDNS, stats.ErrTimeout, stats.ErrRefused, stats.ErrSkip, stats.ErrOther)
		}
		fmt.Fprintln(os.Stderr, "recrawl run: "+robotsLine(stats))
		if ck.Done {
			fmt.Fprintln(os.Stderr, "recrawl run: the work list is finished")
		}
		return runErr
	})
}

// resolveWorkSource turns what someone typed into a dataset to read.
//
// The two shorthands exist because nobody running the fleet should have to
// remember a repo ID and a column name to say "recrawl the domains". A full repo
// ID still works, and then the column has to be named, because we cannot guess
// what an arbitrary dataset calls its URLs.
func resolveWorkSource(from, dir, column string) (ccrawl.WorkSource, error) {
	from = strings.TrimSpace(from)
	if from == "" {
		return ccrawl.WorkSource{}, usageErr("name a work list with --from, either domains, urls, or a dataset repo ID")
	}
	src, ok := datasetShorthand[from]
	if !ok {
		src = ccrawl.WorkSource{Repo: from, Column: column}
		if src.Column == "" {
			return src, usageErr(fmt.Sprintf("--from %s is a repo ID, so say which column holds the work with --column domain or --column url", from))
		}
	}
	if column != "" {
		src.Column = column
	}
	if dir != "" {
		src.Dir = dir
	}
	if err := src.Validate(); err != nil {
		return src, usageErr(err.Error())
	}
	return src, nil
}

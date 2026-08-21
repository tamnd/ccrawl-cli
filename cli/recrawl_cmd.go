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
	Robots    bool          `kit:"flag" help:"check robots.txt before fetching, off by default on a recrawl"`
	NoExtract bool          `kit:"flag,name=no-extract" help:"store the body without rendering it to text and Markdown"`
	Extractor string        `kit:"flag" help:"engine that renders a page to Markdown: h2m, readability, or raw"`
	Format    string        `kit:"flag" default:"parquet" help:"output format: parquet rows with the body inline, or warc"`
	ShardSize int64         `kit:"flag,name=shard-size" help:"rotate to a new output shard past this much payload"`
	Prefix    string        `kit:"flag" help:"file name prefix for the output files"`
	Writers   int           `kit:"flag" help:"output files open at once, each with its own encoder (default 1)"`
	Batch     int           `kit:"flag" help:"work items fetched between checkpoints (default 2000)"`
	Shard     int           `kit:"flag" help:"which partition of the work list this process takes, 0-based"`
	Shards    int           `kit:"flag" default:"1" help:"how many machines are splitting the work list"`
	DNS       int           `kit:"flag,name=dns-lookups" help:"how many DNS lookups may be in flight at once (default: an eighth of --workers, 16 to 128)"`
	RobotsTO  time.Duration `kit:"flag,name=robots-timeout" help:"budget for one host's robots.txt fetch (default 10s)"`
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

Every HTML page is rendered to text and Markdown as it is fetched, into the same
columns open-markdown uses, so a published shard is usable without a second pass
over the corpus. The run is waiting on the network anyway, so the rendering fits
in the gap the fetches leave. Turn it off with --no-extract and pick the engine
with --extractor.

The run keeps its place in --state, which holds a part number and a row offset
and nothing else. It is a few hundred bytes whether the work list has a thousand
rows or a billion. A killed run resumes from the last checkpoint and skips none,
refetching the batch it was working through plus whatever the pool had in the
air behind it, which is a few hundred rows at the default width.

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
		// Off unless asked for. On a link crawl robots.txt is amortised over
		// every page a host gives up, and on a recrawl of a domain corpus it is
		// an extra request for every single page, because every row is a
		// different host. Measured on the live domain list at 256 workers it was
		// 45 percent of the worker time, and a third of the hosts never answered
		// it at all, so the run sat out the whole budget on them and then threw
		// the row away. The rules a crawler is asked to follow are still in the
		// binary and still enforced when this is on.
		cfg.Robots = in.Robots
		cfg.Extract = !in.NoExtract
		cfg.Extractor = in.Extractor
		cfg.MaxPages = in.MaxPages
		// cfg.Crawl arrives from DefaultRecrawlConfig with a patience a recrawl
		// can afford and is not replaced with the crawl default here. It used to
		// be, and that quietly put the two minute timeout back: a batch does not
		// checkpoint until its last item is done, so one host that accepts a
		// connection and then says nothing held a worker for two minutes and the
		// batch behind it. Measured on the live domain list, a 600 page run spent
		// its last 92 seconds finishing four items and fetched nothing in that
		// time. Only --timeout overrides it now, and only when it is set.
		if in.App.Cfg.Timeout > 0 {
			cfg.Crawl.Timeout = in.App.Cfg.Timeout
		}
		cfg.Info = crawlWARCInfo()
		cfg.Format = ccrawl.CaptureFormat(strings.ToLower(strings.TrimSpace(in.Format)))
		if err := cfg.Format.Validate(); err != nil {
			return usageErr(err.Error())
		}
		// The bound on lookups in flight. The default is derived from the worker
		// count and capped, because the machine's own resolver is the thing that
		// falls over first and 128 questions at once is already more than most
		// stubs will take. A machine running a caching resolver of its own has no
		// such ceiling and wants this raised, and past a few hundred workers the
		// default cap becomes the queue: workers wait for a lookup slot, the wait
		// is inside the request's own budget, and the request times out having
		// never been sent. The dns line at the end of a run says when that is
		// happening, by printing the peak next to the bound.
		cfg.Conns.DNSLookups = in.DNS

		// The budget for one robots.txt. It is worth having on the command line
		// because on a domain corpus robots is the single biggest thing a worker
		// does, and a third of the hosts never answer it, so the run pays the
		// whole budget on every one of them. It is not free to shorten: the
		// budget is what a slow but willing host is given, and cutting it turns
		// some of those into a disallow they did not ask for.
		if in.RobotsTO > 0 {
			cfg.RobotsTimeout = in.RobotsTO
		}

		// Taken as given, including zero, the same way crawl run takes it.
		cfg.Delay = in.Delay
		if in.ShardSize > 0 {
			cfg.ShardSize = in.ShardSize
		}
		if in.Prefix != "" {
			cfg.Prefix = in.Prefix
		}
		// How many encoders the run writes through. One is right until the timing
		// line says the writer is busy most of the wall clock, which is where a
		// wide pool ends up once the sink and not the network is the slowest
		// thing in the run. The parts rotate together, so the checkpoint moves at
		// the same rate whatever this is.
		if in.Writers > 0 {
			cfg.Writers = in.Writers
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
			fmt.Fprintf(os.Stderr, "recrawl run: failures by class: dns %d, timeout %d, refused %d, tls %d, skipped %d, other %d\n",
				stats.ErrDNS, stats.ErrTimeout, stats.ErrRefused, stats.ErrTLS, stats.ErrSkip, stats.ErrOther)
			// And what other was, by shape. On the domain corpus it is a tenth of
			// the work list, which is too much of a run to leave with no label on
			// it when the question being asked is why the rate is what it is.
			if line := ccrawl.ErrOtherLine(stats.ErrOther, stats.ErrOtherTop); line != "" {
				fmt.Fprintln(os.Stderr, "recrawl run: "+line)
			}
		}
		// A run with the check off says so in its own summary rather than only in
		// whatever command line somebody typed a week ago. This is the one line
		// that tells a reader of a log which of two quite different things they
		// are looking at.
		if !cfg.Robots {
			fmt.Fprintln(os.Stderr, "recrawl run: robots.txt was not checked, so nothing here was refused on its say so")
		} else {
			fmt.Fprintln(os.Stderr, "recrawl run: "+robotsLine(stats))
			if stats.Robots.Unreachable > 0 {
				fmt.Fprintln(os.Stderr, "recrawl run: "+robotsFailLine(stats.Robots))
			}
		}
		fmt.Fprintln(os.Stderr, "recrawl run: "+dnsLine(r.DNS()))
		t := r.Timing()
		fmt.Fprintf(os.Stderr, "recrawl run: %.1f pages a second, %s an item, %s\n",
			t.Rate(stats.Fetched), t.PerItem().Round(time.Millisecond), t.Line())
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

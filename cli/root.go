// Package cli builds the ccrawl command tree on top of the ccrawl library.
package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// Build metadata, set via -ldflags at release time.
var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

// App carries the resolved configuration and shared clients for a command run.
type App struct {
	Cfg     ccrawl.Config
	HTTP    *ccrawl.HTTPClient
	Cache   *ccrawl.Cache
	Out     *Output
	crawl   string // resolved crawl ID, lazily filled
	noCache bool
	yes     bool
	dryRun  bool
	Limit   int
	Workers int
}

// globalFlags holds the persistent flag values before they are folded into Cfg.
type globalFlags struct {
	crawl    string
	output   string
	fields   string
	limit    int
	dataDir  string
	workers  int
	source   string
	rate     time.Duration
	retries  int
	timeout  time.Duration
	quiet    bool
	verbose  int
	color    string
	noCache  bool
	yes      bool
	dryRun   bool
	noHeader bool
	template string
}

// Root builds the root command and its whole subtree.
func Root() *cobra.Command {
	g := &globalFlags{}
	app := &App{}

	root := &cobra.Command{
		Use:   "ccrawl",
		Short: "A delightful command line for Common Crawl",
		Long: `ccrawl is the fastest way to work with Common Crawl from your terminal.

Find captures in the URL index, fetch the exact bytes of a page Common Crawl saw,
stream WARC/WAT/WET archives, query the columnar Parquet index, look up domain
ranks, and build datasets, all from one binary.

Quick start:
  ccrawl crawls latest                 newest crawl ID
  ccrawl search example.com/*          captures under a path
  ccrawl get example.com --text        the page text Common Crawl captured
  ccrawl table urls --tld gov -o url   bulk URLs from the columnar index`,
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			return app.init(g)
		},
	}

	pf := root.PersistentFlags()
	def := ccrawl.DefaultConfig()
	pf.StringVarP(&g.crawl, "crawl", "c", "latest", "crawl ID, year, or 'latest'/'all'")
	pf.StringVarP(&g.output, "output", "o", "auto", "table|json|jsonl|csv|tsv|url|raw")
	pf.StringVar(&g.fields, "fields", "", "comma-separated columns to show")
	pf.IntVarP(&g.limit, "limit", "n", 0, "max results (0 = unlimited)")
	pf.StringVar(&g.dataDir, "data-dir", def.DataDir, "root data directory")
	pf.IntVarP(&g.workers, "workers", "j", def.Workers, "concurrency")
	pf.StringVar(&g.source, "source", "https", "bulk data source: https|s3")
	pf.DurationVar(&g.rate, "rate", def.Delay, "minimum delay between requests")
	pf.IntVar(&g.retries, "retries", def.Retries, "retry attempts on 429/5xx")
	pf.DurationVar(&g.timeout, "timeout", def.Timeout, "per-request timeout")
	pf.BoolVarP(&g.quiet, "quiet", "q", false, "suppress progress output")
	pf.CountVarP(&g.verbose, "verbose", "v", "increase verbosity (repeatable)")
	pf.StringVar(&g.color, "color", "auto", "color output: auto|always|never")
	pf.BoolVar(&g.noCache, "no-cache", false, "bypass on-disk caches")
	pf.BoolVarP(&g.yes, "yes", "y", false, "assume yes to prompts")
	pf.BoolVar(&g.dryRun, "dry-run", false, "print actions without performing them")
	pf.BoolVar(&g.noHeader, "no-header", false, "omit the header row in table/csv output")
	pf.StringVar(&g.template, "template", "", "Go text/template applied per row")

	root.AddCommand(
		newCrawlsCmd(app),
		newSearchCmd(app),
		newGetCmd(app),
		newFetchCmd(app),
		newDownloadCmd(app),
		newPathsCmd(app),
		newParseCmd(app),
		newExtractCmd(app),
		newNewsCmd(app),
		newTableCmd(app),
		newRankCmd(app),
		newDBCmd(app),
		newConvertCmd(app),
		newStatsCmd(app),
		newConfigCmd(app),
		newCacheCmd(app),
		newVersionCmd(),
	)
	return root
}

func (a *App) init(g *globalFlags) error {
	cfg := ccrawl.DefaultConfig()
	if g.dataDir != "" {
		cfg.DataDir = g.dataDir
		cfg.DBPath = g.dataDir + "/ccrawl.duckdb"
	}
	cfg.Workers = g.workers
	cfg.Delay = g.rate
	cfg.Retries = g.retries
	cfg.Timeout = g.timeout
	cfg.CrawlID = g.crawl
	if g.source == "s3" {
		cfg.Source = ccrawl.SourceS3
	}

	a.Limit = g.limit
	a.Workers = g.workers
	a.Cfg = cfg
	a.HTTP = ccrawl.NewHTTPClient(cfg)
	a.Cache = ccrawl.NewCache(cfg.CacheDir, !g.noCache)
	a.noCache = g.noCache
	a.yes = g.yes
	a.dryRun = g.dryRun
	a.Out = newOutput(g)
	return nil
}

// Crawl resolves the crawl reference once and caches the canonical ID.
func (a *App) Crawl(ctx context.Context) (string, error) {
	if a.crawl != "" {
		return a.crawl, nil
	}
	id, err := ccrawl.ResolveCrawl(ctx, a.HTTP, a.Cache, a.Cfg.CrawlID)
	if err != nil {
		return "", err
	}
	a.crawl = id
	return id, nil
}

// AllCrawls returns the crawl IDs to operate over when -c all/year is given,
// newest first, otherwise the single resolved crawl.
func (a *App) AllCrawls(ctx context.Context) ([]string, error) {
	ref := a.Cfg.CrawlID
	if ref == "all" {
		crawls, err := ccrawl.ListCrawls(ctx, a.HTTP, a.Cache)
		if err != nil {
			return nil, err
		}
		ids := make([]string, len(crawls))
		for i, c := range crawls {
			ids[i] = c.ID
		}
		return ids, nil
	}
	id, err := a.Crawl(ctx)
	if err != nil {
		return nil, err
	}
	return []string{id}, nil
}

// Execute runs the root command, mapping errors to exit codes.
func Execute(ctx context.Context, cmd *cobra.Command) int {
	if err := cmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "ccrawl: "+err.Error())
		if ec, ok := err.(exitCoder); ok {
			return ec.ExitCode()
		}
		return 1
	}
	return 0
}

type exitCoder interface{ ExitCode() int }

type codedError struct {
	err  error
	code int
}

func (e codedError) Error() string { return e.err.Error() }
func (e codedError) ExitCode() int { return e.code }

func noResults(msg string) error { return codedError{fmt.Errorf("%s", msg), 3} }
func usageErr(msg string) error  { return codedError{fmt.Errorf("%s", msg), 2} }

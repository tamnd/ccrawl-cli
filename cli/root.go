// Package cli builds the ccrawl command tree on top of the ccrawl library and
// the any-cli/kit framework. The record-stream commands are kit operations
// (declared once, exposed as CLI, HTTP, and MCP); the byte-fetch, columnar, and
// interactive commands are escape-hatch kit.Command commands that share the same
// run state through the context.
package cli

import (
	"context"
	"os"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/any-cli/kit/errs"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// Build metadata, set via -ldflags at release time.
var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

// builder wires the ccrawl globals and defaults into a kit.App. Holding them on
// a struct lets the WithDefaults/GlobalFlags/SetClient hooks be named methods
// rather than closures, and gives the client factory access to the resolved
// ccrawl defaults and the live global-flag values.
type builder struct {
	dom *domainGlobals
	def ccrawl.Config
	set *settings
}

// newBuilder resolves the config file before anything is registered, because a
// profile changes what the flags default to and a flag can only override a
// default that is already there.
func newBuilder() *builder {
	b := &builder{dom: &domainGlobals{}, def: ccrawl.DefaultConfig(), set: loadSettings(os.Args)}
	b.set.apply(&b.def)
	current = b.set
	return b
}

// NewApp assembles the kit application: identity, the ccrawl-specific global
// flags, the client factory that builds the shared engine, the record-stream
// operations, and the escape-hatch commands.
//
// The error is a config file that could not be read: a bad setting is settled
// before a single flag is registered, since the flags default to what the file
// says, and there is nothing sensible to run on top of a file the program did
// not understand.
func NewApp() (*kit.App, error) {
	b := newBuilder()
	if b.set.err != nil {
		return nil, b.set.err
	}

	app := kit.New(kit.Identity{
		Binary:  "ccrawl",
		Version: Version,
		Short:   "A delightful command line for Common Crawl",
		Long: `ccrawl is the fastest way to work with Common Crawl from your terminal.

Find captures in the URL index, fetch the exact bytes of a page Common Crawl saw,
stream WARC/WAT/WET archives, query the columnar Parquet index, look up domain
ranks, and build datasets, all from one binary.

Quick start:
  ccrawl crawls latest                 newest crawl ID
  ccrawl search example.com/*          captures under a path
  ccrawl get example.com --text        the page text Common Crawl captured
  ccrawl columnar urls --tld gov -o url   bulk URLs from the columnar index

Exit codes:
  0   success
  1   error
  2   usage error, a missing or invalid argument
  3   the query ran and matched nothing
  75  temporary failure, run it again (EX_TEMPFAIL)

Exit 3 is not a failure, it means the query was fine and Common Crawl has
nothing for it. Exit 75 comes from the publish pipelines when a run stalls or
finishes short, and it is the signal for a supervisor to restart the run, which
picks up where it left off.`,
		Site: "https://commoncrawl.org",
		Repo: "https://github.com/tamnd/ccrawl-cli",
	}, kit.WithDefaults(b.defaults))

	app.GlobalFlags(b.globals)
	app.SetClient(b.client)

	registerOps(app)
	registerEscapeHatches(app)
	return app, nil
}

// defaults seeds the framework baseline from the ccrawl defaults, so an unset
// --rate/--retries/--timeout/--data-dir keeps ccrawl's own values.
// The values come from b.def, which is the ccrawl defaults with the config file
// and the environment already folded in, so a setting in the file is what a flag
// overrides rather than something applied after it.
func (b *builder) defaults(c *kit.Config) {
	c.DataDir = b.def.DataDir
	c.CacheDir = b.def.CacheDir
	c.ConfigDir = ccrawl.ConfigDir()
	c.Rate = b.def.Delay
	c.Retries = b.def.Retries
	c.Timeout = b.def.Timeout
	c.Workers = b.def.Workers
	c.UserAgent = b.def.UserAgent
}

// globals registers the ccrawl-specific persistent flags, on top of the kit
// framework globals.
func (b *builder) globals(f *kit.FlagSet) {
	f.StringVarP(&b.dom.crawl, "crawl", "c", b.def.CrawlID, "crawl: ID, year, latest, all, an integer for the newest N, or a comma list")
	f.StringVar(&b.dom.source, "source", string(b.def.Source), "bulk data source: https|s3")
	f.IntVarP(&b.dom.workers, "workers", "j", b.def.Workers, "concurrency")
	f.BoolVar(&b.dom.library, "library", false, "read and write under the structured dataset library")
	f.StringVar(&b.dom.libraryDir, "library-dir", b.set.str("library_dir", ccrawl.LibraryDir()), "root of the dataset library")
	f.BoolVarP(&b.dom.yes, "yes", "y", false, "assume yes to prompts")
	f.StringVar(&b.dom.progress, "progress", "", "progress reporting for long runs: text|json|none (default: text on a terminal, json otherwise)")
	f.StringVar(&b.dom.journal, "journal", "", "append run events as JSON Lines to this file (default: run.jsonl beside the ledger)")
	f.StringVar(&b.dom.metricsAddr, "metrics-addr", "", "serve Prometheus metrics for the run on this address, e.g. :9090")
	f.DurationVar(&b.dom.globalRate, "global-rate", b.def.GlobalRate,
		"minimum gap between Common Crawl requests across every ccrawl process on this host (0 disables)")
}

// client is the factory kit calls once per run to build the shared engine from
// the resolved config and the ccrawl globals.
func (b *builder) client(_ context.Context, c kit.Config) (any, error) {
	app := buildApp(c, b.dom, b.def)
	app.Settings = b.set
	return app, nil
}

// noResults and usageErr classify the two common command failures so kit maps
// them to the stable exit codes (3 and 2) on every surface.
func noResults(msg string) error { return errs.NoResults("%s", msg) }
func usageErr(msg string) error  { return errs.Usage("%s", msg) }

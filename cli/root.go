// Package cli builds the ccrawl command tree on top of the ccrawl library and
// the any-cli/kit framework. The record-stream commands are kit operations
// (declared once, exposed as CLI, HTTP, and MCP); the byte-fetch, columnar, and
// interactive commands are escape-hatch kit.Command commands that share the same
// run state through the context.
package cli

import (
	"context"

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
}

// NewApp assembles the kit application: identity, the ccrawl-specific global
// flags, the client factory that builds the shared engine, the record-stream
// operations, and the escape-hatch commands.
func NewApp() *kit.App {
	b := &builder{dom: &domainGlobals{}, def: ccrawl.DefaultConfig()}

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
  ccrawl columnar urls --tld gov -o url   bulk URLs from the columnar index`,
		Site: "https://commoncrawl.org",
		Repo: "https://github.com/tamnd/ccrawl-cli",
	}, kit.WithDefaults(b.defaults))

	app.GlobalFlags(b.globals)
	app.SetClient(b.client)

	registerOps(app)
	registerEscapeHatches(app)
	return app
}

// defaults seeds the framework baseline from the ccrawl defaults, so an unset
// --rate/--retries/--timeout/--data-dir keeps ccrawl's own values.
func (b *builder) defaults(c *kit.Config) {
	c.DataDir = b.def.DataDir
	c.CacheDir = b.def.CacheDir
	c.Rate = b.def.Delay
	c.Retries = b.def.Retries
	c.Timeout = b.def.Timeout
	c.Workers = b.def.Workers
	c.UserAgent = b.def.UserAgent
}

// globals registers the ccrawl-specific persistent flags, on top of the kit
// framework globals.
func (b *builder) globals(f *kit.FlagSet) {
	f.StringVarP(&b.dom.crawl, "crawl", "c", "latest", "crawl: ID, year, latest, all, an integer for the newest N, or a comma list")
	f.StringVar(&b.dom.source, "source", "https", "bulk data source: https|s3")
	f.IntVarP(&b.dom.workers, "workers", "j", b.def.Workers, "concurrency")
	f.BoolVar(&b.dom.library, "library", false, "read and write under the structured dataset library")
	f.StringVar(&b.dom.libraryDir, "library-dir", ccrawl.LibraryDir(), "root of the dataset library")
	f.BoolVarP(&b.dom.yes, "yes", "y", false, "assume yes to prompts")
}

// client is the factory kit calls once per run to build the shared engine from
// the resolved config and the ccrawl globals.
func (b *builder) client(_ context.Context, c kit.Config) (any, error) {
	return buildApp(c, b.dom), nil
}

// noResults and usageErr classify the two common command failures so kit maps
// them to the stable exit codes (3 and 2) on every surface.
func noResults(msg string) error { return errs.NoResults("%s", msg) }
func usageErr(msg string) error  { return errs.Usage("%s", msg) }

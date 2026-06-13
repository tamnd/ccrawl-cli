// Package cli builds the ccrawl command tree on top of the ccrawl library and
// the any-cli/kit framework. The record-stream commands are kit operations
// (declared once, exposed as CLI, HTTP, and MCP); the byte-fetch, columnar, and
// interactive commands are escape-hatch cobra commands that share the same run
// state through the context.
package cli

import (
	"context"

	"github.com/spf13/pflag"
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

// NewApp assembles the kit application: identity, the ccrawl-specific global
// flags, the client factory that builds the shared engine, the record-stream
// operations, and the escape-hatch commands.
func NewApp() *kit.App {
	dom := &domainGlobals{}
	def := ccrawl.DefaultConfig()

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
  ccrawl table urls --tld gov -o url   bulk URLs from the columnar index`,
		Site: "https://commoncrawl.org",
		Repo: "https://github.com/tamnd/ccrawl-cli",
	}, kit.WithDefaults(func(c *kit.Config) {
		// Seed the framework baseline from the ccrawl defaults so an unset
		// --rate/--retries/--timeout/--data-dir keeps ccrawl's own values.
		c.DataDir = def.DataDir
		c.CacheDir = def.CacheDir
		c.Rate = def.Delay
		c.Retries = def.Retries
		c.Timeout = def.Timeout
		c.Workers = def.Workers
		c.UserAgent = def.UserAgent
	}))

	// ccrawl-specific persistent flags, on top of the kit framework globals.
	app.GlobalFlags(func(fs *pflag.FlagSet) {
		fs.StringVarP(&dom.crawl, "crawl", "c", "latest", "crawl ID, year, or 'latest'/'all'")
		fs.StringVar(&dom.source, "source", "https", "bulk data source: https|s3")
		fs.IntVarP(&dom.workers, "workers", "j", def.Workers, "concurrency")
		fs.BoolVar(&dom.library, "library", false, "read and write under the structured dataset library")
		fs.StringVar(&dom.libraryDir, "library-dir", ccrawl.LibraryDir(), "root of the dataset library")
		fs.BoolVarP(&dom.yes, "yes", "y", false, "assume yes to prompts")
	})

	app.SetClient(func(_ context.Context, c kit.Config) (any, error) {
		return buildApp(c, dom), nil
	})

	registerOps(app)
	registerEscapeHatches(app)
	return app
}

// noResults and usageErr classify the two common command failures so kit maps
// them to the stable exit codes (3 and 2) on every surface.
func noResults(msg string) error { return errs.NoResults("%s", msg) }
func usageErr(msg string) error  { return errs.Usage("%s", msg) }

package cli

import (
	"context"
	"io"
	"os"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/any-cli/kit/render"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// Row is one output record: an ordered set of named columns plus the original
// value rendered by json/jsonl and templates. It is kit's render.Record, so the
// row builders feed straight into the shared renderer with no per-format code.
type Row = render.Record

// App carries the resolved configuration and shared clients for a command run.
// kit builds one per run through the client factory registered in Root, then
// hands it to every operation (injected) and every escape-hatch command (fetched
// from the run context with appFromCtx).
type App struct {
	Cfg        ccrawl.Config
	HTTP       *ccrawl.HTTPClient
	Cache      *ccrawl.Cache
	Out        *render.Renderer
	st         *kit.State // run state, for building a renderer over another writer
	crawl      string     // resolved crawl ID, lazily filled
	yes        bool
	dryRun     bool
	Limit      int
	Workers    int
	UseLibrary bool
	LibraryDir string
}

// domainGlobals holds the ccrawl-specific persistent flags that are not part of
// the kit framework baseline. Root binds them on the root command; the client
// factory and Finalize hook read them back when building the App.
type domainGlobals struct {
	crawl      string
	source     string
	workers    int
	library    bool
	libraryDir string
	yes        bool
}

// buildApp is the client factory kit calls once per run. It folds the resolved
// framework config and the ccrawl globals into a ccrawl.Config and opens the
// shared HTTP client and cache.
func buildApp(kc kit.Config, dom *domainGlobals) *App {
	cfg := ccrawl.DefaultConfig()
	cfg.DataDir = kc.DataDir
	cfg.CacheDir = kc.CacheDir
	cfg.DBPath = kc.DataDir + "/ccrawl.duckdb"
	cfg.Workers = dom.workers
	cfg.Delay = kc.Rate
	cfg.Retries = kc.Retries
	cfg.Timeout = kc.Timeout
	cfg.UserAgent = kc.UserAgent
	cfg.CrawlID = dom.crawl
	if dom.source == "s3" {
		cfg.Source = ccrawl.SourceS3
	}
	return &App{
		Cfg:        cfg,
		HTTP:       ccrawl.NewHTTPClient(cfg),
		Cache:      ccrawl.NewCache(cfg.CacheDir, !kc.NoCache),
		Workers:    dom.workers,
		yes:        dom.yes,
		dryRun:     kc.DryRun,
		UseLibrary: dom.library,
		LibraryDir: dom.libraryDir,
	}
}

// appFromCtx returns the run's App for an escape-hatch command, with the renderer
// and limit stamped from the resolved run state so its output matches every
// operation. Operations receive the same App by injection and ignore Out.
//
// The client factory (buildApp) cannot fail, so a missing or mistyped client is a
// wiring bug rather than a runtime condition; appFromCtx surfaces it as a panic
// instead of threading an impossible error through every command.
func appFromCtx(ctx context.Context) *App {
	app := kit.MustClient[*App](ctx)
	app.st = kit.FromContext(ctx)
	app.Out = app.renderTo(os.Stdout)
	app.Limit = app.st.Globals.Limit
	return app
}

// renderTo builds a renderer over w using the run's resolved output settings. The
// --template was validated when the run state was built, so a renderer over a
// valid writer cannot fail here.
func (a *App) renderTo(w io.Writer) *render.Renderer {
	r, err := a.st.Renderer(w)
	if err != nil {
		panic(err)
	}
	return r
}

// Library resolves the crawl ID and returns the dataset library rooted at the
// configured library dir for that crawl.
func (a *App) Library(ctx context.Context) (ccrawl.Library, error) {
	id, err := a.Crawl(ctx)
	if err != nil {
		return ccrawl.Library{}, err
	}
	return ccrawl.NewLibrary(a.LibraryDir, id), nil
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

// AllCrawls returns the crawl IDs to operate over, newest first. It honors the
// multi-crawl forms of -c: "all", a year (every crawl that year), an integer
// (the newest N crawls), and comma-separated lists of any reference. A single
// reference yields one ID.
func (a *App) AllCrawls(ctx context.Context) ([]string, error) {
	ids, err := ccrawl.ResolveCrawls(ctx, a.HTTP, a.Cache, a.Cfg.CrawlID)
	if err != nil {
		return nil, err
	}
	// When exactly one crawl is in play, cache it so a later Crawl() call is
	// consistent and skips a second resolve.
	if len(ids) == 1 {
		a.crawl = ids[0]
	}
	return ids, nil
}

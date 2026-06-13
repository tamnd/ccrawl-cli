package cli

import (
	"context"
	"fmt"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// App carries the resolved configuration and shared clients for a command run.
// kit builds one per run through the client factory registered in Root, then
// hands it to every operation (injected) and every escape-hatch command (fetched
// from the run context with appFromCtx).
type App struct {
	Cfg        ccrawl.Config
	HTTP       *ccrawl.HTTPClient
	Cache      *ccrawl.Cache
	Out        *Output
	crawl      string // resolved crawl ID, lazily filled
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

// appFromCtx returns the run's App for an escape-hatch command, with the Output
// and limit stamped from the resolved run state so its rendering matches every
// operation. Operations receive the same App by injection and ignore Out.
func appFromCtx(ctx context.Context) (*App, error) {
	st := kit.FromContext(ctx)
	if st == nil {
		return nil, fmt.Errorf("no run state on context")
	}
	c, err := st.Client(ctx)
	if err != nil {
		return nil, err
	}
	app, ok := c.(*App)
	if !ok || app == nil {
		return nil, fmt.Errorf("run client is not a ccrawl app")
	}
	app.Out = newOutputFromState(st)
	app.Limit = st.Globals.Limit
	return app, nil
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

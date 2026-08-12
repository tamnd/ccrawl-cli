package cli

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

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

	// Settings is the config file this run resolved, kept so config show can say
	// where each value came from. Nil in a run that did not go through NewApp.
	Settings *settings

	// Run reporting, from --progress, --journal and --metrics-addr. StartRun
	// turns these into the reporter a long command hands to the pipeline.
	Progress    string
	JournalPath string
	MetricsAddr string
}

// domainGlobals holds the ccrawl-specific persistent flags that are not part of
// the kit framework baseline. Root binds them on the root command; the client
// factory and Finalize hook read them back when building the App.
type domainGlobals struct {
	crawl       string
	source      string
	workers     int
	library     bool
	libraryDir  string
	yes         bool
	progress    string
	journal     string
	metricsAddr string
	globalRate  time.Duration
}

// buildApp is the client factory kit calls once per run. It folds the resolved
// framework config and the ccrawl globals into a ccrawl.Config and opens the
// shared HTTP client and cache.
// base is the ccrawl defaults with the config file already folded in, so the
// settings that have no flag of their own (the backoff pair, the DuckDB path)
// survive into the run.
func buildApp(kc kit.Config, dom *domainGlobals, base ccrawl.Config, set *settings) *App {
	cfg := base
	cfg.DataDir = kc.DataDir
	cfg.CacheDir = cacheDirFor(kc, base, set)
	// The DuckDB file follows the data dir unless the config file put it
	// somewhere else, since a moved data dir with the database left behind in the
	// old one is nobody's intent.
	if base.DBPath == ccrawl.DefaultConfig().DBPath {
		cfg.DBPath = kc.DataDir + "/ccrawl.duckdb"
	}
	cfg.Workers = dom.workers
	cfg.Delay = kc.Rate
	cfg.GlobalRate = dom.globalRate
	cfg.Retries = kc.Retries
	cfg.Timeout = kc.Timeout
	cfg.UserAgent = kc.UserAgent
	cfg.CrawlID = dom.crawl
	cfg.Source = ccrawl.SourceHTTPS
	if dom.source == "s3" {
		cfg.Source = ccrawl.SourceS3
	}
	hc := ccrawl.NewHTTPClient(cfg)
	// The effective rate is a property of the host, not of this run, so a person
	// debugging why a pipeline is slow needs to be told what budget it is sharing.
	// It goes behind -v rather than on every run, because printing a line about
	// rate limiting before the output of ccrawl search would be noise. The one
	// case that is not optional, the limiter failing and silently going per
	// process, warns on its own from inside ccrawl.
	if kc.Verbose > 0 {
		fmt.Fprintf(os.Stderr, "ccrawl: %s\n", hc.GlobalRate())
	}
	return &App{
		Cfg:         cfg,
		HTTP:        hc,
		Cache:       ccrawl.NewCache(cfg.CacheDir, !kc.NoCache),
		Workers:     dom.workers,
		yes:         dom.yes,
		dryRun:      kc.DryRun,
		UseLibrary:  dom.library,
		LibraryDir:  dom.libraryDir,
		Progress:    dom.progress,
		JournalPath: dom.journal,
		MetricsAddr: dom.metricsAddr,
	}
}

// cacheDirFor resolves where the cache lives once --data-dir has had its say.
// The reference has always said the cache dir follows the data dir when it is
// not named itself, and the config file honoured that, but --data-dir did not:
// kit computes its default cache dir from its default data dir before any flag
// is parsed, so moving the data dir with a flag left the cache behind in the old
// tree. That is how a run pointed at an empty directory came back with cached
// answers.
//
// Naming the cache dir is what stops it following, and set is what knows
// whether it was named: the same lookup the config file path uses, so a cache
// dir pinned by CCRAWL_CACHE_DIR or by a cache_dir line stays pinned whichever
// way the data dir moved.
func cacheDirFor(kc kit.Config, base ccrawl.Config, set *settings) string {
	if _, _, named := set.value("cache_dir"); named {
		return kc.CacheDir
	}
	if kc.DataDir == base.DataDir {
		return kc.CacheDir
	}
	return filepath.Join(kc.DataDir, "cache")
}

// StartRun wires up everything a long running command reports through: the
// journal on disk, the progress mode on stderr, and the metrics endpoint. Every
// such command calls it the same way, so --progress, --journal and
// --metrics-addr mean the same thing everywhere.
//
// defaultJournal is where the journal goes when --journal was not given. Pass ""
// for the commands where a journal only makes sense if it was asked for.
//
// The returned stop closes the journal and the metrics server, and must be
// called when the run ends.
func (a *App) StartRun(pipeline, defaultJournal string) (*ccrawl.RunReporter, func(), error) {
	mode, err := ccrawl.ParseProgressMode(a.Progress, stderrTTY())
	if err != nil {
		return nil, nil, usageErr(err.Error())
	}
	path := a.JournalPath
	if path == "" {
		path = defaultJournal
	}
	journal, err := ccrawl.OpenJournal(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open run journal: %w", err)
	}

	var metrics *ccrawl.Metrics
	var srv *http.Server
	if a.MetricsAddr != "" {
		metrics = ccrawl.NewMetrics()
		// Bind now, so a busy port fails the command here rather than an hour in.
		if srv, err = ccrawl.ServeMetrics(a.MetricsAddr, metrics); err != nil {
			_ = journal.Close()
			return nil, nil, err
		}
	}

	rep := ccrawl.NewRunReporter(pipeline, mode, journal, metrics)
	rep.SetOutput(cmdErr) // the swappable stderr, so tests can capture progress
	return rep, func() {
		if srv != nil {
			_ = srv.Close()
		}
		_ = journal.Close()
	}, nil
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

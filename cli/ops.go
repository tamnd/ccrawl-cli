package cli

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/any-cli/kit/errs"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// registerOps registers the record-stream commands as kit operations. Each one
// declares its inputs and emits typed records; kit renders them in every format,
// applies --limit, tees them into --db, and exposes them over serve and mcp. The
// odd commands that do not fit this shape (byte fetches, the DuckDB console, the
// interactive shell) stay as escape-hatch cobra commands in Root.
func registerOps(app *kit.App) {
	registerSearch(app)
	registerCrawlsList(app)
	registerNewsList(app)
	registerRank(app)
	registerStats(app)
	registerHost(app)
	registerContentV2(app)
	registerIndex(app)
	registerSched(app)
	registerCrawl(app)
	registerServe(app)
}

// searchIn is the URL-index query. Out is any because search has three shapes:
// capture records, byte locations (--locations), and a page count (--pages).
type searchIn struct {
	App            *App     `kit:"inject"`
	Pattern        string   `kit:"arg" help:"page URL or wildcard pattern"`
	Match          string   `kit:"flag" help:"match type: exact|prefix|host|domain"`
	From           string   `kit:"flag" help:"earliest capture date (e.g. 2023 or 2023-06)"`
	To             string   `kit:"flag" help:"latest capture date"`
	At             string   `kit:"flag" help:"keep the capture nearest this date, per URL (cdx_toolkit --closest)"`
	Status         string   `kit:"flag" help:"keep only this HTTP status (e.g. 200)"`
	MIME           string   `kit:"flag,name=mime" help:"detected MIME filter"`
	Lang           string   `kit:"flag" help:"language filter (ISO-639-3)"`
	Filter         []string `kit:"flag" help:"raw CDX filter field:regex (repeatable)"`
	URLContains    string   `kit:"flag,name=url-contains" help:"keep only captures whose URL contains this substring"`
	URLNotContains string   `kit:"flag,name=url-not-contains" help:"skip captures whose URL contains this substring"`
	NoPushFilters  bool     `kit:"flag,name=no-push-filters" help:"keep the URL substring filters here instead of sending them to the index server"`
	Explain        bool     `kit:"flag" help:"print what the index server is asked for and what is filtered here"`
	Sort           string   `kit:"flag" help:"crawl visit order: newest|oldest"`
	Pages          bool     `kit:"flag" help:"print the result page count and exit"`
	Estimate       bool     `kit:"flag" help:"print an approximate record count per crawl and exit"`
	Locations      bool     `kit:"flag" help:"emit filename/offset/length records"`
	LatestOnly     bool     `kit:"flag,name=latest-only" help:"keep only the newest capture per URL"`
	Dedup          bool     `kit:"flag" help:"collapse captures with identical content digest"`
	MaxBuffer      int      `kit:"flag,name=max-buffer" help:"records --at, --latest-only and --dedup hold in memory before spilling to disk (default 5000000)"`
	Strict         bool     `kit:"flag" help:"fail the run if an index page cannot be read, rather than skipping it"`
}

// linesPerPage is the rough number of CDX records in one index page, used only
// for the --estimate count. There is no way to read it from the API without
// fetching a page; cdx_toolkit uses the same constant.
const linesPerPage = 3000

func registerSearch(app *kit.App) {
	handle(app, kit.OpMeta{
		Name:    "search",
		Group:   "read",
		Aliases: []string{"cdx"},
		Summary: "Query the URL index for captures of a URL",
		Long: `Search the Common Crawl URL index (CDX) for captures.

Match type is inferred from wildcards: "example.com/*" is a prefix search and
"*.example.com" matches the domain and its subdomains. Override with --match.

Examples:
  ccrawl search example.com/*                      captures under a path
  ccrawl search '*.example.com' --status 200       whole domain, only 200s
  ccrawl search example.com --match exact          one URL, every timestamp
  ccrawl search example.com -o url                 just the URLs, for a pipeline
  ccrawl search example.com --locations | ccrawl fetch -
  ccrawl search example.com -c all -n 50           every crawl, newest first
  ccrawl search example.com -c 6                   the newest 6 crawls
  ccrawl search example.com -c 2023                every crawl from 2023
  ccrawl search '*.example.com' --at 2022-06       the capture nearest a date, per URL
  ccrawl search '*.example.com' --estimate         approximate record count per crawl
  ccrawl search example.com/* --url-contains /blog/ only URLs with /blog/ in them
  ccrawl search example.com/* --url-contains /blog/ --explain  where the work happens`,
		Args: []kit.Arg{{Name: "url-or-pattern", Help: "page URL or wildcard pattern"}},
	}, func(ctx context.Context, in searchIn, emit func(any) error) (err error) {
		app := in.App
		lost := &pageLosses{cmd: "search", strict: in.Strict}
		// A run that emitted nothing and lost part of the query has not found
		// nothing, it has failed to look. Exit 3 says the index answered and had
		// no captures, and a script that branches on it deserves better than an
		// outage wearing that answer's clothes.
		emitted := 0
		inner := emit
		emit = func(v any) error {
			emitted++
			return inner(v)
		}
		defer func() {
			lost.report()
			if err == nil && emitted == 0 && lost.total() > 0 {
				err = fmt.Errorf("nothing came back and part of the query could not be read, so this is not an empty result")
				// The index server refuses connections for days at a time. When
				// that is the whole story the run is worth repeating later, and
				// exit 8 is how a supervisor is told so. Exit 1 would be the code
				// for a command that is wrong, which this one is not.
				if lost.everyLossWasTransport() {
					err = &errs.Error{Kind: errs.KindNetwork, Err: err}
				}
			}
		}()
		q := ccrawl.CDXQuery{
			URL: in.Pattern, Match: in.Match,
			From: in.From, To: in.To,
			Status: in.Status, MIME: in.MIME, Lang: in.Lang,
			Filter:         in.Filter,
			URLContains:    in.URLContains,
			URLNotContains: in.URLNotContains,
			NoPushFilters:  in.NoPushFilters,
			OnPageError:    lost.handler(),
		}
		crawls, err := app.AllCrawls(ctx)
		if err != nil {
			return err
		}
		switch in.Sort {
		case "", "newest":
			// crawls already arrive newest first
		case "oldest":
			for i, j := 0, len(crawls)-1; i < j; i, j = i+1, j-1 {
				crawls[i], crawls[j] = crawls[j], crawls[i]
			}
		default:
			return usageErr(fmt.Sprintf("invalid --sort %q (want newest or oldest)", in.Sort))
		}

		if in.Explain {
			fmt.Fprint(os.Stderr, explainSearch(crawls, q, localFilters(in)))
			defer func() {
				// Both forms: the round one to read, the exact one to compare
				// against the same query run with --no-push-filters.
				n := app.HTTP.CDXBytesRead()
				fmt.Fprintf(os.Stderr, "search: read %s from the index (%d bytes)\n", humanBytes(n), n)
			}()
		}

		// The page count is one request per crawl, and it is the same trade as a
		// page of results: a crawl whose count did not come back is one crawl
		// missing from the total, not a reason to have no total at all.
		countPages := func(id string) (int, bool, error) {
			n, err := ccrawl.CDXNumPages(ctx, app.HTTP, id, q)
			if err == nil {
				return n, true, nil
			}
			h := lost.handler()
			if h == nil {
				return 0, false, err
			}
			return 0, false, h(id, -1, err)
		}

		if in.Pages {
			total := 0
			for _, id := range crawls {
				n, ok, err := countPages(id)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
				total += n
			}
			return emit(pageCount{Pages: total})
		}

		if in.Estimate {
			grand := 0
			for _, id := range crawls {
				n, ok, err := countPages(id)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
				grand += n * linesPerPage
				if err := emit(estimateRow{Crawl: id, Pages: n, Records: n * linesPerPage}); err != nil {
					return err
				}
			}
			if len(crawls) > 1 {
				return emit(estimateRow{Crawl: "total", Pages: 0, Records: grand})
			}
			return nil
		}

		keep := func(r ccrawl.CDXRecord) bool {
			return urlKeep(r.URL, in.URLContains, in.URLNotContains)
		}

		emitRec := func(r ccrawl.CDXRecord) error {
			if in.Locations {
				return emit(r.Location())
			}
			return emit(r)
		}
		maxBuf := in.MaxBuffer
		if maxBuf <= 0 {
			maxBuf = ccrawl.DefaultCDXMaxBuffer
		}

		// --at picks, per URL, the single capture nearest the target date. The
		// index hands back each crawl sorted by urlkey, so the winner inside a
		// crawl is decided as the crawl is read and the crawls are merged at the
		// end, which costs a urlkey group per crawl rather than a map the size
		// of the result.
		if in.At != "" {
			target := looseTS(in.At)
			picker := ccrawl.NewCDXPicker(func(cand, cur ccrawl.CDXRecord) bool {
				return absDiff(cand.Timestamp, target) < absDiff(cur.Timestamp, target)
			}, maxBuf)
			defer func() { _ = picker.Close() }()
			for _, id := range crawls {
				if err := ccrawl.CDXStream(ctx, app.HTTP, id, q, func(r ccrawl.CDXRecord) error {
					if !keep(r) {
						return nil
					}
					return picker.Add(r)
				}); err != nil {
					return err
				}
				if err := picker.EndStream(); err != nil {
					return err
				}
			}
			if picker.Spilled() {
				fmt.Fprintf(os.Stderr, "search: --at passed --max-buffer %d records and is using temporary files, which is why this is slow\n", maxBuf)
			}

			// Winners have always come out newest first, and that ordering needs
			// the whole set in hand. Up to the buffer it still does. Past it the
			// result goes out in index order, which is the only ordering left
			// that does not mean holding everything.
			var recs []ccrawl.CDXRecord
			streaming := false
			if err := picker.Each(func(r ccrawl.CDXRecord) error {
				if streaming {
					return emitRec(r)
				}
				recs = append(recs, r)
				if len(recs) < maxBuf {
					return nil
				}
				fmt.Fprintf(os.Stderr, "search: --at result passed --max-buffer %d records, emitting in index order rather than newest first\n", maxBuf)
				streaming = true
				for _, buffered := range recs {
					if err := emitRec(buffered); err != nil {
						return err
					}
				}
				recs = nil
				return nil
			}); err != nil {
				return err
			}
			if streaming {
				return nil
			}
			sort.Slice(recs, func(i, j int) bool { return recs[i].Timestamp > recs[j].Timestamp })
			if app.Limit > 0 && len(recs) > app.Limit {
				recs = recs[:app.Limit]
			}
			for _, r := range recs {
				if err := emitRec(r); err != nil {
					return err
				}
			}
			return nil
		}

		// --latest-only keeps the first capture of a URL, and this output
		// streams, so the check cannot wait for the end. Each crawl's emitted
		// URLs are written down in urlkey order and the next crawl is checked
		// against them with a cursor that only moves forward.
		var urlLog *ccrawl.CDXURLLog
		if in.LatestOnly {
			urlLog = ccrawl.NewCDXURLLog(maxBuf)
			defer func() { _ = urlLog.Close() }()
		}
		var digests *ccrawl.CDXDigestSet
		warnedDedup := false
		if in.Dedup {
			digests = ccrawl.NewCDXDigestSet(maxBuf)
		}
		send := func(r ccrawl.CDXRecord) error {
			if !keep(r) {
				return nil
			}
			if urlLog != nil {
				seen, err := urlLog.Seen(r)
				if err != nil {
					return err
				}
				if seen {
					return nil
				}
				if err := urlLog.Emitted(r); err != nil {
					return err
				}
			}
			if digests != nil {
				if r.Digest != "" && digests.Add(r.Digest) {
					return nil
				}
				if digests.Evicted() && !warnedDedup {
					warnedDedup = true
					fmt.Fprintf(os.Stderr, "search: --dedup passed --max-buffer %d digests and is forgetting the oldest, so some duplicates will get through\n", maxBuf)
				}
			}
			return emitRec(r)
		}
		for i, id := range crawls {
			if err := ccrawl.CDXStream(ctx, app.HTTP, id, q, send); err != nil {
				return err // a real error, or kit's stop sentinel once --limit is hit
			}
			if urlLog != nil && i < len(crawls)-1 {
				if err := urlLog.EndCrawl(); err != nil {
					return err
				}
			}
		}
		if urlLog != nil && urlLog.Spilled() {
			fmt.Fprintf(os.Stderr, "search: --latest-only passed --max-buffer %d URLs and used temporary files\n", maxBuf)
		}
		return nil
	})
}

// explainSearch says where each part of a query runs. The URL substring filters
// are regexes the index server can apply, so they go on the wire and the pages
// come back already thinned. The rest, which is anything that has to compare one
// record against another, can only happen here.
//
// The request it prints is the real one, minus the page parameter: curl it and
// the rows that come back are the rows the command reads.
func explainSearch(crawls []string, q ccrawl.CDXQuery, local []string) string {
	var b strings.Builder
	shown := crawls
	if len(shown) > 3 {
		shown = shown[:3]
	}
	fmt.Fprintf(&b, "search: %s: %s", plural(len(crawls), "crawl"), strings.Join(shown, ", "))
	if len(shown) < len(crawls) {
		fmt.Fprintf(&b, " and %d more", len(crawls)-len(shown))
	}
	b.WriteString("\n")
	if len(crawls) > 0 {
		fmt.Fprintf(&b, "search: the index server answers %s\n", ccrawl.CDXRequestURL(crawls[0], q))
	}
	pushed := pushedFilters(q)
	if len(pushed) == 0 {
		b.WriteString("search: pushed to the server: nothing beyond the URL pattern\n")
	} else {
		fmt.Fprintf(&b, "search: pushed to the server: %s\n", strings.Join(pushed, ", "))
	}
	if len(local) == 0 {
		b.WriteString("search: applied here: nothing, the server does all of it\n")
	} else {
		fmt.Fprintf(&b, "search: applied here: %s\n", strings.Join(local, ", "))
	}
	return b.String()
}

// pushedFilters names the flags that became server side filters, in the words
// the user typed them, rather than in the regex they turned into.
func pushedFilters(q ccrawl.CDXQuery) []string {
	var out []string
	if q.Status != "" {
		out = append(out, "--status "+q.Status)
	}
	if q.MIME != "" {
		out = append(out, "--mime "+q.MIME)
	}
	if q.Lang != "" {
		out = append(out, "--lang "+q.Lang)
	}
	if !q.NoPushFilters {
		if q.URLContains != "" {
			out = append(out, "--url-contains "+q.URLContains)
		}
		if q.URLNotContains != "" {
			out = append(out, "--url-not-contains "+q.URLNotContains)
		}
	}
	for _, f := range q.Filter {
		out = append(out, "--filter "+f)
	}
	return out
}

// localFilters names the work that stays on this machine. The URL substrings
// are listed even when they were pushed, because they are checked again on the
// way past, which is free and means a server that filters differently from us
// cannot widen the result.
func localFilters(in searchIn) []string {
	var out []string
	if in.URLContains != "" {
		out = append(out, "--url-contains "+in.URLContains+localAgain(in.NoPushFilters))
	}
	if in.URLNotContains != "" {
		out = append(out, "--url-not-contains "+in.URLNotContains+localAgain(in.NoPushFilters))
	}
	if in.At != "" {
		out = append(out, "--at "+in.At)
	}
	if in.LatestOnly {
		out = append(out, "--latest-only")
	}
	if in.Dedup {
		out = append(out, "--dedup")
	}
	return out
}

func localAgain(noPush bool) string {
	if noPush {
		return ""
	}
	return " (again, on what the server sent)"
}

// urlKeep reports whether a URL passes the --url-contains / --url-not-contains
// post-filters: it must contain the include substring (when set) and must not
// contain the exclude substring (when set).
func urlKeep(url, contains, notContains string) bool {
	if contains != "" && !strings.Contains(url, contains) {
		return false
	}
	if notContains != "" && strings.Contains(url, notContains) {
		return false
	}
	return true
}

// estimateRow is one line of the --estimate breakdown: the CDX page count for a
// crawl and an approximate record count (pages * linesPerPage).
type estimateRow struct {
	Crawl   string `json:"crawl" table:"crawl"`
	Pages   int    `json:"pages" table:"pages"`
	Records int    `json:"records" table:"records"`
}

// pageCount is the single record search emits in --pages mode.
type pageCount struct {
	Pages int `json:"pages" table:"pages"`
}

type crawlsListIn struct {
	App *App `kit:"inject"`
}

func registerCrawlsList(app *kit.App) {
	handle(app, kit.OpMeta{
		Name:    "list",
		Parent:  "crawls",
		Summary: "List every available crawl",
	}, func(ctx context.Context, in crawlsListIn, emit func(ccrawl.Crawl) error) error {
		crawls, err := ccrawl.ListCrawls(ctx, in.App.HTTP, in.App.Cache)
		if err != nil {
			return err
		}
		for _, cr := range crawls {
			if err := emit(cr); err != nil {
				return err
			}
		}
		return nil
	})
}

type newsListIn struct {
	App   *App `kit:"inject"`
	Year  int  `kit:"flag" help:"year (0 = all)"`
	Month int  `kit:"flag" help:"month (0 = all months of the year)"`
}

func registerNewsList(app *kit.App) {
	handle(app, kit.OpMeta{
		Name:    "list",
		Parent:  "news",
		Summary: "List CC-NEWS WARC files for a month",
	}, func(ctx context.Context, in newsListIn, emit func(ccrawl.NewsFile) error) error {
		files, err := ccrawl.ListNewsFiles(ctx, in.App.HTTP, in.Year, in.Month)
		if err != nil {
			return err
		}
		for _, f := range files {
			if err := emit(f); err != nil {
				return err
			}
		}
		return nil
	})
}

type rankLookupIn struct {
	App   *App   `kit:"inject"`
	Key   string `kit:"arg" help:"host or domain"`
	Table string `kit:"flag" help:"location of a gzipped rank table, as a URL (default: the release's own table)"`
	Graph string `kit:"flag" help:"release ID of the web graph (default: latest)"`
}

type rankTopIn struct {
	App   *App   `kit:"inject"`
	Table string `kit:"flag" help:"location of a gzipped rank table, as a URL (default: the release's own table)"`
	Graph string `kit:"flag" help:"release ID of the web graph (default: latest)"`
	TLD   string `kit:"flag,name=tld" help:"restrict to a TLD"`
	Limit int    `kit:"flag,inherit" name:"limit"`
}

type rankAllIn struct {
	App   *App   `kit:"inject"`
	Table string `kit:"flag" help:"location of a gzipped rank table, as a URL (default: the release's own table)"`
	Graph string `kit:"flag" help:"release ID of the web graph (default: latest)"`
	TLD   string `kit:"flag,name=tld" help:"restrict to a TLD"`
}

// rankTable is the table a rank command reads. A URL given with --table wins,
// then the release named by --graph, then the newest release published.
//
// The domain ranks resolve through their own call, because a release is listed
// as soon as its host tables land and its domain table follows later, so the
// newest release is often the wrong answer for a domain lookup.
func rankTable(ctx context.Context, app *App, table, graphID string, domain bool) (string, error) {
	if table != "" {
		return table, nil
	}
	if graphID == "" && domain {
		g, err := ccrawl.LatestDomainWebGraph(ctx, app.HTTP, app.Cache)
		if err != nil {
			return "", err
		}
		return g.DomainRankURL(), nil
	}
	g, err := resolveGraph(ctx, app, graphID)
	if err != nil {
		return "", err
	}
	if domain {
		return g.DomainRankURL(), nil
	}
	return g.HostRankURL(), nil
}

func registerRank(app *kit.App) {
	app.CommandGroup("rank", "Look up host and domain ranks from the web graph")

	lookup := func(domain bool) func(context.Context, rankLookupIn, func(ccrawl.Rank) error) error {
		return func(ctx context.Context, in rankLookupIn, emit func(ccrawl.Rank) error) error {
			table, err := rankTable(ctx, in.App, in.Table, in.Graph, domain)
			if err != nil {
				return err
			}
			r, err := ccrawl.RankLookup(ctx, in.App.HTTP, table, in.Key)
			if err != nil {
				return err
			}
			return emit(r)
		}
	}
	handle(app, kit.OpMeta{
		Name: "domain", Parent: "rank", Single: true,
		Summary: "Rank of a registered domain",
		Args:    []kit.Arg{{Name: "domain"}},
	}, lookup(true))
	handle(app, kit.OpMeta{
		Name: "host", Parent: "rank", Single: true,
		Summary: "Rank of a host",
		Args:    []kit.Arg{{Name: "host"}},
	}, lookup(false))

	handle(app, kit.OpMeta{
		Name: "top", Parent: "rank",
		Summary: "Top-ranked hosts or domains",
	}, func(ctx context.Context, in rankTopIn, emit func(ccrawl.Rank) error) error {
		table, err := rankTable(ctx, in.App, in.Table, in.Graph, false)
		if err != nil {
			return err
		}
		n := in.Limit
		if n == 0 {
			n = 50
		}
		ranks, err := ccrawl.RankTop(ctx, in.App.HTTP, table, in.TLD, n)
		if err != nil {
			return err
		}
		for _, r := range ranks {
			if err := emit(r); err != nil {
				return err
			}
		}
		return nil
	})

	handle(app, kit.OpMeta{
		Name: "all", Parent: "rank",
		Summary: "Stream every host from a rank table",
		Long: `Stream all hosts from a Common Crawl web-graph rank table.

The table is the host ranks of the newest web-graph release, unless --graph
names a release or --table gives a URL outright. It is sorted by harmonic
centrality (most central first). Use --tld to restrict output to a single
top-level domain, and --limit to cap the row count.

Examples:
  ccrawl rank all -n 1000
  ccrawl rank all --tld com -n 1000
  ccrawl rank all --graph cc-main-2026-mar-apr-may -o jsonl > hosts.jsonl
  ccrawl rank all --table https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-10/host/cc-main-2024-10-host-ranks.txt.gz`,
	}, func(ctx context.Context, in rankAllIn, emit func(ccrawl.Rank) error) error {
		table, err := rankTable(ctx, in.App, in.Table, in.Graph, false)
		if err != nil {
			return err
		}
		return ccrawl.RankStream(ctx, in.App.HTTP, table, in.TLD, emit)
	})
}

// statRow is one line of the crawl shape summary.
type statRow struct {
	Crawl string `json:"crawl" table:"crawl"`
	Kind  string `json:"kind" table:"kind"`
	Files int    `json:"files" table:"files"`
}

type statsIn struct {
	App   *App     `kit:"inject"`
	Kinds []string `kit:"flag" help:"archive kinds to count (default warc,wat,wet,robotstxt,non200responses,cc-index-table)"`
}

// statKinds are the manifests counted when --kinds says nothing. One list for
// both spellings of this command: stats and crawls info used to carry a default
// each, differing by one kind in each direction, which meant the same question
// got two answers depending on which name you happened to type.
var statKinds = []string{"warc", "wat", "wet", "robotstxt", "non200responses", "cc-index-table"}

// crawlsInfoIn is statsIn plus the positional crawl that spelling has always
// accepted.
type crawlsInfoIn struct {
	App   *App     `kit:"inject"`
	ID    string   `kit:"arg" help:"crawl reference, defaults to -c"`
	Kinds []string `kit:"flag" help:"archive kinds to count (default warc,wat,wet,robotstxt,non200responses,cc-index-table)"`
}

func registerStats(app *kit.App) {
	long := `Summarise a crawl by counting the files in each published manifest (warc, wat,
wet, robotstxt, non200responses, cc-index-table). This reads the small *.paths.gz
manifests, not the archives themselves, so it is quick and cheap.

Examples:
  ccrawl stats                 the latest crawl
  ccrawl stats -c 2024-51      a specific crawl
  ccrawl stats --kinds warc,wet`

	count := func(ctx context.Context, app *App, ref string, kinds []string, emit func(statRow) error) error {
		id, err := app.Crawl(ctx)
		if ref != "" {
			id, err = ccrawl.ResolveCrawl(ctx, app.HTTP, app.Cache, ref)
		}
		if err != nil {
			return err
		}
		if len(kinds) == 0 {
			kinds = statKinds
		}
		for _, kind := range kinds {
			paths, err := ccrawl.FetchPaths(ctx, app.HTTP, app.Cache, id, kind)
			if err != nil {
				if err := emit(statRow{Crawl: id, Kind: kind, Files: -1}); err != nil {
					return err
				}
				continue
			}
			if err := emit(statRow{Crawl: id, Kind: kind, Files: len(paths)}); err != nil {
				return err
			}
		}
		return nil
	}

	handle(app, kit.OpMeta{
		Name:    "stats",
		Group:   "read",
		Summary: "Show the shape of a crawl: file counts per archive kind",
		Long:    long,
	}, func(ctx context.Context, in statsIn, emit func(statRow) error) error {
		return count(ctx, in.App, "", in.Kinds, emit)
	})

	// crawls info is the same question asked from the crawls parent. It used to
	// be an escape-hatch command printing its own text, which meant -o was
	// accepted and ignored; registering it as an operation is what puts the
	// renderer back in the path. The positional crawl it has always taken stays,
	// so a command someone has in a script keeps working.
	handle(app, kit.OpMeta{
		Name:    "info",
		Parent:  "crawls",
		Summary: "Show details for a crawl: file counts per archive kind",
		Long:    long,
		Args:    []kit.Arg{{Name: "id", Help: "crawl reference, defaults to -c", Optional: true}},
	}, func(ctx context.Context, in crawlsInfoIn, emit func(statRow) error) error {
		return count(ctx, in.App, in.ID, in.Kinds, emit)
	})
}

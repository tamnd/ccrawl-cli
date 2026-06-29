package cli

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/tamnd/any-cli/kit"
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
	Pattern        string   `kit:"arg" help:"URL or wildcard pattern"`
	Match          string   `kit:"flag" help:"match type: exact|prefix|host|domain"`
	From           string   `kit:"flag" help:"earliest capture date (e.g. 2023 or 2023-06)"`
	To             string   `kit:"flag" help:"latest capture date"`
	At             string   `kit:"flag" help:"keep the capture nearest this date, per URL (cdx_toolkit --closest)"`
	Status         string   `kit:"flag" help:"HTTP status filter (e.g. 200)"`
	MIME           string   `kit:"flag,name=mime" help:"detected MIME filter"`
	Lang           string   `kit:"flag" help:"language filter (ISO-639-3)"`
	Filter         []string `kit:"flag" help:"raw CDX filter field:regex (repeatable)"`
	URLContains    string   `kit:"flag,name=url-contains" help:"keep only captures whose URL contains this substring"`
	URLNotContains string   `kit:"flag,name=url-not-contains" help:"skip captures whose URL contains this substring"`
	Sort           string   `kit:"flag" help:"crawl visit order: newest|oldest"`
	Pages          bool     `kit:"flag" help:"print the result page count and exit"`
	Estimate       bool     `kit:"flag" help:"print an approximate record count per crawl and exit"`
	Locations      bool     `kit:"flag" help:"emit filename/offset/length records"`
	LatestOnly     bool     `kit:"flag,name=latest-only" help:"keep only the newest capture per URL"`
	Dedup          bool     `kit:"flag" help:"collapse captures with identical content digest"`
}

// linesPerPage is the rough number of CDX records in one index page, used only
// for the --estimate count. There is no way to read it from the API without
// fetching a page; cdx_toolkit uses the same constant.
const linesPerPage = 3000

func registerSearch(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
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
  ccrawl search example.com/* --url-contains /blog/ filter URLs after the query`,
		Args: []kit.Arg{{Name: "url-or-pattern", Help: "URL or wildcard pattern"}},
	}, func(ctx context.Context, in searchIn, emit func(any) error) error {
		app := in.App
		q := ccrawl.CDXQuery{
			URL: in.Pattern, Match: in.Match,
			From: in.From, To: in.To,
			Status: in.Status, MIME: in.MIME, Lang: in.Lang,
			Filter: in.Filter,
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

		if in.Pages {
			total := 0
			for _, id := range crawls {
				n, err := ccrawl.CDXNumPages(ctx, app.HTTP, id, q)
				if err != nil {
					return err
				}
				total += n
			}
			return emit(pageCount{Pages: total})
		}

		if in.Estimate {
			grand := 0
			for _, id := range crawls {
				n, err := ccrawl.CDXNumPages(ctx, app.HTTP, id, q)
				if err != nil {
					return err
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

		// --at picks, per URL, the single capture nearest the target date. That
		// needs every candidate in hand, so it buffers the nearest seen per URL
		// across all crawls and emits at the end rather than streaming.
		if in.At != "" {
			target := looseTS(in.At)
			nearest := map[string]ccrawl.CDXRecord{}
			collect := func(r ccrawl.CDXRecord) error {
				if !keep(r) {
					return nil
				}
				cur, ok := nearest[r.URL]
				if !ok || absDiff(r.Timestamp, target) < absDiff(cur.Timestamp, target) {
					nearest[r.URL] = r
				}
				return nil
			}
			for _, id := range crawls {
				if err := ccrawl.CDXStream(ctx, app.HTTP, id, ccrawl.CDXQuery{
					URL: q.URL, Match: q.Match, From: q.From, To: q.To,
					Status: q.Status, MIME: q.MIME, Lang: q.Lang, Filter: q.Filter,
				}, collect); err != nil {
					return err
				}
			}
			recs := make([]ccrawl.CDXRecord, 0, len(nearest))
			for _, r := range nearest {
				recs = append(recs, r)
			}
			sort.Slice(recs, func(i, j int) bool { return recs[i].Timestamp > recs[j].Timestamp })
			if app.Limit > 0 && len(recs) > app.Limit {
				recs = recs[:app.Limit]
			}
			for _, r := range recs {
				if in.Locations {
					if err := emit(r.Location()); err != nil {
						return err
					}
					continue
				}
				if err := emit(r); err != nil {
					return err
				}
			}
			return nil
		}

		seenURL := map[string]bool{}
		seenDigest := map[string]bool{}
		send := func(r ccrawl.CDXRecord) error {
			if !keep(r) {
				return nil
			}
			if in.LatestOnly {
				if seenURL[r.URL] {
					return nil
				}
				seenURL[r.URL] = true
			}
			if in.Dedup {
				if r.Digest != "" && seenDigest[r.Digest] {
					return nil
				}
				seenDigest[r.Digest] = true
			}
			if in.Locations {
				return emit(r.Location())
			}
			return emit(r)
		}
		for _, id := range crawls {
			if err := ccrawl.CDXStream(ctx, app.HTTP, id, q, send); err != nil {
				return err // a real error, or kit's stop sentinel once --limit is hit
			}
		}
		return nil
	})
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
	kit.Handle(app, kit.OpMeta{
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
	kit.Handle(app, kit.OpMeta{
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
	Table string `kit:"flag" help:"URL of a gzipped rank table"`
}

type rankTopIn struct {
	App   *App   `kit:"inject"`
	Table string `kit:"flag" help:"URL of a gzipped rank table"`
	TLD   string `kit:"flag,name=tld" help:"restrict to a TLD"`
	Limit int    `kit:"flag,inherit" name:"limit"`
}

type rankAllIn struct {
	App   *App   `kit:"inject"`
	Table string `kit:"flag" help:"URL of a gzipped rank table"`
	TLD   string `kit:"flag,name=tld" help:"restrict to a TLD"`
}

func registerRank(app *kit.App) {
	app.CommandGroup("rank", "Look up host and domain ranks from the web graph")

	lookup := func(ctx context.Context, in rankLookupIn, emit func(ccrawl.Rank) error) error {
		if in.Table == "" {
			return usageErr("--table is required (URL of a gzipped rank table)")
		}
		r, err := ccrawl.RankLookup(ctx, in.App.HTTP, in.Table, in.Key)
		if err != nil {
			return err
		}
		return emit(r)
	}
	kit.Handle(app, kit.OpMeta{
		Name: "domain", Parent: "rank", Single: true,
		Summary: "Rank of a registered domain",
		Args:    []kit.Arg{{Name: "domain"}},
	}, lookup)
	kit.Handle(app, kit.OpMeta{
		Name: "host", Parent: "rank", Single: true,
		Summary: "Rank of a host",
		Args:    []kit.Arg{{Name: "host"}},
	}, lookup)

	kit.Handle(app, kit.OpMeta{
		Name: "top", Parent: "rank",
		Summary: "Top-ranked hosts or domains",
	}, func(ctx context.Context, in rankTopIn, emit func(ccrawl.Rank) error) error {
		if in.Table == "" {
			return usageErr("--table is required (URL of a gzipped rank table)")
		}
		n := in.Limit
		if n == 0 {
			n = 50
		}
		ranks, err := ccrawl.RankTop(ctx, in.App.HTTP, in.Table, in.TLD, n)
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

	kit.Handle(app, kit.OpMeta{
		Name: "all", Parent: "rank",
		Summary: "Stream every host from a rank table",
		Long: `Stream all hosts from a Common Crawl web-graph rank table.

The table is sorted by harmonic centrality (most central first). Use --tld to
restrict output to a single top-level domain, and --limit to cap the row count.

Examples:
  ccrawl rank all --table https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-10/host/cc-main-2024-10-host-rank.txt.gz
  ccrawl rank all --table <url> --tld com -n 1000
  ccrawl rank all --table <url> -o jsonl > hosts.jsonl`,
	}, func(ctx context.Context, in rankAllIn, emit func(ccrawl.Rank) error) error {
		if in.Table == "" {
			return usageErr("--table is required (URL of a gzipped rank table)")
		}
		return ccrawl.RankStream(ctx, in.App.HTTP, in.Table, in.TLD, emit)
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
	Kinds []string `kit:"flag" help:"archive kinds to count (default warc,wat,wet,robotstxt,non200responses)"`
}

func registerStats(app *kit.App) {
	kit.Handle(app, kit.OpMeta{
		Name:    "stats",
		Group:   "read",
		Summary: "Show the shape of a crawl: file counts per archive kind",
		Long: `Summarise a crawl by counting the files in each published manifest (warc, wat,
wet, robotstxt, non200responses). This reads the small *.paths.gz manifests, not
the archives themselves, so it is quick and cheap.

Examples:
  ccrawl stats                 the latest crawl
  ccrawl stats -c 2024-51      a specific crawl
  ccrawl stats --kinds warc,wet`,
	}, func(ctx context.Context, in statsIn, emit func(statRow) error) error {
		id, err := in.App.Crawl(ctx)
		if err != nil {
			return err
		}
		kinds := in.Kinds
		if len(kinds) == 0 {
			kinds = []string{"warc", "wat", "wet", "robotstxt", "non200responses"}
		}
		for _, kind := range kinds {
			paths, err := ccrawl.FetchPaths(ctx, in.App.HTTP, in.App.Cache, id, kind)
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
	})
}

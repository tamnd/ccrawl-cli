package cli

import (
	"github.com/spf13/cobra"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

type searchFlags struct {
	match      string
	from, to   string
	status     string
	mime       string
	lang       string
	filter     []string
	pages      bool
	locations  bool
	latestOnly bool
	dedup      bool
}

func newSearchCmd(app *App) *cobra.Command {
	sf := &searchFlags{}
	cmd := &cobra.Command{
		Use:     "search <url-or-pattern>",
		Aliases: []string{"index", "cdx"},
		Short:   "Query the URL index for captures of a URL",
		Long: `Search the Common Crawl URL index (CDX) for captures.

Match type is inferred from wildcards: "example.com/*" is a prefix search and
"*.example.com" matches the domain and its subdomains. Override with --match.

Examples:
  ccrawl search example.com/*                      captures under a path
  ccrawl search '*.example.com' --status 200       whole domain, only 200s
  ccrawl search example.com --match exact          one URL, every timestamp
  ccrawl search example.com -o url                 just the URLs, for a pipeline
  ccrawl search example.com --locations | ccrawl fetch -
  ccrawl search example.com -c all -n 50           every crawl, newest first`,
		Args: cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runSearch(app, c, args[0], sf)
		},
	}
	f := cmd.Flags()
	f.StringVar(&sf.match, "match", "", "match type: exact|prefix|host|domain")
	f.StringVar(&sf.from, "from", "", "earliest capture date (e.g. 2023 or 2023-06)")
	f.StringVar(&sf.to, "to", "", "latest capture date")
	f.StringVar(&sf.status, "status", "", "HTTP status filter (e.g. 200)")
	f.StringVar(&sf.mime, "mime", "", "detected MIME filter")
	f.StringVar(&sf.lang, "lang", "", "language filter (ISO-639-3)")
	f.StringArrayVar(&sf.filter, "filter", nil, "raw CDX filter field:regex (repeatable)")
	f.BoolVar(&sf.pages, "pages", false, "print the result page count and exit")
	f.BoolVar(&sf.locations, "locations", false, "emit filename/offset/length records")
	f.BoolVar(&sf.latestOnly, "latest-only", false, "keep only the newest capture per URL")
	f.BoolVar(&sf.dedup, "dedup", false, "collapse captures with identical content digest")
	return cmd
}

func runSearch(app *App, c *cobra.Command, pattern string, sf *searchFlags) error {
	ctx := c.Context()
	q := ccrawl.CDXQuery{
		URL: pattern, Match: sf.match,
		From: sf.from, To: sf.to,
		Status: sf.status, MIME: sf.mime, Lang: sf.lang,
		Filter: sf.filter, Limit: app.Cfg.Workers * 0, // limit applied below
	}
	q.Limit = limitFrom(app)

	crawls, err := app.AllCrawls(ctx)
	if err != nil {
		return err
	}

	if sf.pages {
		total := 0
		for _, id := range crawls {
			n, err := ccrawl.CDXNumPages(ctx, app.HTTP, id, q)
			if err != nil {
				return err
			}
			total += n
		}
		if err := app.Out.Emit(Row{Cols: []string{"pages"}, Vals: []string{itoa(total)}, Value: map[string]int{"pages": total}}); err != nil {
			return err
		}
		return app.Out.Flush()
	}

	seenURL := map[string]bool{}
	seenDigest := map[string]bool{}
	count := 0
	emit := func(r ccrawl.CDXRecord) error {
		if sf.latestOnly {
			if seenURL[r.URL] {
				return nil
			}
			seenURL[r.URL] = true
		}
		if sf.dedup {
			if r.Digest != "" && seenDigest[r.Digest] {
				return nil
			}
			seenDigest[r.Digest] = true
		}
		count++
		if sf.locations {
			return app.Out.Emit(locationRow(r))
		}
		return app.Out.Emit(cdxRow(r))
	}

	for _, id := range crawls {
		if q.Limit > 0 && count >= q.Limit {
			break
		}
		qq := q
		if q.Limit > 0 {
			qq.Limit = q.Limit - count
		}
		if err := ccrawl.CDXStream(ctx, app.HTTP, id, qq, emit); err != nil {
			return err
		}
	}
	if err := app.Out.Flush(); err != nil {
		return err
	}
	if count == 0 {
		return noResults("no captures found for " + pattern)
	}
	return nil
}

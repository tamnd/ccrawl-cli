package cli

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// crawlsEscapeHatches returns the crawls verbs that emit plain scalars rather
// than record streams, so they attach under the crawls parent next to the list
// operation. The list verb itself is a kit operation (see registerCrawlsList).
func crawlsEscapeHatches() []*cobra.Command {
	return []*cobra.Command{
		{
			Use:   "latest",
			Short: "Print the newest crawl ID",
			RunE: func(c *cobra.Command, _ []string) error {
				app, err := appFromCtx(c.Context())
				if err != nil {
					return err
				}
				crawls, err := ccrawl.ListCrawls(c.Context(), app.HTTP, app.Cache)
				if err != nil {
					return err
				}
				if len(crawls) == 0 {
					return noResults("no crawls available")
				}
				_, _ = fmt.Fprintln(cmdOut, crawls[0].ID)
				return nil
			},
		},
		{
			Use:   "resolve <ref>",
			Short: "Resolve a loose crawl reference to its canonical ID",
			Args:  cobra.ExactArgs(1),
			RunE: func(c *cobra.Command, args []string) error {
				app, err := appFromCtx(c.Context())
				if err != nil {
					return err
				}
				id, err := ccrawl.ResolveCrawl(c.Context(), app.HTTP, app.Cache, args[0])
				if err != nil {
					return err
				}
				_, _ = fmt.Fprintln(cmdOut, id)
				return nil
			},
		},
		{
			Use:   "info <id>",
			Short: "Show details for a crawl (file counts per format)",
			Args:  cobra.MaximumNArgs(1),
			RunE: func(c *cobra.Command, args []string) error {
				app, err := appFromCtx(c.Context())
				if err != nil {
					return err
				}
				return runCrawlsInfo(app, c, args)
			},
		},
	}
}

func runCrawlsInfo(app *App, c *cobra.Command, args []string) error {
	ref := app.Cfg.CrawlID
	if len(args) == 1 {
		ref = args[0]
	}
	id, err := ccrawl.ResolveCrawl(c.Context(), app.HTTP, app.Cache, ref)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmdOut, "Crawl: %s\n", id)
	for _, kind := range []string{"warc", "wat", "wet", "robotstxt", "cc-index-table"} {
		paths, err := ccrawl.FetchPaths(c.Context(), app.HTTP, app.Cache, id, kind)
		if err != nil {
			_, _ = fmt.Fprintf(cmdOut, "  %-16s (unavailable)\n", kind)
			continue
		}
		_, _ = fmt.Fprintf(cmdOut, "  %-16s %d files\n", kind, len(paths))
	}
	return nil
}

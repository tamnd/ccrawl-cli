package cli

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func newCrawlsCmd(app *App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "crawls",
		Short: "Discover Common Crawl collections",
		Long:  "List, resolve, and inspect the monthly Common Crawl collections.",
		RunE:  func(c *cobra.Command, _ []string) error { return runCrawlsList(app, c) },
	}
	cmd.AddCommand(
		&cobra.Command{
			Use:   "list",
			Short: "List every available crawl",
			RunE:  func(c *cobra.Command, _ []string) error { return runCrawlsList(app, c) },
		},
		&cobra.Command{
			Use:   "latest",
			Short: "Print the newest crawl ID",
			RunE: func(c *cobra.Command, _ []string) error {
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
		&cobra.Command{
			Use:   "resolve <ref>",
			Short: "Resolve a loose crawl reference to its canonical ID",
			Args:  cobra.ExactArgs(1),
			RunE: func(c *cobra.Command, args []string) error {
				id, err := ccrawl.ResolveCrawl(c.Context(), app.HTTP, app.Cache, args[0])
				if err != nil {
					return err
				}
				_, _ = fmt.Fprintln(cmdOut, id)
				return nil
			},
		},
		&cobra.Command{
			Use:   "info <id>",
			Short: "Show details for a crawl (file counts per format)",
			Args:  cobra.MaximumNArgs(1),
			RunE:  func(c *cobra.Command, args []string) error { return runCrawlsInfo(app, c, args) },
		},
	)
	return cmd
}

func runCrawlsList(app *App, c *cobra.Command) error {
	crawls, err := ccrawl.ListCrawls(c.Context(), app.HTTP, app.Cache)
	if err != nil {
		return err
	}
	for i, cr := range crawls {
		if app.Limit > 0 && i >= app.Limit {
			break
		}
		if err := app.Out.Emit(crawlRow(cr)); err != nil {
			return err
		}
	}
	return app.Out.Flush()
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

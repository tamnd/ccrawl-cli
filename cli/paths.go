package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func newPathsCmd(app *App) *cobra.Command {
	var segment string
	var kinds bool

	cmd := &cobra.Command{
		Use:   "paths <kind>",
		Short: "List the archive file paths for a crawl",
		Long: `Print the file paths of a crawl, one per line, ready to pipe into download.

Kinds: warc, wat, wet, robotstxt, non200responses, cc-index, cc-index-table, segment.

Examples:
  ccrawl paths warc -c 2026-17               every WARC path for the crawl
  ccrawl paths wet -n 1 | ccrawl download -  download the first WET file
  ccrawl paths warc -o url                   full https URLs`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			if kinds {
				_, _ = fmt.Fprintln(cmdOut, strings.Join(ccrawl.PathKinds, "\n"))
				return nil
			}
			if len(args) == 0 {
				return usageErr("specify a kind (one of: " + strings.Join(ccrawl.PathKinds, ", ") + ")")
			}
			return runPaths(app, c, args[0], segment)
		},
	}
	cmd.Flags().StringVar(&segment, "segment", "", "only paths under this segment prefix")
	cmd.Flags().BoolVar(&kinds, "kinds", false, "list the available path kinds and exit")
	return cmd
}

func runPaths(app *App, c *cobra.Command, kind, segment string) error {
	ctx := c.Context()
	id, err := app.Crawl(ctx)
	if err != nil {
		return err
	}
	limit := app.Limit
	asURL := app.Out.format == "url"
	count := 0
	err = ccrawl.StreamPaths(ctx, app.HTTP, id, kind, func(p string) error {
		if segment != "" && !strings.Contains(p, segment) {
			return nil
		}
		if asURL {
			if _, err := fmt.Fprintln(cmdOut, ccrawl.FileURL(p, app.Cfg.Source)); err != nil {
				return err
			}
		} else {
			if _, err := fmt.Fprintln(cmdOut, p); err != nil {
				return err
			}
		}
		count++
		if limit > 0 && count >= limit {
			return errStopPaths
		}
		return nil
	})
	if err != nil && err != errStopPaths {
		return err
	}
	if count == 0 {
		return noResults("no paths for kind " + kind)
	}
	return nil
}

var errStopPaths = fmt.Errorf("stop")

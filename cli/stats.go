package cli

import (
	"github.com/spf13/cobra"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func newStatsCmd(app *App) *cobra.Command {
	var kinds []string
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Show the shape of a crawl: file counts per archive kind",
		Long: `Summarise a crawl by counting the files in each published manifest (warc, wat,
wet, robotstxt, non200responses). This reads the small *.paths.gz manifests, not
the archives themselves, so it is quick and cheap.

Examples:
  ccrawl stats                 the latest crawl
  ccrawl stats -c 2024-51      a specific crawl
  ccrawl stats --kinds warc,wet`,
		RunE: func(c *cobra.Command, _ []string) error {
			id, err := app.Crawl(c.Context())
			if err != nil {
				return err
			}
			if len(kinds) == 0 {
				kinds = []string{"warc", "wat", "wet", "robotstxt", "non200responses"}
			}
			for _, kind := range kinds {
				paths, err := ccrawl.FetchPaths(c.Context(), app.HTTP, app.Cache, id, kind)
				if err != nil {
					if err := app.Out.Emit(Row{
						Cols:  []string{"crawl", "kind", "files"},
						Vals:  []string{id, kind, "n/a"},
						Value: map[string]any{"crawl": id, "kind": kind, "files": nil},
					}); err != nil {
						return err
					}
					continue
				}
				if err := app.Out.Emit(Row{
					Cols:  []string{"crawl", "kind", "files"},
					Vals:  []string{id, kind, itoa(len(paths))},
					Value: map[string]any{"crawl": id, "kind": kind, "files": len(paths)},
				}); err != nil {
					return err
				}
			}
			return app.Out.Flush()
		},
	}
	cmd.Flags().StringSliceVar(&kinds, "kinds", nil, "archive kinds to count (default warc,wat,wet,robotstxt,non200responses)")
	return cmd
}

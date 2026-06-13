package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

func newCacheCmd(app *App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cache",
		Short: "Inspect and clear the on-disk cache",
		Long:  "ccrawl caches collinfo.json and the per-crawl path manifests so repeated commands stay fast. Manage that cache here.",
	}
	cmd.AddCommand(
		&cobra.Command{
			Use:   "dir",
			Short: "Print the cache directory",
			RunE: func(c *cobra.Command, _ []string) error {
				_, _ = fmt.Fprintln(cmdOut, app.Cache.Dir())
				return nil
			},
		},
		&cobra.Command{
			Use:   "info",
			Short: "Show cache size and entry count",
			RunE: func(c *cobra.Command, _ []string) error {
				n, size := cacheUsage(app.Cache.Dir())
				if err := app.Out.Emit(Row{
					Cols:  []string{"dir", "entries", "size"},
					Vals:  []string{app.Cache.Dir(), itoa(n), humanBytes(size)},
					Value: map[string]any{"dir": app.Cache.Dir(), "entries": n, "size": size},
				}); err != nil {
					return err
				}
				return app.Out.Flush()
			},
		},
		&cobra.Command{
			Use:   "clear",
			Short: "Remove every cached entry",
			RunE: func(c *cobra.Command, _ []string) error {
				if !confirm(app.yes, "clear the ccrawl cache?") {
					return usageErr("cancelled")
				}
				n, err := app.Cache.Clear()
				if err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmdErr, "removed %d cached entries\n", n)
				return nil
			},
		},
	)
	return cmd
}

func cacheUsage(dir string) (int, int64) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0
	}
	var n int
	var size int64
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".cache" {
			continue
		}
		n++
		if info, err := e.Info(); err == nil {
			size += info.Size()
		}
	}
	return n, size
}

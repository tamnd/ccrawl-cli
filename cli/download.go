package cli

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/spf13/cobra"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func newDownloadCmd(app *App) *cobra.Command {
	var outDir string
	var segment string
	var sample float64
	var flat bool

	cmd := &cobra.Command{
		Use:   "download <kind|->",
		Short: "Download archive files for a crawl",
		Long: `Download whole WARC/WAT/WET/index files for a crawl, concurrently and
resumably. Pass a kind to pull from the crawl manifest, or "-" to read an
explicit list of paths on stdin.

Kinds: warc, wat, wet, robotstxt, non200responses, cc-index, cc-index-table.

Examples:
  ccrawl download warc -n 5                first 5 WARC files of the latest crawl
  ccrawl download wet --segment 1700.../   one segment's WET files
  ccrawl paths warc -n 100 | ccrawl download -
  ccrawl download cc-index-table -c 2024-51  the columnar Parquet shards`,
		Args: cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runDownload(app, c, args[0], outDir, segment, sample, flat)
		},
	}
	cmd.Flags().StringVar(&outDir, "out", "", "output directory (default: <data-dir>/raw)")
	cmd.Flags().StringVar(&segment, "segment", "", "only files under this segment prefix")
	cmd.Flags().Float64Var(&sample, "sample", 0, "download a random fraction (0-1) of files")
	cmd.Flags().BoolVar(&flat, "flat", false, "save files flat, not in the source dir tree")
	return cmd
}

func runDownload(app *App, c *cobra.Command, kind, outDir, segment string, sample float64, flat bool) error {
	ctx := c.Context()
	if outDir == "" {
		outDir = app.Cfg.RawDir()
	}

	var paths []string
	if kind == "-" {
		if err := readLines(os.Stdin, func(p string) error {
			paths = append(paths, normalizePath(p))
			return nil
		}); err != nil {
			return err
		}
	} else {
		id, err := app.Crawl(ctx)
		if err != nil {
			return err
		}
		all, err := ccrawl.FetchPaths(ctx, app.HTTP, app.Cache, id, kind)
		if err != nil {
			return err
		}
		paths = filterPaths(all, segment, sample, app.Limit)
	}
	if len(paths) == 0 {
		return noResults("nothing to download")
	}

	if app.dryRun {
		for _, p := range paths {
			_, _ = fmt.Fprintln(cmdOut, ccrawl.FileURL(p, app.Cfg.Source))
		}
		_, _ = fmt.Fprintf(cmdErr, "%d files would be downloaded to %s\n", len(paths), outDir)
		return nil
	}
	if len(paths) > 20 && !confirm(app.yes, fmt.Sprintf("Download %d files to %s?", len(paths), outDir)) {
		return usageErr("aborted")
	}

	var done int64
	var bytes int64
	progress := func(r ccrawl.DownloadResult) {
		n := atomic.AddInt64(&done, 1)
		atomic.AddInt64(&bytes, r.Bytes)
		if r.Err != nil {
			_, _ = fmt.Fprintf(cmdErr, "[%d/%d] FAIL %s: %v\n", n, len(paths), r.Path, r.Err)
			return
		}
		state := "ok"
		if r.Skipped {
			state = "skip"
		}
		if stderrTTY() || !r.Skipped {
			_, _ = fmt.Fprintf(cmdErr, "[%d/%d] %-4s %s (%s)\n", n, len(paths), state, r.LocalPath, humanBytes(r.Bytes))
		}
	}

	err := ccrawl.DownloadFiles(ctx, app.HTTP, app.Cfg.Source, paths, outDir, app.Workers, flat, progress)
	_, _ = fmt.Fprintf(cmdErr, "downloaded %s across %d files\n", humanBytes(atomic.LoadInt64(&bytes)), len(paths))
	if err != nil {
		return codedError{err, 4}
	}
	return nil
}

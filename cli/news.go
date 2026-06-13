package cli

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/spf13/cobra"
	"github.com/tamnd/ccrawl-cli/ccrawl"
	"golang.org/x/sync/errgroup"
)

// newsEscapeHatches returns the news verbs that do not emit a record stream
// (a bulk download and a streamed scan), so they attach under the news parent
// next to the list operation. The list verb is a kit operation (registerNewsList).
func newsEscapeHatches() []*cobra.Command {
	return []*cobra.Command{newNewsDownloadCmd(), newNewsSearchCmd()}
}

func newNewsDownloadCmd() *cobra.Command {
	var year, month int
	var outDir string
	c := &cobra.Command{
		Use:   "download",
		Short: "Download CC-NEWS WARC files",
		RunE: func(c *cobra.Command, _ []string) error {
			app, err := appFromCtx(c.Context())
			if err != nil {
				return err
			}
			files, err := ccrawl.ListNewsFiles(c.Context(), app.HTTP, year, month)
			if err != nil {
				return err
			}
			var paths []string
			for _, f := range files {
				paths = append(paths, f.Path)
			}
			paths = filterPaths(paths, "", 0, app.Limit)
			if len(paths) == 0 {
				return noResults("no CC-NEWS files to download")
			}
			if outDir == "" {
				outDir = app.Cfg.RawDir() + "/news"
			}
			var done int64
			progress := func(r ccrawl.DownloadResult) {
				n := atomic.AddInt64(&done, 1)
				if r.Err != nil {
					_, _ = fmt.Fprintf(cmdErr, "[%d/%d] FAIL %s: %v\n", n, len(paths), r.Path, r.Err)
					return
				}
				_, _ = fmt.Fprintf(cmdErr, "[%d/%d] %s (%s)\n", n, len(paths), r.LocalPath, humanBytes(r.Bytes))
			}
			return ccrawl.DownloadFiles(c.Context(), app.HTTP, app.Cfg.Source, paths, outDir, app.Workers, true, progress)
		},
	}
	c.Flags().IntVar(&year, "year", 0, "year")
	c.Flags().IntVar(&month, "month", 0, "month")
	c.Flags().StringVar(&outDir, "out", "", "output directory")
	return c
}

func newNewsSearchCmd() *cobra.Command {
	var year, month int
	c := &cobra.Command{
		Use:   "search <host>",
		Short: "Scan CC-NEWS WARCs for a host (streamed, no index)",
		Long: `CC-NEWS has no URL index, so this streams the month's WARC files and keeps
records whose target host matches. It is slower than an indexed search; --workers
parallelises across files.`,
		Args: cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			app, err := appFromCtx(c.Context())
			if err != nil {
				return err
			}
			return runNewsSearch(app, c, args[0], year, month)
		},
	}
	c.Flags().IntVar(&year, "year", 0, "year")
	c.Flags().IntVar(&month, "month", 0, "month")
	return c
}

func runNewsSearch(app *App, c *cobra.Command, host string, year, month int) error {
	ctx := c.Context()
	host = strings.ToLower(host)
	files, err := ccrawl.ListNewsFiles(ctx, app.HTTP, year, month)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return noResults("no CC-NEWS files for that period")
	}

	var mu sync.Mutex
	var hits int64
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(app.Workers)
	for _, f := range files {
		g.Go(func() error {
			resp, err := app.HTTP.GetDownload(ctx, ccrawl.FileURL(f.Path, ccrawl.SourceHTTPS))
			if err != nil {
				return nil
			}
			defer func() { _ = resp.Body.Close() }()
			return ccrawl.IterateWARC(resp.Body, func(rec ccrawl.WARCRecord) error {
				if rec.Header.Type != "response" {
					return nil
				}
				if !strings.Contains(strings.ToLower(ccrawl.HostOf(rec.Header.TargetURI)), host) {
					return nil
				}
				mu.Lock()
				err := app.Out.Emit(warcRow(rec))
				mu.Unlock()
				atomic.AddInt64(&hits, 1)
				if app.Limit > 0 && atomic.LoadInt64(&hits) >= int64(app.Limit) {
					return errStopNews
				}
				return err
			})
		})
	}
	gerr := g.Wait()
	if ferr := app.Out.Flush(); ferr != nil && gerr == nil {
		gerr = ferr
	}
	if gerr != nil && gerr != errStopNews {
		return gerr
	}
	if atomic.LoadInt64(&hits) == 0 {
		return noResults("no matching news records")
	}
	return nil
}

var errStopNews = fmt.Errorf("stop")

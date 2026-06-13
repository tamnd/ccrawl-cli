package cli

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
	"golang.org/x/sync/errgroup"
)

// newsEscapeHatches returns the news verbs that do not emit a record stream
// (a bulk download and a streamed scan), so they attach under the news parent
// next to the list operation. The list verb is a kit operation (registerNewsList).
func newsEscapeHatches() []kit.Command {
	return []kit.Command{newNewsDownloadCmd(), newNewsSearchCmd()}
}

// newsDownloadCmd holds the flags for the news download command.
type newsDownloadCmd struct {
	year, month int
	outDir      string
}

func newNewsDownloadCmd() kit.Command {
	n := &newsDownloadCmd{}
	return kit.Command{
		Use:   "download",
		Short: "Download CC-NEWS WARC files",
		Flags: n.flags,
		Run:   n.run,
	}
}

func (n *newsDownloadCmd) flags(f *kit.FlagSet) {
	f.IntVar(&n.year, "year", 0, "year")
	f.IntVar(&n.month, "month", 0, "month")
	f.StringVar(&n.outDir, "out", "", "output directory")
}

func (n *newsDownloadCmd) run(ctx context.Context, _ []string) error {
	app := appFromCtx(ctx)
	files, err := ccrawl.ListNewsFiles(ctx, app.HTTP, n.year, n.month)
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
	outDir := n.outDir
	if outDir == "" {
		outDir = app.Cfg.RawDir() + "/news"
	}
	var done int64
	progress := func(r ccrawl.DownloadResult) {
		i := atomic.AddInt64(&done, 1)
		if r.Err != nil {
			_, _ = fmt.Fprintf(cmdErr, "[%d/%d] FAIL %s: %v\n", i, len(paths), r.Path, r.Err)
			return
		}
		_, _ = fmt.Fprintf(cmdErr, "[%d/%d] %s (%s)\n", i, len(paths), r.LocalPath, humanBytes(r.Bytes))
	}
	return ccrawl.DownloadFiles(ctx, app.HTTP, app.Cfg.Source, paths, outDir, app.Workers, true, progress)
}

// newsSearchCmd holds the flags for the news search command.
type newsSearchCmd struct {
	year, month int
}

func newNewsSearchCmd() kit.Command {
	n := &newsSearchCmd{}
	return kit.Command{
		Use:   "search <host>",
		Short: "Scan CC-NEWS WARCs for a host (streamed, no index)",
		Long: `CC-NEWS has no URL index, so this streams the month's WARC files and keeps
records whose target host matches. It is slower than an indexed search; --workers
parallelises across files.`,
		Args:  kit.ExactArgs(1),
		Flags: n.flags,
		Run:   n.run,
	}
}

func (n *newsSearchCmd) flags(f *kit.FlagSet) {
	f.IntVar(&n.year, "year", 0, "year")
	f.IntVar(&n.month, "month", 0, "month")
}

func (n *newsSearchCmd) run(ctx context.Context, args []string) error {
	return runNewsSearch(ctx, appFromCtx(ctx), args[0], n.year, n.month)
}

func runNewsSearch(ctx context.Context, app *App, host string, year, month int) error {
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

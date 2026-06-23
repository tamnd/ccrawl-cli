package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// pathsCmd holds the flags for the paths command.
type pathsCmd struct {
	segment string
	kinds   bool
}

func newPathsCmd() kit.Command {
	p := &pathsCmd{}
	return kit.Command{
		Use:   "paths <kind>",
		Short: "List the archive file paths for a crawl",
		Long: `Print the file paths of a crawl, one per line, ready to pipe into download.

Kinds: warc, wat, wet, robotstxt, non200responses, cc-index, cc-index-table, segment.

Examples:
  ccrawl paths warc -c 2026-17               every WARC path for the crawl
  ccrawl paths wet -n 1 | ccrawl download -  download the first WET file
  ccrawl paths warc -o url                   full https URLs
  ccrawl paths wet --library                 the WET files already in the library

With --library this lists what you have downloaded locally for a kind rather than
the remote crawl manifest.`,
		Args:  kit.MaximumNArgs(1),
		Flags: p.flags,
		Run:   p.run,
	}
}

func (p *pathsCmd) flags(f *kit.FlagSet) {
	f.StringVar(&p.segment, "segment", "", "only paths under this segment prefix")
	f.BoolVar(&p.kinds, "kinds", false, "list the available path kinds and exit")
}

func (p *pathsCmd) run(ctx context.Context, args []string) error {
	app := appFromCtx(ctx)
	if p.kinds {
		_, _ = fmt.Fprintln(cmdOut, strings.Join(ccrawl.PathKinds, "\n"))
		return nil
	}
	if len(args) == 0 {
		return usageErr("specify a kind (one of: " + strings.Join(ccrawl.PathKinds, ", ") + ")")
	}
	return runPaths(ctx, app, args[0], p.segment)
}

func runPaths(ctx context.Context, app *App, kind, segment string) error {
	if app.UseLibrary {
		return listLibraryPaths(ctx, app, kind, segment)
	}
	id, err := app.Crawl(ctx)
	if err != nil {
		return err
	}
	limit := app.Limit
	asURL := app.Out.Format() == "url"
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

// listLibraryPaths lists the archive files of a kind already downloaded into the
// library, so paths --library tells you what you have locally rather than what
// the remote crawl offers. Honours --segment (substring match) and -n.
func listLibraryPaths(ctx context.Context, app *App, kind, segment string) error {
	lib, err := app.Library(ctx)
	if err != nil {
		return err
	}
	files, err := libraryFiles(lib.RawDir(kind))
	if err != nil {
		return err
	}
	count := 0
	for _, f := range files {
		if segment != "" && !strings.Contains(f, segment) {
			continue
		}
		if _, err := fmt.Fprintln(cmdOut, f); err != nil {
			return err
		}
		count++
		if app.Limit > 0 && count >= app.Limit {
			break
		}
	}
	if count == 0 {
		return noResults("no files in library for kind " + kind + " (download them first)")
	}
	return nil
}

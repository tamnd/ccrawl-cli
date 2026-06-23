package cli

import (
	"context"
	"fmt"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// crawlsEscapeHatches returns the crawls verbs that emit plain scalars rather
// than record streams, so they attach under the crawls parent next to the list
// operation. The list verb itself is a kit operation (see registerCrawlsList).
func crawlsEscapeHatches() []kit.Command {
	return []kit.Command{
		{
			Use:   "latest",
			Short: "Print the newest crawl ID",
			Run:   runCrawlsLatest,
		},
		{
			Use:   "resolve <ref>",
			Short: "Resolve a loose crawl reference to its canonical ID",
			Args:  kit.ExactArgs(1),
			Run:   runCrawlsResolve,
		},
		{
			Use:   "info <id>",
			Short: "Show details for a crawl (file counts per format)",
			Args:  kit.MaximumNArgs(1),
			Run:   runCrawlsInfo,
		},
	}
}

func runCrawlsLatest(ctx context.Context, _ []string) error {
	app := appFromCtx(ctx)
	crawls, err := ccrawl.ListCrawls(ctx, app.HTTP, app.Cache)
	if err != nil {
		return err
	}
	if len(crawls) == 0 {
		return noResults("no crawls available")
	}
	_, _ = fmt.Fprintln(cmdOut, crawls[0].ID)
	return nil
}

func runCrawlsResolve(ctx context.Context, args []string) error {
	app := appFromCtx(ctx)
	id, err := ccrawl.ResolveCrawl(ctx, app.HTTP, app.Cache, args[0])
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintln(cmdOut, id)
	return nil
}

func runCrawlsInfo(ctx context.Context, args []string) error {
	app := appFromCtx(ctx)
	ref := app.Cfg.CrawlID
	if len(args) == 1 {
		ref = args[0]
	}
	id, err := ccrawl.ResolveCrawl(ctx, app.HTTP, app.Cache, ref)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmdOut, "Crawl: %s\n", id)
	for _, kind := range []string{"warc", "wat", "wet", "robotstxt", "cc-index-table"} {
		paths, err := ccrawl.FetchPaths(ctx, app.HTTP, app.Cache, id, kind)
		if err != nil {
			_, _ = fmt.Fprintf(cmdOut, "  %-16s (unavailable)\n", kind)
			continue
		}
		_, _ = fmt.Fprintf(cmdOut, "  %-16s %d files\n", kind, len(paths))
	}
	return nil
}

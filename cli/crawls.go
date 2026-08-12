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

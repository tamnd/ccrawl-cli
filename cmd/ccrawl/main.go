// Command ccrawl is a single-binary command line for Common Crawl.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// kit builds the command tree from the operation registry, exposes the serve,
	// mcp, and tui surfaces, and maps the typed error taxonomy to exit codes.
	os.Exit(kit.Run(ctx, cli.NewApp()))
}

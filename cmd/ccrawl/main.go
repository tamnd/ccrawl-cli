// Command ccrawl is a single-binary command line for Common Crawl.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/any-cli/kit/errs"
	"github.com/tamnd/ccrawl-cli/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// kit builds the command tree from the operation registry, exposes the serve,
	// mcp, and tui surfaces, and maps the typed error taxonomy to exit codes.
	//
	// Building the tree fails only on a config file the program could not read,
	// which is settled before there is a command to attach the message to, so it
	// is reported here and mapped through the same taxonomy.
	app, err := cli.NewApp()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ccrawl: %v\n", err)
		os.Exit(errs.ExitCode(err))
	}
	os.Exit(kit.Run(ctx, app))
}

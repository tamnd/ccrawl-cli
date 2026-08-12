package cli

import (
	"context"
	"errors"
	"net"
	"syscall"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/any-cli/kit/errs"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// The exit codes are a promise to a script: 2 means fix the command, 3 means
// the query was fine and matched nothing, 8 means Common Crawl could not be
// reached and the same command is worth running again in an hour. A command
// keeps that promise only if the error it returns carries a kind, and the
// commands that fail on the network return whatever the transport handed them,
// which is unclassified and comes out as 1, the code for anything at all.
//
// Rather than ask every command to remember, the classification happens where
// the commands are registered. handle wraps a kit operation and addCmd wraps a
// command tree; between them they cover every handler ccrawl has, and
// TestEveryHandlerIsClassified fails on one that goes around them.

// classify gives an unclassified error the kind its cause implies. An error
// that a command classified itself is left alone: the command knows more about
// what went wrong than this does.
func classify(err error) error {
	if err == nil || errs.KindOf(err) != errs.KindGeneric {
		return err
	}
	// Built by hand rather than through errs.Wrap, which wants a message and
	// prints it in front of the cause. There is nothing to add here: the command
	// already said what it was fetching and the transport already said what went
	// wrong, and a kind is all that is missing.
	if errors.Is(err, ccrawl.ErrTransport) || lostMidStream(err) {
		return &errs.Error{Kind: errs.KindNetwork, Err: err}
	}
	return err
}

// lostMidStream reports whether the connection went away after the response
// started arriving. ErrTransport is attached by the retry loop, which only sees
// a request that never got an answer, so a body that stops halfway lands here
// as whatever the network handed back and used to come out as exit 1, the code
// for a command that is wrong. The long commands are the ones this hits: the
// rank table is 262 million rows and the edge files are 7.7 GB, so they hold a
// connection open for minutes and are exactly the runs worth restarting.
//
// A status the server sent is not this. Those never reach the network stack as
// an error at all, so a 503 that survived every retry still exits 1 and a
// supervisor does not sit in a loop against a host that is up and refusing.
// This asks for the concrete socket errors rather than for the net.Error
// interface. os.PathError has Timeout and Temporary methods of its own, so it
// satisfies net.Error, and asking for the interface would report a missing
// local file as a network failure.
func lostMidStream(err error) bool {
	var op *net.OpError
	var dns *net.DNSError
	return errors.As(err, &op) ||
		errors.As(err, &dns) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE)
}

// handle registers a kit operation and classifies whatever it returns.
func handle[In, Out any](app *kit.App, meta kit.OpMeta, fn func(context.Context, In, func(Out) error) error) {
	kit.Handle(app, meta, func(ctx context.Context, in In, emit func(Out) error) error {
		return classify(fn(ctx, in, emit))
	})
}

// addCmd and addCmdUnder register an escape-hatch command and classify what its
// handler returns, down the whole subcommand tree.
func addCmd(app *kit.App, c kit.Command) { app.AddCommand(classifyCmd(c)) }

func addCmdUnder(app *kit.App, parent string, c kit.Command) {
	app.AddCommandUnder(parent, classifyCmd(c))
}

// classifyCmd rebuilds a command with its handler wrapped. A parent with no Run
// of its own still has its children wrapped, since that is where the work is.
func classifyCmd(c kit.Command) kit.Command {
	if run := c.Run; run != nil {
		c.Run = func(ctx context.Context, args []string) error { return classify(run(ctx, args)) }
	}
	for i, sub := range c.Sub {
		c.Sub[i] = classifyCmd(sub)
	}
	return c
}

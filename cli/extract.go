package cli

import (
	"context"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func newExtractCmd() kit.Command {
	return kit.Command{
		Use:   "extract",
		Short: "Extract text, links, title, or Markdown from a captured page",
		Long: `Convenience wrappers over get for a single content transform.

Examples:
  ccrawl extract text example.com
  ccrawl extract links example.com
  ccrawl extract title example.com
  ccrawl extract markdown example.com -O out.md`,
		Sub: []kit.Command{
			newExtractSub("text", "Readable plain text", contentMode{text: true}),
			newExtractSub("markdown", "HTML converted to Markdown", contentMode{markdown: true}),
			newExtractSub("links", "Outbound links", contentMode{links: true}),
			newExtractSub("title", "Page title", contentMode{}),
		},
	}
}

// extractCmd is one extract subcommand: a fixed content mode plus an -O outfile.
type extractCmd struct {
	name    string
	mode    contentMode
	outFile string
}

func newExtractSub(name, short string, mode contentMode) kit.Command {
	e := &extractCmd{name: name, mode: mode}
	return kit.Command{
		Use:   name + " <url>",
		Short: short,
		Args:  kit.ExactArgs(1),
		Flags: e.flags,
		Run:   e.run,
	}
}

func (e *extractCmd) flags(f *kit.FlagSet) {
	f.StringVarP(&e.outFile, "out", "O", "", "write to a file")
}

func (e *extractCmd) run(ctx context.Context, args []string) error {
	app := appFromCtx(ctx)
	if e.name == "title" {
		return runExtractTitle(ctx, app, args[0])
	}
	return runGet(ctx, app, args[0], e.mode, "", false, e.outFile)
}

func runExtractTitle(ctx context.Context, app *App, url string) error {
	rec, err := fetchLatest(ctx, app, url)
	if err != nil {
		return err
	}
	title := ccrawl.ExtractTitle(ccrawl.HTTPBody(rec.Block))
	if title == "" {
		return noResults("no title found")
	}
	_, err = cmdOut.Write([]byte(title + "\n"))
	return err
}

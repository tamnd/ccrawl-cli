package cli

import (
	"github.com/spf13/cobra"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func newExtractCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "extract",
		Short: "Extract text, links, title, or Markdown from a captured page",
		Long: `Convenience wrappers over get for a single content transform.

Examples:
  ccrawl extract text example.com
  ccrawl extract links example.com
  ccrawl extract title example.com
  ccrawl extract markdown example.com -O out.md`,
	}
	cmd.AddCommand(
		extractSub("text", "Readable plain text", contentMode{text: true}),
		extractSub("markdown", "HTML converted to Markdown", contentMode{markdown: true}),
		extractSub("links", "Outbound links", contentMode{links: true}),
		extractSub("title", "Page title", contentMode{}),
	)
	return cmd
}

func extractSub(name, short string, mode contentMode) *cobra.Command {
	var outFile string
	c := &cobra.Command{
		Use:   name + " <url>",
		Short: short,
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			app := appFromCtx(c.Context())
			if name == "title" {
				return runExtractTitle(app, c, args[0])
			}
			return runGet(app, c, args[0], mode, "", false, outFile)
		},
	}
	c.Flags().StringVarP(&outFile, "out", "O", "", "write to a file")
	return c
}

func runExtractTitle(app *App, c *cobra.Command, url string) error {
	rec, err := fetchLatest(app, c, url)
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

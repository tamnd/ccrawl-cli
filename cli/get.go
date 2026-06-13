package cli

import (
	"fmt"
	"os"
	"sort"

	"github.com/spf13/cobra"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func newGetCmd(app *App) *cobra.Command {
	var mode contentMode
	var at string
	var all bool
	var outFile string

	cmd := &cobra.Command{
		Use:   "get <url>",
		Short: "Fetch the content Common Crawl captured for a URL",
		Long: `Resolve the latest capture of a URL and print what Common Crawl saw.

This is curl for Common Crawl: it finds the newest capture in the index, pulls
the exact WARC record with a byte-range request, and prints the response body.
Use the content flags to transform it.

Examples:
  ccrawl get example.com                 the captured HTML
  ccrawl get example.com --text          readable plain text
  ccrawl get example.com --markdown      HTML converted to Markdown
  ccrawl get example.com --headers       the captured HTTP headers
  ccrawl get example.com --at 2023-06    the capture nearest a date
  ccrawl get example.com --all -o jsonl  every capture across crawls`,
		Args: cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runGet(app, c, args[0], mode, at, all, outFile)
		},
	}
	addContentFlags(cmd, &mode)
	cmd.Flags().StringVar(&at, "at", "", "pick the capture nearest this date")
	cmd.Flags().BoolVar(&all, "all", false, "fetch every capture, not just the latest")
	cmd.Flags().StringVarP(&outFile, "out", "O", "", "write content to a file")
	return cmd
}

func runGet(app *App, c *cobra.Command, url string, mode contentMode, at string, all bool, outFile string) (err error) {
	ctx := c.Context()
	crawls, err := app.AllCrawls(ctx)
	if err != nil {
		return err
	}

	// Gather candidate captures across the selected crawls.
	q := ccrawl.CDXQuery{URL: url, Match: "exact"}
	var caps []ccrawl.CDXRecord
	for _, id := range crawls {
		recs, err := ccrawl.CDXSearch(ctx, app.HTTP, id, q)
		if err != nil {
			continue
		}
		caps = append(caps, recs...)
		if !all && len(recs) > 0 && at == "" {
			break // newest crawl already has it
		}
	}
	if len(caps) == 0 {
		return noResults("no capture of " + url + " found in the index")
	}

	// Order newest first; if --at given, sort by distance to the target date.
	sort.Slice(caps, func(i, j int) bool { return caps[i].Timestamp > caps[j].Timestamp })
	if at != "" {
		target := looseTS(at)
		sort.Slice(caps, func(i, j int) bool {
			return absDiff(caps[i].Timestamp, target) < absDiff(caps[j].Timestamp, target)
		})
	}

	chosen := caps
	if !all {
		chosen = caps[:1]
	}

	// Redirect output to a file if requested.
	out := app.Out
	if outFile != "" {
		f, ferr := os.Create(outFile)
		if ferr != nil {
			return ferr
		}
		defer func() {
			if cerr := f.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}()
		out = &Output{w: f, format: app.Out.format}
	}

	for _, rec := range chosen {
		if mode.meta && !all {
			return out.Emit(cdxRow(rec))
		}
		loc := rec.Location()
		warc, err := ccrawl.FetchWARCRecord(ctx, app.HTTP, loc.Filename, loc.Offset, loc.Length)
		if err != nil {
			return err
		}
		if err := mode.render(out, warc); err != nil {
			return err
		}
	}
	if mode.structured() {
		return out.Flush()
	}
	if outFile != "" {
		_, _ = fmt.Fprintln(cmdErr, "wrote "+outFile)
	}
	return nil
}

// fetchLatest resolves and fetches the newest WARC record for a URL.
func fetchLatest(app *App, c *cobra.Command, url string) (ccrawl.WARCRecord, error) {
	ctx := c.Context()
	id, err := app.Crawl(ctx)
	if err != nil {
		return ccrawl.WARCRecord{}, err
	}
	recs, err := ccrawl.CDXSearch(ctx, app.HTTP, id, ccrawl.CDXQuery{URL: url, Match: "exact"})
	if err != nil {
		return ccrawl.WARCRecord{}, err
	}
	if len(recs) == 0 {
		return ccrawl.WARCRecord{}, noResults("no capture of " + url + " found")
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].Timestamp > recs[j].Timestamp })
	loc := recs[0].Location()
	return ccrawl.FetchWARCRecord(ctx, app.HTTP, loc.Filename, loc.Offset, loc.Length)
}

func looseTS(s string) string {
	var digits []rune
	for _, r := range s {
		if r >= '0' && r <= '9' {
			digits = append(digits, r)
		}
	}
	d := string(digits)
	for len(d) < 14 {
		d += "0"
	}
	return d[:14]
}

func absDiff(a, b string) int64 {
	ai, bi := parseTS(a), parseTS(b)
	if ai > bi {
		return ai - bi
	}
	return bi - ai
}

func parseTS(s string) int64 {
	var n int64
	for _, r := range s {
		if r >= '0' && r <= '9' {
			n = n*10 + int64(r-'0')
		}
	}
	return n
}

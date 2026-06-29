package cli

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// exportCmd holds the flags for the warc export command.
type exportCmd struct {
	prefix    string
	subprefix string
	size      int64
	creator   string
	operator  string
	outDir    string
	fgrep     string
	fgrepv    string

	// query flags, used in pattern mode (mirrors search)
	match  string
	from   string
	to     string
	status string
	mime   string
	lang   string
	filter []string
}

func newExportCmd() kit.Command {
	c := &exportCmd{}
	return kit.Command{
		Use:   "export [url-or-pattern | -]",
		Short: "Write matching captures into WARC files with provenance",
		Long: `Export a subset of Common Crawl as standards-compliant WARC files.

This is the reproducible-subset workflow: run a query, pull each matching
capture, and write them into one or more .warc.gz files. Each file opens with a
warcinfo record carrying provenance (the tool and version, the prefix, and the
exact command line), so the output is self-describing. Files rotate once they
pass --size bytes.

Give a URL or wildcard pattern to run a query, or pass "-" to read location
records (filename, offset, length) as JSONL on stdin, exactly what
"ccrawl search --locations" and "ccrawl columnar locations" produce.

Examples:
  ccrawl export example.com/* --prefix example
  ccrawl export '*.example.com' --status 200 --size 100000000
  ccrawl search example.com --locations | ccrawl export - --prefix example
  ccrawl export example.com/* --url-fgrepv /robots.txt --creator "me <me@example.com>"`,
		Flags: c.flags,
		Run:   c.run,
	}
}

func (c *exportCmd) flags(f *kit.FlagSet) {
	f.StringVar(&c.prefix, "prefix", "ccrawl", "prefix for the WARC filenames")
	f.StringVar(&c.subprefix, "subprefix", "", "optional subprefix for the WARC filenames")
	f.Int64Var(&c.size, "size", ccrawl.DefaultWARCSize, "rotate to a new file past this many bytes")
	f.StringVar(&c.creator, "creator", "", "warcinfo creator: a person, organization, or service")
	f.StringVar(&c.operator, "operator", "", "warcinfo operator: a person, when the creator is an organization")
	f.StringVar(&c.outDir, "out-dir", ".", "directory to write the WARC files into")
	f.StringVar(&c.fgrep, "url-fgrep", "", "only export captures whose URL contains this substring")
	f.StringVar(&c.fgrepv, "url-fgrepv", "", "skip captures whose URL contains this substring")

	f.StringVar(&c.match, "match", "", "match type: exact|prefix|host|domain")
	f.StringVar(&c.from, "from", "", "earliest capture date (e.g. 2023 or 2023-06)")
	f.StringVar(&c.to, "to", "", "latest capture date")
	f.StringVar(&c.status, "status", "", "HTTP status filter (e.g. 200)")
	f.StringVar(&c.mime, "mime", "", "detected MIME filter")
	f.StringVar(&c.lang, "lang", "", "language filter (ISO-639-3)")
	f.StringSliceVar(&c.filter, "filter", nil, "raw CDX filter field:regex (repeatable)")
}

func (c *exportCmd) info() ccrawl.WARCInfo {
	isPartOf := c.prefix
	if c.subprefix != "" {
		isPartOf += "-" + c.subprefix
	}
	return ccrawl.WARCInfo{
		Software:    "ccrawl/" + strings.TrimPrefix(Version, "v"),
		IsPartOf:    isPartOf,
		Description: "warc extraction generated with: " + strings.Join(os.Args, " "),
		Format:      "WARC file version 1.0",
		Creator:     c.creator,
		Operator:    c.operator,
	}
}

// urlOK applies the --url-fgrep / --url-fgrepv post-filters.
func (c *exportCmd) urlOK(url string) bool {
	return urlKeep(url, c.fgrep, c.fgrepv)
}

func (c *exportCmd) run(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return usageErr("provide a URL or pattern to export, or - to read locations from stdin")
	}
	app := appFromCtx(ctx)
	exp := ccrawl.NewWARCExporter(c.outDir, c.prefix, c.subprefix, c.size, c.info())

	var runErr error
	if args[0] == "-" {
		runErr = c.exportStdin(ctx, app, exp)
	} else {
		runErr = c.exportQuery(ctx, app, exp, args[0])
	}
	if cerr := exp.Close(); cerr != nil && runErr == nil {
		runErr = cerr
	}
	if runErr != nil {
		return runErr
	}
	if exp.Records() == 0 {
		return noResults("no captures matched; nothing exported")
	}
	_, _ = fmt.Fprintf(cmdErr, "exported %d records into %d file(s) under %s\n", exp.Records(), len(exp.Files), c.outDir)
	for _, p := range exp.Files {
		_, _ = fmt.Fprintln(cmdOut, p)
	}
	return nil
}

// writeLoc fetches the raw record for a location and writes it, honoring the
// URL filters and the --limit cap. It returns false once the cap is reached.
func (c *exportCmd) writeLoc(ctx context.Context, app *App, exp *ccrawl.WARCExporter, loc ccrawl.Location) (bool, error) {
	if app.Limit > 0 && exp.Records() >= app.Limit {
		return false, nil
	}
	if !c.urlOK(loc.URL) {
		return true, nil
	}
	raw, err := ccrawl.FetchWARCRecordRaw(ctx, app.HTTP, loc.Filename, loc.Offset, loc.Length)
	if err != nil {
		_, _ = fmt.Fprintln(cmdErr, "warn: "+err.Error())
		return true, nil
	}
	if err := exp.Write(raw); err != nil {
		return false, err
	}
	if app.Limit > 0 && exp.Records() >= app.Limit {
		return false, nil
	}
	return true, nil
}

func (c *exportCmd) exportQuery(ctx context.Context, app *App, exp *ccrawl.WARCExporter, pattern string) error {
	crawls, err := app.AllCrawls(ctx)
	if err != nil {
		return err
	}
	q := ccrawl.CDXQuery{
		URL: pattern, Match: c.match,
		From: c.from, To: c.to,
		Status: c.status, MIME: c.mime, Lang: c.lang,
		Filter: c.filter,
	}
	for _, id := range crawls {
		stop := false
		err := ccrawl.CDXStream(ctx, app.HTTP, id, q, func(r ccrawl.CDXRecord) error {
			cont, werr := c.writeLoc(ctx, app, exp, r.Location())
			if werr != nil {
				return werr
			}
			if !cont {
				stop = true
				return errStopExport
			}
			return nil
		})
		if err != nil && err != errStopExport {
			return err
		}
		if stop {
			break
		}
	}
	return nil
}

func (c *exportCmd) exportStdin(ctx context.Context, app *App, exp *ccrawl.WARCExporter) error {
	var locs []ccrawl.Location
	if err := readLines(os.Stdin, func(line string) error {
		if loc, ok := parseLocationLine(line); ok {
			locs = append(locs, loc)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, loc := range locs {
		cont, err := c.writeLoc(ctx, app, exp, loc)
		if err != nil {
			return err
		}
		if !cont {
			break
		}
	}
	return nil
}

var errStopExport = fmt.Errorf("export limit reached")

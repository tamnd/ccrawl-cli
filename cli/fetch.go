package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
	"golang.org/x/sync/errgroup"
)

// fetchCmd holds the flags for the fetch command.
type fetchCmd struct {
	mode           contentMode
	file           string
	offset, length int64
	outDir         string
	asDir          bool
}

func newFetchCmd() kit.Command {
	c := &fetchCmd{}
	return kit.Command{
		Use:   "fetch [-]",
		Short: "Retrieve WARC records by location",
		Long: `Fetch one or many WARC records by their byte location.

Give an explicit --file/--offset/--length, or pass "-" to read location records
(filename, offset, length) as JSONL on stdin, which is exactly what
"ccrawl search --locations" and "ccrawl columnar locations" produce.

Examples:
  ccrawl fetch --file crawl-data/.../x.warc.gz --offset 123 --length 4567 --text
  ccrawl search example.com --locations | ccrawl fetch - --markdown
  ccrawl columnar locations --domain example.com -o jsonl | ccrawl fetch - --output dir --out-dir pages/`,
		Flags: c.flags,
		Run:   c.run,
	}
}

func (c *fetchCmd) flags(f *kit.FlagSet) {
	c.mode.bind(f)
	f.StringVar(&c.file, "file", "", "WARC file path (relative to data.commoncrawl.org)")
	f.Int64Var(&c.offset, "offset", 0, "byte offset of the record")
	f.Int64Var(&c.length, "length", 0, "byte length of the record")
	f.StringVar(&c.outDir, "out-dir", "pages", "output directory when --output dir")
	f.BoolVar(&c.asDir, "dir", false, "write one file per record into --out-dir")
}

func (c *fetchCmd) run(ctx context.Context, args []string) error {
	app := appFromCtx(ctx)
	if c.file != "" {
		loc := ccrawl.Location{Filename: c.file, Offset: c.offset, Length: c.length}
		return runFetchOne(ctx, app, loc, c.mode)
	}
	if len(args) == 1 && args[0] == "-" {
		return runFetchStdin(ctx, app, c.mode, c.outDir, c.asDir)
	}
	return usageErr("provide --file/--offset/--length or pass - to read locations from stdin")
}

func runFetchOne(ctx context.Context, app *App, loc ccrawl.Location, mode contentMode) error {
	rec, err := ccrawl.FetchWARCRecord(ctx, app.HTTP, loc.Filename, loc.Offset, loc.Length)
	if err != nil {
		return err
	}
	if err := mode.render(app.Out, rec); err != nil {
		return err
	}
	if mode.structured() {
		return app.Out.Flush()
	}
	return nil
}

func runFetchStdin(ctx context.Context, app *App, mode contentMode, outDir string, asDir bool) error {
	asDir = asDir || app.Out.Format() == "dir"

	var locs []ccrawl.Location
	if err := readLines(os.Stdin, func(line string) error {
		loc, ok := parseLocationLine(line)
		if ok {
			locs = append(locs, loc)
		}
		return nil
	}); err != nil {
		return err
	}
	if len(locs) == 0 {
		return noResults("no location records on stdin")
	}

	if asDir {
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			return err
		}
		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(app.Workers)
		var n int
		var mu sync.Mutex
		for i, loc := range locs {
			g.Go(func() error {
				rec, err := ccrawl.FetchWARCRecord(ctx, app.HTTP, loc.Filename, loc.Offset, loc.Length)
				if err != nil {
					return err
				}
				name := fmt.Sprintf("%06d-%s", i, safeName(loc.URL))
				path := filepath.Join(outDir, name)
				data := contentBytes(mode, rec)
				if err := os.WriteFile(path, data, 0o644); err != nil {
					return err
				}
				mu.Lock()
				n++
				mu.Unlock()
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(cmdErr, "wrote %d files to %s\n", n, outDir)
		return nil
	}

	// Sequential stream to stdout (order preserved).
	for _, loc := range locs {
		rec, err := ccrawl.FetchWARCRecord(ctx, app.HTTP, loc.Filename, loc.Offset, loc.Length)
		if err != nil {
			_, _ = fmt.Fprintln(cmdErr, "warn: "+err.Error())
			continue
		}
		if err := mode.render(app.Out, rec); err != nil {
			return err
		}
	}
	if mode.structured() {
		return app.Out.Flush()
	}
	return nil
}

// parseLocationLine reads one JSON object and pulls out a record location. It
// accepts both the typed locations from "ccrawl columnar locations" and the raw CDX
// rows from "ccrawl search -o jsonl", where offset and length arrive as quoted
// strings. The "warc_filename"/"warc_record_offset" columnar names are honoured too.
func parseLocationLine(line string) (ccrawl.Location, bool) {
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		return ccrawl.Location{}, false
	}
	pick := func(keys ...string) any {
		for _, k := range keys {
			if v, ok := m[k]; ok {
				return v
			}
		}
		return nil
	}
	loc := ccrawl.Location{
		Filename: str(pick("filename", "warc_filename")),
		Offset:   toInt64(pick("offset", "warc_record_offset")),
		Length:   toInt64(pick("length", "warc_record_length")),
		URL:      str(pick("url")),
	}
	if loc.Filename == "" || loc.Length <= 0 {
		return ccrawl.Location{}, false
	}
	return loc, true
}

func contentBytes(mode contentMode, rec ccrawl.WARCRecord) []byte {
	switch {
	case mode.raw:
		return rec.Block
	case mode.headers:
		return ccrawl.HTTPHeaders(rec.Block)
	case mode.text:
		return []byte(ccrawl.ExtractText(ccrawl.HTTPBody(rec.Block)))
	case mode.markdown:
		md, _ := ccrawl.ExtractMarkdown(ccrawl.HTTPBody(rec.Block))
		return []byte(md)
	default:
		return ccrawl.HTTPBody(rec.Block)
	}
}

func safeName(url string) string {
	url = strings.TrimPrefix(url, "http://")
	url = strings.TrimPrefix(url, "https://")
	r := strings.NewReplacer("/", "_", "?", "_", "&", "_", ":", "_", "=", "_", " ", "_")
	name := r.Replace(url)
	if len(name) > 100 {
		name = name[:100]
	}
	if name == "" {
		name = "record"
	}
	return name + ".html"
}

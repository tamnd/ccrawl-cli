package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/spf13/cobra"
	"github.com/tamnd/ccrawl-cli/ccrawl"
	"golang.org/x/sync/errgroup"
)

func newFetchCmd(app *App) *cobra.Command {
	var mode contentMode
	var file string
	var offset, length int64
	var outDir string
	var asDir bool

	cmd := &cobra.Command{
		Use:   "fetch [-]",
		Short: "Retrieve WARC records by location",
		Long: `Fetch one or many WARC records by their byte location.

Give an explicit --file/--offset/--length, or pass "-" to read location records
(filename, offset, length) as JSONL on stdin, which is exactly what
"ccrawl search --locations" and "ccrawl table locations" produce.

Examples:
  ccrawl fetch --file crawl-data/.../x.warc.gz --offset 123 --length 4567 --text
  ccrawl search example.com --locations | ccrawl fetch - --markdown
  ccrawl table locations --domain example.com -o jsonl | ccrawl fetch - --output dir --out-dir pages/`,
		RunE: func(c *cobra.Command, args []string) error {
			if file != "" {
				return runFetchOne(app, c, ccrawl.Location{Filename: file, Offset: offset, Length: length}, mode)
			}
			if len(args) == 1 && args[0] == "-" {
				return runFetchStdin(app, c, mode, outDir, asDir)
			}
			return usageErr("provide --file/--offset/--length or pass - to read locations from stdin")
		},
	}
	addContentFlags(cmd, &mode)
	cmd.Flags().StringVar(&file, "file", "", "WARC file path (relative to data.commoncrawl.org)")
	cmd.Flags().Int64Var(&offset, "offset", 0, "byte offset of the record")
	cmd.Flags().Int64Var(&length, "length", 0, "byte length of the record")
	cmd.Flags().StringVar(&outDir, "out-dir", "pages", "output directory when --output dir")
	cmd.Flags().BoolVar(&asDir, "dir", false, "write one file per record into --out-dir")
	return cmd
}

func runFetchOne(app *App, c *cobra.Command, loc ccrawl.Location, mode contentMode) error {
	rec, err := ccrawl.FetchWARCRecord(c.Context(), app.HTTP, loc.Filename, loc.Offset, loc.Length)
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

func runFetchStdin(app *App, c *cobra.Command, mode contentMode, outDir string, asDir bool) error {
	ctx := c.Context()
	asDir = asDir || app.Out.format == "dir"

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
// accepts both the typed locations from "ccrawl table locations" and the raw CDX
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

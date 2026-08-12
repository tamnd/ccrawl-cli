package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

	batch   bool
	gap     int64
	maxSpan int64
	order   string
	ledger  string
	window  int
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

With --batch the locations are sorted by file and offset first, and records that
sit within --gap bytes of each other are read in one ranged GET instead of one
each. That is the mode for millions of locations: it turns random access across
thousands of WARC files into a sweep through each file in turn. Pass --ledger to
make the run resumable, and --order input to keep the output in the order the
locations arrived rather than the order they sit on disk.

Examples:
  ccrawl fetch --file crawl-data/.../x.warc.gz --offset 123 --length 4567 --text
  ccrawl search example.com --locations | ccrawl fetch - --markdown
  ccrawl columnar locations --domain example.com -o jsonl | ccrawl fetch - --output dir --out-dir pages/
  ccrawl columnar locations --tld vn -o jsonl | ccrawl fetch - --batch --ledger fetched.txt --dir`,
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
	f.BoolVar(&c.batch, "batch", false, "coalesce nearby records in the same WARC file into shared ranged GETs")
	f.Int64Var(&c.gap, "gap", ccrawl.DefaultFetchGap, "coalesce records at most this many bytes apart")
	f.Int64Var(&c.maxSpan, "max-span", ccrawl.DefaultFetchMaxSpan, "never read more than this in one GET")
	f.StringVar(&c.order, "order", "file", "input|file: emit in the order given or the order on disk")
	f.StringVar(&c.ledger, "ledger", "", "file of finished locations, to skip on a resume")
	f.IntVar(&c.window, "lookahead", 64, "ranged GETs allowed to run ahead of the writer")
}

func (c *fetchCmd) run(ctx context.Context, args []string) error {
	app := appFromCtx(ctx)
	if c.file != "" {
		loc := ccrawl.Location{Filename: c.file, Offset: c.offset, Length: c.length}
		return runFetchOne(ctx, app, loc, c.mode)
	}
	if len(args) == 1 && args[0] == "-" {
		if c.batch {
			return c.runBatch(ctx, app)
		}
		return runFetchStdin(ctx, app, c.mode, c.outDir, c.asDir)
	}
	return usageErr("provide --file/--offset/--length or pass - to read locations from stdin")
}

// runBatch is the --batch path: read every location, group them, and fetch the
// groups. The records come back through the same renderers as the one at a time
// path, so the only thing that changes is how many requests it took.
func (c *fetchCmd) runBatch(ctx context.Context, app *App) error {
	switch c.order {
	case "input", "file":
	default:
		return usageErr("pass --order input or --order file")
	}
	asDir := c.asDir || app.Out.Format() == "dir"

	locs, err := readLocations(os.Stdin)
	if err != nil {
		return err
	}
	if len(locs) == 0 {
		return noResults("no location records on stdin")
	}
	if asDir {
		if err := os.MkdirAll(c.outDir, 0o755); err != nil {
			return err
		}
	}

	// Grouping is pure arithmetic on the locations, so a dry run can report
	// exactly what the real run would ask for without asking for any of it. That
	// is the cheap way to pick a --gap: try a few and read the ratio.
	if app.dryRun {
		groups := ccrawl.GroupLocations(locs, c.gap, c.maxSpan)
		var span, want int64
		for _, g := range groups {
			span += g.Span()
		}
		for _, l := range locs {
			want += l.Length
		}
		_, _ = fmt.Fprintf(cmdErr, "%d locations in %d requests, %.1fx fewer than one at a time; %s read for %s of records, %.1fx amplification\n",
			len(locs), len(groups), float64(len(locs))/float64(len(groups)),
			humanBytes(span), humanBytes(want), float64(span)/float64(want))
		return nil
	}

	ledger, err := ccrawl.OpenKeyLedger(c.ledger)
	if err != nil {
		return fmt.Errorf("open ledger: %w", err)
	}
	defer func() { _ = ledger.Close() }()

	rep, stopRun, err := app.StartRun("fetch", "")
	if err != nil {
		return err
	}
	defer stopRun()
	sp := ccrawl.StartStreamProgress(rep, "records", len(locs), 0)
	defer sp.Stop()

	// The index in the filename is the position in the input, not the position
	// in the output, so the same location lands in the same file whichever
	// order the run emitted it in and whether or not it resumed.
	at := make(map[string]int, len(locs))
	for i, l := range locs {
		if _, seen := at[ccrawl.LocationKey(l)]; !seen {
			at[ccrawl.LocationKey(l)] = i
		}
	}

	var failed int
	cfg := ccrawl.BatchFetchConfig{
		Locations: locs,
		Gap:       c.gap,
		MaxSpan:   c.maxSpan,
		Workers:   app.Workers,
		InOrder:   c.order == "input",
		Window:    c.window,
		Ledger:    ledger,
		Progress:  sp,
		OnError: func(loc ccrawl.Location, err error) {
			failed++
			rep.Textf("warn: %s at %d: %v\n", loc.Filename, loc.Offset, err)
		},
	}
	if asDir {
		cfg.OnRecord = func(loc ccrawl.Location, rec ccrawl.WARCRecord) error {
			name := fmt.Sprintf("%06d-%s", at[ccrawl.LocationKey(loc)], safeName(loc.URL))
			return os.WriteFile(filepath.Join(c.outDir, name), contentBytes(c.mode, rec), 0o644)
		}
	} else {
		cfg.OnRecord = func(_ ccrawl.Location, rec ccrawl.WARCRecord) error {
			return c.mode.render(app.Out, rec)
		}
	}

	stats, runErr := ccrawl.RunBatchFetch(ctx, app.HTTP, cfg)
	sp.Stop()
	if runErr != nil {
		return runErr
	}
	if c.mode.structured() && !asDir {
		if err := app.Out.Flush(); err != nil {
			return err
		}
	}

	// The request count against the record count is the whole argument for the
	// mode, so it is reported rather than left to be measured from outside.
	line := fmt.Sprintf("fetched %d records in %d requests (%s read)",
		stats.Records, stats.Requests, humanBytes(stats.Bytes))
	if stats.Requests > 0 {
		line += fmt.Sprintf(", %.1fx fewer requests than one at a time",
			float64(stats.Records+stats.Failed+stats.Skipped)/float64(stats.Requests))
	}
	if stats.Skipped > 0 {
		line += fmt.Sprintf("; skipped %d already in the ledger", stats.Skipped)
	}
	if failed > 0 {
		line += fmt.Sprintf("; %d failed", failed)
	}
	rep.Textf("%s\n", line)
	return nil
}

// readLocations pulls every location record off a stream of JSONL, skipping the
// lines that are not one.
func readLocations(r io.Reader) ([]ccrawl.Location, error) {
	var locs []ccrawl.Location
	err := readLines(r, func(line string) error {
		if loc, ok := parseLocationLine(line); ok {
			locs = append(locs, loc)
		}
		return nil
	})
	return locs, err
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

	locs, err := readLocations(os.Stdin)
	if err != nil {
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

package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

type tableFlags struct {
	domain     string
	host       string
	tld        string
	mime       string
	lang       string
	status     int
	pathPrefix string
	subset     string
	engine     string
	print      bool

	notTLD    string
	notMIME   string
	notLang   string
	notStatus int

	hostsFile   string
	domainsFile string
}

// query renders the flags as a ColumnarQuery. It reads the set files, so it can
// fail, which is why it returns an error rather than a bare query.
func (tf *tableFlags) query(crawl string) (ccrawl.ColumnarQuery, error) {
	q := ccrawl.ColumnarQuery{
		Crawl: crawl, Subset: tf.subset,
		Domain: tf.domain, Host: tf.host, TLD: tf.tld,
		MIME: tf.mime, Lang: tf.lang, Status: tf.status,
		PathPrefix: tf.pathPrefix,
		NotTLD:     tf.notTLD, NotMIME: tf.notMIME,
		NotLang: tf.notLang, NotStatus: tf.notStatus,
	}
	var err error
	if q.Hosts, err = readSetFile("hosts", tf.hostsFile); err != nil {
		return q, err
	}
	if q.Domains, err = readSetFile("domains", tf.domainsFile); err != nil {
		return q, err
	}
	return q, nil
}

// readSetFile reads one value per line, skipping blanks and # comments so a host
// list can carry a note about where it came from. An empty path reads nothing.
// A file with no values left after the skipping is an error: it means the caller
// asked to filter on a set and would otherwise silently get every row back.
// kind names the list in the error, since the two callers read the same shape of
// file for different columns.
func readSetFile(kind, path string) ([]string, error) {
	if path == "" {
		return nil, nil
	}
	fail := func(err error) error { return fmt.Errorf("read the %s file: %w", kind, err) }
	r, _, closeFn, err := openInput(path)
	if err != nil {
		return nil, fail(err)
	}
	defer closeFn()
	var vals []string
	err = readLines(r, func(line string) error {
		if strings.HasPrefix(line, "#") {
			return nil
		}
		vals = append(vals, line)
		return nil
	})
	if err != nil {
		return nil, fail(err)
	}
	if len(vals) == 0 {
		return nil, fail(fmt.Errorf("no values in %s", path))
	}
	return vals, nil
}

// bind registers the columnar filter flags shared by every table subcommand. It
// is a method so a command can wire it as kit.Command.Flags without a closure.
func (tf *tableFlags) bind(f *kit.FlagSet) {
	f.StringVar(&tf.domain, "domain", "", "match url_host_registered_domain")
	f.StringVar(&tf.host, "host", "", "match url_host_name")
	f.StringVar(&tf.tld, "tld", "", "match url_host_tld (e.g. gov)")
	f.StringVar(&tf.mime, "mime", "", "match content_mime_detected")
	f.StringVar(&tf.lang, "lang", "", "match a value in content_languages")
	f.IntVar(&tf.status, "status", 0, "match fetch_status (e.g. 200)")
	f.StringVar(&tf.pathPrefix, "path-prefix", "", "match a url_path prefix")
	f.StringVar(&tf.subset, "subset", "warc", "subset of the columnar index: warc|crawldiagnostics|robotstxt")
	f.StringVar(&tf.notTLD, "not-tld", "", "keep rows where url_host_tld is missing or something else")
	f.StringVar(&tf.notMIME, "not-mime", "", "keep rows where content_mime_detected is missing or something else")
	f.StringVar(&tf.notLang, "not-lang", "", "keep rows where content_languages is missing or does not contain this")
	f.IntVar(&tf.notStatus, "not-status", 0, "keep rows where fetch_status is missing or something else")
	f.StringVar(&tf.hostsFile, "hosts-file", "", "file of url_host_name values, one per line (- for stdin)")
	f.StringVar(&tf.domainsFile, "domains-file", "", "file of url_host_registered_domain values, one per line")
	f.StringVar(&tf.engine, "engine", "auto", "query engine: auto|duckdb|native|print")
	f.BoolVar(&tf.print, "print", false, "print the SQL and exit")
}

func newTableCmd() kit.Command {
	return kit.Command{
		Use:     "columnar",
		Aliases: []string{"table", "athena"},
		Short:   "Query the columnar Parquet index",
		Long: `Query Common Crawl's columnar (Parquet) index, the fastest way to answer bulk
questions like "every PDF on .gov domains" without touching a single WARC.

urls, locations, count, langs, mimes and schema are answered by the built-in
native engine, which reads the Parquet files directly over ranged HTTP and skips
the row groups whose statistics rule them out. Nothing needs installing for it.

"columnar query" and "columnar sql --run" take arbitrary SQL, so they run
against a local duckdb binary. With --engine print the ready-to-run SQL is
printed so you can paste it into Athena, Spark, Trino, or DuckDB yourself. Use
--engine duckdb to send a filter query through duckdb too.

Filters have negated forms (--not-tld, --not-mime, --not-lang, --not-status),
where a row with the column missing counts as a match, and set forms
(--hosts-file, --domains-file) that read one value per line.

Examples:
  ccrawl columnar urls --domain example.com --status 200 -o url
  ccrawl columnar count --tld gov -c 2024-51
  ccrawl columnar count --tld vn --not-lang vie
  ccrawl columnar urls --hosts-file hosts.txt --not-tld vn -o url
  ccrawl columnar locations --domain example.com -o jsonl | ccrawl fetch -
  ccrawl columnar sql --tld gov --mime application/pdf --print
  ccrawl columnar query "SELECT url FROM ccindex LIMIT 10"`,
		Sub: []kit.Command{
			newTableURLsCmd(),
			newTableLocationsCmd(),
			newTableCountCmd(),
			newTableBreakdownCmd("langs", "content_languages"),
			newTableBreakdownCmd("mimes", "match content_mime_detected"),
			newTableSQLCmd(),
			newTableQueryCmd(),
			newTableSchemaCmd(),
		},
	}
}

// tableCmd is a table subcommand whose run logic is selected by use. The shared
// columnar filter flags live in tf; breakdown carries the column it groups by.
type tableCmd struct {
	use      string
	tf       tableFlags
	groupCol string
}

func newTableURLsCmd() kit.Command      { return (&tableCmd{use: "urls"}).command() }
func newTableLocationsCmd() kit.Command { return (&tableCmd{use: "locations"}).command() }
func newTableCountCmd() kit.Command     { return (&tableCmd{use: "count"}).command() }
func newTableSQLCmd() kit.Command       { return (&tableCmd{use: "sql"}).command() }
func newTableQueryCmd() kit.Command     { return (&tableCmd{use: "query"}).command() }
func newTableSchemaCmd() kit.Command    { return (&tableCmd{use: "schema"}).command() }

func newTableBreakdownCmd(name, col string) kit.Command {
	return (&tableCmd{use: name, groupCol: col}).command()
}

func (t *tableCmd) command() kit.Command {
	c := kit.Command{Use: t.use, Short: t.short(), Flags: t.tf.bind, Run: t.run}
	switch t.use {
	case "locations":
		c.Long = "Output is the location JSONL that ccrawl fetch reads on stdin."
	case "query":
		c.Use = "query <sql>"
		c.Long = "The token 'ccindex' is replaced with the read_parquet(...) source for the crawl."
		c.Args = kit.ExactArgs(1)
	}
	return c
}

func (t *tableCmd) short() string {
	switch t.use {
	case "urls":
		return "List matching URLs from the columnar index"
	case "locations":
		return "Emit filename/offset/length records for matching captures"
	case "count":
		return "Count matching captures"
	case "sql":
		return "Build SQL from the filter flags (and print or run it)"
	case "query":
		return "Run raw SQL against the columnar index (ccindex view)"
	case "schema":
		return "Show the columns of the columnar index for a crawl"
	default:
		return "Breakdown of captures by " + t.groupCol
	}
}

func (t *tableCmd) run(ctx context.Context, args []string) error {
	app := appFromCtx(ctx)
	id, err := app.Crawl(ctx)
	if err != nil {
		return err
	}
	switch t.use {
	case "urls":
		return t.runURLs(ctx, app, id)
	case "locations":
		return t.runLocations(ctx, app, id)
	case "count":
		return t.runCount(ctx, app, id)
	case "sql":
		q, err := t.tf.query(id)
		if err != nil {
			return err
		}
		q.Limit = app.Limit
		_, _ = fmt.Fprintln(cmdOut, q.SQL(app.Cfg.Source))
		return nil
	case "query":
		return t.runQuery(ctx, app, id, args[0])
	case "schema":
		return t.runSchema(ctx, app, id)
	default:
		return t.runBreakdown(ctx, app, id)
	}
}

func (t *tableCmd) runURLs(ctx context.Context, app *App, id string) error {
	q, err := t.tf.query(id)
	if err != nil {
		return err
	}
	q.Select = []string{"url", "fetch_status", "content_mime_detected", "content_languages"}
	q.Limit = app.Limit
	return runColumnar(ctx, app, q, &t.tf, func(row map[string]any) error {
		return app.Out.Emit(mapRow(row, "url", "fetch_status", "content_mime_detected", "content_languages"))
	})
}

func (t *tableCmd) runLocations(ctx context.Context, app *App, id string) error {
	q, err := t.tf.query(id)
	if err != nil {
		return err
	}
	q.Select = ccrawl.LocationColumns
	q.Limit = app.Limit
	return runColumnar(ctx, app, q, &t.tf, func(row map[string]any) error {
		loc := ccrawl.Location{
			Filename: str(row["warc_filename"]),
			Offset:   toInt64(row["warc_record_offset"]),
			Length:   toInt64(row["warc_record_length"]),
			URL:      str(row["url"]),
		}
		return app.Out.Emit(Row{
			Cols:  []string{"filename", "offset", "length", "url"},
			Vals:  []string{loc.Filename, itoa64(loc.Offset), itoa64(loc.Length), loc.URL},
			Value: loc,
		})
	})
}

func (t *tableCmd) runCount(ctx context.Context, app *App, id string) error {
	q, err := t.tf.query(id)
	if err != nil {
		return err
	}
	q.Select = []string{"count(*) AS n"}
	return runColumnar(ctx, app, q, &t.tf, func(row map[string]any) error {
		return app.Out.Emit(Row{Cols: []string{"count"}, Vals: []string{str(row["n"])}, Value: row})
	})
}

func (t *tableCmd) runBreakdown(ctx context.Context, app *App, id string) error {
	col := t.groupCol
	q, err := t.tf.query(id)
	if err != nil {
		return err
	}
	q.Select = []string{col, "count(*) AS n"}
	sql := q.SQL(app.Cfg.Source) + "\nGROUP BY " + col + "\nORDER BY n DESC"
	if app.Limit > 0 {
		sql += fmt.Sprintf("\nLIMIT %d", app.Limit)
	}
	scan := nativeScan(q, app)
	scan.Aggregate = ccrawl.NativeGroupCount
	scan.GroupBy = col
	// The native engine returns the group under "value" rather than under the
	// column's own name, since one field name keeps the sink generic. Both
	// spellings are read here, and the row is rebuilt under the column's own
	// name, so the JSON is the same whichever engine ran.
	return runColumnarScan(ctx, app, sql, scan, &t.tf, func(row map[string]any) error {
		v, ok := row[col]
		if !ok {
			v = row["value"]
		}
		n := row["n"]
		return app.Out.Emit(Row{
			Cols:  []string{col, "count"},
			Vals:  []string{str(v), str(n)},
			Value: map[string]any{col: v, "n": n},
		})
	})
}

func (t *tableCmd) runQuery(ctx context.Context, app *App, id, sql string) error {
	src := ccrawl.ColumnarSource(id, t.tf.subset, app.Cfg.Source)
	return runColumnarSQL(ctx, app, replaceCCIndex(sql, src), &t.tf, func(row map[string]any) error {
		return app.Out.Emit(genericRow(row))
	})
}

func (t *tableCmd) runSchema(ctx context.Context, app *App, id string) error {
	src := ccrawl.ColumnarSource(id, t.tf.subset, app.Cfg.Source)
	// Wrap the DESCRIBE in a SELECT so it always renders as a normal result set.
	// Older duckdb (1.5.1) prints a bare DESCRIBE with the box renderer even in
	// -json mode, which yields no JSON rows; the subquery makes the output
	// consistent across duckdb versions.
	sql := fmt.Sprintf("SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('%s', hive_partitioning=1) LIMIT 1)", src)
	emit := func(name, typ string) error {
		return app.Out.Emit(Row{
			Cols:  []string{"column", "type"},
			Vals:  []string{name, typ},
			Value: map[string]any{"column_name": name, "column_type": typ},
		})
	}
	switch pickEngine(&t.tf, true) {
	case enginePrint:
		_, _ = fmt.Fprintln(cmdOut, sql)
		return nil
	case engineNative:
		// The schema is the same in every part, so reading one footer answers it.
		urls, err := ccrawl.ColumnarParquetURLs(ctx, app.HTTP, app.Cache, id, t.tf.subset, app.Cfg.Source)
		if err != nil {
			return err
		}
		if len(urls) == 0 {
			return noResults("no parquet files for this crawl and subset")
		}
		cols, err := ccrawl.NativeSchema(ctx, app.HTTP, urls[0])
		if err != nil {
			return err
		}
		// crawl and subset are hive partitions in the path rather than columns
		// in the file, so duckdb reports them and the footer does not. They are
		// appended by hand to keep the two engines printing the same table.
		cols = append(cols, [2]string{"crawl", "VARCHAR"}, [2]string{"subset", "VARCHAR"})
		for _, c := range cols {
			if err := emit(c[0], c[1]); err != nil {
				return err
			}
		}
		return app.Out.Flush()
	}
	return runColumnarSQL(ctx, app, sql, &t.tf, func(row map[string]any) error {
		return emit(str(row["column_name"]), str(row["column_type"]))
	})
}

// runColumnar renders the SQL from q, builds the equivalent native scan, and
// dispatches to whichever engine is going to run.
func runColumnar(ctx context.Context, app *App, q ccrawl.ColumnarQuery, tf *tableFlags, emit func(map[string]any) error) error {
	scan := nativeScan(q, app)
	scan.Aggregate = ccrawl.NativeRows
	scan.Select = q.Select
	if len(q.Select) == 1 && strings.HasPrefix(q.Select[0], "count(") {
		scan.Aggregate = ccrawl.NativeCount
		scan.Select = nil
	}
	return runColumnarScan(ctx, app, q.SQL(app.Cfg.Source), scan, tf, emit)
}

// nativeScan carries the parts of a columnar query the native engine shares
// with the SQL, leaving the caller to say what it wants produced.
func nativeScan(q ccrawl.ColumnarQuery, app *App) ccrawl.NativeScan {
	return ccrawl.NativeScan{Query: q, Limit: app.Limit, Workers: app.Workers}
}

// resolveGlobForDuckDB rewrites the quoted `*.parquet` glob in sql into a
// read_parquet list literal of real file URLs so a local duckdb run works
// without bucket listing. If sql does not contain the glob (custom SQL that
// names files directly) it is returned unchanged.
func resolveGlobForDuckDB(ctx context.Context, app *App, tf *tableFlags, sql string) (string, error) {
	id, err := app.Crawl(ctx)
	if err != nil {
		return "", err
	}
	glob := "'" + ccrawl.ColumnarSource(id, tf.subset, app.Cfg.Source) + "'"
	if !strings.Contains(sql, glob) {
		return sql, nil
	}
	urls, err := ccrawl.ColumnarParquetURLs(ctx, app.HTTP, app.Cache, id, tf.subset, app.Cfg.Source)
	if err != nil {
		return "", err
	}
	return strings.ReplaceAll(sql, glob, ccrawl.ParquetListLiteral(urls)), nil
}

// chosen is which engine a command is about to use.
type chosen int

const (
	engineDuckDB chosen = iota
	engineNative
	enginePrint
)

// pickEngine resolves --engine and --print into the engine that will actually
// run. Auto takes native whenever native can answer the query, installed duckdb
// or not, so the same command behaves the same way on every box. What is left
// is the arbitrary SQL, which goes to duckdb where duckdb exists and is printed
// where it does not. expressible says whether the native engine could answer
// this one at all.
func pickEngine(tf *tableFlags, expressible bool) chosen {
	switch {
	case tf.print || tf.engine == "print":
		return enginePrint
	case tf.engine == "native":
		return engineNative
	case tf.engine == "duckdb":
		return engineDuckDB
	case expressible:
		return engineNative
	case ccrawl.DuckDBAvailable():
		return engineDuckDB
	default:
		return enginePrint
	}
}

// runColumnarScan runs a query that both engines can answer. scan is the native
// form and sql is the duckdb form; they have to agree, which is why they are
// built side by side by the caller.
func runColumnarScan(ctx context.Context, app *App, sql string, scan ccrawl.NativeScan, tf *tableFlags, emit func(map[string]any) error) error {
	switch pickEngine(tf, ccrawl.NativeExpressible(scan)) {
	case enginePrint:
		if !tf.print && tf.engine == "auto" {
			_, _ = fmt.Fprintln(cmdErr, "no duckdb binary found and this query needs SQL; printing it instead")
		}
		_, _ = fmt.Fprintln(cmdOut, sql)
		return nil
	case engineNative:
		if !ccrawl.NativeExpressible(scan) {
			return usageErr("the native engine cannot answer this query; use --engine duckdb")
		}
		return runColumnarNative(ctx, app, scan, tf, emit)
	}
	return runDuckDB(ctx, app, sql, tf, emit)
}

func runColumnarSQL(ctx context.Context, app *App, sql string, tf *tableFlags, emit func(map[string]any) error) error {
	switch pickEngine(tf, false) {
	case enginePrint:
		if !tf.print && tf.engine == "auto" {
			_, _ = fmt.Fprintln(cmdErr, "no duckdb binary found; printing SQL (install duckdb or use --engine duckdb)")
		}
		_, _ = fmt.Fprintln(cmdOut, sql)
		return nil
	case engineNative:
		return usageErr("the native engine does not run SQL; use --engine duckdb")
	}
	return runDuckDB(ctx, app, sql, tf, emit)
}

func runDuckDB(ctx context.Context, app *App, sql string, tf *tableFlags, emit func(map[string]any) error) error {
	// The printed SQL carries the `*.parquet` glob, which Athena and Spark expand
	// themselves. duckdb cannot list the bucket, so for the duckdb run we swap the
	// glob for the explicit file list from the crawl's manifest.
	runSQL, err := resolveGlobForDuckDB(ctx, app, tf, sql)
	if err != nil {
		return err
	}
	n := 0
	if err := ccrawl.RunColumnarDuckDB(ctx, runSQL, func(row map[string]any) error {
		n++
		return emit(row)
	}); err != nil {
		return err
	}
	if err := app.Out.Flush(); err != nil {
		return err
	}
	if n == 0 {
		return noResults("query returned no rows")
	}
	return nil
}

func runColumnarNative(ctx context.Context, app *App, scan ccrawl.NativeScan, tf *tableFlags, emit func(map[string]any) error) error {
	id, err := app.Crawl(ctx)
	if err != nil {
		return err
	}
	scan.URLs, err = ccrawl.ColumnarParquetURLs(ctx, app.HTTP, app.Cache, id, tf.subset, app.Cfg.Source)
	if err != nil {
		return err
	}
	// A count over the whole index reports 0 rather than nothing, so the empty
	// result only applies to the queries that list rows.
	n := 0
	if err := ccrawl.RunColumnarNative(ctx, app.HTTP, scan, func(row map[string]any) error {
		n++
		return emit(row)
	}); err != nil {
		return err
	}
	if err := app.Out.Flush(); err != nil {
		return err
	}
	if n == 0 {
		return noResults("query returned no rows")
	}
	return nil
}

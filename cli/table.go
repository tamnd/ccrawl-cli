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
}

func (tf *tableFlags) query(crawl string) ccrawl.ColumnarQuery {
	return ccrawl.ColumnarQuery{
		Crawl: crawl, Subset: tf.subset,
		Domain: tf.domain, Host: tf.host, TLD: tf.tld,
		MIME: tf.mime, Lang: tf.lang, Status: tf.status,
		PathPrefix: tf.pathPrefix,
	}
}

// bind registers the columnar filter flags shared by every table subcommand. It
// is a method so a command can wire it as kit.Command.Flags without a closure.
func (tf *tableFlags) bind(f *kit.FlagSet) {
	f.StringVar(&tf.domain, "domain", "", "url_host_registered_domain")
	f.StringVar(&tf.host, "host", "", "url_host_name")
	f.StringVar(&tf.tld, "tld", "", "url_host_tld (e.g. gov)")
	f.StringVar(&tf.mime, "mime", "", "content_mime_detected")
	f.StringVar(&tf.lang, "lang", "", "content_languages contains")
	f.IntVar(&tf.status, "status", 0, "fetch_status (e.g. 200)")
	f.StringVar(&tf.pathPrefix, "path-prefix", "", "url_path prefix")
	f.StringVar(&tf.subset, "subset", "warc", "warc|crawldiagnostics|robotstxt")
	f.StringVar(&tf.engine, "engine", "auto", "auto|duckdb|print")
	f.BoolVar(&tf.print, "print", false, "print the SQL and exit")
}

func newTableCmd() kit.Command {
	return kit.Command{
		Use:     "table",
		Aliases: []string{"columnar", "athena"},
		Short:   "Query the columnar Parquet index",
		Long: `Query Common Crawl's columnar (Parquet) index, the fastest way to answer bulk
questions like "every PDF on .gov domains" without touching a single WARC.

By default the SQL runs against a local duckdb binary over the public Parquet
files. With --engine print (or no duckdb installed) the ready-to-run SQL is
printed so you can paste it into Athena, Spark, Trino, or DuckDB yourself.

Examples:
  ccrawl table urls --domain example.com --status 200 -o url
  ccrawl table count --tld gov -c 2024-51
  ccrawl table locations --domain example.com -o jsonl | ccrawl fetch -
  ccrawl table sql --tld gov --mime application/pdf --print
  ccrawl table query "SELECT url FROM ccindex LIMIT 10"`,
		Sub: []kit.Command{
			newTableURLsCmd(),
			newTableLocationsCmd(),
			newTableCountCmd(),
			newTableBreakdownCmd("langs", "content_languages"),
			newTableBreakdownCmd("mimes", "content_mime_detected"),
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
		q := t.tf.query(id)
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
	q := t.tf.query(id)
	q.Select = []string{"url", "fetch_status", "content_mime_detected", "content_languages"}
	q.Limit = app.Limit
	return runColumnar(ctx, app, q, &t.tf, func(row map[string]any) error {
		return app.Out.Emit(mapRow(row, "url", "fetch_status", "content_mime_detected", "content_languages"))
	})
}

func (t *tableCmd) runLocations(ctx context.Context, app *App, id string) error {
	q := t.tf.query(id)
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
	q := t.tf.query(id)
	q.Select = []string{"count(*) AS n"}
	return runColumnar(ctx, app, q, &t.tf, func(row map[string]any) error {
		return app.Out.Emit(Row{Cols: []string{"count"}, Vals: []string{str(row["n"])}, Value: row})
	})
}

func (t *tableCmd) runBreakdown(ctx context.Context, app *App, id string) error {
	col := t.groupCol
	q := t.tf.query(id)
	q.Select = []string{col, "count(*) AS n"}
	sql := q.SQL(app.Cfg.Source) + "\nGROUP BY " + col + "\nORDER BY n DESC"
	if app.Limit > 0 {
		sql += fmt.Sprintf("\nLIMIT %d", app.Limit)
	}
	return runColumnarSQL(ctx, app, sql, &t.tf, func(row map[string]any) error {
		return app.Out.Emit(Row{Cols: []string{col, "count"}, Vals: []string{str(row[col]), str(row["n"])}, Value: row})
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
	return runColumnarSQL(ctx, app, sql, &t.tf, func(row map[string]any) error {
		return app.Out.Emit(Row{
			Cols:  []string{"column", "type"},
			Vals:  []string{str(row["column_name"]), str(row["column_type"])},
			Value: row,
		})
	})
}

// runColumnar renders the SQL from q and dispatches to the engine.
func runColumnar(ctx context.Context, app *App, q ccrawl.ColumnarQuery, tf *tableFlags, emit func(map[string]any) error) error {
	return runColumnarSQL(ctx, app, q.SQL(app.Cfg.Source), tf, emit)
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

func runColumnarSQL(ctx context.Context, app *App, sql string, tf *tableFlags, emit func(map[string]any) error) error {
	engine := tf.engine
	if tf.print || engine == "print" {
		_, _ = fmt.Fprintln(cmdOut, sql)
		return nil
	}
	if engine == "auto" && !ccrawl.DuckDBAvailable() {
		_, _ = fmt.Fprintln(cmdErr, "no duckdb binary found; printing SQL (install duckdb or use --engine duckdb)")
		_, _ = fmt.Fprintln(cmdOut, sql)
		return nil
	}
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

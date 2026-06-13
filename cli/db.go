package cli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func newDBCmd() kit.Command {
	return kit.Command{
		Use:   "db",
		Short: "Build and query a local DuckDB database",
		Long: `Materialise a slice of Common Crawl into a local DuckDB file you can query
offline, again and again, without re-downloading. Everything here shells out to
the duckdb binary, so the ccrawl binary itself stays pure Go.

The default database lives at <data-dir>/ccrawl.duckdb.

Examples:
  ccrawl db load --tld gov --status 200          pull matching captures into a table
  ccrawl db sql "SELECT count(*) FROM captures"   query the local database
  ccrawl db shell                                 open an interactive duckdb session
  ccrawl db path                                  print the database path`,
		Sub: []kit.Command{
			newDBLoadCmd(),
			newDBSQLCmd(),
			newDBShellCmd(),
			newDBPathCmd(),
		},
	}
}

func requireDuckDB() error {
	if !ccrawl.DuckDBAvailable() {
		return usageErr("this command needs the duckdb binary on PATH; install it from https://duckdb.org/docs/installation")
	}
	return nil
}

// dbLoadCmd holds the columnar filter flags and the destination table name.
type dbLoadCmd struct {
	tf    tableFlags
	table string
}

func newDBLoadCmd() kit.Command {
	d := &dbLoadCmd{}
	return kit.Command{
		Use:   "load",
		Short: "Load matching captures from the columnar index into a local table",
		Write: true,
		Flags: d.flags,
		Run:   d.run,
	}
}

func (d *dbLoadCmd) flags(f *kit.FlagSet) {
	d.tf.bind(f)
	f.StringVar(&d.table, "table", "captures", "destination table name")
}

func (d *dbLoadCmd) run(ctx context.Context, _ []string) error {
	if err := requireDuckDB(); err != nil {
		return err
	}
	app := appFromCtx(ctx)
	id, err := app.Crawl(ctx)
	if err != nil {
		return err
	}
	q := d.tf.query(id)
	q.Select = ccrawl.DefaultColumnarColumns
	q.Limit = app.Limit
	if err := os.MkdirAll(filepath.Dir(app.Cfg.DBPath), 0o755); err != nil {
		return err
	}
	stmt := fmt.Sprintf("CREATE OR REPLACE TABLE %s AS\n%s", d.table, q.SQL(app.Cfg.Source))
	if app.dryRun {
		_, _ = fmt.Fprintln(cmdOut, stmt)
		return nil
	}
	// Expand the bucket glob to the explicit file list duckdb can read.
	stmt, err = resolveGlobForDuckDB(ctx, app, &d.tf, stmt)
	if err != nil {
		return err
	}
	if err := runDuckDBFile(ctx, app.Cfg.DBPath, stmt); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmdErr, "loaded table %q into %s\n", d.table, app.Cfg.DBPath)
	return runDuckDBStream(ctx, app, fmt.Sprintf("SELECT count(*) AS rows FROM %s", d.table), func(row map[string]any) error {
		return app.Out.Emit(Row{Cols: []string{"table", "rows"}, Vals: []string{d.table, str(row["rows"])}, Value: row})
	})
}

func newDBSQLCmd() kit.Command {
	return kit.Command{
		Use:   "sql <query>",
		Short: "Run SQL against the local DuckDB database",
		Args:  kit.ExactArgs(1),
		Run:   runDBSQL,
	}
}

func runDBSQL(ctx context.Context, args []string) error {
	if err := requireDuckDB(); err != nil {
		return err
	}
	app := appFromCtx(ctx)
	if _, err := os.Stat(app.Cfg.DBPath); err != nil {
		return noResults("no local database yet; run 'ccrawl db load' first")
	}
	return runDuckDBStream(ctx, app, args[0], func(row map[string]any) error {
		return app.Out.Emit(genericRow(row))
	})
}

func newDBShellCmd() kit.Command {
	return kit.Command{
		Use:   "shell",
		Short: "Open an interactive duckdb session on the local database",
		Run:   runDBShell,
	}
}

func runDBShell(ctx context.Context, _ []string) error {
	if err := requireDuckDB(); err != nil {
		return err
	}
	app := appFromCtx(ctx)
	cmd := exec.CommandContext(ctx, "duckdb", app.Cfg.DBPath)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func newDBPathCmd() kit.Command {
	return kit.Command{
		Use:   "path",
		Short: "Print the local database path",
		Run:   runDBPath,
	}
}

func runDBPath(ctx context.Context, _ []string) error {
	app := appFromCtx(ctx)
	_, _ = fmt.Fprintln(cmdOut, app.Cfg.DBPath)
	return nil
}

// runDuckDBFile runs a statement against a persistent duckdb file, discarding rows.
func runDuckDBFile(ctx context.Context, dbPath, sql string) error {
	full := ccrawl.DuckDBPrelude + "\n" + sql + ";"
	cmd := exec.CommandContext(ctx, "duckdb", dbPath, "-c", full)
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// runDuckDBStream runs a query against the local database and streams JSON rows.
func runDuckDBStream(ctx context.Context, app *App, sql string, emit func(map[string]any) error) error {
	n := 0
	if err := ccrawl.RunDuckDBJSON(ctx, app.Cfg.DBPath, sql, func(row map[string]any) error {
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

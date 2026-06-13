package cli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func newDBCmd(app *App) *cobra.Command {
	cmd := &cobra.Command{
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
	}
	cmd.AddCommand(
		newDBLoadCmd(app),
		newDBSQLCmd(app),
		newDBShellCmd(app),
		newDBPathCmd(app),
	)
	return cmd
}

func requireDuckDB() error {
	if !ccrawl.DuckDBAvailable() {
		return usageErr("this command needs the duckdb binary on PATH; install it from https://duckdb.org/docs/installation")
	}
	return nil
}

func newDBLoadCmd(app *App) *cobra.Command {
	tf := &tableFlags{}
	var table string
	c := &cobra.Command{
		Use:   "load",
		Short: "Load matching captures from the columnar index into a local table",
		RunE: func(c *cobra.Command, _ []string) error {
			if err := requireDuckDB(); err != nil {
				return err
			}
			id, err := app.Crawl(c.Context())
			if err != nil {
				return err
			}
			q := tf.query(id)
			q.Select = ccrawl.DefaultColumnarColumns
			q.Limit = app.Limit
			if err := os.MkdirAll(filepath.Dir(app.Cfg.DBPath), 0o755); err != nil {
				return err
			}
			stmt := fmt.Sprintf("CREATE OR REPLACE TABLE %s AS\n%s", table, q.SQL(app.Cfg.Source))
			if app.dryRun {
				fmt.Fprintln(cmdOut, stmt)
				return nil
			}
			if err := runDuckDBFile(c.Context(), app.Cfg.DBPath, stmt); err != nil {
				return err
			}
			fmt.Fprintf(cmdErr, "loaded table %q into %s\n", table, app.Cfg.DBPath)
			return runDuckDBStream(c.Context(), app, fmt.Sprintf("SELECT count(*) AS rows FROM %s", table), func(row map[string]any) error {
				return app.Out.Emit(Row{Cols: []string{"table", "rows"}, Vals: []string{table, str(row["rows"])}, Value: row})
			})
		},
	}
	addTableFlags(c, tf)
	c.Flags().StringVar(&table, "table", "captures", "destination table name")
	return c
}

func newDBSQLCmd(app *App) *cobra.Command {
	c := &cobra.Command{
		Use:   "sql <query>",
		Short: "Run SQL against the local DuckDB database",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			if err := requireDuckDB(); err != nil {
				return err
			}
			if _, err := os.Stat(app.Cfg.DBPath); err != nil {
				return noResults("no local database yet; run 'ccrawl db load' first")
			}
			return runDuckDBStream(c.Context(), app, args[0], func(row map[string]any) error {
				return app.Out.Emit(genericRow(row))
			})
		},
	}
	return c
}

func newDBShellCmd(app *App) *cobra.Command {
	return &cobra.Command{
		Use:   "shell",
		Short: "Open an interactive duckdb session on the local database",
		RunE: func(c *cobra.Command, _ []string) error {
			if err := requireDuckDB(); err != nil {
				return err
			}
			cmd := exec.CommandContext(c.Context(), "duckdb", app.Cfg.DBPath)
			cmd.Stdin = os.Stdin
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			return cmd.Run()
		},
	}
}

func newDBPathCmd(app *App) *cobra.Command {
	return &cobra.Command{
		Use:   "path",
		Short: "Print the local database path",
		RunE: func(c *cobra.Command, _ []string) error {
			fmt.Fprintln(cmdOut, app.Cfg.DBPath)
			return nil
		},
	}
}

// runDuckDBFile runs a statement against a persistent duckdb file, discarding rows.
func runDuckDBFile(ctx context.Context, dbPath, sql string) error {
	full := "INSTALL httpfs; LOAD httpfs; SET enable_progress_bar=false;\n" + sql + ";"
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

package ccrawl

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// ColumnarQuery builds SQL against the columnar (Parquet) index. The zero value
// selects everything; set fields to add WHERE clauses.
type ColumnarQuery struct {
	Crawl      string
	Subset     string // warc (default) | crawldiagnostics | robotstxt
	Domain     string // url_host_registered_domain
	Host       string // url_host_name
	TLD        string // url_host_tld
	MIME       string // content_mime_detected
	Lang       string // content_languages (substring match)
	PathPrefix string // url_path prefix
	Status     int    // fetch_status (0 = any)
	Select     []string
	Limit      int
}

// DefaultColumnarColumns are the columns selected when none are given.
var DefaultColumnarColumns = []string{
	"url", "url_host_registered_domain", "fetch_status",
	"content_mime_detected", "content_languages",
	"warc_filename", "warc_record_offset", "warc_record_length",
}

// LocationColumns return just the fields needed to range-fetch a record.
var LocationColumns = []string{"url", "warc_filename", "warc_record_offset", "warc_record_length"}

// SQL renders the query as a runnable DuckDB statement reading parquet over the
// given source. The same text runs in Athena or Spark after swapping read_parquet
// for the engine's table reference.
func (q ColumnarQuery) SQL(src Source) string {
	cols := q.Select
	if len(cols) == 0 {
		cols = DefaultColumnarColumns
	}
	src2 := ColumnarSource(q.Crawl, q.Subset, src)
	var where []string
	if q.Domain != "" {
		where = append(where, eq("url_host_registered_domain", q.Domain))
	}
	if q.Host != "" {
		where = append(where, eq("url_host_name", q.Host))
	}
	if q.TLD != "" {
		where = append(where, eq("url_host_tld", q.TLD))
	}
	if q.MIME != "" {
		where = append(where, eq("content_mime_detected", q.MIME))
	}
	if q.Lang != "" {
		where = append(where, fmt.Sprintf("content_languages LIKE '%%%s%%'", sqlEscape(q.Lang)))
	}
	if q.PathPrefix != "" {
		where = append(where, fmt.Sprintf("url_path LIKE '%s%%'", sqlEscape(q.PathPrefix)))
	}
	if q.Status != 0 {
		where = append(where, "fetch_status = "+strconv.Itoa(q.Status))
	}

	var b strings.Builder
	fmt.Fprintf(&b, "SELECT %s\nFROM read_parquet('%s', hive_partitioning=1)", strings.Join(cols, ", "), src2)
	if len(where) > 0 {
		b.WriteString("\nWHERE " + strings.Join(where, "\n  AND "))
	}
	if q.Limit > 0 {
		fmt.Fprintf(&b, "\nLIMIT %d", q.Limit)
	}
	return b.String()
}

func eq(col, val string) string { return fmt.Sprintf("%s = '%s'", col, sqlEscape(val)) }
func sqlEscape(s string) string { return strings.ReplaceAll(s, "'", "''") }

// DuckDBPrelude is prepended to every statement ccrawl sends to the duckdb
// binary. httpfs reads remote Parquet over HTTPS or S3; the progress bar is
// noise on a pipe; and allow_asterisks_in_http_paths is required because the
// columnar index is addressed with a glob (subset=warc/*.parquet) over HTTP,
// which duckdb refuses by default.
const DuckDBPrelude = "INSTALL httpfs; LOAD httpfs; SET enable_progress_bar=false; SET allow_asterisks_in_http_paths=true;"

// DuckDBAvailable reports whether a duckdb binary is on PATH.
func DuckDBAvailable() bool {
	_, err := exec.LookPath("duckdb")
	return err == nil
}

// RunColumnarDuckDB executes sql with the local duckdb binary, installing the
// httpfs extension for S3/HTTPS parquet access, and streams JSON rows to emit.
func RunColumnarDuckDB(ctx context.Context, sql string, emit func(map[string]any) error) error {
	return RunDuckDBJSON(ctx, "", sql, emit)
}

// RunDuckDBJSON runs sql with the local duckdb binary and streams JSON rows to
// emit. An empty dbPath runs against an in-memory database; a path opens (and
// creates) a persistent database file. httpfs is loaded so remote parquet over
// HTTPS or S3 works either way.
func RunDuckDBJSON(ctx context.Context, dbPath, sql string, emit func(map[string]any) error) error {
	if !DuckDBAvailable() {
		return fmt.Errorf("duckdb binary not found on PATH; install duckdb or use --engine print")
	}
	full := DuckDBPrelude + "\n" + ensureSemicolon(sql)
	args := []string{"-json"}
	if dbPath != "" {
		args = append(args, dbPath)
	}
	args = append(args, "-c", full)
	cmd := exec.CommandContext(ctx, "duckdb", args...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	dec := json.NewDecoder(bufio.NewReaderSize(out, 1<<20))
	// duckdb -json prints a single JSON array.
	tok, err := dec.Token()
	if err == nil {
		if d, ok := tok.(json.Delim); ok && d == '[' {
			for dec.More() {
				var row map[string]any
				if err := dec.Decode(&row); err != nil {
					break
				}
				if err := emit(row); err != nil {
					_ = cmd.Process.Kill()
					return err
				}
			}
		}
	}
	if werr := cmd.Wait(); werr != nil {
		return fmt.Errorf("duckdb: %v: %s", werr, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func ensureSemicolon(sql string) string {
	sql = strings.TrimSpace(sql)
	if !strings.HasSuffix(sql, ";") {
		sql += ";"
	}
	return sql
}

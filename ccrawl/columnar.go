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

	// Negated filters. A null counts as a match for all of these: a row whose
	// content_languages is missing is exactly what "not Vietnamese" is hunting
	// for, and SQL's own <> would drop it silently.
	NotTLD    string // url_host_tld is null or something else
	NotMIME   string // content_mime_detected is null or something else
	NotLang   string // content_languages is null or does not contain this
	NotStatus int    // fetch_status is null or something else (0 = unset)

	// Set filters, for the cases where one host is not the question.
	Hosts   []string // url_host_name in this set
	Domains []string // url_host_registered_domain in this set

	Select []string
	Limit  int
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
		// url_surtkey is the column the Parquet files are sorted on, so a prefix
		// predicate on it lets the engine skip whole row groups by their min/max
		// stats instead of scanning every file. The registered-domain equality
		// above keeps the result exact; this only narrows the bytes read. The two
		// patterns cover the apex (com,example)/...) and every subdomain
		// (com,example,www)...) without also matching example2.com.
		if rev := surtHostKey(q.Domain); rev != "" {
			r := sqlEscape(rev)
			where = append(where, fmt.Sprintf("(url_surtkey LIKE '%s)%%' OR url_surtkey LIKE '%s,%%')", r, r))
		}
	}
	if q.Host != "" {
		where = append(where, eq("url_host_name", q.Host))
		// Same row-group pruning for an exact host: its surtkey is the reversed
		// host followed by ')'.
		if rev := surtHostKey(q.Host); rev != "" {
			where = append(where, fmt.Sprintf("url_surtkey LIKE '%s)%%'", sqlEscape(rev)))
		}
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
	if len(q.Hosts) > 0 {
		where = append(where, inSet("url_host_name", q.Hosts))
	}
	if len(q.Domains) > 0 {
		where = append(where, inSet("url_host_registered_domain", q.Domains))
	}
	// The negated forms all spell out IS NULL rather than relying on <>, which
	// in SQL is unknown against a null and therefore drops the row. A row with
	// no language label is the single most interesting row for --not-lang, so
	// dropping it would defeat the flag.
	if q.NotTLD != "" {
		where = append(where, notEq("url_host_tld", q.NotTLD))
	}
	if q.NotMIME != "" {
		where = append(where, notEq("content_mime_detected", q.NotMIME))
	}
	if q.NotLang != "" {
		where = append(where, fmt.Sprintf(
			"(content_languages IS NULL OR content_languages NOT LIKE '%%%s%%')", sqlEscape(q.NotLang)))
	}
	if q.NotStatus != 0 {
		where = append(where, fmt.Sprintf(
			"(fetch_status IS NULL OR fetch_status <> %d)", q.NotStatus))
	}

	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "SELECT %s\nFROM read_parquet('%s', hive_partitioning=1)", strings.Join(cols, ", "), src2)
	if len(where) > 0 {
		b.WriteString("\nWHERE " + strings.Join(where, "\n  AND "))
	}
	if q.Limit > 0 {
		_, _ = fmt.Fprintf(&b, "\nLIMIT %d", q.Limit)
	}
	return b.String()
}

func eq(col, val string) string { return fmt.Sprintf("%s = '%s'", col, sqlEscape(val)) }
func sqlEscape(s string) string { return strings.ReplaceAll(s, "'", "''") }

// notEq renders a negated equality that a null satisfies.
func notEq(col, val string) string {
	return fmt.Sprintf("(%s IS NULL OR %s <> '%s')", col, col, sqlEscape(val))
}

// inSet renders a set membership test. duckdb handles a large IN list fine, and
// it is one query rather than one query per entry, which is the whole point.
func inSet(col string, vals []string) string {
	quoted := make([]string, len(vals))
	for i, v := range vals {
		quoted[i] = "'" + sqlEscape(v) + "'"
	}
	return col + " IN (" + strings.Join(quoted, ", ") + ")"
}

// surtHostKey reverses a host's labels into the comma-separated form that begins
// every url_surtkey: "www.example.com" -> "com,example,www". Unlike SURT it
// keeps a leading "www." because Common Crawl's url_surtkey does too, and it
// returns just the host portion (no trailing ')').
func surtHostKey(host string) string {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), ".")
	if host == "" {
		return ""
	}
	labels := strings.Split(host, ".")
	for i, j := 0, len(labels)-1; i < j; i, j = i+1, j-1 {
		labels[i], labels[j] = labels[j], labels[i]
	}
	return strings.Join(labels, ",")
}

// DuckDBPrelude is prepended to every statement ccrawl sends to the duckdb
// binary. httpfs reads remote Parquet over HTTPS; the progress bar is noise
// on a pipe.
const DuckDBPrelude = "INSTALL httpfs; LOAD httpfs; SET enable_progress_bar=false;"

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

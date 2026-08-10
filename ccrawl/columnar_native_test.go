package ccrawl

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// indexRow mirrors the columns of the Common Crawl columnar index the native
// engine reads. Optional is on every string column because the real files have
// nulls in most of them, and null handling is the part worth testing.
type indexRow struct {
	URLSurtkey  string  `parquet:"url_surtkey,optional,dict"`
	URL         string  `parquet:"url,optional,dict"`
	HostName    string  `parquet:"url_host_name,optional,dict"`
	HostTLD     string  `parquet:"url_host_tld,optional,dict"`
	HostDomain  string  `parquet:"url_host_registered_domain,optional,dict"`
	Path        string  `parquet:"url_path,optional,dict"`
	FetchStatus int32   `parquet:"fetch_status,optional"`
	MIME        string  `parquet:"content_mime_detected,optional,dict"`
	Languages   *string `parquet:"content_languages,optional,dict"`
	WARCFile    string  `parquet:"warc_filename,optional,dict"`
	Offset      int64   `parquet:"warc_record_offset,optional"`
	Length      int64   `parquet:"warc_record_length,optional"`
}

// strp is here so a fixture row can hold an empty language rather than no
// language, which are two different things in the real index and have to stay
// two different things through the engine.
func strp(s string) *string { return &s }

// row builds one index row from a host and path, filling the derived columns
// the way the real index does so the surtkey pruning has something to prune on.
func row(host, path, mime string, lang *string, status int32) indexRow {
	dom := host
	if n := len(host) - len(".example.com"); n > 0 && host[n:] == ".example.com" {
		dom = "example.com"
	}
	return indexRow{
		URLSurtkey: surtHostKey(host) + ")" + path, URL: "https://" + host + path,
		HostName: host, HostTLD: host[len(host)-3:], HostDomain: dom, Path: path,
		FetchStatus: status, MIME: mime, Languages: lang,
		WARCFile: "crawl-data/CC-MAIN-2025-05/segments/1/warc/" + host + ".warc.gz",
		Offset:   1024, Length: 2048,
	}
}

// testRows are sorted by surtkey, like the real files, so row group statistics
// are non-overlapping and pruning has a chance to work.
func testRows() []indexRow {
	rows := []indexRow{
		row("a.example.com", "/one", "text/html", strp("eng"), 200),
		row("a.example.com", "/two", "text/html", strp("eng,fra"), 200),
		row("b.example.com", "/three", "application/pdf", strp("eng"), 404),
		row("example.com", "/", "text/html", strp("vie"), 200),
		row("example.com", "/about", "text/html", strp("vie,eng"), 200),
		row("other.org", "/x", "text/html", strp("deu"), 200),
		// One row with an empty language and one with none at all, which the
		// breakdown has to report as two groups rather than one.
		row("other.org", "/y", "text/plain", strp(""), 500),
		row("shop.example.net", "/p", "text/html", strp("eng"), 200),
		row("null.example.org", "/n", "text/html", nil, 200),
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].URLSurtkey < rows[j].URLSurtkey })
	return rows
}

// writeTestParquet writes the rows into a Parquet file with small row groups,
// so a single file exercises the multi row group path.
func writeTestParquet(t *testing.T, rows []indexRow) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "part.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[indexRow](f, parquet.MaxRowsPerRowGroup(3), parquet.PageBufferSize(1<<10))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// serveFile puts the file behind an HTTP server that honors range requests, so
// the test drives the same httpReaderAt path the real command does.
func serveFile(t *testing.T, path string) (*HTTPClient, string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, path)
	}))
	t.Cleanup(srv.Close)
	return NewHTTPClient(Config{}), srv.URL + "/part.parquet"
}

// scanFixture runs a scan against the fixture file and returns the emitted rows.
func scanFixture(t *testing.T, s NativeScan) []map[string]any {
	t.Helper()
	h, url := serveFile(t, writeTestParquet(t, testRows()))
	s.URLs = []string{url}
	var got []map[string]any
	if err := RunColumnarNative(context.Background(), h, s, func(m map[string]any) error {
		got = append(got, m)
		return nil
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return got
}

func TestNativeCount(t *testing.T) {
	cases := []struct {
		name string
		q    ColumnarQuery
		want int64
	}{
		{"everything", ColumnarQuery{}, 9},
		{"tld", ColumnarQuery{TLD: "com"}, 5},
		{"domain includes apex and subdomains", ColumnarQuery{Domain: "example.com"}, 5},
		{"host is exact", ColumnarQuery{Host: "example.com"}, 2},
		{"status", ColumnarQuery{Status: 200}, 7},
		{"mime", ColumnarQuery{MIME: "application/pdf"}, 1},
		{"lang substring", ColumnarQuery{Lang: "eng"}, 5},
		{"path prefix", ColumnarQuery{PathPrefix: "/t"}, 2},
		{"combined", ColumnarQuery{Domain: "example.com", Status: 200, MIME: "text/html"}, 4},
		{"matches nothing", ColumnarQuery{Domain: "absent.example"}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := scanFixture(t, NativeScan{Query: tc.q, Aggregate: NativeCount})
			if len(got) != 1 {
				t.Fatalf("want one count row, got %d", len(got))
			}
			if got[0]["n"] != tc.want {
				t.Errorf("count = %v, want %d", got[0]["n"], tc.want)
			}
		})
	}
}

// The domain filter is the one that has to match both the apex host's captures
// and every subdomain's, which are two different surtkey shapes. Getting this
// wrong silently drops the apex, so it gets its own test.
func TestNativeDomainMatchesApexAndSubdomains(t *testing.T) {
	got := scanFixture(t, NativeScan{
		Query:     ColumnarQuery{Domain: "example.com"},
		Aggregate: NativeRows,
		Select:    []string{"url_host_name"},
	})
	seen := map[string]int{}
	for _, r := range got {
		seen[r["url_host_name"].(string)]++
	}
	want := map[string]int{"example.com": 2, "a.example.com": 2, "b.example.com": 1}
	if len(seen) != len(want) {
		t.Fatalf("hosts = %v, want %v", seen, want)
	}
	for h, n := range want {
		if seen[h] != n {
			t.Errorf("host %s = %d, want %d", h, seen[h], n)
		}
	}
}

func TestNativeRowsProjectAndLimit(t *testing.T) {
	got := scanFixture(t, NativeScan{
		Query:     ColumnarQuery{Host: "a.example.com"},
		Aggregate: NativeRows,
		Select:    LocationColumns,
	})
	if len(got) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(got), got)
	}
	for _, r := range got {
		if len(r) != len(LocationColumns) {
			t.Errorf("row has %d columns, want %d: %v", len(r), len(LocationColumns), r)
		}
		if r["warc_record_offset"] != int64(1024) || r["warc_record_length"] != int64(2048) {
			t.Errorf("int columns did not survive: %v", r)
		}
		if _, ok := r["url"].(string); !ok {
			t.Errorf("url is %T, want string", r["url"])
		}
	}

	// The limit has to stop the scan, not just truncate what was collected.
	lim := scanFixture(t, NativeScan{
		Aggregate: NativeRows, Select: []string{"url"}, Limit: 3, Workers: 1,
	})
	if len(lim) != 3 {
		t.Errorf("limit 3 emitted %d rows", len(lim))
	}
}

func TestNativeGroupCount(t *testing.T) {
	got := scanFixture(t, NativeScan{
		Query:     ColumnarQuery{},
		Aggregate: NativeGroupCount,
		GroupBy:   "content_mime_detected",
	})
	if len(got) != 3 {
		t.Fatalf("want 3 mime groups, got %d: %v", len(got), got)
	}
	// Ordered by count descending, so the most common MIME is first.
	if got[0]["value"] != "text/html" || got[0]["n"] != int64(7) {
		t.Errorf("top group = %v, want text/html 7", got[0])
	}
	for i := 1; i < len(got); i++ {
		if got[i-1]["n"].(int64) < got[i]["n"].(int64) {
			t.Errorf("groups are not ordered by count: %v", got)
		}
	}
}

// An empty string and a null are different values, and duckdb reports them as
// two groups. The engine keeps them apart with a sentinel key, so this checks
// they stay apart and that the sentinel never leaks into the output.
func TestNativeGroupCountKeepsNullApartFromEmpty(t *testing.T) {
	got := scanFixture(t, NativeScan{
		Aggregate: NativeGroupCount,
		GroupBy:   "content_languages",
	})
	var empty, null, total int64
	for _, r := range got {
		total += r["n"].(int64)
		switch v := r["value"].(type) {
		case nil:
			null += r["n"].(int64)
		case string:
			if v == nullGroup {
				t.Fatalf("the null sentinel leaked into the output: %v", got)
			}
			if v == "" {
				empty += r["n"].(int64)
			}
		}
	}
	if empty != 1 || null != 1 {
		t.Errorf("empty = %d and null = %d, want one of each: %v", empty, null, got)
	}
	if total != 9 {
		t.Errorf("groups total %d rows, want 9", total)
	}
}

func TestNativeGroupCountLimit(t *testing.T) {
	got := scanFixture(t, NativeScan{
		Aggregate: NativeGroupCount, GroupBy: "url_host_tld", Limit: 1,
	})
	if len(got) != 1 {
		t.Fatalf("want 1 group, got %d", len(got))
	}
	if got[0]["value"] != "com" || got[0]["n"] != int64(5) {
		t.Errorf("top tld = %v, want com 5", got[0])
	}
}

func TestNativeSchema(t *testing.T) {
	h, url := serveFile(t, writeTestParquet(t, testRows()))
	cols, err := NativeSchema(context.Background(), h, url)
	if err != nil {
		t.Fatal(err)
	}
	types := map[string]string{}
	for _, c := range cols {
		types[c[0]] = c[1]
	}
	if types["url"] != "VARCHAR" {
		t.Errorf("url is %q, want VARCHAR", types["url"])
	}
	if types["fetch_status"] != "INTEGER" {
		t.Errorf("fetch_status is %q, want INTEGER", types["fetch_status"])
	}
	if types["warc_record_offset"] != "BIGINT" {
		t.Errorf("warc_record_offset is %q, want BIGINT", types["warc_record_offset"])
	}
}

// The point of the engine is reading less, so pruning gets tested on the
// decision itself rather than only on the answers. Counting bytes would prove
// nothing on a fixture this small, since one range request covers the file.
func TestNativePrunesRowGroups(t *testing.T) {
	f, err := os.Open(writeTestParquet(t, testRows()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	if len(pf.RowGroups()) != 3 {
		t.Fatalf("fixture has %d row groups, want the 3 the counts below assume", len(pf.RowGroups()))
	}

	// Three row groups of three rows each, in surtkey order:
	//   0: example.com/, example.com/about, a.example.com/one
	//   1: a.example.com/two, b.example.com/three, shop.example.net/p
	//   2: null.example.org/n, other.org/x, other.org/y
	// The counts below are what min and max on those groups can actually rule
	// out. A group whose bounds straddle the wanted value survives, which is
	// the honest answer, so the expectations are exact rather than "some".
	cases := []struct {
		name string
		q    ColumnarQuery
		want int
	}{
		// Every group's tld is below "zzz", so nothing has to be read at all.
		{"absent tld prunes every group", ColumnarQuery{TLD: "zzz"}, 3},
		// Group 1 runs from a.example.com to shop.example.net, and the wanted
		// host sorts inside that, so bounds cannot rule it out.
		{"absent host prunes what bounds allow", ColumnarQuery{Host: "nope.example.com"}, 2},
		// Group 2 holds a 200 and a 500, so 418 sorts inside its range.
		{"absent status prunes what bounds allow", ColumnarQuery{Status: 418}, 2},
		// Group 1 survives the host name bounds and is then pruned by the
		// surtkey prefix, which is the pruning this engine is built around.
		{"a present host prunes the groups it is not in", ColumnarQuery{Host: "other.org"}, 2},
		// Group 2 is all .org.
		{"a present tld still prunes", ColumnarQuery{TLD: "com"}, 1},
		{"no filter prunes nothing", ColumnarQuery{}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := newScanPlan(NativeScan{Query: tc.q, Aggregate: NativeCount})
			if err != nil {
				t.Fatal(err)
			}
			idx := map[string]int{}
			for _, c := range plan.filterCols {
				leaf, ok := pf.Schema().Lookup(c)
				if !ok {
					t.Fatalf("column %s is missing from the fixture", c)
				}
				idx[c] = leaf.ColumnIndex
			}
			pruned := 0
			for _, rg := range pf.RowGroups() {
				if pruneRowGroup(plan, idx, rg.ColumnChunks()) {
					pruned++
				}
			}
			if pruned != tc.want {
				t.Errorf("pruned %d of %d row groups, want %d", pruned, len(pf.RowGroups()), tc.want)
			}
		})
	}
}

// The fixture holds one row with no language at all and one with an empty
// string, so the two things a negated filter is easy to get wrong are both in
// front of it. A row Common Crawl never labelled is the row --not-lang exists
// to find, and SQL's own <> would drop it.
func TestNativeNegation(t *testing.T) {
	cases := []struct {
		name string
		q    ColumnarQuery
		want int64
	}{
		// 9 rows, 2 of them Vietnamese, so 7, and the unlabelled row is one of
		// the 7 rather than lost.
		{"not lang keeps the null and the empty", ColumnarQuery{NotLang: "vie"}, 7},
		{"not lang eng", ColumnarQuery{NotLang: "eng"}, 4},
		{"not tld", ColumnarQuery{NotTLD: "com"}, 4},
		{"not mime", ColumnarQuery{NotMIME: "text/html"}, 2},
		{"not status", ColumnarQuery{NotStatus: 200}, 2},
		// The gao recovery pass: everything on one TLD that is not labelled
		// with the language, including the rows with no label.
		{"tld and not lang", ColumnarQuery{TLD: "com", NotLang: "eng"}, 1},
		{"not lang matches nothing", ColumnarQuery{Domain: "example.com", NotTLD: "com"}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := scanFixture(t, NativeScan{Query: tc.q, Aggregate: NativeCount})
			if got[0]["n"] != tc.want {
				t.Errorf("count = %v, want %d", got[0]["n"], tc.want)
			}
		})
	}
}

// The unlabelled row has to come back by name, not just be counted, since a
// count that happens to be right can still be right for the wrong rows.
func TestNativeNotLangReturnsTheUnlabelledRow(t *testing.T) {
	got := scanFixture(t, NativeScan{
		Query:     ColumnarQuery{NotLang: "eng"},
		Aggregate: NativeRows,
		Select:    []string{"url_host_name", "content_languages"},
	})
	var hosts []string
	for _, m := range got {
		hosts = append(hosts, m["url_host_name"].(string))
	}
	sort.Strings(hosts)
	want := []string{"example.com", "null.example.org", "other.org", "other.org"}
	if fmt.Sprint(hosts) != fmt.Sprint(want) {
		t.Errorf("hosts = %v, want %v", hosts, want)
	}
	for _, m := range got {
		if m["url_host_name"] == "null.example.org" && m["content_languages"] != nil {
			t.Errorf("the unlabelled row came back with a language: %v", m["content_languages"])
		}
	}
}

func TestNativeSets(t *testing.T) {
	cases := []struct {
		name string
		q    ColumnarQuery
		want int64
	}{
		{"hosts", ColumnarQuery{Hosts: []string{"a.example.com", "other.org"}}, 4},
		{"hosts with a miss in the set", ColumnarQuery{Hosts: []string{"other.org", "absent.example"}}, 2},
		{"domains", ColumnarQuery{Domains: []string{"example.com", "other.org"}}, 7},
		{"a set of one is the same as the single flag", ColumnarQuery{Hosts: []string{"example.com"}}, 2},
		{"a set that matches nothing", ColumnarQuery{Hosts: []string{"absent.example"}}, 0},
		// Query 3 of the recovery pass: a host list, minus one TLD.
		{"hosts and not tld", ColumnarQuery{Hosts: []string{"a.example.com", "other.org"}, NotTLD: "com"}, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := scanFixture(t, NativeScan{Query: tc.q, Aggregate: NativeCount})
			if got[0]["n"] != tc.want {
				t.Errorf("count = %v, want %d", got[0]["n"], tc.want)
			}
		})
	}
}

// A negated predicate must never prune. Page statistics say nothing about the
// nulls in a page, so a group whose min and max both rule the value out can
// still hold the unlabelled rows the flag is hunting for. A set predicate does
// prune, on the span of the whole set rather than one member at a time.
func TestNativePruningWithNegationAndSets(t *testing.T) {
	f, err := os.Open(writeTestParquet(t, testRows()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		q    ColumnarQuery
		want int
	}{
		// Group 2 is all .org, so an equality on com prunes it. The negation of
		// the same thing prunes nothing, on purpose.
		{"not tld prunes nothing", ColumnarQuery{NotTLD: "com"}, 0},
		{"not lang prunes nothing", ColumnarQuery{NotLang: "vie"}, 0},
		{"not status prunes nothing", ColumnarQuery{NotStatus: 200}, 0},
		// Group 2 runs from null.example.org to other.org on host name, and
		// both wanted hosts sort below it.
		{"a set prunes on its span", ColumnarQuery{Hosts: []string{"a.example.com", "b.example.com"}}, 1},
		{"a set matching nothing prunes everything", ColumnarQuery{Hosts: []string{"zzz.example"}}, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := newScanPlan(NativeScan{Query: tc.q, Aggregate: NativeCount})
			if err != nil {
				t.Fatal(err)
			}
			idx := map[string]int{}
			for _, c := range plan.filterCols {
				leaf, ok := pf.Schema().Lookup(c)
				if !ok {
					t.Fatalf("column %s is missing from the fixture", c)
				}
				idx[c] = leaf.ColumnIndex
			}
			pruned := 0
			for _, rg := range pf.RowGroups() {
				if pruneRowGroup(plan, idx, rg.ColumnChunks()) {
					pruned++
				}
			}
			if pruned != tc.want {
				t.Errorf("pruned %d of %d row groups, want %d", pruned, len(pf.RowGroups()), tc.want)
			}
		})
	}
}

func TestNativeExpressible(t *testing.T) {
	if !NativeExpressible(NativeScan{Select: LocationColumns}) {
		t.Error("the location columns should be expressible")
	}
	if !NativeExpressible(NativeScan{Select: DefaultColumnarColumns}) {
		t.Error("the default columns should be expressible")
	}
	if NativeExpressible(NativeScan{Select: []string{"url", "content_charset"}}) {
		t.Error("a column the engine does not know should not be expressible")
	}
	if NativeExpressible(NativeScan{GroupBy: "warc_segment"}) {
		t.Error("grouping by an unknown column should not be expressible")
	}
}

func TestNativeErrors(t *testing.T) {
	h := NewHTTPClient(Config{})
	nop := func(map[string]any) error { return nil }
	if err := RunColumnarNative(context.Background(), h, NativeScan{Aggregate: NativeCount}, nop); err == nil {
		t.Error("a scan with no files should fail")
	}
	scan := NativeScan{URLs: []string{"https://example.invalid/x.parquet"}, Aggregate: NativeRows}
	if err := RunColumnarNative(context.Background(), h, scan, nop); err == nil {
		t.Error("a row scan with no columns should fail")
	}
}

// An emit that fails has to stop the scan and surface, rather than being
// swallowed by a worker.
func TestNativeEmitErrorPropagates(t *testing.T) {
	h, url := serveFile(t, writeTestParquet(t, testRows()))
	want := fmt.Errorf("downstream is gone")
	err := RunColumnarNative(context.Background(), h, NativeScan{
		URLs: []string{url}, Aggregate: NativeRows, Select: []string{"url"},
	}, func(map[string]any) error { return want })
	if err == nil || err.Error() != want.Error() {
		t.Errorf("err = %v, want %v", err, want)
	}
}

package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// The engine picker is where a wrong branch means a query silently runs
// somewhere the user did not ask for, so every combination gets pinned. The
// duckdb-available cases are the ones that depend on the machine, so they are
// split out below rather than guessed at.
func TestPickEngineExplicit(t *testing.T) {
	cases := []struct {
		name        string
		tf          tableFlags
		expressible bool
		want        chosen
	}{
		{"print flag beats everything", tableFlags{engine: "native", print: true}, true, enginePrint},
		{"engine print", tableFlags{engine: "print"}, true, enginePrint},
		{"engine native", tableFlags{engine: "native"}, true, engineNative},
		// Asking for native on a query it cannot answer still picks native, so
		// the caller can refuse rather than quietly running duckdb instead.
		{"engine native on SQL", tableFlags{engine: "native"}, false, engineNative},
		{"engine duckdb", tableFlags{engine: "duckdb"}, true, engineDuckDB},
		{"engine duckdb on SQL", tableFlags{engine: "duckdb"}, false, engineDuckDB},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pickEngine(&tc.tf, tc.expressible); got != tc.want {
				t.Errorf("pickEngine = %v, want %v", got, tc.want)
			}
		})
	}
}

// Auto has to land on native for anything native can answer, whether or not
// duckdb happens to be installed, or the same command would do different things
// on two boxes. Only the arbitrary SQL is allowed to depend on the machine.
func TestPickEngineAuto(t *testing.T) {
	tf := tableFlags{engine: "auto"}
	if got := pickEngine(&tf, true); got != engineNative {
		t.Errorf("auto on an expressible query = %v, want native", got)
	}
	want := enginePrint
	if ccrawl.DuckDBAvailable() {
		want = engineDuckDB
	}
	if got := pickEngine(&tf, false); got != want {
		t.Errorf("auto on SQL = %v, want %v", got, want)
	}
}

// A host list is something a person edits by hand, so it carries blank lines and
// notes about where it came from, and a file that reads as empty has to fail
// rather than quietly widen the query to every row in the crawl.
func TestReadSetFile(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}

	got, err := readSetFile("hosts", write("hosts.txt", "# from the 2025-40 seed list\n\nvnexpress.net\n  tuoitre.vn  \n\n# tail comment\n"))
	if err != nil {
		t.Fatalf("readSetFile: %v", err)
	}
	want := []string{"vnexpress.net", "tuoitre.vn"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("values = %v, want %v", got, want)
	}

	if got, err := readSetFile("hosts", ""); got != nil || err != nil {
		t.Errorf("no path should read nothing, got %v %v", got, err)
	}
	if _, err := readSetFile("hosts", write("empty.txt", "# nothing but a note\n\n")); err == nil {
		t.Error("a file with no values should be an error")
	}
	if _, err := readSetFile("hosts", filepath.Join(dir, "absent.txt")); err == nil {
		t.Error("a missing file should be an error")
	}
}

// The flags have to reach the query, since a filter that is parsed and then
// dropped returns a plausible answer to a question nobody asked.
func TestTableFlagsQueryCarriesNegationAndSets(t *testing.T) {
	dir := t.TempDir()
	hosts := filepath.Join(dir, "hosts.txt")
	if err := os.WriteFile(hosts, []byte("a.example.com\nb.example.com\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tf := tableFlags{
		notTLD: "vn", notMIME: "text/html", notLang: "vie", notStatus: 200,
		hostsFile: hosts,
	}
	q, err := tf.query("CC-MAIN-2026-25")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if q.NotTLD != "vn" || q.NotMIME != "text/html" || q.NotLang != "vie" || q.NotStatus != 200 {
		t.Errorf("negated filters did not reach the query: %+v", q)
	}
	if len(q.Hosts) != 2 || q.Hosts[0] != "a.example.com" {
		t.Errorf("hosts = %v, want the two from the file", q.Hosts)
	}
	if q.Domains != nil {
		t.Errorf("domains = %v, want nothing", q.Domains)
	}

	tf.hostsFile = filepath.Join(dir, "absent.txt")
	if _, err := tf.query("CC-MAIN-2026-25"); err == nil {
		t.Error("an unreadable hosts file should fail the command")
	}
}

// The columns each subcommand selects have to be ones the native engine knows,
// or auto would fall through to printing SQL on a machine with no duckdb, which
// is exactly the case the engine exists for.
func TestColumnarSelectionsAreNativelyExpressible(t *testing.T) {
	cases := []struct {
		name string
		scan ccrawl.NativeScan
	}{
		{"urls", ccrawl.NativeScan{Select: []string{"url", "fetch_status", "content_mime_detected", "content_languages"}}},
		{"locations", ccrawl.NativeScan{Select: ccrawl.LocationColumns}},
		{"count", ccrawl.NativeScan{Aggregate: ccrawl.NativeCount}},
		{"langs", ccrawl.NativeScan{Aggregate: ccrawl.NativeGroupCount, GroupBy: "content_languages"}},
		{"mimes", ccrawl.NativeScan{Aggregate: ccrawl.NativeGroupCount, GroupBy: "content_mime_detected"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !ccrawl.NativeExpressible(tc.scan) {
				t.Errorf("%s is not natively expressible", tc.name)
			}
		})
	}
}

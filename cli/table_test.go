package cli

import (
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

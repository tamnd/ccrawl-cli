package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func TestParseShardRange(t *testing.T) {
	total := 1000
	cases := []struct {
		spec string
		want []int
	}{
		{"0", []int{0}},
		{"5", []int{5}},
		{"0-4", []int{0, 1, 2, 3, 4}},
		{"0,2,4", []int{0, 2, 4}},
		{"0-2,5", []int{0, 1, 2, 5}},
		{"3,3,3", []int{3}}, // deduplication
	}
	for _, tc := range cases {
		got, err := parseShardRange(tc.spec, total)
		if err != nil {
			t.Errorf("parseShardRange(%q): unexpected error %v", tc.spec, err)
			continue
		}
		if len(got) != len(tc.want) {
			t.Errorf("parseShardRange(%q) = %v, want %v", tc.spec, got, tc.want)
			continue
		}
		for i, g := range got {
			if g != tc.want[i] {
				t.Errorf("parseShardRange(%q)[%d] = %d, want %d", tc.spec, i, g, tc.want[i])
			}
		}
	}
}

func TestParseShardRangeErrors(t *testing.T) {
	if _, err := parseShardRange("9999", 100); err == nil {
		t.Error("expected error for out-of-bounds shard")
	}
	if _, err := parseShardRange("notanumber", 100); err == nil {
		t.Error("expected error for non-numeric shard")
	}
	if _, err := parseShardRange("", 100); err == nil {
		t.Error("expected error for empty spec")
	}
}

func TestParseShardRangeAll(t *testing.T) {
	got, err := parseShardRange("all", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("all with total=5 gave %d shards, want 5", len(got))
	}
	for i, v := range got {
		if v != i {
			t.Errorf("[%d] = %d, want %d", i, v, i)
		}
	}
}

// writeLocationsFile lays down the JSONL a columnar locations query produces, so
// the test reads the format the pipeline actually pipes rather than a struct
// literal that cannot go stale in the same way.
func writeLocationsFile(t *testing.T, n int) string {
	t.Helper()
	var b strings.Builder
	for i := range n {
		fmt.Fprintf(&b, "{\"url\":\"https://example.com/%d\",\"filename\":\"crawl-data/CC-MAIN-2026-30/segments/a/warc/x.warc.gz\",\"offset\":%d,\"length\":900}\n", i, i*1000)
	}
	path := filepath.Join(t.TempDir(), "locations.jsonl")
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestReadLocationParts covers the cut. A part is the unit that gets one parquet
// file and one ledger entry, so the sizes and the order have to be exactly what
// the input said, including the short part at the end.
func TestReadLocationParts(t *testing.T) {
	h2m, err := ccrawl.LookupExtractor("h2m")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name      string
		locs      int
		partSize  int
		wantParts []int
	}{
		{"one full part", 10, 10, []int{10}},
		{"short tail", 10, 4, []int{4, 4, 2}},
		{"one per part", 3, 1, []int{1, 1, 1}},
		{"part larger than input", 3, 500, []int{3}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := &markdownExportCmd{locations: writeLocationsFile(t, tc.locs), partSize: tc.partSize}
			parts, err := v.readLocationParts(h2m)
			if err != nil {
				t.Fatalf("readLocationParts: %v", err)
			}
			if len(parts) != len(tc.wantParts) {
				t.Fatalf("cut into %d parts, want %d", len(parts), len(tc.wantParts))
			}
			for i, want := range tc.wantParts {
				if len(parts[i]) != want {
					t.Errorf("part %d holds %d locations, want %d", i, len(parts[i]), want)
				}
			}
			if got := countLocations(parts); got != tc.locs {
				t.Errorf("parts hold %d locations, want the %d that went in", got, tc.locs)
			}
			// Order is what makes a ledger from an interrupted run still mean what
			// it said, so the same input has to cut the same way every time.
			for i, l := range parts[0] {
				if want := int64(i * 1000); l.Offset != want {
					t.Errorf("part 0 location %d has offset %d, want %d", i, l.Offset, want)
				}
			}
		})
	}
}

func TestReadLocationPartsErrors(t *testing.T) {
	h2m, err := ccrawl.LookupExtractor("h2m")
	if err != nil {
		t.Fatal(err)
	}
	wet, err := ccrawl.LookupExtractor("wet")
	if err != nil {
		t.Fatal(err)
	}

	// WET shards have no record offsets to point at, so the combination is a
	// mistake worth naming rather than an empty run.
	v := &markdownExportCmd{locations: writeLocationsFile(t, 4), partSize: 2}
	if _, err := v.readLocationParts(wet); err == nil {
		t.Error("expected an error for --locations with the wet extractor")
	}

	v = &markdownExportCmd{locations: writeLocationsFile(t, 4), partSize: 0}
	if _, err := v.readLocationParts(h2m); err == nil {
		t.Error("expected an error for part-size 0")
	}

	empty := filepath.Join(t.TempDir(), "empty.jsonl")
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	v = &markdownExportCmd{locations: empty, partSize: 10}
	if _, err := v.readLocationParts(h2m); err == nil {
		t.Error("expected an error for an empty location stream")
	}

	v = &markdownExportCmd{locations: filepath.Join(t.TempDir(), "nope.jsonl"), partSize: 10}
	if _, err := v.readLocationParts(h2m); err == nil {
		t.Error("expected an error for a missing location file")
	}
}

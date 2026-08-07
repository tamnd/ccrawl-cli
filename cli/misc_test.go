package cli

import "testing"

func TestParseLocationLine(t *testing.T) {
	// CDX-style row, offset and length as strings.
	loc, ok := parseLocationLine(`{"filename":"x.warc.gz","offset":"100","length":"50","url":"https://a/"}`)
	if !ok || loc.Filename != "x.warc.gz" || loc.Offset != 100 || loc.Length != 50 {
		t.Errorf("cdx location parse: %+v ok=%v", loc, ok)
	}
	// Columnar-style row with warc_* names and numeric values.
	loc, ok = parseLocationLine(`{"warc_filename":"y.warc.gz","warc_record_offset":7,"warc_record_length":9}`)
	if !ok || loc.Filename != "y.warc.gz" || loc.Offset != 7 || loc.Length != 9 {
		t.Errorf("columnar location parse: %+v ok=%v", loc, ok)
	}
	// Non-location JSON is rejected.
	if _, ok := parseLocationLine(`{"hello":"world"}`); ok {
		t.Errorf("expected non-location to be rejected")
	}
}

// Both engines feed the same row map to the same output code, and they do not
// agree on the Go type of a number: duckdb goes through encoding/json and gives
// float64, the native engine gives the Parquet column's own int64. Missing one
// of those silently zeroed every warc offset the native engine produced.
func TestToInt64AcceptsBothEngines(t *testing.T) {
	cases := map[string]struct {
		in   any
		want int64
	}{
		"duckdb json number": {float64(1234), 1234},
		"native int64":       {int64(1234), 1234},
		"native int32":       {int32(1234), 1234},
		"plain int":          {1234, 1234},
		"cdx string":         {"1234", 1234},
		"missing":            {nil, 0},
		"not a number":       {"abc", 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := toInt64(tc.in); got != tc.want {
				t.Errorf("toInt64(%#v) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestHumanBytes(t *testing.T) {
	cases := map[int64]string{
		512:             "512 B",
		1024:            "1.0 KB",
		1024 * 1024:     "1.0 MB",
		5 * 1024 * 1024: "5.0 MB",
	}
	for in, want := range cases {
		if got := humanBytes(in); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", in, got, want)
		}
	}
}

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

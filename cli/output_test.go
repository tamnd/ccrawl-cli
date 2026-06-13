package cli

import (
	"bytes"
	"strings"
	"testing"
)

func renderRows(format Format, fields []string, rows ...Row) string {
	var buf bytes.Buffer
	o := &Output{w: &buf, format: format, fields: fields}
	for _, r := range rows {
		o.Emit(r)
	}
	o.Flush()
	return buf.String()
}

func TestOutputCSV(t *testing.T) {
	out := renderRows(FormatCSV, nil,
		Row{Cols: []string{"url", "status"}, Vals: []string{"https://a/", "200"}},
		Row{Cols: []string{"url", "status"}, Vals: []string{"https://b/", "404"}},
	)
	if !strings.HasPrefix(out, "url,status\n") {
		t.Errorf("missing header: %q", out)
	}
	if !strings.Contains(out, "https://a/,200") {
		t.Errorf("missing row: %q", out)
	}
}

func TestOutputFields(t *testing.T) {
	out := renderRows(FormatCSV, []string{"status"},
		Row{Cols: []string{"url", "status"}, Vals: []string{"https://a/", "200"}},
	)
	if strings.Contains(out, "https://a/") {
		t.Errorf("field projection leaked url: %q", out)
	}
	if !strings.Contains(out, "200") {
		t.Errorf("field projection dropped status: %q", out)
	}
}

func TestOutputJSONArray(t *testing.T) {
	out := renderRows(FormatJSON, nil,
		Row{Value: map[string]any{"n": 1}},
		Row{Value: map[string]any{"n": 2}},
	)
	if !strings.HasPrefix(strings.TrimSpace(out), "[") || !strings.HasSuffix(strings.TrimSpace(out), "]") {
		t.Errorf("not a json array: %q", out)
	}
}

func TestOutputURL(t *testing.T) {
	out := renderRows(FormatURL, nil,
		Row{Cols: []string{"timestamp", "url"}, Vals: []string{"2026", "https://a/"}},
	)
	if strings.TrimSpace(out) != "https://a/" {
		t.Errorf("url format = %q", out)
	}
}

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

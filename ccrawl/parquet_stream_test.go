package ccrawl

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestStreamingParquetWriterRoundTrips(t *testing.T) {
	var buf bytes.Buffer
	w := NewStreamingParquetWriter(&buf)

	cols := []string{"url", "status", "mime"}
	rows := [][]string{
		{"https://example.com/", "200", "text/html"},
		{"https://example.com/a.pdf", "200", "application/pdf"},
		{"https://example.com/gone", "404", ""},
	}
	for _, r := range rows {
		if err := w.EmitRow(cols, r); err != nil {
			t.Fatalf("EmitRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rd := bytes.NewReader(buf.Bytes())
	pf, err := parquet.OpenFile(rd, int64(buf.Len()))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if got := pf.NumRows(); got != int64(len(rows)) {
		t.Fatalf("NumRows = %d, want %d", got, len(rows))
	}

	// The schema must carry every column we emitted.
	have := map[string]bool{}
	for _, f := range pf.Schema().Fields() {
		have[f.Name()] = true
	}
	for _, c := range cols {
		if !have[c] {
			t.Errorf("schema missing column %q", c)
		}
	}

	// Read the rows back and confirm the first row's url value survives.
	urlCol := -1
	for i, path := range pf.Schema().Columns() {
		if len(path) == 1 && path[0] == "url" {
			urlCol = i
		}
	}
	if urlCol < 0 {
		t.Fatal("url column not found in schema")
	}
	rr := pf.RowGroups()[0].Rows()
	defer rr.Close()
	got := make([]parquet.Row, len(rows))
	n, err := rr.ReadRows(got)
	if err != nil && n == 0 {
		t.Fatalf("ReadRows: %v", err)
	}
	if n != len(rows) {
		t.Fatalf("read %d rows, want %d", n, len(rows))
	}
	if s := got[0][urlCol].String(); s != "https://example.com/" {
		t.Errorf("row0 url = %q, want the first url", s)
	}
}

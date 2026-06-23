package ccrawl

import (
	"context"
	"path/filepath"
	"testing"
)

// writeTestCDX writes a small Parquet file with the CDX columns the seed
// exporter reads, so the round-trip can run without a network shard.
func writeTestCDX(t *testing.T, rows []CDXRawRow) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "shard.parquet")
	w, err := NewParquetWriter[CDXRawRow](path)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestExportSeedFromCDX(t *testing.T) {
	rows := []CDXRawRow{
		{URL: "https://a.com/", URLHostName: "a.com", FetchStatus: 200, Digest: "D1", ContentMIMEDetected: "text/html", ContentLanguages: "eng"},
		{URL: "https://b.com/", URLHostName: "b.com", FetchStatus: 404, Digest: "D2", ContentMIMEDetected: "text/html"},
		{URL: "https://c.com/x.pdf", URLHostName: "c.com", FetchStatus: 200, Digest: "D3", ContentMIMEDetected: "application/pdf"},
	}
	shard := writeTestCDX(t, rows)

	// Default keeps only status 200.
	var got []SeedRow
	st, err := ExportSeedFromCDX(context.Background(), shard, DefaultSeedExportOptions(), func(r SeedRow) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.Scanned != 3 || st.Written != 2 {
		t.Fatalf("scanned=%d written=%d, want 3/2", st.Scanned, st.Written)
	}
	if got[0].URL != "https://a.com/" || got[0].Digest != "D1" {
		t.Fatalf("unexpected first row: %+v", got[0])
	}

	// MIME filter keeps only the HTML capture.
	got = nil
	_, err = ExportSeedFromCDX(context.Background(), shard, SeedExportOptions{Status: 200, MIME: "text/html"}, func(r SeedRow) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].URL != "https://a.com/" {
		t.Fatalf("MIME filter failed: %+v", got)
	}

	// Limit stops early.
	got = nil
	st, err = ExportSeedFromCDX(context.Background(), shard, SeedExportOptions{Limit: 1}, func(r SeedRow) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.Written != 1 || len(got) != 1 {
		t.Fatalf("limit failed: written=%d len=%d", st.Written, len(got))
	}
}

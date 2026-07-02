package ccrawl

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeTestCorpusFile writes a WET Parquet file with n rows so the publish scan
// has a real footer to count.
func writeTestCorpusFile(t *testing.T, path string, n int) {
	t.Helper()
	w, err := NewParquetWriter[WETParquetRow](path)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	rows := make([]WETParquetRow, n)
	for i := range rows {
		rows[i] = WETParquetRow{
			RecordID: "rec", CrawlID: "CC-MAIN-2026-25",
			URL: "https://example.com/", Text: "hello world",
		}
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatalf("write rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestScanDatasetFiles proves the scan finds every .parquet file in name order,
// maps each to the crawl-partitioned repo path, and reads the true row count out
// of the footer.
func TestScanDatasetFiles(t *testing.T) {
	dir := t.TempDir()
	writeTestCorpusFile(t, filepath.Join(dir, "part-00001.parquet"), 7)
	writeTestCorpusFile(t, filepath.Join(dir, "part-00000.parquet"), 3)
	// A non-parquet file must be ignored.
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("skip me"), 0o644); err != nil {
		t.Fatal(err)
	}

	files, err := scanDatasetFiles(dir, "CC-MAIN-2026-25")
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("found %d files, want 2", len(files))
	}
	if files[0].name != "part-00000.parquet" || files[1].name != "part-00001.parquet" {
		t.Fatalf("files not in name order: %q, %q", files[0].name, files[1].name)
	}
	if files[0].pathInRepo != "data/crawl=CC-MAIN-2026-25/part-00000.parquet" {
		t.Fatalf("repo path %q", files[0].pathInRepo)
	}
	if files[0].rows != 3 || files[1].rows != 7 {
		t.Fatalf("row counts %d, %d, want 3, 7", files[0].rows, files[1].rows)
	}
	if files[0].bytes <= 0 {
		t.Fatalf("byte size not read: %d", files[0].bytes)
	}
}

// TestRunDatasetPublishNoPush proves a no-push run walks every file and totals
// the rows without touching HuggingFace, which is the dry-run path.
func TestRunDatasetPublishNoPush(t *testing.T) {
	dir := t.TempDir()
	writeTestCorpusFile(t, filepath.Join(dir, "a.parquet"), 5)
	writeTestCorpusFile(t, filepath.Join(dir, "b.parquet"), 11)

	st, err := RunDatasetPublish(context.Background(), NewHFClient(""), DatasetPublishConfig{
		SrcDir:  dir,
		CrawlID: "CC-MAIN-2026-25",
		Subset:  "wet",
		Push:    false,
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if st.Total != 2 || st.Published != 2 || st.Skipped != 0 || st.Failed != 0 {
		t.Fatalf("stats total=%d published=%d skipped=%d failed=%d", st.Total, st.Published, st.Skipped, st.Failed)
	}
	if st.Rows != 16 {
		t.Fatalf("rows=%d, want 16", st.Rows)
	}
}

// TestGenerateCorpusREADME proves the card names the crawl, the subset schema,
// and the pull command a reader needs to restore the cache.
func TestGenerateCorpusREADME(t *testing.T) {
	card := GenerateCorpusREADME(CorpusCardStats{
		Repo:           "open-index/commoncrawl-2026-25-text",
		CrawlID:        "CC-MAIN-2026-25",
		Subset:         "wet",
		PublishedFiles: 500,
		TotalFiles:     500,
		Rows:           41_000_000,
		ParquetBytes:   41 << 30,
	})
	for _, want := range []string{
		"CC-MAIN-2026-25",
		"content_language",
		"ccrawl dataset pull --repo open-index/commoncrawl-2026-25-text",
		"odc-by",
	} {
		if !strings.Contains(card, want) {
			t.Fatalf("card missing %q", want)
		}
	}
}

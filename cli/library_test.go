package cli

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLibraryFiles(t *testing.T) {
	dir := t.TempDir()
	// Out-of-order names plus a subdirectory that must be ignored.
	for _, name := range []string{"b.warc.gz", "a.warc.gz", "c.warc.gz"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}

	files, err := libraryFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 3 {
		t.Fatalf("got %d files, want 3: %v", len(files), files)
	}
	// Sorted by name, directories excluded.
	want := []string{"a.warc.gz", "b.warc.gz", "c.warc.gz"}
	for i, f := range files {
		if filepath.Base(f) != want[i] {
			t.Errorf("files[%d] = %q, want base %q", i, f, want[i])
		}
	}
}

func TestLibraryFilesMissingDir(t *testing.T) {
	files, err := libraryFiles(filepath.Join(t.TempDir(), "does-not-exist"))
	if err != nil {
		t.Fatalf("missing dir should not error, got %v", err)
	}
	if len(files) != 0 {
		t.Errorf("want no files, got %v", files)
	}
}

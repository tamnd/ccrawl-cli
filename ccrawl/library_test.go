package ccrawl

import (
	"path/filepath"
	"testing"
)

func TestLibraryLayout(t *testing.T) {
	lib := NewLibrary("/lib", "CC-MAIN-2024-51")

	if got, want := lib.CrawlDir(), "/lib/CC-MAIN-2024-51"; got != want {
		t.Errorf("CrawlDir = %q, want %q", got, want)
	}
	if got, want := lib.RawDir("warc"), "/lib/CC-MAIN-2024-51/warc"; got != want {
		t.Errorf("RawDir = %q, want %q", got, want)
	}
	if got, want := lib.ProcessedDir("parquet", "wet"), "/lib/CC-MAIN-2024-51/parquet/wet"; got != want {
		t.Errorf("ProcessedDir = %q, want %q", got, want)
	}
}

func TestLibraryRawPath(t *testing.T) {
	lib := NewLibrary("/lib", "CC-MAIN-2024-51")
	cc := "crawl-data/CC-MAIN-2024-51/segments/1733/warc/CC-MAIN-20241201-00000.warc.gz"
	got := lib.RawPath("warc", cc)
	want := "/lib/CC-MAIN-2024-51/warc/CC-MAIN-20241201-00000.warc.gz"
	if got != want {
		t.Errorf("RawPath = %q, want %q", got, want)
	}
}

func TestNewLibraryDefaultsRoot(t *testing.T) {
	t.Setenv("CCRAWL_LIBRARY", "/custom/lib")
	lib := NewLibrary("", "CC-MAIN-2024-51")
	if got, want := lib.Root, "/custom/lib"; got != want {
		t.Errorf("Root = %q, want %q", got, want)
	}
}

func TestLibraryDirEnvOverride(t *testing.T) {
	t.Setenv("CCRAWL_LIBRARY", "/somewhere/ccrawl")
	if got := LibraryDir(); got != "/somewhere/ccrawl" {
		t.Errorf("LibraryDir = %q, want /somewhere/ccrawl", got)
	}
}

func TestLibraryDirDefault(t *testing.T) {
	t.Setenv("CCRAWL_LIBRARY", "")
	got := LibraryDir()
	if filepath.Base(got) != "ccrawl" || filepath.Base(filepath.Dir(got)) != "notes" {
		t.Errorf("LibraryDir = %q, want a path ending in notes/ccrawl", got)
	}
}

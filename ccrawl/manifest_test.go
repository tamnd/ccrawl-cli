package ccrawl

import (
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// writeArtifact puts a file in a library at a path the layout recognises, and
// returns its absolute path.
func writeArtifact(t *testing.T, root, rel, body string) string {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return full
}

func TestAManifestSurvivesARoundTrip(t *testing.T) {
	root := t.TempDir()
	created := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	err := UpdateManifest(root, func(m *Manifest) error {
		m.Put(Artifact{
			Path: "CC-MAIN-2026-30/wet/a.warc.wet.gz", Crawl: "CC-MAIN-2026-30",
			Kind: "wet", Bytes: 12, SHA256: "abc", CreatedAt: created, Producer: "ccrawl/v0.10.0",
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	m, err := LoadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	a, ok := m.Lookup("CC-MAIN-2026-30/wet/a.warc.wet.gz")
	if !ok {
		t.Fatal("the artifact did not come back")
	}
	if a.Bytes != 12 || a.SHA256 != "abc" || a.Kind != "wet" {
		t.Errorf("artifact came back changed: %+v", a)
	}
	if !a.CreatedAt.Equal(created) {
		t.Errorf("created_at is %v, want %v", a.CreatedAt, created)
	}
}

func TestAMissingManifestIsAnEmptyOne(t *testing.T) {
	m, err := LoadManifest(filepath.Join(t.TempDir(), "nothing-here"))
	if err != nil {
		t.Fatalf("a library with no manifest should load clean: %v", err)
	}
	if m.Len() != 0 {
		t.Errorf("empty manifest has %d artifacts", m.Len())
	}
}

func TestAManifestFromTheFutureIsRefused(t *testing.T) {
	root := t.TempDir()
	body := `{"version":99,"artifacts":[]}`
	if err := os.WriteFile(ManifestPath(root), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(root); err == nil {
		t.Fatal("a manifest version this ccrawl does not know was accepted")
	}
}

func TestPutReplacesAndRemoveDrops(t *testing.T) {
	m, err := LoadManifest(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m.Put(Artifact{Path: "c/wet/a", Bytes: 1})
	m.Put(Artifact{Path: "c/wet/a", Bytes: 2})
	if m.Len() != 1 {
		t.Fatalf("the same path was recorded twice: %d", m.Len())
	}
	if a, _ := m.Lookup("c/wet/a"); a.Bytes != 2 {
		t.Errorf("the second Put did not win: %d bytes", a.Bytes)
	}
	m.Remove("c/wet/a")
	if m.Len() != 0 {
		t.Errorf("Remove left %d artifacts", m.Len())
	}
}

func TestCrawlsComeBackNewestFirst(t *testing.T) {
	m, err := LoadManifest(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []string{"CC-MAIN-2025-33", "CC-MAIN-2026-30", "CC-MAIN-2026-05"} {
		m.Put(Artifact{Path: c + "/wet/a", Crawl: c})
	}
	got := m.Crawls()
	want := []string{"CC-MAIN-2026-30", "CC-MAIN-2026-05", "CC-MAIN-2025-33"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("crawls came back %v, want %v", got, want)
		}
	}
}

func TestTwoWritersDoNotLoseEachOther(t *testing.T) {
	// The reason the lock exists: a download of WET files and a download of WARC
	// files into one library, running at the same time, must both be recorded.
	root := t.TempDir()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := UpdateManifest(root, func(m *Manifest) error {
				m.Put(Artifact{Path: filepath.ToSlash(filepath.Join("CC-MAIN-2026-30", "wet", string(rune('a'+i))))})
				return nil
			})
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()

	m, err := LoadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	if m.Len() != 8 {
		t.Errorf("8 concurrent writers recorded %d artifacts, so one overwrote another", m.Len())
	}
}

func TestClassifyPathReadsTheLayoutBack(t *testing.T) {
	cases := []struct {
		rel                 string
		crawl, format, kind string
		ok                  bool
	}{
		{"CC-MAIN-2026-30/wet/a.warc.wet.gz", "CC-MAIN-2026-30", "", "wet", true},
		{"CC-MAIN-2026-30/parquet/wet/a.parquet", "CC-MAIN-2026-30", "parquet", "wet", true},
		{"CC-MAIN-2026-30/jsonl/warc/a.jsonl", "CC-MAIN-2026-30", "jsonl", "warc", true},
		// Four parts whose second is not a format is not the processed layout.
		{"CC-MAIN-2026-30/wet/nested/a.gz", "", "", "", false},
		{"library.json", "", "", "", false},
		{"CC-MAIN-2026-30/notes.md", "", "", "", false},
	}
	for _, c := range cases {
		crawl, format, kind, ok := ClassifyPath(c.rel)
		if ok != c.ok || crawl != c.crawl || format != c.format || kind != c.kind {
			t.Errorf("ClassifyPath(%q) = %q %q %q %v, want %q %q %q %v",
				c.rel, crawl, format, kind, ok, c.crawl, c.format, c.kind, c.ok)
		}
	}
}

func TestWalkLibrarySkipsWhatIsNotAnArtifact(t *testing.T) {
	root := t.TempDir()
	writeArtifact(t, root, "CC-MAIN-2026-30/wet/a.warc.wet.gz", "one")
	writeArtifact(t, root, "CC-MAIN-2026-30/parquet/wet/a.parquet", "two")
	// Things a person leaves in a directory they own, and the manifest itself.
	writeArtifact(t, root, "README.md", "notes")
	writeArtifact(t, root, "library.json", "{}")
	writeArtifact(t, root, "CC-MAIN-2026-30/notes.md", "more notes")

	var seen []string
	err := WalkLibrary(root, func(rel string, _ fs.FileInfo) error {
		seen = append(seen, rel)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 {
		t.Errorf("the walk found %v, want only the two archives", seen)
	}
}

func TestLibraryPathRefusesToEscapeTheRoot(t *testing.T) {
	root := t.TempDir()
	if _, ok := LibraryPath(root, filepath.Join(root, "..", "elsewhere", "x")); ok {
		t.Error("a path outside the library was accepted as a library path")
	}
	rel, ok := LibraryPath(root, filepath.Join(root, "a", "b", "c"))
	if !ok || rel != "a/b/c" {
		t.Errorf("LibraryPath returned %q %v, want a/b/c true", rel, ok)
	}
}

func TestFileSHA256IsTheRealChecksum(t *testing.T) {
	path := writeArtifact(t, t.TempDir(), "CC-MAIN-2026-30/wet/a", "hello")
	sum, err := FileSHA256(path)
	if err != nil {
		t.Fatal(err)
	}
	// sha256 of "hello", so the manifest holds what every other tool prints.
	const want = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	if sum != want {
		t.Errorf("sha256 is %s, want %s", sum, want)
	}
}

func TestSaveLeavesNoTemporaryFilesBehind(t *testing.T) {
	root := t.TempDir()
	if err := UpdateManifest(root, func(m *Manifest) error {
		m.Put(Artifact{Path: "c/wet/a"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != ManifestName && e.Name() != manifestLockName {
			t.Errorf("Save left %s behind", e.Name())
		}
	}
}

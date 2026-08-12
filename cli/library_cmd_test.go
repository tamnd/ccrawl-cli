package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/ccrawl-cli/ccrawl"
	"github.com/tamnd/ccrawl-cli/internal/fakecc"
)

// libRun drives one command against the library the test pinned. The rate flags
// are the same ones run uses, and are here rather than in run because these
// tests need several invokes to share one library root.
func libRun(t *testing.T, args ...string) result {
	t.Helper()
	full := append([]string{"ccrawl", "--rate", "1ns", "--global-rate", "0"}, args...)
	code, out, errOut := invoke(t, "", full)
	return result{Code: code, Out: out, Err: errOut}
}

// unpack is the exit code, stdout and stderr of a run, for the assertions that
// read better as three values than as a result.
func (r result) unpack() (int, string, string) { return r.Code, r.Out, r.Err }

// withLibrary pins a library root for the whole test, so several invokes share
// one tree instead of each getting a temp dir of its own from the harness.
func withLibrary(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	t.Setenv("CCRAWL_LIBRARY", root)
	return root
}

// put writes a file into a library at a path the layout recognises.
func put(t *testing.T, root, rel, body string) string {
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

// backdate moves a recorded artifact into the past, which is how a gc cutoff is
// tested without waiting ninety days.
func backdate(t *testing.T, root, rel string, d time.Duration) {
	t.Helper()
	err := ccrawl.UpdateManifest(root, func(m *ccrawl.Manifest) error {
		a, ok := m.Lookup(rel)
		if !ok {
			t.Fatalf("%s is not in the manifest", rel)
		}
		a.CreatedAt = time.Now().Add(-d)
		m.Put(a)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDownloadRecordsWhatItPutInTheLibrary(t *testing.T) {
	fakecc.Start(t)
	root := withLibrary(t)
	code, _, errOut := libRun(t, "download", "wet", "--library", "--yes").unpack()
	if code != 0 {
		t.Fatalf("download exited %d: %s", code, errOut)
	}

	m, err := ccrawl.LoadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture publishes one archive per kind, so one file arrived.
	if m.Len() != 1 {
		t.Fatalf("the manifest records %d artifacts, want 1: %+v", m.Len(), m.Artifacts())
	}
	a := m.Artifacts()[0]
	if a.Kind != "wet" {
		t.Errorf("%s recorded as kind %q", a.Path, a.Kind)
	}
	if a.Crawl == "" {
		t.Errorf("%s recorded with no crawl", a.Path)
	}
	if a.Format != "" {
		t.Errorf("a raw archive recorded with format %q", a.Format)
	}
	if a.Bytes <= 0 {
		t.Errorf("%s recorded as %d bytes", a.Path, a.Bytes)
	}
	if !strings.HasPrefix(a.Producer, "ccrawl/") {
		t.Errorf("%s recorded with producer %q", a.Path, a.Producer)
	}
	// The checksum has to be the one the file actually has, not one the writer
	// imagined on the way past.
	sum, err := ccrawl.FileSHA256(filepath.Join(root, filepath.FromSlash(a.Path)))
	if err != nil {
		t.Fatal(err)
	}
	if sum != a.SHA256 {
		t.Errorf("%s: the manifest checksum is not the file's", a.Path)
	}
}

func TestDownloadingTwiceDoesNotDoubleTheManifest(t *testing.T) {
	fakecc.Start(t)
	root := withLibrary(t)
	for i := 0; i < 2; i++ {
		if code, _, errOut := libRun(t, "download", "wet", "--library", "--yes").unpack(); code != 0 {
			t.Fatalf("download %d exited %d: %s", i, code, errOut)
		}
	}
	m, err := ccrawl.LoadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	if m.Len() != 1 {
		t.Errorf("two runs over the same file recorded %d artifacts", m.Len())
	}
}

func TestVerifyCatchesACorruptedFile(t *testing.T) {
	fakecc.Start(t)
	root := withLibrary(t)
	if code, _, errOut := libRun(t, "download", "wet", "--library", "--yes").unpack(); code != 0 {
		t.Fatalf("download exited %d: %s", code, errOut)
	}

	// A freshly downloaded library verifies.
	code, out, errOut := libRun(t, "library", "verify").unpack()
	if code != 0 {
		t.Fatalf("a freshly downloaded library did not verify: exit %d\n%s\n%s", code, out, errOut)
	}

	// Corrupt one file in place, keeping its size, which is the failure a size
	// check cannot see and the reason the manifest stores a checksum at all.
	m, err := ccrawl.LoadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	victim := m.Artifacts()[0]
	full := filepath.Join(root, filepath.FromSlash(victim.Path))
	body, err := os.ReadFile(full)
	if err != nil {
		t.Fatal(err)
	}
	body[len(body)/2] ^= 0xff
	if err := os.WriteFile(full, body, 0o644); err != nil {
		t.Fatal(err)
	}

	code, out, errOut = libRun(t, "library", "verify").unpack()
	if code == 0 {
		t.Fatalf("verify passed a corrupted file\n%s\n%s", out, errOut)
	}
	if !strings.Contains(out, "corrupt") || !strings.Contains(out, victim.Path) {
		t.Errorf("verify did not name the corrupted file:\n%s", out)
	}
	// The same corruption is invisible to a size-only check, which is what
	// --quick promises and why it is not the default.
	if code, out, _ := libRun(t, "library", "verify", "--quick").unpack(); code != 0 {
		t.Errorf("--quick failed on a file whose size is right: exit %d\n%s", code, out)
	}
}

func TestVerifyCatchesAMissingFileAndAnUntrackedOne(t *testing.T) {
	root := withLibrary(t)
	put(t, root, "CC-MAIN-2026-30/wet/a.warc.wet.gz", "one")
	put(t, root, "CC-MAIN-2026-30/wet/b.warc.wet.gz", "two")
	if code, _, errOut := libRun(t, "library", "scan").unpack(); code != 0 {
		t.Fatalf("scan exited %d: %s", code, errOut)
	}
	if err := os.Remove(filepath.Join(root, "CC-MAIN-2026-30", "wet", "a.warc.wet.gz")); err != nil {
		t.Fatal(err)
	}
	put(t, root, "CC-MAIN-2026-30/wet/stranger.warc.wet.gz", "not from here")

	code, out, _ := libRun(t, "library", "verify").unpack()
	if code == 0 {
		t.Fatalf("verify passed a library missing a file:\n%s", out)
	}
	if !strings.Contains(out, "missing") {
		t.Errorf("verify did not report the deleted file:\n%s", out)
	}
	if !strings.Contains(out, "untracked") || !strings.Contains(out, "stranger") {
		t.Errorf("verify did not report the file nobody recorded:\n%s", out)
	}
}

func TestVerifyCatchesAFileThatChangedSize(t *testing.T) {
	root := withLibrary(t)
	put(t, root, "CC-MAIN-2026-30/wet/a.warc.wet.gz", "the original bytes")
	if code, _, _ := libRun(t, "library", "scan").unpack(); code != 0 {
		t.Fatal("scan failed")
	}
	put(t, root, "CC-MAIN-2026-30/wet/a.warc.wet.gz", "truncated")

	code, out, _ := libRun(t, "library", "verify").unpack()
	if code == 0 {
		t.Fatalf("verify passed a file that changed size:\n%s", out)
	}
	if !strings.Contains(out, "resized") {
		t.Errorf("verify did not report the size change:\n%s", out)
	}
	// A size change is the one thing --quick does catch.
	if code, out, _ := libRun(t, "library", "verify", "--quick").unpack(); code == 0 {
		t.Errorf("--quick passed a file that changed size:\n%s", out)
	}
}

func TestDuReportsPerCrawlTotals(t *testing.T) {
	root := withLibrary(t)
	put(t, root, "CC-MAIN-2026-30/wet/a.warc.wet.gz", strings.Repeat("a", 100))
	put(t, root, "CC-MAIN-2026-30/wet/b.warc.wet.gz", strings.Repeat("b", 200))
	put(t, root, "CC-MAIN-2025-33/warc/c.warc.gz", strings.Repeat("c", 50))
	if code, _, errOut := libRun(t, "library", "scan").unpack(); code != 0 {
		t.Fatalf("scan exited %d: %s", code, errOut)
	}

	code, out, errOut := libRun(t, "library", "du").unpack()
	if code != 0 {
		t.Fatalf("du exited %d: %s", code, errOut)
	}
	for _, want := range []string{
		`"crawl":"CC-MAIN-2026-30"`, `"bytes":300`,
		`"crawl":"CC-MAIN-2025-33"`, `"bytes":50`,
		`"crawl":"total"`, `"bytes":350`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("du output is missing %s:\n%s", want, out)
		}
	}
	// Newest crawl first, the way every other crawl listing reads.
	if strings.Index(out, "CC-MAIN-2026-30") > strings.Index(out, "CC-MAIN-2025-33") {
		t.Errorf("du listed the older crawl first:\n%s", out)
	}

	// The same bytes, grouped the other way.
	code, out, _ = libRun(t, "library", "du", "--by", "kind").unpack()
	if code != 0 {
		t.Fatalf("du --by kind exited %d", code)
	}
	if !strings.Contains(out, `"kind":"wet"`) || !strings.Contains(out, `"kind":"warc"`) {
		t.Errorf("du --by kind did not group by kind:\n%s", out)
	}

	// And one crawl at a time, which is the question "what is this crawl costing
	// me" that makes gc worth running.
	code, out, _ = libRun(t, "library", "du", "-c", "CC-MAIN-2025-33").unpack()
	if code != 0 {
		t.Fatalf("du -c exited %d", code)
	}
	if strings.Contains(out, "CC-MAIN-2026-30") {
		t.Errorf("du -c reported a crawl it was not asked about:\n%s", out)
	}
}

func TestGCRemovesOnlyWhatItsDryRunSaid(t *testing.T) {
	root := withLibrary(t)
	put(t, root, "CC-MAIN-2025-33/wet/old-a.warc.wet.gz", "old a")
	put(t, root, "CC-MAIN-2025-33/wet/old-b.warc.wet.gz", "old b")
	put(t, root, "CC-MAIN-2026-30/wet/new.warc.wet.gz", "new")
	if code, _, errOut := libRun(t, "library", "scan").unpack(); code != 0 {
		t.Fatalf("scan exited %d: %s", code, errOut)
	}
	backdate(t, root, "CC-MAIN-2025-33/wet/old-a.warc.wet.gz", 100*24*time.Hour)
	backdate(t, root, "CC-MAIN-2025-33/wet/old-b.warc.wet.gz", 100*24*time.Hour)

	code, dry, errOut := libRun(t, "library", "gc", "--older-than", "90d").unpack()
	if code != 0 {
		t.Fatalf("the dry run exited %d: %s", code, errOut)
	}
	if !strings.Contains(errOut, "would delete") {
		t.Errorf("the dry run did not say it was one:\n%s", errOut)
	}
	// Nothing went yet.
	for _, rel := range []string{"CC-MAIN-2025-33/wet/old-a.warc.wet.gz", "CC-MAIN-2026-30/wet/new.warc.wet.gz"} {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel))); err != nil {
			t.Errorf("the dry run deleted %s", rel)
		}
	}

	code, real, errOut := libRun(t, "library", "gc", "--older-than", "90d", "--yes").unpack()
	if code != 0 {
		t.Fatalf("gc exited %d: %s", code, errOut)
	}
	// The contract of the command: the real run removed exactly the list the dry
	// run printed, no more and no less.
	if real != dry {
		t.Errorf("gc deleted a different set than its dry run said\ndry run:\n%s\nreal run:\n%s", dry, real)
	}
	for _, rel := range []string{"CC-MAIN-2025-33/wet/old-a.warc.wet.gz", "CC-MAIN-2025-33/wet/old-b.warc.wet.gz"} {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel))); !os.IsNotExist(err) {
			t.Errorf("gc did not delete %s", rel)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "CC-MAIN-2026-30", "wet", "new.warc.wet.gz")); err != nil {
		t.Errorf("gc deleted a file the dry run never mentioned: %v", err)
	}

	// And the manifest agrees with the disk afterwards, so verify is clean.
	m, err := ccrawl.LoadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	if m.Len() != 1 {
		t.Errorf("after gc the manifest records %d artifacts, want 1", m.Len())
	}
	if code, out, _ := libRun(t, "library", "verify").unpack(); code != 0 {
		t.Errorf("the library did not verify after a gc:\n%s", out)
	}
}

func TestGCLeavesEverythingInsideTheCutoff(t *testing.T) {
	root := withLibrary(t)
	put(t, root, "CC-MAIN-2026-30/wet/a.warc.wet.gz", "recent")
	if code, _, _ := libRun(t, "library", "scan").unpack(); code != 0 {
		t.Fatal("scan failed")
	}
	code, _, errOut := libRun(t, "library", "gc", "--older-than", "90d", "--yes").unpack()
	if code != 3 {
		t.Fatalf("a gc with nothing to do exited %d, want the no-results code: %s", code, errOut)
	}
	if _, err := os.Stat(filepath.Join(root, "CC-MAIN-2026-30", "wet", "a.warc.wet.gz")); err != nil {
		t.Error("gc deleted a file inside the cutoff")
	}
}

func TestGCNeedsSomethingToSelectOn(t *testing.T) {
	withLibrary(t)
	code, _, errOut := libRun(t, "library", "gc", "--yes").unpack()
	if code != 2 {
		t.Fatalf("a gc with no selection exited %d, want a usage error", code)
	}
	if !strings.Contains(errOut, "older-than") {
		t.Errorf("the error did not say what was missing: %s", errOut)
	}
}

func TestGCTakesACrawlWithoutAnAge(t *testing.T) {
	root := withLibrary(t)
	put(t, root, "CC-MAIN-2025-33/wet/a.warc.wet.gz", "gone")
	put(t, root, "CC-MAIN-2026-30/wet/b.warc.wet.gz", "kept")
	if code, _, _ := libRun(t, "library", "scan").unpack(); code != 0 {
		t.Fatal("scan failed")
	}
	if code, _, errOut := libRun(t, "library", "gc", "-c", "CC-MAIN-2025-33", "--yes").unpack(); code != 0 {
		t.Fatalf("gc exited %d: %s", code, errOut)
	}
	if _, err := os.Stat(filepath.Join(root, "CC-MAIN-2025-33", "wet", "a.warc.wet.gz")); !os.IsNotExist(err) {
		t.Error("gc kept the crawl it was pointed at")
	}
	if _, err := os.Stat(filepath.Join(root, "CC-MAIN-2026-30", "wet", "b.warc.wet.gz")); err != nil {
		t.Error("gc deleted a crawl it was not pointed at")
	}
}

func TestScanRecordsALibraryThatPredatesTheManifest(t *testing.T) {
	root := withLibrary(t)
	put(t, root, "CC-MAIN-2026-30/wet/a.warc.wet.gz", "one")
	put(t, root, "CC-MAIN-2026-30/parquet/wet/a.parquet", "two")
	put(t, root, "README.md", "a note to self")

	code, _, errOut := libRun(t, "library", "scan").unpack()
	if code != 0 {
		t.Fatalf("scan exited %d: %s", code, errOut)
	}
	if !strings.Contains(errOut, "2 new") {
		t.Errorf("scan did not report what it recorded: %s", errOut)
	}
	m, err := ccrawl.LoadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	if m.Len() != 2 {
		t.Fatalf("scan recorded %d artifacts, want the 2 archives and not the note", m.Len())
	}
	if a, ok := m.Lookup("CC-MAIN-2026-30/parquet/wet/a.parquet"); !ok || a.Format != "parquet" {
		t.Errorf("processed output was not recorded as parquet: %+v", a)
	}

	// A second scan is a no-op, which is what makes it safe to run on a cron.
	code, _, errOut = libRun(t, "library", "scan").unpack()
	if code != 0 {
		t.Fatalf("the second scan exited %d: %s", code, errOut)
	}
	if !strings.Contains(errOut, "0 new") || !strings.Contains(errOut, "2 already recorded") {
		t.Errorf("the second scan did work it did not need to: %s", errOut)
	}
}

func TestListSaysWhatIsInTheLibrary(t *testing.T) {
	root := withLibrary(t)
	put(t, root, "CC-MAIN-2026-30/wet/a.warc.wet.gz", "one")
	put(t, root, "CC-MAIN-2025-33/warc/b.warc.gz", "two")
	if code, _, _ := libRun(t, "library", "scan").unpack(); code != 0 {
		t.Fatal("scan failed")
	}

	code, out, _ := libRun(t, "library", "list").unpack()
	if code != 0 {
		t.Fatalf("list exited %d", code)
	}
	if !strings.Contains(out, "a.warc.wet.gz") || !strings.Contains(out, "b.warc.gz") {
		t.Errorf("list did not show both artifacts:\n%s", out)
	}

	code, out, _ = libRun(t, "library", "list", "--kind", "wet").unpack()
	if code != 0 {
		t.Fatalf("list --kind exited %d", code)
	}
	if strings.Contains(out, "b.warc.gz") {
		t.Errorf("list --kind wet showed a warc:\n%s", out)
	}

	code, out, _ = libRun(t, "library", "list", "--format", "raw").unpack()
	if code != 0 {
		t.Fatalf("list --format raw exited %d", code)
	}
	if !strings.Contains(out, `"format":"raw"`) {
		t.Errorf("a downloaded archive did not list as raw:\n%s", out)
	}
}

func TestAnEmptyLibraryPointsAtScan(t *testing.T) {
	root := withLibrary(t)

	// Nothing at all is nothing at all.
	code, _, errOut := libRun(t, "library", "list").unpack()
	if code != 3 {
		t.Fatalf("an empty library exited %d, want the no-results code", code)
	}
	if !strings.Contains(errOut, "empty") {
		t.Errorf("the message did not say the library is empty: %s", errOut)
	}

	// Files with no manifest is a different problem, with a different fix, and
	// the message has to say which one it is.
	put(t, root, "CC-MAIN-2026-30/wet/a.warc.wet.gz", "one")
	code, _, errOut = libRun(t, "library", "list").unpack()
	if code != 3 {
		t.Fatalf("an unrecorded library exited %d, want the no-results code", code)
	}
	if !strings.Contains(errOut, "scan") {
		t.Errorf("the message did not point at library scan: %s", errOut)
	}
}

func TestConvertRecordsItsOutputInTheLibrary(t *testing.T) {
	fakecc.Start(t)
	root := withLibrary(t)
	if code, _, errOut := libRun(t, "download", "wet", "--library", "--yes").unpack(); code != 0 {
		t.Fatalf("download exited %d: %s", code, errOut)
	}
	code, _, errOut := libRun(t, "convert", "wet", "--library", "--to", "jsonl").unpack()
	if code != 0 {
		t.Fatalf("convert exited %d: %s", code, errOut)
	}

	m, err := ccrawl.LoadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	var processed []ccrawl.Artifact
	for _, a := range m.Artifacts() {
		if a.Format != "" {
			processed = append(processed, a)
		}
	}
	if len(processed) != 1 {
		t.Fatalf("convert recorded %d processed artifacts, want 1: %+v", len(processed), m.Artifacts())
	}
	if processed[0].Format != "jsonl" || processed[0].Kind != "wet" {
		t.Errorf("processed output recorded as %+v", processed[0])
	}
	if len(processed[0].SHA256) != 64 {
		t.Errorf("processed output recorded with checksum %q", processed[0].SHA256)
	}
	// Everything the library holds, raw and processed, verifies together.
	if code, out, _ := libRun(t, "library", "verify").unpack(); code != 0 {
		t.Errorf("the library did not verify after a convert:\n%s", out)
	}
}

func TestParseAgeReadsWhatAPersonTypes(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"30d", 30 * 24 * time.Hour},
		{"8w", 8 * 7 * 24 * time.Hour},
		{"12h", 12 * time.Hour},
		{"90m", 90 * time.Minute},
	}
	for _, c := range cases {
		got, err := parseAge(c.in)
		if err != nil || got != c.want {
			t.Errorf("parseAge(%q) = %v, %v, want %v", c.in, got, err, c.want)
		}
	}
	for _, bad := range []string{"", "soon", "3days", "d", "3.5d", "-2w"} {
		if _, err := parseAge(bad); err == nil {
			t.Errorf("parseAge(%q) was accepted", bad)
		}
	}
}

func TestLibraryRootForOnlyClaimsWhatIsInside(t *testing.T) {
	root := t.TempDir()
	app := &App{LibraryDir: root}
	if got := libraryRootFor(app, filepath.Join(root, "CC-MAIN-2026-30", "wet")); got == "" {
		t.Error("an output directory inside the library was not recognised")
	}
	if got := libraryRootFor(app, t.TempDir()); got != "" {
		t.Errorf("an output directory outside the library was claimed as %q", got)
	}
	if got := libraryRootFor(&App{}, root); got != "" {
		t.Errorf("a run with no library claimed %q", got)
	}
}

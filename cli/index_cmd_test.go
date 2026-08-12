package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// index build and index search never touch Common Crawl, so these run without a
// fake server behind them: anything that reaches the network here is a bug.
//
// The failure worth guarding is not a wrong BM25 score, it is a build that
// reports success and indexes nothing. --input was declared for a long time and
// never read, so `index build --dir d --input docs.jsonl` printed
// {"docs_added":0} and exited 0.

const twoDocs = `{"url":"https://a.test/","title":"Widget review","text":"the widget is a small blue widget"}
{"url":"https://b.test/","title":"Sprocket review","text":"the sprocket is a large red sprocket"}
`

func writeJSONL(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "docs.jsonl")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// buildResult is the single row index build prints.
type buildResult struct {
	IndexDir    string `json:"index_dir"`
	DocsAdded   int    `json:"docs_added"`
	DocsSkipped int    `json:"docs_skipped"`
	Terms       int    `json:"terms"`
}

func buildFrom(t *testing.T, r result) buildResult {
	t.Helper()
	var got buildResult
	lines := r.Lines()
	if len(lines) == 0 {
		t.Fatalf("index build printed nothing\nstderr:\n%s", r.Err)
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &got); err != nil {
		t.Fatalf("index build printed %q, which is not the result row: %v", lines[len(lines)-1], err)
	}
	return got
}

func TestIndexBuildFromAJSONLFileIsSearchable(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "idx")
	got := buildFrom(t, runNoServer(t, "index", "build", "--dir", dir, "--input", writeJSONL(t, twoDocs), "-o", "jsonl").wantCode(t, 0))
	if got.DocsAdded != 2 {
		t.Fatalf("indexed %d documents, want 2 (result was %+v)", got.DocsAdded, got)
	}
	if got.Terms == 0 {
		t.Fatalf("indexed 2 documents and no terms: %+v", got)
	}

	// The four files the guide documents, and none of them empty.
	for _, name := range []string{"terms.dat", "postings.dat", "stats.dat", "forward.jsonl"} {
		fi, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("index is missing %s: %v", name, err)
		}
		if fi.Size() == 0 {
			t.Fatalf("%s is empty", name)
		}
	}

	r := runNoServer(t, "index", "search", "widget", "--dir", dir, "-o", "jsonl").wantCode(t, 0)
	r.wantOut(t, "https://a.test/", "Widget review")
	r.wantNotOut(t, "https://b.test/")
}

// The pipeline the guide shows is a WET file parsed to JSONL, which writes the
// Go field names rather than lowercase keys. Both have to read.
func TestIndexBuildReadsWETShapedJSONL(t *testing.T) {
	body := `{"RecordID":"<urn:uuid:1>","CrawlID":"CC-MAIN-2026-30","URL":"https://wet.test/page","ContentLanguage":"eng","Text":"a page about sprockets"}`
	dir := filepath.Join(t.TempDir(), "idx")
	got := buildFrom(t, runNoServer(t, "index", "build", "--dir", dir, "--input", writeJSONL(t, body), "-o", "jsonl").wantCode(t, 0))
	if got.DocsAdded != 1 {
		t.Fatalf("a WET-shaped row did not index: %+v", got)
	}
	runNoServer(t, "index", "search", "sprockets", "--dir", dir, "-o", "jsonl").
		wantCode(t, 0).
		wantOut(t, "https://wet.test/page")

	// The language came off the WET field name, which is the whole point of
	// reading that shape, so check it landed in the forward index.
	fwd, err := os.ReadFile(filepath.Join(dir, "forward.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(fwd), `"language":"eng"`) {
		t.Fatalf("the forward index lost the WET language: %s", fwd)
	}
}

func TestIndexBuildReadsStdin(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "idx")
	code, out, errOut := invoke(t, twoDocs, []string{"ccrawl", "index", "build", "--dir", dir, "--input", "-", "-o", "jsonl"})
	if code != 0 {
		t.Fatalf("index build - exited %d\nstderr:\n%s", code, errOut)
	}
	got := buildFrom(t, result{Code: code, Out: out, Err: errOut})
	if got.DocsAdded != 2 {
		t.Fatalf("stdin build indexed %d documents, want 2", got.DocsAdded)
	}
}

// A line that is not a document is counted, not fatal: a real WET file run
// through a language filter always has some.
func TestIndexBuildCountsSkippedLines(t *testing.T) {
	body := twoDocs + "not json\n" + `{"url":"","text":"no url"}` + "\n" + `{"url":"https://c.test/","text":""}` + "\n"
	dir := filepath.Join(t.TempDir(), "idx")
	got := buildFrom(t, runNoServer(t, "index", "build", "--dir", dir, "--input", writeJSONL(t, body), "-o", "jsonl").wantCode(t, 0))
	if got.DocsAdded != 2 || got.DocsSkipped != 3 {
		t.Fatalf("want 2 added and 3 skipped, got %+v", got)
	}
}

// Building with no input used to write four empty files and exit 0, which reads
// as success and is the worst answer available.
func TestIndexBuildWithNothingToIndexIsAUsageError(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "idx")
	r := runNoServer(t, "index", "build", "--dir", dir).wantCode(t, 2)
	if !strings.Contains(r.Err, "--input") {
		t.Fatalf("the error does not say what is missing: %q", r.Err)
	}
	if _, err := os.Stat(filepath.Join(dir, "terms.dat")); err == nil {
		t.Fatal("a build with no input still wrote an index")
	}
}

// A second build over the same directory is a rebuild, not an append. The
// postings are rewritten either way, so a forward index that grew would leave
// documents in it that nothing can score.
func TestIndexBuildRebuildsRatherThanAppends(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "idx")
	input := writeJSONL(t, twoDocs)
	for range 2 {
		runNoServer(t, "index", "build", "--dir", dir, "--input", input, "-o", "jsonl").wantCode(t, 0)
	}
	b, err := os.ReadFile(filepath.Join(dir, "forward.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(strings.Split(strings.TrimSpace(string(b)), "\n")); n != 2 {
		t.Fatalf("forward index holds %d rows after two builds of 2 documents", n)
	}
}

func TestIndexSearchHonoursTheLimit(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "idx")
	runNoServer(t, "index", "build", "--dir", dir, "--input", writeJSONL(t, twoDocs), "-o", "jsonl").wantCode(t, 0)

	// Both documents hold "review", so the query matches two and -n cuts it to one.
	all := runNoServer(t, "index", "search", "review", "--dir", dir, "-o", "jsonl").wantCode(t, 0)
	if len(all.Lines()) != 2 {
		t.Fatalf("an ORed two-word corpus returned %d rows, want 2:\n%s", len(all.Lines()), all.Out)
	}
	one := runNoServer(t, "index", "search", "review", "--dir", dir, "-n", "1", "-o", "jsonl").wantCode(t, 0)
	if len(one.Lines()) != 1 {
		t.Fatalf("-n 1 returned %d rows:\n%s", len(one.Lines()), one.Out)
	}
}

func TestIndexSearchWithNoMatchExitsThree(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "idx")
	runNoServer(t, "index", "build", "--dir", dir, "--input", writeJSONL(t, twoDocs), "-o", "jsonl").wantCode(t, 0)
	runNoServer(t, "index", "search", "zzzznotaword", "--dir", dir, "-o", "jsonl").wantCode(t, 3)
}

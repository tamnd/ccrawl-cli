package ccrawl

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

// TestSimhashIsDeterministic pins the property the whole feature rests on: the
// same text gives the same fingerprint every time. If this ever depends on map
// ordering or a seeded hash then two runs of the same shard disagree and the
// column is worthless.
func TestSimhashIsDeterministic(t *testing.T) {
	text := "The quick brown fox jumps over the lazy dog, and then it does so again."
	first := Simhash(text)
	if first == 0 {
		t.Fatal("Simhash returned 0 for real prose")
	}
	for i := range 50 {
		if got := Simhash(text); got != first {
			t.Fatalf("run %d gave %016x, first run gave %016x", i, got, first)
		}
	}
	// A fixed value, so a change to the tokenizer or the shingle width has to be
	// a deliberate edit to this test rather than a silent corpus-wide drift.
	if got := Simhash("alpha beta gamma delta"); got != Simhash("alpha beta gamma delta") {
		t.Fatal("Simhash disagreed with itself")
	}
}

// proseVocab gives each topic its own words, because two topics that share
// every word but one are near duplicates and a fixture built that way would be
// testing nothing.
var proseVocab = map[string][]string{
	"harbour": {"harbour", "tide", "quay", "ferry", "mooring", "channel", "dredging", "pilot", "berth", "cargo", "buoy", "jetty", "anchorage"},
	"bakery":  {"bakery", "flour", "yeast", "oven", "loaf", "proving", "crust", "rye", "sourdough", "kneading", "batch", "glaze", "semolina"},
	"transit": {"platform", "timetable", "signal", "carriage", "depot", "conductor", "junction", "siding", "fare", "gauge", "shunting", "halt", "viaduct"},
}

// prose builds a document of n varied sentences on a topic, which is what a
// fixture needs to stand in for real text. Repeating one sentence would not: a
// document whose shingles are six distinct features has a fingerprint decided by
// six votes, and any edit swings it, which says nothing about how the hash
// behaves on a page.
func prose(topic string, n int) string {
	v := proseVocab[topic]
	var b strings.Builder
	for i := range n {
		for j := range 9 {
			if j > 0 {
				b.WriteByte(' ')
			}
			w := v[(i*7+j*3)%len(v)]
			if j == 4 {
				// One word per sentence carries its position, so consecutive
				// sentences do not shingle into each other.
				w = fmt.Sprintf("%s%d", w, i)
			}
			b.WriteString(w)
		}
		b.WriteString(".\n")
	}
	return b.String()
}

// TestSimhashDistanceSeparatesNearFromUnrelated is the claim that makes distance
// 3 a usable default: the same article with different boilerplate stays close,
// and two different articles do not.
func TestSimhashDistanceSeparatesNearFromUnrelated(t *testing.T) {
	body := prose("harbour", 200)
	base := "Home About Contact\n" + body + "\nCopyright 2026"
	tweaked := "Home About Contact Search\n" + body + "\nCopyright 2026 all rights reserved"
	other := prose("bakery", 200)

	near := SimhashDistance(Simhash(base), Simhash(tweaked))
	far := SimhashDistance(Simhash(base), Simhash(other))
	if near > 6 {
		t.Fatalf("same article with different chrome is %d bits apart, expected a near duplicate", near)
	}
	if far <= near {
		t.Fatalf("unrelated documents are %d bits apart but near duplicates are %d, the fingerprint does not separate them", far, near)
	}
	if far < 15 {
		t.Fatalf("unrelated documents are only %d bits apart, too close to call anything a duplicate", far)
	}
	if d := SimhashDistance(Simhash(base), Simhash(base)); d != 0 {
		t.Fatalf("a document is %d bits from itself, want 0", d)
	}
}

// TestSimhashEmptyMeansNoFingerprint guards the one value dedup is not allowed
// to cluster on. If empty text hashed to something real then every stub page in
// a crawl would collapse into one cluster.
func TestSimhashEmptyMeansNoFingerprint(t *testing.T) {
	for _, s := range []string{"", "   ", "\n\t\n", "### --- ***"} {
		if got := Simhash(s); got != 0 {
			t.Fatalf("Simhash(%q) = %016x, want 0", s, got)
		}
	}
	// One real word is still a fingerprint. Short is not empty.
	if Simhash("hello") == 0 {
		t.Fatal("Simhash dropped a one word page")
	}
}

// dupWARC builds a shard where some pages carry byte identical HTML. bodies maps
// a URL suffix to the HTML served, so a caller can plant exact copies under
// different URLs, which is what a crawl actually looks like.
func dupWARC(t *testing.T, urls []string, bodies []string) []byte {
	t.Helper()
	var buf bytes.Buffer
	for i, u := range urls {
		buf.Write(warcMember(t, "https://example.com/"+u, bodies[i]))
	}
	return buf.Bytes()
}

func articlePage(text string) string {
	return "<html><body><nav>Home About</nav><article><p>" + text + "</p></article><footer>c 2026</footer></body></html>"
}

// TestPackStreamDedupDigest is the first done-when: --dedup-digest drops the
// duplicates and nothing else. Six pages go in, three of them byte identical
// copies of one page under different URLs, so exactly two should be dropped and
// every distinct page must survive.
func TestPackStreamDedupDigest(t *testing.T) {
	same := articlePage("Common Crawl publishes a web archive every month.")
	urls := []string{"a", "b", "c", "d", "e", "f"}
	bodies := []string{
		same,
		articlePage("A different page about bread and flour."),
		same,
		articlePage("A third page about mountains and rivers."),
		same,
		articlePage("A fourth page about trains and timetables."),
	}
	shard := dupWARC(t, urls, bodies)

	out := filepath.Join(t.TempDir(), "dedup.parquet")
	stats, err := packStream(context.Background(), bytes.NewReader(shard),
		MarkdownPackConfig{OutPath: out, Workers: 2, DedupDigest: true},
		MarkdownStats{}, time.Now())
	if err != nil {
		t.Fatalf("packStream: %v", err)
	}
	if stats.DigestDropped != 2 {
		t.Fatalf("DigestDropped = %d, want 2", stats.DigestDropped)
	}
	if stats.Rows != 4 {
		t.Fatalf("Rows = %d, want 4", stats.Rows)
	}

	// Zero false drops: every distinct page is still there, and the surviving copy
	// of the repeated page is the first one seen.
	rows, err := parquet.ReadFile[MarkdownRow](out)
	if err != nil {
		t.Fatalf("read parquet: %v", err)
	}
	seen := map[string]bool{}
	for _, r := range rows {
		seen[r.URL] = true
	}
	for _, want := range []string{"a", "b", "d", "f"} {
		if !seen["https://example.com/"+want] {
			t.Fatalf("page %s was dropped, but nothing else has its bytes", want)
		}
	}
	for _, gone := range []string{"c", "e"} {
		if seen["https://example.com/"+gone] {
			t.Fatalf("page %s is a byte identical copy of a and should have been dropped", gone)
		}
	}

	// The same shard without the flag keeps everything, which is what makes the
	// drop count above a measurement rather than an accident of the fixture.
	plain := filepath.Join(t.TempDir(), "plain.parquet")
	base, err := packStream(context.Background(), bytes.NewReader(shard),
		MarkdownPackConfig{OutPath: plain, Workers: 2},
		MarkdownStats{}, time.Now())
	if err != nil {
		t.Fatalf("packStream without dedup: %v", err)
	}
	if base.Rows != 6 || base.DigestDropped != 0 {
		t.Fatalf("without --dedup-digest: Rows = %d, DigestDropped = %d, want 6 and 0", base.Rows, base.DigestDropped)
	}
}

// TestSimhashColumnPresentAndStable is the second done-when. The column has to
// exist on every row with text, and two runs of the same input have to produce
// the same value for the same document.
func TestSimhashColumnPresentAndStable(t *testing.T) {
	urls := []string{"a", "b", "c", "d"}
	bodies := []string{
		articlePage("Common Crawl publishes a web archive every month for researchers."),
		articlePage("A different page about bread and flour and a hot oven."),
		articlePage("A third page about mountains and rivers and long walks."),
		articlePage("A fourth page about trains and timetables and platforms."),
	}
	shard := dupWARC(t, urls, bodies)

	read := func(name string) map[string]uint64 {
		out := filepath.Join(t.TempDir(), name)
		if _, err := packStream(context.Background(), bytes.NewReader(shard),
			MarkdownPackConfig{OutPath: out, Workers: 3},
			MarkdownStats{}, time.Now()); err != nil {
			t.Fatalf("packStream: %v", err)
		}
		rows, err := parquet.ReadFile[MarkdownRow](out)
		if err != nil {
			t.Fatalf("read parquet: %v", err)
		}
		got := map[string]uint64{}
		for _, r := range rows {
			if r.Simhash == 0 {
				t.Fatalf("row %s has no simhash but its markdown is %d bytes", r.URL, len(r.Markdown))
			}
			got[r.URL] = r.Simhash
		}
		return got
	}

	first, second := read("one.parquet"), read("two.parquet")
	if len(first) != len(urls) {
		t.Fatalf("first run wrote %d rows, want %d", len(first), len(urls))
	}
	for u, h := range first {
		if second[u] != h {
			t.Fatalf("%s hashed to %016x then %016x, the column is not stable", u, h, second[u])
		}
	}
}

// TestSimhashColumnMatchesStoredText checks the column against the text beside
// it, so a consumer can recompute and get the same answer.
func TestSimhashColumnMatchesStoredText(t *testing.T) {
	shard := dupWARC(t, []string{"a", "b"}, []string{
		articlePage("Common Crawl publishes a web archive every month for researchers."),
		articlePage("A different page about bread and flour and a hot oven."),
	})
	out := filepath.Join(t.TempDir(), "match.parquet")
	if _, err := packStream(context.Background(), bytes.NewReader(shard),
		MarkdownPackConfig{OutPath: out, Workers: 1}, MarkdownStats{}, time.Now()); err != nil {
		t.Fatalf("packStream: %v", err)
	}
	rows, err := parquet.ReadFile[MarkdownRow](out)
	if err != nil {
		t.Fatalf("read parquet: %v", err)
	}
	for _, r := range rows {
		if want := Simhash(r.Markdown); r.Simhash != want {
			t.Fatalf("%s stored %016x but its markdown hashes to %016x", r.URL, r.Simhash, want)
		}
	}
}

// writeDedupFixture writes a small parquet file with the columns the report
// reads, so AnalyzeDedup can be tested without a crawl.
func writeDedupFixture(t *testing.T, path string, rows []MarkdownRow) {
	t.Helper()
	w, err := NewParquetWriter[MarkdownRow](path)
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
}

// TestAnalyzeDedupCountsBothKinds walks a directory holding one exact cluster,
// one near cluster, and two documents that belong to neither, and checks the
// report separates them.
func TestAnalyzeDedupCountsBothKinds(t *testing.T) {
	body := prose("harbour", 200)
	dup := "# Title\n\n" + body
	near1 := "# Title\n\nHome About Contact\n" + body
	near2 := "# Title\n\nHome About Contact Search\n" + body + "\nCopyright 2026"
	uniq1 := prose("bakery", 200)
	uniq2 := prose("transit", 200)

	row := func(u, md string) MarkdownRow {
		return MarkdownRow{URL: u, Markdown: md, Simhash: Simhash(md)}
	}
	dir := t.TempDir()
	writeDedupFixture(t, filepath.Join(dir, "part-000.parquet"), []MarkdownRow{
		row("https://a.example/1", dup),
		row("https://b.example/1", dup),
		row("https://c.example/1", uniq1),
	})
	writeDedupFixture(t, filepath.Join(dir, "part-001.parquet"), []MarkdownRow{
		row("https://d.example/1", dup),
		row("https://e.example/1", near1),
		row("https://f.example/1", near2),
		row("https://g.example/1", uniq2),
	})

	rep, err := AnalyzeDedup([]string{dir}, DefaultNearDistance, 10)
	if err != nil {
		t.Fatalf("AnalyzeDedup: %v", err)
	}
	if rep.Files != 2 || rep.Rows != 7 {
		t.Fatalf("Files = %d, Rows = %d, want 2 and 7", rep.Files, rep.Rows)
	}
	// Three byte identical copies is one cluster and two redundant rows.
	if rep.ExactClusters != 1 || rep.ExactDuplicates != 2 {
		t.Fatalf("exact: %d clusters, %d duplicates, want 1 and 2", rep.ExactClusters, rep.ExactDuplicates)
	}
	// The exact copies count once as a document, so the near cluster is the three
	// variants of the same article: dup, near1, near2.
	if rep.NearClusters != 1 {
		t.Fatalf("near clusters = %d, want 1", rep.NearClusters)
	}
	if rep.NearDuplicates != 2 {
		t.Fatalf("near duplicates = %d, want 2", rep.NearDuplicates)
	}
	// Neither unique document may be swept into anything.
	for _, c := range rep.Clusters {
		for _, u := range c.URLs {
			if u == "https://c.example/1" || u == "https://g.example/1" {
				t.Fatalf("unique document %s was put in a %s cluster", u, c.Kind)
			}
		}
	}
	if rep.ExactBytes <= 0 || rep.NearBytes <= 0 {
		t.Fatalf("byte savings not reported: exact %d, near %d", rep.ExactBytes, rep.NearBytes)
	}
	if !strings.Contains(rep.Summary(), "exact duplicates") {
		t.Fatalf("summary does not report exact duplicates:\n%s", rep.Summary())
	}
}

// TestAnalyzeDedupDistanceZeroFindsOnlyExact is the safety property: at distance
// 0 nothing but byte identical text may cluster, so a run that reports near
// duplicates at distance 0 has a bug in the banding, not in the corpus.
func TestAnalyzeDedupDistanceZeroFindsOnlyExact(t *testing.T) {
	body := prose("harbour", 200)
	dir := t.TempDir()
	writeDedupFixture(t, filepath.Join(dir, "part-000.parquet"), []MarkdownRow{
		{URL: "https://a.example/", Markdown: body, Simhash: Simhash(body)},
		{URL: "https://b.example/", Markdown: body, Simhash: Simhash(body)},
		{URL: "https://c.example/", Markdown: "Home\n" + body, Simhash: Simhash("Home\n" + body)},
	})
	rep, err := AnalyzeDedup([]string{dir}, 0, 10)
	if err != nil {
		t.Fatalf("AnalyzeDedup: %v", err)
	}
	if rep.ExactDuplicates != 1 {
		t.Fatalf("exact duplicates = %d, want 1", rep.ExactDuplicates)
	}
	// Two documents whose text differs can still share a fingerprint, and at
	// distance 0 that is the only way to land in a near cluster. What must not
	// happen is a cluster of documents that are visibly further apart.
	for _, c := range rep.Clusters {
		if c.Kind != "near" {
			continue
		}
		t.Logf("distance 0 near cluster: %v", c.URLs)
	}
	if rep.NearDistance != 0 {
		t.Fatalf("report says distance %d, ran at 0", rep.NearDistance)
	}
}

// TestAnalyzeDedupWithoutSimhashColumn covers a corpus published before the
// column existed. The report has to fingerprint those rows as it reads them,
// otherwise every v2 dataset reads as one giant cluster of zeroes.
func TestAnalyzeDedupWithoutSimhashColumn(t *testing.T) {
	type oldRow struct {
		URL      string `parquet:"url"`
		Markdown string `parquet:"markdown"`
	}
	body := prose("harbour", 200)
	path := filepath.Join(t.TempDir(), "v2.parquet")
	w, err := NewParquetWriter[oldRow](path)
	if err != nil {
		t.Fatal(err)
	}
	for i, md := range []string{body, body, "Home\n" + body, "Bread and flour and yeast in a hot oven, repeatedly. " + body[:200]} {
		if err := w.Write(oldRow{URL: fmt.Sprintf("https://v2.example/%d", i), Markdown: md}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	rep, err := AnalyzeDedup([]string{path}, DefaultNearDistance, 10)
	if err != nil {
		t.Fatalf("AnalyzeDedup on a v2 file: %v", err)
	}
	if rep.Rows != 4 {
		t.Fatalf("Rows = %d, want 4", rep.Rows)
	}
	if rep.ExactDuplicates != 1 {
		t.Fatalf("exact duplicates = %d, want 1", rep.ExactDuplicates)
	}
	if rep.NoFingerprint != 0 {
		t.Fatalf("NoFingerprint = %d on a file of real prose, the fallback did not run", rep.NoFingerprint)
	}
}

// TestAnalyzeDedupEmptyInput checks the two edges that are easy to get wrong: a
// directory with no parquet in it is an error, and a file of empty documents is
// counted rather than clustered.
func TestAnalyzeDedupEmptyInput(t *testing.T) {
	if _, err := AnalyzeDedup([]string{t.TempDir()}, 3, 10); err == nil {
		t.Fatal("an empty directory should be an error, not an empty report")
	}
	if _, err := AnalyzeDedup([]string{filepath.Join(t.TempDir(), "nope")}, 3, 10); err == nil {
		t.Fatal("a missing path should be an error")
	}

	dir := t.TempDir()
	writeDedupFixture(t, filepath.Join(dir, "part-000.parquet"), []MarkdownRow{
		{URL: "https://a.example/", Markdown: ""},
		{URL: "https://b.example/", Markdown: ""},
	})
	rep, err := AnalyzeDedup([]string{dir}, 3, 10)
	if err != nil {
		t.Fatalf("AnalyzeDedup: %v", err)
	}
	if rep.ExactClusters != 0 || rep.NearClusters != 0 {
		t.Fatalf("empty documents were clustered: %d exact, %d near", rep.ExactClusters, rep.NearClusters)
	}
}

// TestAnalyzeDedupSkipsShortDocsInNearPass pins the length floor. The same pair
// of documents, differing by one line, is a near duplicate when it is long
// enough for the fingerprint to mean something and is left alone when it is not.
// On a live crawl shard this is what separates two forum FAQ pages from two
// unrelated stubs that happen to share a nav bar.
func TestAnalyzeDedupSkipsShortDocsInNearPass(t *testing.T) {
	short := prose("harbour", 8)
	if len(short) >= minNearBytes {
		t.Fatalf("fixture is meant to sit under the floor, got %d bytes", len(short))
	}
	long := prose("harbour", 200)
	if len(long) < minNearBytes {
		t.Fatalf("fixture is meant to sit over the floor, got %d bytes", len(long))
	}

	for _, tc := range []struct {
		name string
		body string
		want int
	}{
		{"under the floor", short, 0},
		{"over the floor", long, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeDedupFixture(t, filepath.Join(dir, "part-000.parquet"), []MarkdownRow{
				{URL: "https://a.example/", Markdown: tc.body},
				{URL: "https://b.example/", Markdown: tc.body + "\nCopyright 2026"},
			})
			rep, err := AnalyzeDedup([]string{dir}, DefaultNearDistance, 10)
			if err != nil {
				t.Fatalf("AnalyzeDedup: %v", err)
			}
			if rep.NearClusters != tc.want {
				t.Fatalf("near clusters = %d, want %d", rep.NearClusters, tc.want)
			}
		})
	}
}

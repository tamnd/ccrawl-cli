package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/tamnd/ccrawl-cli/internal/fakecc"
)

// Every command in here runs the way a person runs it: an argv in, an exit code
// and bytes out. What they check is the wiring, because that is what the unit
// tests below cannot see.

func TestCrawlsList(t *testing.T) {
	r := run(t, "crawls", "list", "-o", "jsonl").wantCode(t, 0)
	if got := len(r.Lines()); got != len(fakecc.Crawls) {
		t.Fatalf("listed %d crawls, want %d\n%s", got, len(fakecc.Crawls), r.Out)
	}
	r.wantOut(t, fakecc.Crawls[0], fakecc.Crawls[len(fakecc.Crawls)-1])
}

func TestCrawlsLatestIsTheNewest(t *testing.T) {
	run(t, "crawls", "latest").wantCode(t, 0).wantOut(t, fakecc.Crawls[0])
}

// A single crawl reference has four spellings and they all have to land on the
// same canonical ID, because everything downstream keys off it: the cache, the
// library layout, the CDX collection.
func TestCrawlsResolveSingleReference(t *testing.T) {
	cases := map[string]string{
		"latest":          fakecc.Crawls[0],
		"CC-MAIN-2026-05": "CC-MAIN-2026-05",
		"2026-05":         "CC-MAIN-2026-05",
		"2026":            "CC-MAIN-2026-30", // the newest crawl of the year
	}
	for ref, want := range cases {
		t.Run(ref, func(t *testing.T) {
			run(t, "crawls", "resolve", ref).wantCode(t, 0).wantOut(t, want)
		})
	}
	run(t, "crawls", "resolve", "last-tuesday").wantCode(t, 1)
	run(t, "crawls", "resolve", "1999").wantCode(t, 1)
}

// The multi-crawl forms of -c are the ones most likely to be quietly wrong,
// because every one of them returns a plausible answer. A year that resolved to
// the newest crawl instead of the year's crawls, or an integer read as a year,
// produces results nobody would question.
//
// --estimate is the cheapest command that reports which crawls a run visited:
// one row per crawl, plus a total when there is more than one.
func TestMultiCrawlResolutionForEveryForm(t *testing.T) {
	cases := []struct {
		ref  string
		want []string
	}{
		{"latest", fakecc.Crawls[:1]},
		{"all", fakecc.Crawls},
		{"2", fakecc.Crawls[:2]},
		{"2026", []string{"CC-MAIN-2026-30", "CC-MAIN-2026-05"}},
		{"2025", []string{"CC-MAIN-2025-33"}},
		{"CC-MAIN-2026-05", []string{"CC-MAIN-2026-05"}},
		{"2026-05", []string{"CC-MAIN-2026-05"}},
		{"2025-33,CC-MAIN-2026-30", []string{"CC-MAIN-2025-33", "CC-MAIN-2026-30"}},
		// A crawl named twice in a list is visited once, or every command that
		// iterates crawls would do the work twice and report duplicate rows.
		{"latest,CC-MAIN-2026-30", []string{"CC-MAIN-2026-30"}},
	}
	for _, tc := range cases {
		t.Run(tc.ref, func(t *testing.T) {
			r := run(t, "search", "example.com/*", "--estimate", "-c", tc.ref, "-o", "jsonl").wantCode(t, 0)
			var got []string
			for _, line := range r.Lines() {
				var row struct {
					Crawl string `json:"crawl"`
				}
				if err := json.Unmarshal([]byte(line), &row); err != nil {
					t.Fatalf("estimate line is not JSON: %v\n%s", err, line)
				}
				if row.Crawl != "total" {
					got = append(got, row.Crawl)
				}
			}
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("-c %s visited %v, want %v", tc.ref, got, tc.want)
			}
		})
	}
}

func TestMultiCrawlResolutionRejectsNonsense(t *testing.T) {
	run(t, "search", "example.com/*", "-c", "last-tuesday").wantCode(t, 1)
	run(t, "search", "example.com/*", "-c", "1999").wantCode(t, 1)
	run(t, "search", "example.com/*", "-c", "0").wantCode(t, 1)
}

// A search has to page through the index, retry the 403 the fronting throws on
// the way, and come back with every capture. Losing the last page to an early
// stop, or a page to a transient 403, looks exactly like a URL with fewer
// captures than it has.
func TestSearchPagesAndRetries(t *testing.T) {
	r := run(t, "search", "example.com", "--match", "domain", "-o", "url").wantCode(t, 0)
	got := r.Lines()
	if len(got) != 3 {
		t.Fatalf("search returned %d URLs, want the 3 under example.com\n%s", len(got), r.Out)
	}
	r.wantOut(t, "https://example.com/", "https://example.com/about", "https://www.example.com/gone")
	r.wantNotOut(t, "other.test")
	// Three captures at two per page is two pages, and page 1 is the one the
	// fake 403s once, so a second request for it proves the retry ran.
	if n := r.Server.Hits("CC-MAIN-2026-30-index"); n < 4 {
		t.Fatalf("the index was asked %d times, want the page count, page 0, and page 1 twice", n)
	}
}

func TestSearchLimitStopsWithoutFailing(t *testing.T) {
	r := run(t, "search", "example.com/*", "-o", "url", "-n", "2").wantCode(t, 0)
	if got := len(r.Lines()); got != 2 {
		t.Fatalf("search -n 2 returned %d URLs\n%s", got, r.Out)
	}
}

// A query the index has nothing for is not a failure. Exit 3 is what a pipeline
// reads to tell "no captures" from "the run broke", and the CDX server reports
// it as a 404, so the whole path from an HTTP 404 to exit 3 is here.
func TestSearchWithNoCapturesExitsThree(t *testing.T) {
	run(t, "search", "nothing-here.invalid/*").wantCode(t, 3)
}

func TestSearchFiltersReachTheServer(t *testing.T) {
	// --status is a server side filter, so the 404 capture never comes back.
	run(t, "search", "example.com", "--match", "domain", "--status", "200", "-o", "url").
		wantCode(t, 0).
		wantOut(t, "https://example.com/").
		wantNotOut(t, "/gone")

	// --url-contains is applied here rather than by the server, and has to
	// narrow the same result set the same way.
	run(t, "search", "example.com", "--match", "domain", "--url-contains", "/about", "-o", "url").
		wantCode(t, 0).
		wantOut(t, "/about").
		wantNotOut(t, "/gone")

	run(t, "search", "example.com/*", "--url-not-contains", "/about", "-o", "url").
		wantCode(t, 0).
		wantNotOut(t, "/about")

	// A time bound that excludes everything is still a clean empty result.
	run(t, "search", "example.com/*", "--to", "2025").wantCode(t, 3)
}

func TestSearchMatchTypes(t *testing.T) {
	run(t, "search", "example.com/about", "--match", "exact", "-o", "url").
		wantCode(t, 0).wantOut(t, "/about").wantNotOut(t, "example.com/\n")
	// host matches one host, domain reaches www as well, and the difference is
	// the whole reason both exist.
	run(t, "search", "example.com", "--match", "host", "-o", "url").
		wantCode(t, 0).wantNotOut(t, "www.example.com")
	run(t, "search", "example.com", "--match", "domain", "-o", "url").
		wantCode(t, 0).wantOut(t, "www.example.com")
}

func TestSearchSortOldestReversesTheCrawlOrder(t *testing.T) {
	run(t, "search", "example.com/*", "--sort", "sideways").wantCode(t, 2)
}

// --locations is the handoff between search and fetch. If the offsets it prints
// are not the ones the record occupies, nothing downstream works and nothing
// upstream looks wrong.
func TestSearchLocationsMatchTheWARC(t *testing.T) {
	r := run(t, "search", "example.com/*", "--locations", "-o", "jsonl").wantCode(t, 0)
	want, ok := r.Server.Capture("https://example.com/")
	if !ok {
		t.Fatal("the fixture lost its first capture")
	}
	var found bool
	for _, line := range r.Lines() {
		var loc struct {
			Filename string `json:"filename"`
			Offset   int64  `json:"offset"`
			Length   int64  `json:"length"`
			URL      string `json:"url"`
		}
		if err := json.Unmarshal([]byte(line), &loc); err != nil {
			t.Fatalf("location line is not JSON: %v\n%s", err, line)
		}
		if loc.URL != want.URL {
			continue
		}
		found = true
		if loc.Filename != fakecc.WARCPath {
			t.Errorf("filename = %q, want the crawl's WARC path", loc.Filename)
		}
		if loc.Offset == 0 && loc.Length == 0 {
			t.Error("offset and length are both zero, so the record cannot be fetched")
		}
	}
	if !found {
		t.Fatalf("no location for %s\n%s", want.URL, r.Out)
	}
}

func TestSearchPagesFlagCountsPages(t *testing.T) {
	// Three captures at two records to a page, so the count is the paging the
	// stream is about to do rather than a constant.
	run(t, "search", "example.com", "--match", "domain", "--pages", "-o", "jsonl").
		wantCode(t, 0).wantOut(t, `"pages":2`)
}

// get is the whole read path in one command: resolve the crawl, query the index,
// range-fetch the record out of the WARC, and unwrap it down to the bytes the
// page was served with.
func TestGetReturnsTheCapturedPage(t *testing.T) {
	run(t, "get", "https://example.com/").
		wantCode(t, 0).
		wantOut(t, "<title>Example Domain</title>", "illustrative examples")
}

func TestGetText(t *testing.T) {
	r := run(t, "get", "https://example.com/", "--text").wantCode(t, 0)
	r.wantOut(t, "illustrative examples")
	if strings.Contains(r.Out, "<p>") {
		t.Fatalf("--text still carries markup:\n%s", r.Out)
	}
}

func TestGetURLWithNoCaptureExitsThree(t *testing.T) {
	run(t, "get", "https://nothing-here.invalid/").wantCode(t, 3)
}

// fetch reads the locations search prints. Feeding it back what search produced
// is the round trip that proves the two agree on the format.
func TestFetchReadsLocationsFromStdin(t *testing.T) {
	srv := fakecc.Start(t)
	loc := srv.Location("https://example.com/")
	if loc == "" {
		t.Fatal("the fixture has no location for the first capture")
	}
	code, out, errOut := invoke(t, loc+"\n",
		[]string{"ccrawl", "--rate", "1ns", "--global-rate", "0", "fetch", "-", "--text"})
	r := result{Code: code, Out: out, Err: errOut}
	r.wantCode(t, 0).wantOut(t, "illustrative examples")
}

func TestPathsListsTheCrawlManifest(t *testing.T) {
	run(t, "paths", "warc").wantCode(t, 0).wantOut(t, fakecc.WARCPath)
}

// A kind nobody publishes is a 404 on the manifest, and the message has to name
// the kind, because "HTTP 404" on its own tells a person nothing about the typo
// they made.
func TestPathsForAnUnknownKindFails(t *testing.T) {
	r := run(t, "paths", "gopher").wantCode(t, 1)
	if !strings.Contains(r.Err, "gopher") {
		t.Fatalf("the error does not name the kind:\n%s", r.Err)
	}
}

// paths --kinds is the list a person checks after that failure, so it has to
// hold every kind the manifests actually offer.
func TestPathsKindsListsThem(t *testing.T) {
	runNoServer(t, "paths", "--kinds").wantCode(t, 0).
		wantOut(t, "warc", "wet", "cc-index-table", "segment")
}

// download writes real files, so this checks the bytes on disk rather than the
// line it prints about them. --flat is what puts the file straight in --out
// rather than under the crawl-data tree the path came from.
func TestDownloadWritesTheArchive(t *testing.T) {
	dir := t.TempDir()
	r := run(t, "download", "warc", "--out", dir, "--flat", "-n", "1").wantCode(t, 0)
	got, err := filepath.Glob(filepath.Join(dir, "*.warc.gz"))
	if err != nil || len(got) != 1 {
		t.Fatalf("download left %v in %s (err %v)\nstdout:\n%s", got, dir, err, r.Out)
	}
	b, err := os.ReadFile(got[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != len(r.Server.WARC) {
		t.Fatalf("downloaded %d bytes, the server has %d", len(b), len(r.Server.WARC))
	}
}

// parse works on a local file, so the fixture is written out and read back. It
// is also the only command that proves the WARC the fake serves is one this
// program's own parser accepts.
func TestParseDecodesTheFixtureWARC(t *testing.T) {
	srv := fakecc.Start(t)
	path := filepath.Join(t.TempDir(), "fixture.warc.gz")
	if err := os.WriteFile(path, srv.WARC, 0o600); err != nil {
		t.Fatal(err)
	}
	code, out, errOut := invoke(t, "", []string{"ccrawl", "parse", path, "-o", "jsonl"})
	r := result{Code: code, Out: out, Err: errOut}
	r.wantCode(t, 0)
	if got := len(r.Lines()); got != len(fakecc.Captures) {
		t.Fatalf("parsed %d records, want %d\n%s", got, len(fakecc.Captures), r.Out)
	}
	r.wantOut(t, "https://example.com/about", "other.test")

	// A filter that matches nothing is an empty result, not a failure.
	code, _, _ = invoke(t, "", []string{"ccrawl", "parse", path, "--type", "request"})
	if code != 3 {
		t.Fatalf("parse --type request exited %d, want 3", code)
	}
}

func TestParseMissingFileFails(t *testing.T) {
	runNoServer(t, "parse", filepath.Join(t.TempDir(), "absent.warc.gz")).wantCode(t, 1)
}

// export writes WARC files with provenance. The check that matters is that the
// bytes it wrote parse back, since an export nobody can read is worse than none.
func TestExportWritesAReadableWARC(t *testing.T) {
	dir := t.TempDir()
	r := run(t, "export", "example.com/*", "--out-dir", dir, "--prefix", "test").wantCode(t, 0)
	files, err := filepath.Glob(filepath.Join(dir, "test*.warc.gz"))
	if err != nil || len(files) == 0 {
		t.Fatalf("export left nothing in %s\nstdout:\n%s\nstderr:\n%s", dir, r.Out, r.Err)
	}
	code, out, _ := invoke(t, "", []string{"ccrawl", "parse", files[0], "-o", "jsonl"})
	if code != 0 {
		t.Fatalf("the exported WARC does not parse, exit %d", code)
	}
	if !strings.Contains(out, "https://example.com/") {
		t.Fatalf("the exported WARC lost its captures:\n%s", out)
	}
}

// export sends --url-fgrep to the index server too, and the WARC it writes has
// to hold exactly the captures the substring names. A pushed filter that the
// server reads differently would show up here as an empty archive or an extra
// record.
func TestExportPushesTheURLFilter(t *testing.T) {
	dir := t.TempDir()
	run(t, "export", "example.com", "--match", "domain", "--url-fgrep", "/about",
		"--out-dir", dir, "--prefix", "test").wantCode(t, 0)
	files, err := filepath.Glob(filepath.Join(dir, "test*.warc.gz"))
	if err != nil || len(files) == 0 {
		t.Fatalf("export left nothing in %s", dir)
	}
	_, out, _ := invoke(t, "", []string{"ccrawl", "parse", files[0], "-o", "url"})
	got := strings.Fields(out)
	if len(got) != 1 || got[0] != "https://example.com/about" {
		t.Fatalf("the export holds %v, want only the /about capture", got)
	}
}

func TestExportWithNoMatchesExitsThree(t *testing.T) {
	run(t, "export", "nothing-here.invalid/*", "--out-dir", t.TempDir()).wantCode(t, 3)
}

func TestExportNeedsAPattern(t *testing.T) {
	run(t, "export").wantCode(t, 2)
}

// The columnar commands print SQL without touching anything, which is the only
// part of them a test with no parquet file can reach. It is worth pinning: the
// printed SQL is what people paste into Athena.
func TestColumnarPrintsSQL(t *testing.T) {
	r := run(t, "columnar", "sql", "--tld", "gov", "--mime", "application/pdf", "--print").wantCode(t, 0)
	r.wantOut(t, "url_host_tld", "gov", "content_mime_detected", "read_parquet", "subset=warc")
}

func TestColumnarNegatedFiltersReachTheSQL(t *testing.T) {
	run(t, "columnar", "sql", "--not-lang", "vie", "--not-status", "200", "--print").
		wantCode(t, 0).
		wantOut(t, "content_languages", "fetch_status")
}

func TestColumnarQueryRefusesTheNativeEngine(t *testing.T) {
	run(t, "columnar", "query", "SELECT 1", "--engine", "native").wantCode(t, 2)
}

// config show prints the settings a run resolved, and the point of the test is
// that the directories it reports are the ones in force: the harness overrides
// them per test, so a hardcoded default leaking through would show up here.
func TestConfigShowsTheResolvedPaths(t *testing.T) {
	r := runNoServer(t, "config", "show").wantCode(t, 0)
	r.wantOut(t, "data_dir", "cache_dir", "library_dir", os.Getenv("CCRAWL_DATA_DIR"))
}

// The cache commands are the ones a person reaches for when something is stale,
// so they have to work on a cache directory that does not exist yet rather than
// fail on it.
func TestCacheCommands(t *testing.T) {
	runNoServer(t, "cache", "dir").wantCode(t, 0)
	runNoServer(t, "cache", "info").wantCode(t, 0)
	runNoServer(t, "cache", "clear", "--yes").wantCode(t, 0)
}

func TestVersion(t *testing.T) {
	runNoServer(t, "version").wantCode(t, 0).wantOut(t, "ccrawl")
}

// The version line is six facts glued into a sentence, and a CI job that wants
// one of them should not have to write a regular expression for it. Asking for
// a format gets the same six as data.
func TestVersionRendersAsData(t *testing.T) {
	r := runNoServer(t, "version", "-o", "jsonl").wantCode(t, 0)
	var got map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(r.Out)), &got); err != nil {
		t.Fatalf("-o jsonl is not JSON: %v\n%s", err, r.Out)
	}
	for _, k := range []string{"version", "commit", "built", "os", "arch", "go"} {
		if _, ok := got[k]; !ok {
			t.Errorf("no %q field: %v", k, got)
		}
	}
	runNoServer(t, "version", "-o", "csv").wantCode(t, 0).wantOut(t, "version,commit,built,os,arch,go")

	// auto keeps the sentence, in a pipe as well as on a terminal, because that
	// is what ccrawl version has always printed and what anything grepping it
	// expects. A test run is a pipe, so this is the case that would break.
	runNoServer(t, "version").wantCode(t, 0).
		wantOut(t, "ccrawl").
		wantNotOut(t, `"version"`)
	runNoServer(t, "version", "--short").wantCode(t, 0).wantNotOut(t, "commit")
}

// A name ccrawl does not know is never answered with help on stdout and never
// with exit 0. #141.
//
// The exit code is 2 everywhere except a misspelling at the very top, which
// stays 1. That one is caught by the argument parser inside its command lookup,
// before ccrawl is reached at all, so its error never gets a kind. Moving it
// would mean giving up the lookup, and the lookup is what makes
// "ccrawl serch --help" an error rather than the root's help printed for a
// command that does not exist. The framework carries the same note.
func TestUnknownCommandAndFlagExitTwo(t *testing.T) {
	runNoServer(t, "definitely-not-a-command").wantCode(t, 1).wantOut(t, "")
	runNoServer(t, "serch", "--help").wantCode(t, 1).wantOut(t, "")
	runNoServer(t, "search", "example.com", "--not-a-flag").wantCode(t, 2)

	// Every group command, not just the root. These are the two ways ccrawl gets
	// one: generated from an operation's parent, and written by hand.
	for _, parent := range []string{"host", "index", "urls", "cache", "columnar", "db", "library"} {
		r := runNoServer(t, parent, "zzznope").wantCode(t, 2)
		if r.Out != "" {
			t.Errorf("%s zzznope wrote %d bytes to stdout, which a redirect would have kept:\n%s", parent, len(r.Out), r.Out)
		}
	}

	// A group with nothing after it is a question, not a mistake, and still
	// answers on stdout with exit 0.
	runNoServer(t, "host").wantCode(t, 0).wantOut(t, "Enumerate and enrich hosts")
}

// crawls info walks every manifest a crawl publishes, so it is the one command
// that fails if a kind stops resolving. The positional crawl is checked here
// too: it predates the move to an operation and scripts pass it.
func TestCrawlsInfoCountsEveryKind(t *testing.T) {
	r := run(t, "crawls", "info", "2026-30").wantCode(t, 0)
	r.wantOut(t, "CC-MAIN-2026-30", "warc", "wet", "cc-index-table")
	if strings.Contains(r.Out, "-1") {
		t.Fatalf("a manifest did not resolve:\n%s", r.Out)
	}
}

// TestCrawlsInfoRendersInEveryFormat is #140. It used to write its own text with
// fmt.Fprintf, so -o was accepted and ignored and a pipeline asking for JSONL
// got a paragraph and exit 0.
func TestCrawlsInfoRendersInEveryFormat(t *testing.T) {
	jsonl := run(t, "crawls", "info", "2026-30", "-o", "jsonl").wantCode(t, 0)
	for _, line := range jsonl.Lines() {
		var row struct {
			Crawl string `json:"crawl"`
			Kind  string `json:"kind"`
			Files int    `json:"files"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("-o jsonl did not write JSON: %v\n%s", err, line)
		}
		if row.Crawl != "CC-MAIN-2026-30" || row.Kind == "" {
			t.Fatalf("row is missing fields: %s", line)
		}
	}

	csv := run(t, "crawls", "info", "2026-30", "-o", "csv").wantCode(t, 0)
	if !strings.HasPrefix(csv.Out, "crawl,kind,files") {
		t.Fatalf("-o csv did not write a CSV header:\n%s", csv.Out)
	}
}

// TestCrawlsInfoAndStatsCountTheSameKinds pins the two spellings together. They
// used to carry a default kind list each, differing by one kind in each
// direction, so the same question answered differently depending on which name
// you typed.
func TestCrawlsInfoAndStatsCountTheSameKinds(t *testing.T) {
	kinds := func(args ...string) []string {
		r := run(t, append(args, "-o", "csv")...).wantCode(t, 0)
		var out []string
		for _, line := range r.Lines()[1:] {
			out = append(out, strings.Split(line, ",")[1])
		}
		return out
	}
	info := kinds("crawls", "info", "2026-30")
	stats := kinds("stats", "-c", "2026-30")
	if !slices.Equal(info, stats) {
		t.Fatalf("crawls info counts %v, stats counts %v", info, stats)
	}
}

// get -O writes the body to a file rather than stdout, and --at picks a capture
// by date instead of taking the newest. Both are one line of wiring each and
// both are silently wrong if that line is missing: the file is empty, or the
// date is ignored and you get the latest capture anyway.
func TestGetWritesToAFileAndHonoursAt(t *testing.T) {
	out := filepath.Join(t.TempDir(), "page.html")
	run(t, "get", "https://example.com/", "-O", out).wantCode(t, 0)
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "Example Domain") {
		t.Fatalf("the file does not hold the captured page:\n%s", b)
	}

	srv := fakecc.Start(t)
	cap0, ok := srv.Capture("https://example.com/")
	if !ok {
		t.Fatal("the fixture lost its first capture")
	}
	code, got, errOut := invoke(t, "", []string{"ccrawl", "--rate", "1ns", "--global-rate", "0",
		"get", "https://example.com/", "--at", cap0.Timestamp[:6], "--headers"})
	result{Code: code, Out: got, Err: errOut}.wantCode(t, 0).wantOut(t, "HTTP/1.1 200")
}

// extract is get with the transform fixed, and title is the one that does not
// go through get at all, so both branches are worth a run.
func TestExtractSubcommands(t *testing.T) {
	run(t, "extract", "text", "https://example.com/").wantCode(t, 0).
		wantOut(t, "illustrative examples")
	run(t, "extract", "title", "https://example.com/").wantCode(t, 0).
		wantOut(t, "Example Domain")
	run(t, "extract", "links", "https://example.com/").wantCode(t, 0)
}

// fetch --dir writes one file per record, which is the mode a person uses to
// pull a batch of pages onto disk. --batch is the shared-range path, and it has
// to produce the same bytes as the plain one.
func TestFetchWritesADirectoryAndBatches(t *testing.T) {
	srv := fakecc.Start(t)
	var locs []string
	for _, u := range []string{"https://example.com/", "https://example.com/about"} {
		if loc := srv.Location(u); loc != "" {
			locs = append(locs, loc)
		}
	}
	if len(locs) != 2 {
		t.Fatalf("the fixture has %d of the 2 locations this needs", len(locs))
	}
	dir := t.TempDir()
	code, _, errOut := invoke(t, strings.Join(locs, "\n")+"\n",
		[]string{"ccrawl", "--rate", "1ns", "--global-rate", "0", "fetch", "-",
			"--dir", "--out-dir", dir, "--batch"})
	if code != 0 {
		t.Fatalf("fetch --dir exited %d\nstderr:\n%s", code, errOut)
	}
	files, err := filepath.Glob(filepath.Join(dir, "*"))
	if err != nil || len(files) != 2 {
		t.Fatalf("fetch --dir left %v in %s (err %v)", files, dir, err)
	}
}

// convert turns a local archive into JSONL. Parquet needs a writer this test
// cannot assume, JSONL needs nothing, and the record count is the part that
// proves the conversion read the whole file rather than the first member.
func TestConvertWARCToJSONL(t *testing.T) {
	srv := fakecc.Start(t)
	in := filepath.Join(t.TempDir(), "in.warc.gz")
	if err := os.WriteFile(in, srv.WARC, 0o600); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "out.jsonl")
	code, _, errOut := invoke(t, "", []string{"ccrawl", "convert", in, "--to", "jsonl", "-O", out})
	if code != 0 {
		t.Fatalf("convert exited %d\nstderr:\n%s", code, errOut)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("line %d is not JSON: %v", n+1, err)
		}
		n++
	}
	if n != len(fakecc.Captures) {
		t.Fatalf("converted %d records, want %d", n, len(fakecc.Captures))
	}
}

// Every columnar subcommand can print its SQL without a parquet file or a
// duckdb binary, and that is the only path CI can take. It is still worth
// taking: the SQL each one builds is the command, and --print is what people
// paste into Athena.
func TestColumnarSubcommandsPrintTheirSQL(t *testing.T) {
	cases := []struct {
		args []string
		want []string
	}{
		{[]string{"urls"}, []string{"SELECT", "url", "read_parquet"}},
		{[]string{"locations"}, []string{"warc_filename", "warc_record_offset"}},
		{[]string{"count"}, []string{"count(*)"}},
		{[]string{"langs"}, []string{"GROUP BY", "content_languages"}},
		{[]string{"mimes"}, []string{"GROUP BY", "content_mime_detected"}},
		{[]string{"schema"}, []string{"DESCRIBE"}},
		{[]string{"query", "SELECT 1 FROM ccindex"}, []string{"SELECT 1 FROM"}},
	}
	for _, c := range cases {
		t.Run(c.args[0], func(t *testing.T) {
			run(t, append(append([]string{"columnar"}, c.args...), "--print")...).
				wantCode(t, 0).wantOut(t, c.want...)
		})
	}
}

// -o table is the default a person sees at a terminal, and it goes through a
// renderer none of the machine formats touch.
func TestSearchRendersATable(t *testing.T) {
	r := run(t, "search", "example.com", "--match", "domain", "-o", "table").wantCode(t, 0)
	r.wantOut(t, "example.com", "20260101")
}

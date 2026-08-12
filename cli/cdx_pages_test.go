package cli

import (
	"strings"
	"testing"

	"github.com/tamnd/ccrawl-cli/internal/fakecc"
)

// runFaulty is run with a fake Common Crawl that has been told to misbehave
// first. The setup has to happen before the command starts, which is why it
// cannot use run: that one brings the server up and runs the command in the
// same call.
func runFaulty(t *testing.T, setup func(*fakecc.Server), args ...string) result {
	t.Helper()
	srv := fakecc.Start(t)
	setup(srv)
	full := append([]string{"ccrawl", "--rate", "1ns", "--global-rate", "0"}, args...)
	code, out, errOut := invoke(t, "", full)
	return result{Code: code, Out: out, Err: errOut, Server: srv}
}

// TestSearchKeepsGoingPastAPageTheIndexWillNotServe is #102. A wide query is
// thousands of pages and the index drops one often enough that ending the run
// on it threw away everything that had already arrived, sometimes after an
// hour.
func TestSearchKeepsGoingPastAPageTheIndexWillNotServe(t *testing.T) {
	r := runFaulty(t, func(s *fakecc.Server) { s.DeadPage = 1 },
		"search", "example.com", "--match", "domain", "-o", "url", "--retries", "1").wantCode(t, 0)

	got := r.Lines()
	if len(got) != fakecc.PageSize {
		t.Fatalf("search returned %d URLs, want the %d from the page that did arrive\n%s", len(got), fakecc.PageSize, r.Out)
	}
	if !strings.Contains(r.Err, "CDX page 1") || !strings.Contains(r.Err, "skipping the page") {
		t.Fatalf("stderr does not say which page was lost:\n%s", r.Err)
	}
	if !strings.Contains(r.Err, "incomplete") {
		t.Fatalf("stderr does not say the result is incomplete:\n%s", r.Err)
	}
}

// TestSearchStrictFailsOnAPageItCannotRead is the other half of the contract:
// a caller who would rather have nothing than a result with a hole in it says
// so and gets the old behaviour back.
func TestSearchStrictFailsOnAPageItCannotRead(t *testing.T) {
	r := runFaulty(t, func(s *fakecc.Server) { s.DeadPage = 1 },
		"search", "example.com", "--match", "domain", "-o", "url", "--retries", "1", "--strict")
	if r.Code == 0 {
		t.Fatalf("--strict exited 0 with a page missing\n%s%s", r.Out, r.Err)
	}
	if !strings.Contains(r.Err, "page 1") {
		t.Fatalf("the error does not name the page:\n%s", r.Err)
	}
}

// TestSearchRetriesAPageThatArrivesShort is the quiet half of the bug. The
// request succeeds, the status is 200, and the connection dies part way through
// the records, so the run finishes clean with fewer captures than the index
// holds. Two runs of the same query then disagree and neither looks wrong.
func TestSearchRetriesAPageThatArrivesShort(t *testing.T) {
	r := runFaulty(t, func(s *fakecc.Server) { s.TruncatePage, s.TruncateTimes = 0, 1 },
		"search", "example.com", "--match", "domain", "-o", "url").wantCode(t, 0)

	if got := len(r.Lines()); got != 3 {
		t.Fatalf("search returned %d URLs, want all 3: a page that came back short has to be read again\n%s", got, r.Out)
	}
	if strings.Contains(r.Err, "incomplete") {
		t.Fatalf("a page that succeeded on the retry was still reported as lost:\n%s", r.Err)
	}
}

// TestSearchSaysNothingWhenNothingWasLost keeps the report off a clean run,
// where a line about incomplete results would be worse than no line at all.
func TestSearchSaysNothingWhenNothingWasLost(t *testing.T) {
	r := run(t, "search", "example.com", "--match", "domain", "-o", "url").wantCode(t, 0)
	if strings.Contains(r.Err, "incomplete") {
		t.Fatalf("a clean run reported losses:\n%s", r.Err)
	}
}

// TestExportKeepsGoingPastALostPage covers the other command that pages the
// index. An export writes files as it goes, so ending the run on one page means
// the WARC files on disk are a subset nobody wrote down.
func TestExportKeepsGoingPastALostPage(t *testing.T) {
	dir := t.TempDir()
	r := runFaulty(t, func(s *fakecc.Server) { s.DeadPage = 1 },
		"export", "example.com", "--match", "domain", "--out-dir", dir, "--retries", "1").wantCode(t, 0)
	if !strings.Contains(r.Err, "incomplete") {
		t.Fatalf("export did not report the page it lost:\n%s", r.Err)
	}
}

// TestSearchDoesNotCallAnOutageAnEmptyResult guards the distinction exit 3 is
// for. A run that lost every crawl and emitted nothing has not found that the
// index holds no captures, it has failed to ask, and a pipeline branching on
// exit 3 would record the difference as fact.
func TestSearchDoesNotCallAnOutageAnEmptyResult(t *testing.T) {
	// other.test holds one capture, so the fixture serves it in one page and a
	// dead page 0 is the whole query.
	r := runFaulty(t, func(s *fakecc.Server) { s.DeadPage = 0 },
		"search", "other.test", "--match", "domain", "-o", "url", "--retries", "1")
	if r.Code == 3 {
		t.Fatalf("a query whose only page failed exited 3, which means no captures\n%s", r.Err)
	}
	if r.Code == 0 {
		t.Fatalf("a query that read nothing exited 0\n%s%s", r.Out, r.Err)
	}
	if !strings.Contains(r.Err, "not an empty result") {
		t.Fatalf("the error does not say why this is not exit 3:\n%s", r.Err)
	}
	// A 503 is the server answering, so this is exit 1 and not the exit 8 the
	// next test is about. The two look identical from the shell without this.
	if r.Code != 1 {
		t.Fatalf("a query the server refused exited %d, want 1\n%s", r.Code, r.Err)
	}
}

// TestSearchExitsEightWhenTheIndexNeverAnswered is the other half of that
// distinction. Exit 1 tells a supervisor the command is wrong and to stop; exit
// 8 tells it Common Crawl is unreachable and the same command is worth running
// in an hour. The index server refused connections for three days while this
// was written, so this is not a hypothetical.
func TestSearchExitsEightWhenTheIndexNeverAnswered(t *testing.T) {
	r := runFaulty(t, func(s *fakecc.Server) { s.HangupPage = 0 },
		"search", "other.test", "--match", "domain", "-o", "url", "--retries", "1")
	if r.Code != 8 {
		t.Fatalf("a query whose only page never answered exited %d, want 8\n%s%s", r.Code, r.Out, r.Err)
	}
}

// TestSearchExitsOneWhenOnlySomeOfTheLossWasTransport keeps exit 8 honest. A
// run that hit a hangup and a 503 is not a clean outage, and telling a
// supervisor to retry it forever against a server that is up and refusing is
// how a backoff loop becomes a hot loop.
func TestSearchExitsOneWhenOnlySomeOfTheLossWasTransport(t *testing.T) {
	r := runFaulty(t, func(s *fakecc.Server) { s.HangupPage, s.DeadPage = 0, 1 },
		"search", "example.com", "--match", "domain", "-o", "url", "--retries", "1")
	if r.Code != 1 {
		t.Fatalf("a run that lost one page to a hangup and one to a 503 exited %d, want 1\n%s%s", r.Code, r.Out, r.Err)
	}
}

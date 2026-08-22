package ccrawl

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestRecrawlFeederSaysWhereItsOwnTimeWent is the feeder half of the timing
// done-when. The pool's phase shares can say the pool was idle and they cannot
// say why, and the answer is one of two things: the feeder could not read rows
// fast enough, or it read them faster than the pool would take them. This is the
// second case, built on purpose with one worker and a slow site, so the feeder
// has to be found waiting to hand out rather than waiting to read.
func TestRecrawlFeederSaysWhereItsOwnTimeWent(t *testing.T) {
	site := newRecrawlSite()
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond)
		site.ServeHTTP(w, r)
	}))
	defer slow.Close()

	want := paths(12)
	parts := writeWorkList(t, slow.URL, want, 4)

	cfg := testRecrawlConfig(filepath.Join(t.TempDir(), "state.json"))
	cfg.OutDir = t.TempDir()
	cfg.Workers = 1
	r := newTestRecrawler(t, cfg, parts)
	defer func() { _ = r.Close() }()

	if _, err := r.Run(context.Background(), func(CrawlPage) error { return nil }); err != nil {
		t.Fatal(err)
	}

	tm := r.Timing()
	if tm.Rows != int64(len(want)) {
		t.Fatalf("the feeder counted %d rows handed out and the work list had %d", tm.Rows, len(want))
	}
	if tm.Read <= 0 {
		t.Fatal("the feeder read a work list off disk and reports no time spent reading it")
	}
	if tm.Hand <= tm.Read {
		t.Fatalf("one worker against a site that takes 20ms a page spent %s reading and %s waiting to hand out, so the feeder is not attributing the wait to the pool", tm.Read, tm.Hand)
	}
	if tm.Feed <= 0 || tm.Feed > tm.Wall {
		t.Fatalf("the feeder was alive for %s of a %s run, which is not a life it could have had", tm.Feed, tm.Wall)
	}
	if line := tm.FeedLine(); !strings.Contains(line, "rows a second") {
		t.Fatalf("the feeder line reads %q", line)
	}
}

// TestFeedLineIsAgainstTheFeedersOwnClock is the trap the first cut of this fell
// into. A bounded run stops feeding at the page limit and then waits for the
// pool to drain, and on the probe that found this the drain was 40 seconds of a
// 62 second run. Sharing the feeder's time against the run's wall clock there
// reported a feeder idle for two thirds of a life it had already finished.
func TestFeedLineIsAgainstTheFeedersOwnClock(t *testing.T) {
	tm := RecrawlTiming{
		Wall: 100 * time.Second,
		Feed: 20 * time.Second,
		Read: 5 * time.Second,
		Hand: 15 * time.Second,
		Rows: 400,
	}
	got := tm.FeedLine()
	want := "feeder: alive for 20% of the run, and in that time reading the work list 25%, waiting for a free worker 75%, 20.0 rows a second"
	if got != want {
		t.Fatalf("the feeder line reads\n%s\nand should read\n%s", got, want)
	}
}

// TestFeedLineWithNothingMeasuredSaysSo keeps the line honest on a run that
// never started, where dividing by the wall clock would be a divide by zero and
// every share would read as a real zero rather than as no data.
func TestFeedLineWithNothingMeasuredSaysSo(t *testing.T) {
	var tm RecrawlTiming
	if got := tm.FeedLine(); got != "feeder: nothing measured" {
		t.Fatalf("a run with no wall clock reports %q", got)
	}
}

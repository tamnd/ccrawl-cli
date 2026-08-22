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
	if line := tm.FeedLine(); !strings.Contains(line, "rows a second") {
		t.Fatalf("the feeder line reads %q", line)
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

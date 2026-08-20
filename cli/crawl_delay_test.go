package cli

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// crawlTarget stands up one HTML page per path on its own host:port, so a set of
// them looks to the crawler like a set of unrelated sites.
func crawlTarget(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("User-agent: *\nAllow: /\n"))
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte("<html><head><title>page</title></head><body>hello</body></html>"))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// seedFile writes a seed list and returns its path.
func seedFile(t *testing.T, urls []string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "seeds.txt")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range urls {
		if _, err := fmt.Fprintln(f, u); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestCrawlDelayZeroIsDifferentFromUnset is the regression test for --delay 0
// meaning one second.
//
// The flag was only copied into the config when it was positive, and the
// crawler put the default back when it was not, so no flag and an explicit zero
// were the same input and there was no way to ask for no delay. Both halves have
// to hold: unset still has to be polite, or the fix would have turned a polite
// default into a fast one.
//
// Three URLs on one host, so the per host delay is the only thing that can
// space them. Unset means two gaps of a second, and zero means none.
func TestCrawlDelayZeroIsDifferentFromUnset(t *testing.T) {
	srv := crawlTarget(t)
	seeds := seedFile(t, []string{
		srv.URL + "/a",
		srv.URL + "/b",
		srv.URL + "/c",
	})

	elapsed := func(args ...string) time.Duration {
		t.Helper()
		state := filepath.Join(t.TempDir(), "crawl.db")
		argv := append([]string{
			"ccrawl", "crawl", "run",
			"--seeds", seeds, "--state", state, "--no-robots", "-q",
		}, args...)
		start := time.Now()
		code, _, errOut := invoke(t, "", argv)
		took := time.Since(start)
		if code != 0 {
			t.Fatalf("crawl run %v exited %d: %s", args, code, errOut)
		}
		return took
	}

	// Two gaps of a second between three same-host fetches. The floor is well
	// under two seconds so a slow machine does not fail the test, but far enough
	// above zero that the delay has to have been applied.
	if took := elapsed(); took < 1500*time.Millisecond {
		t.Errorf("an unset --delay crawled three pages on one host in %v, so the polite default is not being applied", took)
	}
	// Nothing but the fetches themselves, which are loopback.
	if took := elapsed("--delay", "0"); took > time.Second {
		t.Errorf("--delay 0 took %v for three loopback pages, so the delay is still in force", took)
	}
}

// TestRobotsDoesNotPayTheCommonCrawlDelay is the regression test for a live
// crawl borrowing the Common Crawl transport.
//
// The crawler was handed App.HTTP, which spaces every request by --rate, 200ms
// by default, because that budget exists to be polite to data.commoncrawl.org.
// The page fetches go through CrawlURL and never saw it, but robots.txt did, and
// that delay is per process rather than per host: robots was fetched at five
// hosts a second however many workers were running. Twelve unrelated hosts took
// twelve times 200ms before a single page was counted, and the recrawl corpus
// has 121 million of them.
//
// Twelve hosts at a --rate of 250ms is three seconds of waiting if the bug is
// back, against loopback fetches that cost nothing.
func TestRobotsDoesNotPayTheCommonCrawlDelay(t *testing.T) {
	const hosts = 12

	urls := make([]string, 0, hosts)
	for range hosts {
		urls = append(urls, crawlTarget(t).URL+"/page")
	}
	seeds := seedFile(t, urls)
	state := filepath.Join(t.TempDir(), "crawl.db")

	start := time.Now()
	code, _, errOut := invoke(t, "", []string{
		"ccrawl", "--rate", "250ms", "crawl", "run",
		"--seeds", seeds, "--state", state, "--delay", "0", "-q",
	})
	took := time.Since(start)
	if code != 0 {
		t.Fatalf("crawl run exited %d: %s", code, errOut)
	}
	// Twelve hosts through a 250ms process-wide gate is three seconds. Half that
	// is comfortably above what twelve loopback robots fetches cost and well
	// below what the bug costs.
	if took > 1500*time.Millisecond {
		t.Errorf("crawling %d hosts took %v, so robots.txt is still going through the Common Crawl rate budget", hosts, took)
	}
}

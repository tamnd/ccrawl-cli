package cli

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// The content commands fetch the live web rather than Common Crawl, so these
// stand a couple of pages up on localhost and point the commands at those. The
// fake Common Crawl behind run() is not used by any of them, and that is the
// point: a content command that started talking to the index would fail here.

// contentPages serves the three pages the tests below score and link over.
func contentPages(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/article", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = fmt.Fprintf(w, `<html><head><title>A long enough article</title></head><body>
			<nav><a href="/">home</a></nav>
			<p>%s</p>
			<p>See <a href="https://example.org/a">one</a> and <a href="/relative">two</a>.</p>
			</body></html>`, strings.Repeat("word ", 200))
	})
	mux.HandleFunc("/parked", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = fmt.Fprint(w, `<html><head><title>This domain is for sale</title></head>
			<body><p>Buy this domain. Click here to make money, act now, cheap.</p></body></html>`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// deadURL is an address nothing is listening on, for the failure paths. The
// server is started so the port is real and then stopped so the connection is
// refused rather than left hanging on a firewall.
func deadURL(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL + "/gone"
	srv.Close()
	return url
}

func TestContentQualityOneURL(t *testing.T) {
	srv := contentPages(t)
	run(t, "content", "quality", srv.URL+"/article", "-o", "jsonl").
		wantCode(t, 0).
		wantOut(t, `"word_count":204`, `"has_main_content":true`, `"is_parked":false`, `"spam_score":0`)

	// The parked page trips three signals at once, which is the case the
	// command exists for.
	run(t, "content", "quality", srv.URL+"/parked", "-o", "jsonl").
		wantCode(t, 0).
		wantOut(t, `"is_parked":true`, `"has_main_content":false`).
		wantNotOut(t, `"spam_score":0,`)
}

// The stream form, which is what the guide promises and what the command did
// not have. A URL per line, and a JSONL line with a url field, which is what
// search and crawl fetch write.
func TestContentQualityStdin(t *testing.T) {
	srv := contentPages(t)
	stdin := srv.URL + "/article\n" +
		fmt.Sprintf(`{"url":%q,"status":200}`, srv.URL+"/parked") + "\n"

	r := runStdin(t, stdin, "content", "quality", "-", "-o", "jsonl").wantCode(t, 0)
	if len(r.Lines()) != 2 {
		t.Fatalf("want a row per URL, got %d:\n%s", len(r.Lines()), r.Out)
	}
	r.wantOut(t, "/article", "/parked", `"is_parked":true`)
}

// One dead URL in a list does not take the rest of the list with it. This is
// the whole reason the stream form does not simply return the first error.
func TestContentQualityStdinSkipsDeadURL(t *testing.T) {
	srv := contentPages(t)
	stdin := deadURL(t) + "\n" + srv.URL + "/article\n"

	r := runStdin(t, stdin, "content", "quality", "-", "-o", "jsonl").wantCode(t, 0)
	if len(r.Lines()) != 1 {
		t.Fatalf("want the one page that answered, got %d:\n%s", len(r.Lines()), r.Out)
	}
	if !strings.Contains(r.Err, "skipping it") || !strings.Contains(r.Err, "1 of the 2 URLs") {
		t.Errorf("the skipped URL should be named on stderr, got:\n%s", r.Err)
	}
}

// Nothing fetched is an error, not an empty result. A stream where every URL
// failed learned nothing about those pages, the same distinction exit 3 rests
// on for a search that could not read the index.
func TestContentQualityStdinAllDead(t *testing.T) {
	stdin := deadURL(t) + "\n" + deadURL(t) + "\n"
	runStdin(t, stdin, "content", "quality", "-", "-o", "jsonl").
		wantCode(t, 1)
}

// Empty stdin is an empty result, exit 3, which is what dropping Single from
// these ops buys. A pipeline whose filter matched nothing should not look like
// a success with no rows.
func TestContentQualityStdinEmpty(t *testing.T) {
	runStdin(t, "", "content", "quality", "-", "-o", "jsonl").wantCode(t, 3)
}

// A link with no source is not an edge. Over a stream the rows of every page
// arrive in one file, so each one has to say where it came from.
func TestContentOutlinksCarriesSource(t *testing.T) {
	srv := contentPages(t)
	r := runStdin(t, srv.URL+"/article\n", "content", "outlinks", "-", "-o", "jsonl").
		wantCode(t, 0).
		wantOut(t, `"source":"`+srv.URL+`/article"`, "https://example.org/a")
	// The relative href is resolved against the page it was found on.
	r.wantOut(t, srv.URL+"/relative")
}

func TestContentExtractAndLang(t *testing.T) {
	srv := contentPages(t)
	run(t, "content", "extract", srv.URL+"/article", "-o", "jsonl").
		wantCode(t, 0).
		wantOut(t, `"title":"A long enough article"`, `"word_count":204`)
	run(t, "content", "lang", srv.URL+"/article", "-o", "jsonl").
		wantCode(t, 0).
		wantOut(t, `"language":"`)
}

// A bad URL still ends a single-URL run, which is the one case where there is
// nothing else the command could be doing. A host that will not answer is a
// transport failure like any other and exits 8, so a supervisor can back off
// and try the same fetch later. A URL that does not parse is the command being
// wrong and stays at 1.
func TestContentQualityDeadURLAlone(t *testing.T) {
	run(t, "content", "quality", deadURL(t), "-o", "jsonl").wantCode(t, 8)
	run(t, "content", "quality", "http://[::1", "-o", "jsonl").wantCode(t, 1)
}

// A JSON line that is not a URL is a bad input rather than a page that failed,
// so it stops the run instead of being counted as a skip.
func TestContentStdinRejectsJSONWithoutURL(t *testing.T) {
	runStdin(t, `{"hello":"world"}`+"\n", "content", "quality", "-", "-o", "jsonl").
		wantCode(t, 1)
}

// The documented pipeline is 'search -o jsonl | content quality -', and what
// makes it work is that a CDX row carries a url field. That is a contract
// between two commands rather than a property of either, so it is checked
// against real search output rather than against a hand-written line.
func TestSearchOutputFeedsContentStdin(t *testing.T) {
	r := run(t, "search", "example.com/*", "-c", "1", "-o", "jsonl").wantCode(t, 0)
	lines := r.Lines()
	if len(lines) == 0 {
		t.Fatal("no rows to feed anything with")
	}
	for _, line := range lines {
		got, err := urlFromLine(line)
		if err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		if !strings.HasPrefix(got, "http") {
			t.Fatalf("a search row should hand back its URL, got %q from %s", got, line)
		}
	}
}

// "-" is the caller's stdin, and under the servers stdin is not the caller's.
func TestStdinBelongsToCaller(t *testing.T) {
	old := os.Args
	t.Cleanup(func() { os.Args = old })
	for _, tc := range []struct {
		argv []string
		want bool
	}{
		{[]string{"ccrawl", "content", "quality", "-"}, true},
		{[]string{"ccrawl", "mcp"}, false},
		{[]string{"ccrawl", "serve", "--addr", ":8080"}, false},
		{[]string{"ccrawl", "api"}, false},
	} {
		os.Args = tc.argv
		if got := stdinBelongsToCaller(); got != tc.want {
			t.Errorf("%v: got %v, want %v", tc.argv, got, tc.want)
		}
	}
}

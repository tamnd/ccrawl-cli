package cli

import (
	"strconv"
	"strings"
	"testing"
)

func TestURLKeep(t *testing.T) {
	cases := []struct {
		url, contains, not string
		want               bool
	}{
		{"https://example.com/blog/post", "", "", true},
		{"https://example.com/blog/post", "/blog/", "", true},
		{"https://example.com/about", "/blog/", "", false},
		{"https://example.com/robots.txt", "", "/robots.txt", false},
		{"https://example.com/blog/post", "/blog/", "/robots.txt", true},
		{"https://example.com/blog/robots.txt", "/blog/", "/robots.txt", false},
	}
	for _, c := range cases {
		if got := urlKeep(c.url, c.contains, c.not); got != c.want {
			t.Errorf("urlKeep(%q, %q, %q) = %v, want %v", c.url, c.contains, c.not, got, c.want)
		}
	}
}

// The two done-when conditions of pushing a filter to the index server, checked
// against a server that filters the way the real one does: the same rows come
// back, and fewer bytes are moved to get them.
func TestSearchPushesTheURLFilterAndReadsLessOfTheIndex(t *testing.T) {
	pushed := run(t, "search", "example.com", "--match", "domain",
		"--url-contains", "/about", "--explain", "-o", "url").wantCode(t, 0)
	local := run(t, "search", "example.com", "--match", "domain",
		"--url-contains", "/about", "--no-push-filters", "--explain", "-o", "url").wantCode(t, 0)

	if strings.Join(pushed.Lines(), "\n") != strings.Join(local.Lines(), "\n") {
		t.Fatalf("pushing the filter changed the results\nserver side:\n%s\nclient side:\n%s", pushed.Out, local.Out)
	}
	pushed.wantOut(t, "https://example.com/about")
	if len(pushed.Lines()) != 1 {
		t.Fatalf("got %d URLs, want the one with /about in it\n%s", len(pushed.Lines()), pushed.Out)
	}

	pushedBytes, localBytes := indexBytes(t, pushed.Err), indexBytes(t, local.Err)
	if pushedBytes >= localBytes {
		t.Fatalf("pushing the filter read %d index bytes, client side read %d, want fewer", pushedBytes, localBytes)
	}
}

// A filter the server never saw is a filter that saved nothing, and the flag
// that turns the push off is the escape hatch for a server that disagrees with
// us, so it has to actually keep the filter off the wire.
func TestSearchExplainSaysWhereEachFilterRuns(t *testing.T) {
	r := run(t, "search", "example.com", "--match", "domain",
		"--url-contains", "/about", "--dedup", "--explain", "-o", "url").wantCode(t, 0)
	for _, want := range []string{
		"search: 1 crawl: CC-MAIN-2026-30",
		"the index server answers",
		"filter=url%3A.%2A%2Fabout.%2A",
		"pushed to the server: --url-contains /about",
		"applied here: --url-contains /about (again, on what the server sent), --dedup",
	} {
		if !strings.Contains(r.Err, want) {
			t.Errorf("--explain did not say %q\n%s", want, r.Err)
		}
	}

	off := run(t, "search", "example.com", "--match", "domain",
		"--url-contains", "/about", "--no-push-filters", "--explain", "-o", "url").wantCode(t, 0)
	if strings.Contains(off.Err, "filter=url") {
		t.Errorf("--no-push-filters still sent the filter to the server\n%s", off.Err)
	}
	if !strings.Contains(off.Err, "pushed to the server: nothing beyond the URL pattern") {
		t.Errorf("--no-push-filters did not say the server got nothing\n%s", off.Err)
	}
}

// indexBytes reads the exact count out of the --explain line, which is there so
// two runs of the same query can be compared.
func indexBytes(t *testing.T, stderr string) int {
	t.Helper()
	for _, line := range strings.Split(stderr, "\n") {
		_, rest, ok := strings.Cut(line, "from the index (")
		if !ok {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimSpace(rest), " bytes)"))
		if err != nil {
			t.Fatalf("cannot read the byte count out of %q: %v", line, err)
		}
		return n
	}
	t.Fatalf("no index byte count in:\n%s", stderr)
	return 0
}

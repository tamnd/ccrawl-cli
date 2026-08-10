package ccrawl

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestRobotsAgainstRealSites replays a corpus of robots.txt files served by
// real sites and checks every verdict against a second implementation of the
// same spec.
//
// The hand written tests prove the parser matches RFC 9309 as we read it, which
// is worth something and is not the same as being right. The web is full of
// files with typos, duplicate groups, rules before any user agent line, and
// patterns nobody would think to invent, and the way to find out what we do
// with those is to run them.
//
// The corpus is built by scripts/robots-corpus.py, which fetches the files and
// records what protego, the parser Scrapy uses, concludes about each one:
//
//	CCRAWL_ROBOTS_CORPUS=/tmp/robots-corpus go test ./ccrawl -run RealSites -v
//
// It is skipped by default because it depends on files this repository does not
// own and on a Python package to regenerate.
func TestRobotsAgainstRealSites(t *testing.T) {
	dir := os.Getenv("CCRAWL_ROBOTS_CORPUS")
	if dir == "" {
		t.Skip("set CCRAWL_ROBOTS_CORPUS to a directory built by scripts/robots-corpus.py")
	}
	var corpus struct {
		UserAgent string `json:"user_agent"`
		Sites     map[string]struct {
			Allowed    map[string]bool `json:"allowed"`
			Skipped    []string        `json:"skipped"`
			CrawlDelay float64         `json:"crawl_delay"`
			Sitemaps   []string        `json:"sitemaps"`
		} `json:"sites"`
	}
	raw, err := os.ReadFile(filepath.Join(dir, "verdicts.json"))
	if err != nil {
		t.Fatalf("read verdicts: %v", err)
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatalf("parse verdicts: %v", err)
	}

	var probes, skipped, disagreements int
	for site, want := range corpus.Sites {
		body, err := os.ReadFile(filepath.Join(dir, site+".txt"))
		if err != nil {
			t.Fatalf("read %s: %v", site, err)
		}
		e := parseRobots(strings.NewReader(string(body)), corpus.UserAgent)
		skipped += len(want.Skipped)

		// Sorted, because a map iterates differently every run and a failure
		// should name the same first path every time.
		paths := make([]string, 0, len(want.Allowed))
		for p := range want.Allowed {
			paths = append(paths, p)
		}
		sort.Strings(paths)
		for _, p := range paths {
			probes++
			if got := e.IsAllowed(p); got != want.Allowed[p] {
				disagreements++
				t.Errorf("%s: IsAllowed(%q) = %v, protego says %v", site, p, got, want.Allowed[p])
			}
		}
		if wantDelay := time.Duration(want.CrawlDelay * float64(time.Second)); e.CrawlDelay != wantDelay {
			t.Errorf("%s: CrawlDelay = %s, protego says %s", site, e.CrawlDelay, wantDelay)
		}
		if got := len(e.Sitemaps); got != len(want.Sitemaps) {
			t.Errorf("%s: found %d sitemaps, protego found %d", site, got, len(want.Sitemaps))
		}
	}
	if probes == 0 {
		t.Fatal("the corpus is empty, rebuild it with scripts/robots-corpus.py")
	}
	t.Logf("%d sites, %d probes, %d disagreements, %d probes protego cannot answer",
		len(corpus.Sites), probes, disagreements, skipped)
}

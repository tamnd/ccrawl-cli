package cli

import (
	"context"
	"testing"
)

// TestRankTablePicksTheTable covers the two branches that settle without asking
// the network: a URL given outright, and a release named with --graph. The
// third branch, the newest release, is a live lookup and is checked by running
// the commands against the real web graph.
//
// What this is really guarding is the host and domain split. They are different
// tables with different numbers in them, and reading a host rank out of the
// domain table is the kind of wrong answer that looks right.
func TestRankTablePicksTheTable(t *testing.T) {
	const graph = "cc-main-2026-mar-apr-may"
	const base = "https://data.commoncrawl.org/projects/hyperlinkgraph/" + graph + "/"

	cases := []struct {
		name   string
		table  string
		graph  string
		domain bool
		want   string
	}{
		{"a table given outright wins", "https://example.com/ranks.txt.gz", graph, false, "https://example.com/ranks.txt.gz"},
		{"a table wins for a domain too", "https://example.com/ranks.txt.gz", graph, true, "https://example.com/ranks.txt.gz"},
		{"a named release, host ranks", "", graph, false, base + "host/" + graph + "-host-ranks.txt.gz"},
		{"a named release, domain ranks", "", graph, true, base + "domain/" + graph + "-domain-ranks.txt.gz"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := rankTable(context.Background(), &App{}, c.table, c.graph, c.domain)
			if err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Errorf("rankTable = %q, want %q", got, c.want)
			}
		})
	}
}

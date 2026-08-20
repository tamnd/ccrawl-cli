package ccrawl

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRecrawlLedgerPathGivesEachWriterItsOwnFile(t *testing.T) {
	cases := []struct {
		server string
		shard  int
		shards int
		want   string
	}{
		{"server1", 0, 3, "ledger/server1-shard0of3.csv"},
		{"Server-2", 1, 3, "ledger/server-2-shard1of3.csv"},
		{"box a/b", 2, 3, "ledger/box-a-b-shard2of3.csv"},
		{"  ", 0, 1, "ledger/server-shard0of1.csv"},
	}
	for _, tc := range cases {
		if got := RecrawlLedgerPath(tc.server, tc.shard, tc.shards); got != tc.want {
			t.Errorf("RecrawlLedgerPath(%q, %d, %d) = %q, want %q", tc.server, tc.shard, tc.shards, got, tc.want)
		}
	}
	// Two machines in the same fleet must never land on the same path, or the
	// whole one-writer-per-file argument falls apart.
	a := RecrawlLedgerPath("server1", 0, 3)
	b := RecrawlLedgerPath("server1", 1, 3)
	if a == b {
		t.Fatalf("the same server on two slices shares the path %s", a)
	}
}

func TestRecrawlStatsRoundTrip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "ledger.csv")
	in := []RecrawlStat{{
		Server: "server2", Shard: 1, Shards: 3,
		Files: 12, Rows: 480_000, Bytes: 9_100_000_000,
		Part: 7, Row: 1_200_000, Done: true,
		FirstCommitted: "2026-08-01T00:00:00Z", LastCommitted: "2026-08-19T12:00:00Z",
	}}
	if err := WriteRecrawlStats(p, in); err != nil {
		t.Fatal(err)
	}
	out, err := ReadRecrawlStats(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0] != in[0] {
		t.Fatalf("read back %+v, want %+v", out, in)
	}
}

func TestReadRecrawlStatsOnAMissingFileIsAnEmptyLedger(t *testing.T) {
	rows, err := ReadRecrawlStats(filepath.Join(t.TempDir(), "nothing.csv"))
	if err != nil {
		t.Fatalf("a missing ledger should read as empty, got %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("read %d rows out of nothing", len(rows))
	}
}

func TestMergeRecrawlStatsKeepsTheNewestRowPerWriter(t *testing.T) {
	old := []RecrawlStat{
		{Server: "server1", Shard: 0, Files: 2, Rows: 100, LastCommitted: "2026-08-01T00:00:00Z"},
		{Server: "server2", Shard: 1, Files: 5, Rows: 900, LastCommitted: "2026-08-02T00:00:00Z"},
	}
	fresh := []RecrawlStat{
		{Server: "server1", Shard: 0, Files: 9, Rows: 4000, LastCommitted: "2026-08-19T00:00:00Z"},
		{Server: "server3", Shard: 2, Files: 1, Rows: 10, LastCommitted: "2026-08-19T00:00:00Z"},
	}
	got := MergeRecrawlStats(old, fresh)
	if len(got) != 3 {
		t.Fatalf("merged to %d rows, want one per server and shard", len(got))
	}
	if got[0].Server != "server1" || got[0].Rows != 4000 {
		t.Errorf("server1 merged to %+v, want the newer row of 4000 rows", got[0])
	}
	if got[1].Server != "server2" || got[1].Rows != 900 {
		t.Errorf("server2 was lost or rewritten: %+v", got[1])
	}
	if got[2].Server != "server3" {
		t.Errorf("server3 is missing from the merge: %+v", got)
	}
	// The order has to be stable, or every commit rewrites the card and the
	// ledger as a reshuffle and the diffs become unreadable.
	if got[0].Shard > got[1].Shard || got[1].Shard > got[2].Shard {
		t.Errorf("merged rows are not in shard order: %+v", got)
	}
}

func TestTotalRecrawlStatsCountsTheFleetNotTheRows(t *testing.T) {
	rows := []RecrawlStat{
		{Server: "server1", Shard: 0, Shards: 3, Files: 4, Rows: 100, Bytes: 10, Done: true},
		{Server: "server2", Shard: 1, Shards: 3, Files: 6, Rows: 250, Bytes: 20},
		{Server: "server3", Shard: 2, Shards: 3, Files: 1, Rows: 50, Bytes: 5},
	}
	got := TotalRecrawlStats(rows)
	want := RecrawlTotals{Servers: 3, Shards: 3, Slices: 3, Files: 11, Rows: 400, Bytes: 35, Done: 1}
	if got != want {
		t.Fatalf("totals = %+v, want %+v", got, want)
	}
	if empty := TotalRecrawlStats(nil); empty != (RecrawlTotals{}) {
		t.Fatalf("an empty ledger totalled to %+v", empty)
	}
}

func TestGenerateRecrawlREADMEStatesOnlyWhatIsPublished(t *testing.T) {
	rows := []RecrawlStat{
		{Server: "server1", Shard: 0, Shards: 3, Files: 4, Rows: 120_000, Bytes: 3_000_000_000, Part: 2, Row: 400_000},
		{Server: "server2", Shard: 1, Shards: 3, Files: 6, Rows: 250_000, Bytes: 5_000_000_000, Part: 3, Row: 800_000, Done: true},
	}
	card := GenerateRecrawlREADME("open-index/ccrawl-recrawl-domains", "domains", rows)
	for _, want := range []string{
		"370,000",                           // the two row counts, and nothing invented
		"8.0 GB",                            // the two byte counts
		"10 shards",                         // the two file counts
		"2 of 3",                            // slices covered out of the fleet width
		"part 2 row 400,000",                // position rather than a percentage
		"open-index/ccrawl-recrawl-domains", // the repo it is a card for
		"Common Crawl Domain Recrawl",       // the domain flavour of the prose
	} {
		if !strings.Contains(card, want) {
			t.Errorf("the card does not mention %q", want)
		}
	}
	if strings.Contains(card, "{{") {
		t.Error("the card has an unrendered template action in it")
	}
	if strings.Contains(card, "URL Recrawl") {
		t.Error("the domains card carries the URL prose")
	}

	// A repo with nothing in it yet must not claim any coverage at all. A card
	// that promises the whole work list while the repo holds four shards is
	// worse than a card with no numbers on it.
	empty := GenerateRecrawlREADME("open-index/ccrawl-recrawl-urls", "urls", nil)
	if !strings.Contains(empty, "The first shards are publishing now") {
		t.Error("the empty card does not say the dataset is still filling up")
	}
	if !strings.Contains(empty, "Common Crawl URL Recrawl") {
		t.Error("the urls card does not carry the URL prose")
	}
	if strings.Contains(empty, "| **Total**") {
		t.Error("the empty card printed a totals row for a repo with nothing in it")
	}
}

func TestGenerateRecrawlREADMESaysWhenTheFleetIsFinished(t *testing.T) {
	rows := []RecrawlStat{
		{Server: "server1", Shard: 0, Shards: 2, Files: 4, Rows: 10, Done: true},
		{Server: "server2", Shard: 1, Shards: 2, Files: 4, Rows: 10, Done: true},
	}
	card := GenerateRecrawlREADME("open-index/ccrawl-recrawl-domains", "domains", rows)
	if !strings.Contains(card, "Every slice of the work list has been walked out") {
		t.Error("a finished fleet is not reported as complete")
	}
	if strings.Contains(card, "still fetching") {
		t.Error("a finished fleet is still described as running")
	}
}

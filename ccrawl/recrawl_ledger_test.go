package ccrawl

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
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

// TestRecrawlCardExplainsTheOutcomes is the difference between a card that
// describes a schema and one that describes a dataset.
//
// A recrawl is harder to read honestly than an index. A reader who assumes a
// missing row means a dead page, or that a 404 means a page is gone, or that
// fetched_at is anywhere near the crawl the work list came from, will draw a
// wrong conclusion from the whole thing and will do it confidently. Those are
// the four the card has to answer out loud, so they are pinned here rather than
// left to survive an edit by luck.
func TestRecrawlCardExplainsTheOutcomes(t *testing.T) {
	for _, kind := range []string{"domains", "urls"} {
		card := GenerateRecrawlREADME("open-index/ccrawl-recrawl-"+kind, kind, []RecrawlStat{
			{Server: "server1", Shard: 0, Shards: 3, Files: 2, Rows: 10, Bytes: 100},
		})
		for _, want := range []string{
			// A failed fetch is a row, so a count of rows is not a count of pages.
			"count of rows",
			"status = 200 AND error = ''",
			// The fetch date is not the crawl date, and the gap is months.
			"fetched_at",
			"has nothing to do with when Common Crawl saw the page",
			// What a 404 is and is not evidence of.
			"A 404 means the server answered",
			// What a 304 is, and how to get the body it refers to.
			"unchanged",
			"304 Not Modified",
			"look up the same `url` in an earlier pass",
			// Robots is off on these two runs, and a card may not leave that to
			// be inferred from silence.
			"is not consulted on these two recrawls",
			// The error column is a class and not the raw message, which is the
			// difference between a GROUP BY that answers and one that returns a
			// row per URL.
			"`meta_json` under `error_detail`",
		} {
			if !strings.Contains(card, want) {
				t.Errorf("the %s card does not explain %q", kind, want)
			}
		}

		// The card claimed for a long time that the run asks robots.txt once per
		// host, which stopped being true when it was turned off by default. A
		// card that describes a policy the code does not follow is worse than
		// one that says nothing.
		if strings.Contains(card, "honours `robots.txt`") {
			t.Errorf("the %s card still claims the run honours robots.txt", kind)
		}
	}
}

// TestRecrawlCardNamesEveryErrorClass ties the vocabulary printed on the card to
// the one the crawler can actually write.
//
// The card tells a reader that error holds one of six words and that grouping on
// it is therefore meaningful. A seventh class added to the crawler would show up
// in the data with nothing on the card to say what it is, and two classes that
// rendered to the same word would quietly merge two different failures into one
// number. Neither would fail any other test here.
func TestRecrawlCardNamesEveryErrorClass(t *testing.T) {
	card := GenerateRecrawlREADME("open-index/ccrawl-recrawl-domains", "domains", nil)
	seen := map[string]bool{}
	for c := errClassOther; c <= errClassTLS; c++ {
		w := c.String()
		if seen[w] {
			t.Fatalf("two error classes both call themselves %q, so the column cannot be grouped on", w)
		}
		seen[w] = true
		if !strings.Contains(card, "`"+w+"`") {
			t.Errorf("the crawler writes error %q and the card never names it", w)
		}
	}
}

// TestRecrawlCardDocumentsEveryColumn checks the card against the schema rather
// than against a list somebody kept up to date by hand. A column added to
// Capture and not to the card is a column a reader meets in the data with no
// explanation, and nothing else in the build would notice.
func TestRecrawlCardDocumentsEveryColumn(t *testing.T) {
	documented := map[string]string{}
	for _, c := range captureColumnDocs {
		if c[0] == "" || c[3] == "" {
			t.Fatalf("a column doc row is incomplete: %v", c)
		}
		switch c[2] {
		case "served", "computed", "measured", "asked":
		default:
			t.Errorf("column %s says it came from %q, which is not one of the four the card explains", c[0], c[2])
		}
		if documented[c[0]] != "" {
			t.Errorf("column %s is documented twice", c[0])
		}
		documented[c[0]] = c[2]
	}

	for _, name := range captureSchemaColumns(t) {
		if documented[name] == "" {
			t.Errorf("the schema has a column %s that the card does not document", name)
		}
		delete(documented, name)
	}
	for name := range documented {
		t.Errorf("the card documents a column %s that the schema does not have", name)
	}
}

// captureSchemaColumns reads the column names straight out of the Parquet
// schema, which is the same source the writer uses, so the check above cannot
// be satisfied by a stale copy of the field list.
func captureSchemaColumns(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, path := range parquet.SchemaOf(Capture{}).Columns() {
		out = append(out, strings.Join(path, "."))
	}
	if len(out) == 0 {
		t.Fatal("the capture schema has no columns, so this test is checking nothing")
	}
	return out
}

// TestRecrawlCardIsCleanlyRendered is the check that stops two habits from
// having to be habits.
//
// A `<no value>` is a template field that was renamed on one side and not the
// other, and it renders as that string rather than as an error, so it publishes
// to the hub and sits on the front page of the dataset. Em dashes are a house
// style thing and every card here is written without them, which is a rule that
// only holds if something enforces it.
func TestRecrawlCardIsCleanlyRendered(t *testing.T) {
	cases := []struct {
		name string
		rows []RecrawlStat
	}{
		{"empty", nil},
		{"one machine", []RecrawlStat{{Server: "server1", Shard: 0, Shards: 3, Files: 2, Rows: 10, Bytes: 100}}},
		{"a finished fleet", []RecrawlStat{
			{Server: "server1", Shard: 0, Shards: 2, Files: 2, Rows: 10, Bytes: 100, Done: true},
			{Server: "server2", Shard: 1, Shards: 2, Files: 2, Rows: 10, Bytes: 100, Done: true},
		}},
	}
	for _, tc := range cases {
		for _, kind := range []string{"domains", "urls"} {
			card := GenerateRecrawlREADME("open-index/ccrawl-recrawl-"+kind, kind, tc.rows)
			if strings.Contains(card, "<no value>") {
				t.Errorf("%s, %s: the card has a <no value> in it, so a template field lost its data", tc.name, kind)
			}
			if strings.Contains(card, "—") || strings.Contains(card, "–") {
				t.Errorf("%s, %s: the card has a dash that is not a hyphen in it", tc.name, kind)
			}
			if strings.Contains(card, "{{") || strings.Contains(card, "}}") {
				t.Errorf("%s, %s: the card has an unrendered template action in it", tc.name, kind)
			}
		}
	}
}

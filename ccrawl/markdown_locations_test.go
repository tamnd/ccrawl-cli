package ccrawl

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// htmlWARCFile builds a WARC file of HTML response records and returns the bytes
// plus a location per record, with filler records in between so a coalesced range
// has something to read over and a per-record range has something to skip.
func htmlWARCFile(t *testing.T, name string, bodies []string) ([]byte, []Location) {
	t.Helper()
	var buf bytes.Buffer
	var locs []Location
	for i, body := range bodies {
		off := int64(buf.Len())
		url := fmt.Sprintf("https://pages.example/%d", i)
		m := warcMember(t, url, body)
		buf.Write(m)
		locs = append(locs, Location{Filename: name, Offset: off, Length: int64(len(m)), URL: url})
		buf.Write(warcMember(t, "https://filler.example/", "<html><body>"+strings.Repeat("f", 4096)+"</body></html>"))
	}
	return buf.Bytes(), locs
}

// TestPackLocationsConvertsOnlyWhatWasAskedFor is the recovery pass in miniature:
// an index query picked six of the twelve records in a file, and the pack has to
// come back with those six and nothing else, having never read the file whole.
func TestPackLocationsConvertsOnlyWhatWasAskedFor(t *testing.T) {
	bodies := make([]string, 6)
	for i := range bodies {
		bodies[i] = articlePage(fmt.Sprintf("Page number %d is about harbours and the boats moored there.", i))
	}
	data, locs := htmlWARCFile(t, "part.warc.gz", bodies)
	h, base, reqs := serveWARCs(t, map[string][]byte{"part.warc.gz": data})
	locs = rebase(locs, base)

	out := filepath.Join(t.TempDir(), "part.parquet")
	stats, err := PackMarkdownShard(context.Background(), h, MarkdownPackConfig{
		CrawlID:   "CC-MAIN-TEST",
		Locations: locs,
		OutPath:   out,
		Workers:   2,
	})
	if err != nil {
		t.Fatalf("PackMarkdownShard: %v", err)
	}

	rows, err := parquet.ReadFile[MarkdownRow](out)
	if err != nil {
		t.Fatalf("read parquet: %v", err)
	}
	if len(rows) != len(bodies) {
		t.Fatalf("wrote %d rows, want %d", len(rows), len(bodies))
	}
	if stats.Rows != int64(len(rows)) {
		t.Fatalf("stats say %d rows, parquet holds %d", stats.Rows, len(rows))
	}
	// The filler records sit between the ones asked for, so their presence in the
	// output would mean the pack read past the ranges it was given.
	for _, r := range rows {
		if strings.Contains(r.URL, "filler") {
			t.Fatalf("converted a record nobody asked for: %s", r.URL)
		}
		if r.Simhash == 0 {
			t.Fatalf("no fingerprint on %s", r.URL)
		}
	}
	if *reqs == 0 {
		t.Fatal("no requests reached the server")
	}
	// Six adjacent records with filler between them coalesce, so this must cost
	// fewer requests than one per record. That is the entire reason the location
	// path exists rather than a loop over ccrawl fetch.
	if int(*reqs) >= len(locs) {
		t.Fatalf("%d requests for %d locations, coalescing did nothing", *reqs, len(locs))
	}
	if stats.WARCBytes <= 0 || stats.WARCBytes >= int64(len(data)) {
		t.Fatalf("warc bytes %d, want more than zero and less than the whole %d byte file", stats.WARCBytes, len(data))
	}
}

// TestPackLocationsAppliesLangAndDedup checks the location source gets the same
// treatment a shard does. Anything else would make a recovery pass a different
// corpus from an export of the same pages.
func TestPackLocationsAppliesLangAndDedup(t *testing.T) {
	same := articlePage("Common Crawl publishes a web archive every single month of the year.")
	bodies := []string{
		same,
		articlePage("Une page entierement differente au sujet du pain et de la farine."),
		same,
		articlePage("A third page about mountains and rivers and the valleys between them."),
	}
	data, locs := htmlWARCFile(t, "part.warc.gz", bodies)
	h, base, _ := serveWARCs(t, map[string][]byte{"part.warc.gz": data})
	locs = rebase(locs, base)

	run := func(name string, cfg MarkdownPackConfig) ([]MarkdownRow, MarkdownStats) {
		t.Helper()
		cfg.CrawlID = "CC-MAIN-TEST"
		cfg.Locations = locs
		cfg.OutPath = filepath.Join(t.TempDir(), name)
		cfg.Workers = 2
		stats, err := PackMarkdownShard(context.Background(), h, cfg)
		if err != nil {
			t.Fatalf("PackMarkdownShard(%s): %v", name, err)
		}
		rows, rerr := parquet.ReadFile[MarkdownRow](cfg.OutPath)
		if rerr != nil {
			t.Fatalf("read %s: %v", name, rerr)
		}
		return rows, stats
	}

	plain, _ := run("plain.parquet", MarkdownPackConfig{})
	if len(plain) != 4 {
		t.Fatalf("plain run wrote %d rows, want 4", len(plain))
	}

	deduped, stats := run("dedup.parquet", MarkdownPackConfig{DedupDigest: true})
	if len(deduped) != 3 {
		t.Fatalf("dedup run wrote %d rows, want 3", len(deduped))
	}
	if stats.DigestDropped != 1 {
		t.Fatalf("digest dropped %d, want 1", stats.DigestDropped)
	}

	eng, _ := run("eng.parquet", MarkdownPackConfig{Lang: "eng"})
	if len(eng) == 0 || len(eng) >= 4 {
		t.Fatalf("--lang eng kept %d of 4 rows, want some but not all", len(eng))
	}
	for _, r := range eng {
		if r.Language != "eng" {
			t.Fatalf("--lang eng kept a %s row: %s", r.Language, r.URL)
		}
	}
}

// TestPackLocationsSurvivesABadLocation checks one unfetchable location does not
// take the part down with it. A recovery pass runs against an index that can
// disagree with the archive, and a run that dies on the first disagreement never
// finishes.
func TestPackLocationsSurvivesABadLocation(t *testing.T) {
	bodies := []string{
		articlePage("A page about harbours and the boats moored along the quay."),
		articlePage("A page about bakeries and the bread they sell each morning."),
	}
	data, locs := htmlWARCFile(t, "part.warc.gz", bodies)
	h, base, _ := serveWARCs(t, map[string][]byte{"part.warc.gz": data})
	locs = rebase(locs, base)
	locs = append(locs, Location{Filename: base + "missing.warc.gz", Offset: 0, Length: 100, URL: "https://gone.example/"})

	out := filepath.Join(t.TempDir(), "part.parquet")
	stats, err := PackMarkdownShard(context.Background(), h, MarkdownPackConfig{
		CrawlID:   "CC-MAIN-TEST",
		Locations: locs,
		OutPath:   out,
		Workers:   2,
	})
	if err != nil {
		t.Fatalf("one missing location killed the part: %v", err)
	}
	if stats.Rows != 2 {
		t.Fatalf("wrote %d rows, want the 2 that were there", stats.Rows)
	}
}

package ccrawl

import (
	"bufio"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

// TestCDXSpillAgainstRealIndexRecords replays a real CDX response through the
// maps this replaces and through the picker and the URL log, and fails if they
// disagree. The synthetic fixture is regular by construction and a real
// response is not: urlkeys shared by several URLs, capture counts all over the
// place, and crawls that skip a URL entirely.
//
//	ccrawl search 'vi.wikipedia.org/wiki/*' -c CC-MAIN-2024-10,CC-MAIN-2023-14 -o jsonl > /tmp/cdx.jsonl
//	CCRAWL_CDX_JSONL=/tmp/cdx.jsonl go test ./ccrawl -run RealIndexRecords -v
//
// It is off by default because it needs a file the repository does not carry.
func TestCDXSpillAgainstRealIndexRecords(t *testing.T) {
	path := os.Getenv("CCRAWL_CDX_JSONL")
	if path == "" {
		t.Skip("set CCRAWL_CDX_JSONL to a file of real CDX records to run this")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = f.Close() }()

	// Records arrive one crawl at a time, so a change of crawl id starts a new
	// stream, the same way the search command reads them.
	var streams [][]CDXRecord
	crawl := ""
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var r CDXRecord
		if err := json.Unmarshal(line, &r); err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		if r.CrawlID != crawl || len(streams) == 0 {
			streams, crawl = append(streams, nil), r.CrawlID
		}
		streams[len(streams)-1] = append(streams[len(streams)-1], r)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	total := 0
	for _, s := range streams {
		total += len(s)
	}
	if total == 0 {
		t.Fatalf("%s has no records", path)
	}
	t.Logf("%d records over %d crawls", total, len(streams))

	// The index is only sorted inside a crawl, so a stream that is not sorted
	// is a bad input file rather than a failure of the code under test.
	for i, s := range streams {
		for j := 1; j < len(s); j++ {
			if s[j].URLKey < s[j-1].URLKey {
				t.Fatalf("crawl %d is not sorted by urlkey at record %d, the file is not a CDX response", i, j)
			}
		}
	}

	const target = "20210601000000"
	// A budget under a thousandth of the result forces the disk paths.
	const maxBuffer = 1000

	want := nearestBuffered(streams, target)
	got, p := pickNearest(streams, target, maxBuffer, t)
	defer func() { _ = p.Close() }()
	if !p.Spilled() {
		t.Errorf("a %d record budget over %d records should have spilled", maxBuffer, total)
	}
	if len(got) != len(want) {
		t.Fatalf("--at picked %d URLs, the map picked %d", len(got), len(want))
	}
	if !reflect.DeepEqual(byURL(got), byURL(want)) {
		t.Error("--at picked a different capture than the map for at least one URL")
	}
	t.Logf("--at agrees with the map on %d URLs", len(want))

	wantLatest := latestBuffered(streams)
	log := NewCDXURLLog(maxBuffer)
	defer func() { _ = log.Close() }()
	var gotLatest []CDXRecord
	for i, recs := range streams {
		for _, r := range recs {
			seen, err := log.Seen(r)
			if err != nil {
				t.Fatalf("Seen: %v", err)
			}
			if seen {
				continue
			}
			gotLatest = append(gotLatest, r)
			if err := log.Emitted(r); err != nil {
				t.Fatalf("Emitted: %v", err)
			}
		}
		if i < len(streams)-1 {
			if err := log.EndCrawl(); err != nil {
				t.Fatalf("EndCrawl: %v", err)
			}
		}
	}
	if !log.Spilled() {
		t.Errorf("a %d entry budget over %d records should have spilled", maxBuffer, total)
	}
	if !reflect.DeepEqual(gotLatest, wantLatest) {
		t.Fatalf("--latest-only kept %d records, the map kept %d, in the same order",
			len(gotLatest), len(wantLatest))
	}
	t.Logf("--latest-only agrees with the map on %d records", len(wantLatest))
}

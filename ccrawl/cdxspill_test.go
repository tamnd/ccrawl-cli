package ccrawl

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

// cdxFixture builds a set of streams shaped like the index returns them: sorted
// by urlkey, several captures per URL, and the same URLs turning up in more than
// one crawl. Two URLs share a urlkey in every group so the group reduction is
// exercised rather than a one slot shortcut.
func cdxFixture(streams, urls, capturesPerURL int, seed int64) [][]CDXRecord {
	rng := rand.New(rand.NewSource(seed))
	out := make([][]CDXRecord, streams)
	for s := 0; s < streams; s++ {
		var recs []CDXRecord
		for u := 0; u < urls; u++ {
			key := fmt.Sprintf("com,example)/p/%06d", u)
			for _, scheme := range []string{"http", "https"} {
				full := scheme + "://example.com/p/" + strconv.Itoa(u)
				for c := 0; c < capturesPerURL; c++ {
					// A crawl that is not visited every capture keeps the
					// fixture from being a regular grid.
					if rng.Intn(10) == 0 {
						continue
					}
					recs = append(recs, CDXRecord{
						CrawlID:   fmt.Sprintf("CC-MAIN-20%02d-01", 20+s),
						URLKey:    key,
						URL:       full,
						Timestamp: fmt.Sprintf("20%02d%02d01000000", 20+s, 1+c),
						Digest:    fmt.Sprintf("D%04d%02d", u%37, c),
						Status:    "200",
					})
				}
			}
		}
		out[s] = recs
	}
	return out
}

// tsDiff is the distance between two CDX timestamps, the same comparison the
// search command's --at makes.
func tsDiff(a, b string) int64 {
	ai, bi := tsInt(a), tsInt(b)
	if ai > bi {
		return ai - bi
	}
	return bi - ai
}

func tsInt(s string) int64 {
	var n int64
	for _, r := range s {
		if r >= '0' && r <= '9' {
			n = n*10 + int64(r-'0')
		}
	}
	return n
}

// nearestBuffered is the implementation this replaces: one map over every
// stream, then a sort. It is the oracle the picker is checked against.
func nearestBuffered(streams [][]CDXRecord, target string) []CDXRecord {
	nearest := map[string]CDXRecord{}
	var order []string
	for _, recs := range streams {
		for _, r := range recs {
			cur, ok := nearest[r.URL]
			if !ok {
				nearest[r.URL] = r
				order = append(order, r.URL)
				continue
			}
			if tsDiff(r.Timestamp, target) < tsDiff(cur.Timestamp, target) {
				nearest[r.URL] = r
			}
		}
	}
	out := make([]CDXRecord, 0, len(nearest))
	for _, u := range order {
		out = append(out, nearest[u])
	}
	return out
}

func pickNearest(streams [][]CDXRecord, target string, maxBuffer int, t *testing.T) ([]CDXRecord, *CDXPicker) {
	t.Helper()
	p := NewCDXPicker(func(cand, cur CDXRecord) bool {
		return tsDiff(cand.Timestamp, target) < tsDiff(cur.Timestamp, target)
	}, maxBuffer)
	for _, recs := range streams {
		for _, r := range recs {
			if err := p.Add(r); err != nil {
				t.Fatalf("Add: %v", err)
			}
		}
		if err := p.EndStream(); err != nil {
			t.Fatalf("EndStream: %v", err)
		}
	}
	var got []CDXRecord
	if err := p.Each(func(r CDXRecord) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("Each: %v", err)
	}
	return got, p
}

// byURL indexes records so two results can be compared without depending on
// which order they came out in.
func byURL(recs []CDXRecord) map[string]CDXRecord {
	m := make(map[string]CDXRecord, len(recs))
	for _, r := range recs {
		m[r.URL] = r
	}
	return m
}

func TestCDXPickerMatchesTheBufferedImplementation(t *testing.T) {
	streams := cdxFixture(4, 50, 3, 7)
	const target = "20220101000000"
	want := nearestBuffered(streams, target)

	for _, maxBuffer := range []int{0, 1, 7, 100000} {
		got, p := pickNearest(streams, target, maxBuffer, t)
		if len(got) != len(want) {
			t.Fatalf("--max-buffer %d: picked %d URLs, want %d", maxBuffer, len(got), len(want))
		}
		if !reflect.DeepEqual(byURL(got), byURL(want)) {
			t.Errorf("--max-buffer %d: picked a different capture than the buffered version", maxBuffer)
		}
		if err := p.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}

func TestCDXPickerEmitsInIndexOrder(t *testing.T) {
	streams := cdxFixture(3, 20, 2, 11)
	got, p := pickNearest(streams, "20210101000000", 4, t)
	defer func() { _ = p.Close() }()

	prev := ""
	for _, r := range got {
		if r.URLKey < prev {
			t.Fatalf("urlkey %q came after %q, the merge is not sorted", r.URLKey, prev)
		}
		prev = r.URLKey
	}
	seen := map[string]bool{}
	for _, r := range got {
		if seen[r.URL] {
			t.Fatalf("%s came out twice", r.URL)
		}
		seen[r.URL] = true
	}
}

func TestCDXPickerKeepsTheFirstStreamOnATie(t *testing.T) {
	// Both captures sit the same distance from the target, so the stream that
	// was read first has to win, the way one pass over both would resolve it.
	early := CDXRecord{URLKey: "com,example)/", URL: "http://example.com/", Timestamp: "20200101000000"}
	late := CDXRecord{URLKey: "com,example)/", URL: "http://example.com/", Timestamp: "20200103000000"}
	streams := [][]CDXRecord{{early}, {late}}

	got, p := pickNearest(streams, "20200102000000", 1, t)
	defer func() { _ = p.Close() }()
	if len(got) != 1 {
		t.Fatalf("picked %d records, want 1", len(got))
	}
	if got[0].Timestamp != early.Timestamp {
		t.Errorf("picked %s on a tie, want the first stream's %s", got[0].Timestamp, early.Timestamp)
	}
}

func TestCDXPickerSpillsPastTheBudgetAndCleansUp(t *testing.T) {
	streams := cdxFixture(2, 40, 2, 13)
	got, p := pickNearest(streams, "20200101000000", 3, t)
	if !p.Spilled() {
		t.Fatal("a 3 record budget over 40 URLs should have spilled to disk")
	}
	if len(got) == 0 {
		t.Fatal("spilling lost the whole result")
	}
	dir := p.dir
	if dir == "" {
		t.Fatal("no temp dir was created")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("temp dir missing while the picker is open: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		left, _ := filepath.Glob(filepath.Join(dir, "*"))
		t.Fatalf("Close left %s behind with %v", dir, left)
	}
}

func TestCDXPickerHandlesEmptyStreams(t *testing.T) {
	p := NewCDXPicker(func(cand, cur CDXRecord) bool { return false }, 0)
	defer func() { _ = p.Close() }()
	for i := 0; i < 3; i++ {
		if err := p.EndStream(); err != nil {
			t.Fatalf("EndStream on an empty stream: %v", err)
		}
	}
	n := 0
	if err := p.Each(func(CDXRecord) error { n++; return nil }); err != nil {
		t.Fatalf("Each: %v", err)
	}
	if n != 0 {
		t.Fatalf("an empty picker emitted %d records", n)
	}
}

// latestBuffered is the map based --latest-only this replaces.
func latestBuffered(streams [][]CDXRecord) []CDXRecord {
	seen := map[string]bool{}
	var out []CDXRecord
	for _, recs := range streams {
		for _, r := range recs {
			if seen[r.URL] {
				continue
			}
			seen[r.URL] = true
			out = append(out, r)
		}
	}
	return out
}

func TestCDXURLLogMatchesTheBufferedImplementation(t *testing.T) {
	streams := cdxFixture(4, 60, 3, 17)
	want := latestBuffered(streams)

	for _, maxBuffer := range []int{0, 1, 5, 100000} {
		log := NewCDXURLLog(maxBuffer)
		var got []CDXRecord
		for i, recs := range streams {
			for _, r := range recs {
				seen, err := log.Seen(r)
				if err != nil {
					t.Fatalf("Seen: %v", err)
				}
				if seen {
					continue
				}
				got = append(got, r)
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
		if !reflect.DeepEqual(got, want) {
			t.Errorf("--max-buffer %d: kept %d records, want %d, in the same order",
				maxBuffer, len(got), len(want))
		}
		if maxBuffer == 1 && !log.Spilled() {
			t.Error("a one entry budget over 60 URLs should have spilled to disk")
		}
		if err := log.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}

func TestCDXURLLogRemovesItsTempFiles(t *testing.T) {
	streams := cdxFixture(3, 30, 2, 19)
	log := NewCDXURLLog(2)
	for i, recs := range streams {
		for _, r := range recs {
			seen, err := log.Seen(r)
			if err != nil {
				t.Fatalf("Seen: %v", err)
			}
			if seen {
				continue
			}
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
	dir := log.dir
	if dir == "" {
		t.Fatal("a two entry budget should have spilled")
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("Close left %s behind", dir)
	}
}

func TestCDXDigestSetIsExactUnderItsCeiling(t *testing.T) {
	s := NewCDXDigestSet(1000)
	digests := make([]string, 500)
	for i := range digests {
		digests[i] = fmt.Sprintf("SHA1DIGEST%06d", i)
	}
	for _, d := range digests {
		if s.Add(d) {
			t.Fatalf("%s reported as a duplicate on first sight", d)
		}
	}
	for _, d := range digests {
		if !s.Add(d) {
			t.Fatalf("%s was not remembered", d)
		}
	}
	if s.Evicted() {
		t.Error("500 digests should not have pushed a 1000 entry set past its ceiling")
	}
}

func TestCDXDigestSetForgetsRatherThanDropsUniques(t *testing.T) {
	// The failure that matters is a unique record reported as a duplicate, so
	// the check is that nothing brand new ever comes back true, however far
	// past the ceiling the set is pushed.
	s := NewCDXDigestSet(64)
	for i := 0; i < 10000; i++ {
		if s.Add(fmt.Sprintf("UNIQUE%08d", i)) {
			t.Fatalf("digest %d was new and the set called it a duplicate", i)
		}
	}
	if !s.Evicted() {
		t.Error("10000 digests through a 64 entry set should have reported eviction")
	}
}

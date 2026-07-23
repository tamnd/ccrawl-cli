package ccrawl

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// TestStreamDomainColumnAt writes a shard with the full six-column DomainRow
// schema and reads it back projecting only the domain column, in order. This is
// the read the diff relies on: a one-column projection over a wider shard.
func TestStreamDomainColumnAt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "part-000.parquet")
	w, err := NewParquetWriter[DomainRow](path)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"example.com", "github.com", "golang.org"}
	for i, d := range want {
		if err := w.Write(DomainRow{
			Domain:      d,
			HarmonicPos: int64(i),
			HarmonicVal: float64(i) * 1.5,
			PagerankPos: int64(i),
			PagerankVal: float64(i) * 2.5,
			NHosts:      int64(i + 1),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	var got []string
	if err := streamDomainColumnAt(f, fi.Size(), func(d string) error {
		got = append(got, d)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("read %d domains %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("domain[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestDomainDiffCounts exercises the diff algorithm end to end over two in-memory
// domain sets: hash the older set, sort it, scan the newer set against it, and
// derive added, removed, and shared the same way DiffDomainReleases does.
func TestDomainDiffCounts(t *testing.T) {
	from := []string{"a.com", "b.com", "c.com", "d.com"}
	to := []string{"c.com", "d.com", "e.com", "f.com"}

	keys := make([]uint64, 0, len(from))
	for _, d := range from {
		keys = append(keys, hashDomain(d))
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	var added []string
	var shared int64
	for _, d := range to {
		if keyInSet(keys, hashDomain(d)) {
			shared++
			continue
		}
		added = append(added, d)
	}
	fromTotal := int64(len(from))
	toTotal := int64(len(to))
	diff := DomainDiff{
		From:      "old",
		To:        "new",
		FromTotal: fromTotal,
		ToTotal:   toTotal,
		Added:     toTotal - shared,
		Removed:   fromTotal - shared,
		Shared:    shared,
	}

	if diff.Shared != 2 {
		t.Errorf("shared = %d, want 2", diff.Shared)
	}
	if diff.Added != 2 {
		t.Errorf("added = %d, want 2", diff.Added)
	}
	if diff.Removed != 2 {
		t.Errorf("removed = %d, want 2", diff.Removed)
	}
	if len(added) != 2 || added[0] != "e.com" || added[1] != "f.com" {
		t.Errorf("added domains = %v, want [e.com f.com]", added)
	}
}

// TestKeyInSet checks membership at the ends and middle of a sorted key set and a
// value that is absent.
func TestKeyInSet(t *testing.T) {
	keys := []uint64{1, 5, 9, 20, 100}
	for _, k := range keys {
		if !keyInSet(keys, k) {
			t.Errorf("keyInSet miss for present %d", k)
		}
	}
	for _, k := range []uint64{0, 2, 50, 101} {
		if keyInSet(keys, k) {
			t.Errorf("keyInSet hit for absent %d", k)
		}
	}
}

// TestTwoNewestDomainReleases picks the two newest complete releases, older then
// newer, ignoring incomplete rows and page order.
func TestTwoNewestDomainReleases(t *testing.T) {
	ledger := []DomainGraphStat{
		{Graph: "cc-main-2026-jan-feb-mar", Shards: 24, Complete: true},
		{Graph: "cc-main-2026-apr-may-jun", Shards: 25, Complete: true},
		{Graph: "cc-main-2026-mar-apr-may", Shards: 25, Complete: true},
		{Graph: "cc-main-2026-may-jun-jul", Shards: 3, Complete: false}, // partial, ignored
	}
	from, to, err := TwoNewestDomainReleases(ledger)
	if err != nil {
		t.Fatal(err)
	}
	if to != "cc-main-2026-apr-may-jun" {
		t.Errorf("to = %q, want cc-main-2026-apr-may-jun", to)
	}
	if from != "cc-main-2026-mar-apr-may" {
		t.Errorf("from = %q, want cc-main-2026-mar-apr-may", from)
	}

	// Fewer than two complete releases is an error, not a silent pick.
	_, _, err = TwoNewestDomainReleases([]DomainGraphStat{
		{Graph: "cc-main-2026-apr-may-jun", Shards: 25, Complete: true},
	})
	if err == nil {
		t.Error("want an error with only one complete release")
	}
}

func TestCommaInt(t *testing.T) {
	cases := map[int64]string{
		0:         "0",
		999:       "999",
		1000:      "1,000",
		121091933: "121,091,933",
		-4200:     "-4,200",
	}
	for n, want := range cases {
		if got := commaInt(n); got != want {
			t.Errorf("commaInt(%d) = %q, want %q", n, got, want)
		}
	}
}

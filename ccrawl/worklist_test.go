package ccrawl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// writeDomainPart writes a part file with one domain column, the shape the
// published domain ranks have.
func writeDomainPart(t *testing.T, path string, domains []string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create part: %v", err)
	}
	defer func() { _ = f.Close() }()
	w := parquet.NewGenericWriter[domainWork](f, parquet.MaxRowsPerRowGroup(64), parquet.PageBufferSize(1<<10))
	rows := make([]domainWork, len(domains))
	for i, d := range domains {
		rows[i] = domainWork{Domain: d}
	}
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
}

// writeURLPart writes a part file with one url column, the shape the published
// URL index has.
func writeURLPart(t *testing.T, path string, urls []string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create part: %v", err)
	}
	defer func() { _ = f.Close() }()
	w := parquet.NewGenericWriter[urlWork](f, parquet.MaxRowsPerRowGroup(64), parquet.PageBufferSize(1<<10))
	rows := make([]urlWork, len(urls))
	for i, u := range urls {
		rows[i] = urlWork{URL: u}
	}
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
}

// localParts points a work list at files on disk instead of a published
// dataset, so the streaming can be tested without a network.
func localParts(w *WorkList, dir string) {
	w.open = func(ctx context.Context, p int) (io.ReaderAt, int64, error) {
		f, err := os.Open(filepath.Join(dir, fmt.Sprintf("part-%03d.parquet", p)))
		if os.IsNotExist(err) {
			return nil, 0, errPartNotPublished
		}
		if err != nil {
			return nil, 0, err
		}
		st, err := f.Stat()
		if err != nil {
			return nil, 0, err
		}
		return f, st.Size(), nil
	}
}

func domainSource() WorkSource {
	return WorkSource{Repo: "open-index/ccrawl-domains", Dir: "data/test", Column: "domain"}
}

// drain reads a whole work list through a small buffer and returns the URLs in
// order.
func drainWork(t *testing.T, w *WorkList, bufSize int) []string {
	t.Helper()
	var got []string
	buf := make([]WorkItem, bufSize)
	for {
		n, err := w.Next(context.Background(), buf)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		for _, it := range buf[:n] {
			got = append(got, it.URL)
		}
		if n == 0 {
			return got
		}
	}
}

func TestWorkListStreamsEveryPartInOrder(t *testing.T) {
	dir := t.TempDir()
	writeDomainPart(t, filepath.Join(dir, "part-000.parquet"), []string{"a.com", "b.com", "c.com"})
	writeDomainPart(t, filepath.Join(dir, "part-001.parquet"), []string{"d.com", "e.com"})

	w, err := NewWorkList(domainSource(), Shard{Count: 1}, nil, Checkpoint{})
	if err != nil {
		t.Fatal(err)
	}
	localParts(w, dir)
	defer w.Close()

	got := drainWork(t, w, 2)
	want := []string{"https://a.com/", "https://b.com/", "https://c.com/", "https://d.com/", "https://e.com/"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("got %v, want %v", got, want)
	}
	if _, _, done := w.Position(); !done {
		t.Fatal("the work list read to its end and did not say so")
	}
}

func TestWorkListTreatsAMissingPartAsTheEnd(t *testing.T) {
	dir := t.TempDir()
	writeDomainPart(t, filepath.Join(dir, "part-000.parquet"), []string{"a.com"})
	// part-001 is absent, and part-002 exists to prove the walk stops at the
	// first gap rather than sweeping for whatever else is lying around.
	writeDomainPart(t, filepath.Join(dir, "part-002.parquet"), []string{"z.com"})

	w, err := NewWorkList(domainSource(), Shard{Count: 1}, nil, Checkpoint{})
	if err != nil {
		t.Fatal(err)
	}
	localParts(w, dir)
	defer w.Close()

	got := drainWork(t, w, 8)
	if len(got) != 1 || got[0] != "https://a.com/" {
		t.Fatalf("got %v, want just a.com", got)
	}
}

func TestWorkListKeepsURLsAsTheyAre(t *testing.T) {
	dir := t.TempDir()
	writeURLPart(t, filepath.Join(dir, "part-000.parquet"), []string{
		"https://a.com/page?q=1",
		"http://b.com/deep/path",
	})
	src := WorkSource{Repo: "open-index/ccrawl-urls", Dir: "data/test", Column: "url"}
	w, err := NewWorkList(src, Shard{Count: 1}, nil, Checkpoint{})
	if err != nil {
		t.Fatal(err)
	}
	localParts(w, dir)
	defer w.Close()

	got := drainWork(t, w, 8)
	want := []string{"https://a.com/page?q=1", "http://b.com/deep/path"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// TestWorkListResumesWithoutSkippingOrRepeating is the property the whole
// checkpoint exists for. A run stopped at every possible offset and resumed from
// its checkpoint has to produce exactly what an uninterrupted run produces.
func TestWorkListResumesWithoutSkippingOrRepeating(t *testing.T) {
	dir := t.TempDir()
	writeDomainPart(t, filepath.Join(dir, "part-000.parquet"), []string{"a.com", "b.com", "c.com", "d.com"})
	writeDomainPart(t, filepath.Join(dir, "part-001.parquet"), []string{"e.com", "f.com", "g.com"})

	open := func(at Checkpoint) *WorkList {
		w, err := NewWorkList(domainSource(), Shard{Count: 1}, nil, at)
		if err != nil {
			t.Fatal(err)
		}
		localParts(w, dir)
		return w
	}

	full := open(Checkpoint{})
	want := drainWork(t, full, 3)
	full.Close()
	if len(want) != 7 {
		t.Fatalf("the uninterrupted run read %d rows, want 7", len(want))
	}

	// Stop after every prefix length, save the checkpoint the recrawler would
	// have saved, and resume from it.
	for stop := 0; stop <= len(want); stop++ {
		var got []string
		w := open(Checkpoint{})
		buf := make([]WorkItem, 1)
		for len(got) < stop {
			n, err := w.Next(context.Background(), buf)
			if err != nil || n == 0 {
				t.Fatalf("stop %d: Next returned %d, %v", stop, n, err)
			}
			got = append(got, buf[0].URL)
		}
		part, row, done := w.Position()
		w.Close()

		at := Checkpoint{Source: domainSource().Key(), Shards: 1, Part: part, Row: row, Done: done}
		b, err := json.Marshal(at)
		if err != nil {
			t.Fatal(err)
		}
		var back Checkpoint
		if err := json.Unmarshal(b, &back); err != nil {
			t.Fatal(err)
		}

		w2 := open(back)
		got = append(got, drainWork(t, w2, 2)...)
		w2.Close()

		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("stopping after %d rows and resuming gave %v, want %v", stop, got, want)
		}
	}
}

func TestWorkListResumeCountsRowsBeforeTheShardFilter(t *testing.T) {
	dir := t.TempDir()
	// Enough domains that every shard of 3 gets some, so the row offset and the
	// number of items handed out are different numbers.
	var domains []string
	for i := range 60 {
		domains = append(domains, fmt.Sprintf("site%02d.example", i))
	}
	writeDomainPart(t, filepath.Join(dir, "part-000.parquet"), domains)

	shard := Shard{Index: 1, Count: 3}
	open := func(at Checkpoint) *WorkList {
		w, err := NewWorkList(domainSource(), shard, nil, at)
		if err != nil {
			t.Fatal(err)
		}
		localParts(w, dir)
		return w
	}

	full := open(Checkpoint{})
	want := drainWork(t, full, 4)
	full.Close()
	if len(want) == 0 || len(want) == len(domains) {
		t.Fatalf("shard 1 of 3 took %d of %d domains, which is not a partition", len(want), len(domains))
	}

	w := open(Checkpoint{})
	buf := make([]WorkItem, 3)
	n, err := w.Next(context.Background(), buf)
	if err != nil || n != 3 {
		t.Fatalf("Next returned %d, %v", n, err)
	}
	got := []string{buf[0].URL, buf[1].URL, buf[2].URL}
	part, row, done := w.Position()
	w.Close()
	if row <= 3 {
		t.Fatalf("row offset is %d after handing out 3 of a 3 way shard, so it is counting items and not rows", row)
	}

	w2 := open(Checkpoint{Source: domainSource().Key(), Shard: shard.Index, Shards: shard.Count, Part: part, Row: row, Done: done})
	got = append(got, drainWork(t, w2, 4)...)
	w2.Close()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("resumed shard read %v, want %v", got, want)
	}
}

func TestWorkListRefusesACheckpointFromAnotherSource(t *testing.T) {
	at := Checkpoint{Source: "open-index/ccrawl-urls/data/CC-MAIN-2026-25#url", Shards: 1, Part: 4, Row: 900}
	_, err := NewWorkList(domainSource(), Shard{Count: 1}, nil, at)
	if err == nil {
		t.Fatal("a checkpoint for another dataset was accepted, and its row offset means nothing here")
	}
	if !strings.Contains(err.Error(), "points at nothing") {
		t.Fatalf("unhelpful error: %v", err)
	}
}

func TestWorkListRefusesACheckpointFromAnotherShard(t *testing.T) {
	at := Checkpoint{Source: domainSource().Key(), Shard: 0, Shards: 3, Part: 1, Row: 10}
	_, err := NewWorkList(domainSource(), Shard{Index: 1, Count: 3}, nil, at)
	if err == nil {
		t.Fatal("shard 1 accepted shard 0's checkpoint, which would skip everything shard 0 had not reached")
	}
	if !strings.Contains(err.Error(), "skip work") {
		t.Fatalf("unhelpful error: %v", err)
	}
}

// TestWorkListMemoryIsFlatAcrossAPart is the bounded memory done-when in
// miniature. A work list holds one part and one fixed buffer, so reading a
// hundred thousand rows must not cost measurably more than reading a hundred.
func TestWorkListMemoryIsFlatAcrossAPart(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a hundred thousand row part")
	}
	dir := t.TempDir()
	const rows = 100000
	domains := make([]string, rows)
	for i := range domains {
		domains[i] = fmt.Sprintf("site%07d.example", i)
	}
	writeDomainPart(t, filepath.Join(dir, "part-000.parquet"), domains)

	w, err := NewWorkList(domainSource(), Shard{Count: 1}, nil, Checkpoint{})
	if err != nil {
		t.Fatal(err)
	}
	localParts(w, dir)
	defer w.Close()

	buf := make([]WorkItem, 1000)
	read := func(items int) int {
		got := 0
		for got < items {
			n, err := w.Next(context.Background(), buf)
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			if n == 0 {
				break
			}
			got += n
		}
		return got
	}

	// Warm up first, so the comparison is steady state against steady state and
	// not against the buffers the first read allocates.
	read(5000)
	var early, late runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&early)
	if n := read(80000); n < 80000 {
		t.Fatalf("read %d rows, want 80000", n)
	}
	runtime.GC()
	runtime.ReadMemStats(&late)

	grew := int64(late.HeapAlloc) - int64(early.HeapAlloc)
	if grew > 4<<20 {
		t.Fatalf("the heap grew %d bytes over 80000 rows, so the work list is accumulating rather than streaming", grew)
	}
}

func TestCheckpointSaveIsAtomicAndSmall(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")

	got, err := LoadCheckpoint(path)
	if err != nil {
		t.Fatalf("a missing checkpoint should read as a zero one: %v", err)
	}
	if got != (Checkpoint{}) {
		t.Fatalf("a missing checkpoint read as %+v", got)
	}

	// A tiny work list and an enormous one have to leave the same state behind,
	// or the state is the frontier again under another name.
	small := Checkpoint{Source: domainSource().Key(), Shards: 1, Part: 0, Row: 12, Fetched: 12}
	huge := Checkpoint{Source: domainSource().Key(), Shards: 1, Part: 299, Row: 4999999, Fetched: 6300000000}
	for _, c := range []Checkpoint{small, huge} {
		if err := c.Save(path); err != nil {
			t.Fatalf("save: %v", err)
		}
		back, err := LoadCheckpoint(path)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if back != c {
			t.Fatalf("saved %+v and read back %+v", c, back)
		}
		st, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if st.Size() > 512 {
			t.Fatalf("the checkpoint is %d bytes, and it is supposed to be bounded", st.Size())
		}
	}

	// The temporary file the atomic write goes through must not be left behind,
	// or a hundred day run litters its state directory.
	ents, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 1 {
		t.Fatalf("the state directory holds %d files, want just the checkpoint", len(ents))
	}
}

func TestCheckpointRejectsGarbage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte("this is not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCheckpoint(path); err == nil {
		t.Fatal("a corrupt checkpoint was read as a position, and the run would resume somewhere arbitrary")
	}
}

func TestWorkSourcePartURL(t *testing.T) {
	src := WorkSource{Repo: "open-index/ccrawl-urls", Dir: "data/CC-MAIN-2026-25", Column: "url"}
	got := src.PartURL(7)
	want := "https://huggingface.co/datasets/open-index/ccrawl-urls/resolve/main/data/CC-MAIN-2026-25/part-007.parquet"
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
	bare := WorkSource{Repo: "open-index/ccrawl-domains", Column: "domain"}
	if got := bare.PartURL(0); !strings.HasSuffix(got, "/resolve/main/part-000.parquet") {
		t.Fatalf("a source with no directory built %s", got)
	}
}

func TestWorkSourceValidate(t *testing.T) {
	cases := []struct {
		name string
		src  WorkSource
		ok   bool
	}{
		{"domains", WorkSource{Repo: "open-index/ccrawl-domains", Column: "domain"}, true},
		{"urls", WorkSource{Repo: "open-index/ccrawl-urls", Column: "url"}, true},
		{"no repo", WorkSource{Column: "url"}, false},
		{"unknown column", WorkSource{Repo: "open-index/ccrawl-urls", Column: "link"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.src.Validate()
			if c.ok && err != nil {
				t.Fatalf("valid source rejected: %v", err)
			}
			if !c.ok && err == nil {
				t.Fatal("invalid source accepted")
			}
		})
	}
}

func TestSortDatasetDirsPutsTheNewestFirst(t *testing.T) {
	// The three directories the domain ranks actually publish, in the order the
	// repo API happens to hand them over.
	dirs := []string{
		"data/cc-main-2026-feb-mar-apr",
		"data/cc-main-2026-apr-may-jun",
		"data/cc-main-2026-mar-apr-may",
	}
	sortDatasetDirs(dirs)
	if dirs[0] != "data/cc-main-2026-apr-may-jun" {
		t.Fatalf("newest release is %s, want data/cc-main-2026-apr-may-jun", dirs[0])
	}

	crawls := []string{"data/CC-MAIN-2026-05", "data/CC-MAIN-2026-25", "data/CC-MAIN-2025-51"}
	sortDatasetDirs(crawls)
	if crawls[0] != "data/CC-MAIN-2026-25" {
		t.Fatalf("newest crawl is %s, want data/CC-MAIN-2026-25", crawls[0])
	}
}

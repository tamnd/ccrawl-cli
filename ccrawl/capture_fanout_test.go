package ccrawl

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestCaptureFanoutKeepsEveryRow writes through all the parts at once and reads
// the whole directory back.
//
// The thing worth checking is not that a Parquet file round trips, which
// capture_test already covers. It is that spreading rows over several encoders
// loses none of them and duplicates none of them, and that two writers never
// pick the same file name, since that is the failure a fanout invites and it is
// silent: the second writer would truncate the first one's shard and the run
// would report every row written.
func TestCaptureFanoutKeepsEveryRow(t *testing.T) {
	dir := t.TempDir()
	sink, err := NewCaptureFanout(FormatParquet, dir, "captures", 1<<20, WARCInfo{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	rows, ok := sink.(captureRowSink)
	if !ok {
		t.Fatal("a parquet fanout does not take built rows, so a run through it would drop the text columns")
	}

	const n = 400
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < n; i += 4 {
				c := NewCapture(fakeResult())
				c.URL = fmt.Sprintf("https://example.com/%d", i)
				c.Markdown = "hello"
				if err := rows.Write(c); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}

	files := sink.Files()
	if len(files) != 4 {
		t.Fatalf("a fanout of 4 wrote %d files: %v", len(files), files)
	}
	seen := map[string]bool{}
	for _, f := range files {
		if seen[f] {
			t.Fatalf("two parts wrote the same file %s, so one of them overwrote the other", f)
		}
		seen[f] = true
	}

	got := map[string]int{}
	on, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range on {
		read, err := parquet.ReadFile[Capture](f)
		if err != nil {
			t.Fatalf("reading %s: %v", f, err)
		}
		for _, c := range read {
			got[c.URL]++
			if c.Markdown != "hello" {
				t.Fatalf("%s came back without the column the caller filled", c.URL)
			}
		}
	}
	if len(got) != n {
		t.Fatalf("wrote %d rows and read back %d distinct", n, len(got))
	}
	for u, c := range got {
		if c != 1 {
			t.Fatalf("%s was written %d times", u, c)
		}
	}
}

// TestCaptureFanoutRotatesTogether is about the checkpoint rather than the
// files.
//
// A checkpoint may only move when everything behind it is readable, and a
// Parquet shard is readable only once its footer is written. If the parts
// rotated on their own, a sync would have to catch all of them empty at the same
// moment to report durable, which at any real rate never happens, and the
// checkpoint would sit where the run started for the length of the run. So one
// part filling seals the lot.
func TestCaptureFanoutRotatesTogether(t *testing.T) {
	dir := t.TempDir()
	// A target small enough that a handful of rows fills it.
	sink, err := NewCaptureFanout(FormatParquet, dir, "captures", 1<<10, WARCInfo{}, 3)
	if err != nil {
		t.Fatal(err)
	}
	rows := sink.(captureRowSink)

	// Enough rows that one part is over the target and the others hold rows too.
	for i := range 24 {
		c := NewCapture(fakeResult())
		c.URL = fmt.Sprintf("https://example.com/%d", i)
		if err := rows.Write(c); err != nil {
			t.Fatal(err)
		}
	}
	durable, err := sink.Sync(false)
	if err != nil {
		t.Fatal(err)
	}
	if !durable {
		t.Fatal("no part sealed, so the checkpoint could not move and the run would replay from the start")
	}

	sealed, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) != 3 {
		t.Fatalf("one part filled and %d shards were sealed, want all 3: %v", len(sealed), sealed)
	}
	total := 0
	for _, f := range sealed {
		read, err := parquet.ReadFile[Capture](f)
		if err != nil {
			t.Fatalf("reading %s: %v", f, err)
		}
		total += len(read)
	}
	if total != 24 {
		t.Fatalf("sealed shards hold %d rows of the 24 written", total)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestCaptureFanoutOfOneIsAPlainSink checks the default costs nothing. A run
// that did not ask for this should get exactly the writer it got before, not a
// wrapper around one.
func TestCaptureFanoutOfOneIsAPlainSink(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []int{0, 1} {
		sink, err := NewCaptureFanout(FormatParquet, filepath.Join(dir, fmt.Sprint(n)), "captures", 1<<20, WARCInfo{}, n)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := sink.(*CaptureWriter); !ok {
			t.Fatalf("--writers %d wrapped the sink in %T", n, sink)
		}
		if err := sink.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestCaptureFanoutWARCDoesNotTakeRows keeps the assertion honest. Whether a
// sink takes a built row is also what turns extraction on, so a WARC fanout
// answering yes would have every worker render Markdown into a format with
// nowhere to put it.
func TestCaptureFanoutWARCDoesNotTakeRows(t *testing.T) {
	dir := t.TempDir()
	sink, err := NewCaptureFanout(FormatWARC, dir, "captures", 1<<20, WARCInfo{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := sink.(captureRowSink); ok {
		t.Fatal("a warc fanout claims to take built rows")
	}
	if err := sink.WriteCapture(fakeResult()); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteCapture(fakeResult()); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	files := sink.Files()
	sort.Strings(files)
	if len(files) != 2 {
		t.Fatalf("a warc fanout of 2 wrote %v", files)
	}
	for _, f := range files {
		if !strings.HasSuffix(f, ".warc.gz") {
			t.Fatalf("a warc fanout wrote %s", f)
		}
	}
}

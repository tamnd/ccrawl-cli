package ccrawl

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	pqzstd "github.com/parquet-go/parquet-go/compress/zstd"
)

// fileStore stands in for the hub. It serves the shards out of a directory over
// a server that honors range requests, so the checks run through the same
// httpReaderAt the real command uses.
type fileStore struct {
	dir  string
	base string
	// deny holds the paths the server refuses to serve, which is how a private
	// repo behaves against the unauthenticated reads verify makes.
	deny map[string]bool
}

func newFileStore(t *testing.T) fileStore {
	t.Helper()
	dir := t.TempDir()
	s := fileStore{dir: dir, deny: map[string]bool{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.deny[strings.TrimPrefix(r.URL.Path, "/")] {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		http.ServeFile(w, r, filepath.Join(dir, filepath.Clean(r.URL.Path)))
	}))
	t.Cleanup(srv.Close)
	s.base = srv.URL
	return s
}

func (s fileStore) Sizes(_ context.Context, paths []string) (map[string]int64, error) {
	out := map[string]int64{}
	for _, p := range paths {
		fi, err := os.Stat(filepath.Join(s.dir, p))
		if err != nil {
			continue
		}
		out[p] = fi.Size()
	}
	return out, nil
}

func (s fileStore) URL(path string) string { return s.base + "/" + path }

// writeShard writes a shard of n URL rows into the store, compressed the way the
// publish path compresses them so a corrupted page fails to decode.
func writeShard(t *testing.T, s fileStore, path string, n int) string {
	t.Helper()
	full := filepath.Join(s.dir, path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(full)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[URLRow](f,
		parquet.Compression(&pqzstd.Codec{Level: pqzstd.SpeedFastest}),
		parquet.MaxRowsPerRowGroup(64), parquet.PageBufferSize(1<<12))
	rows := make([]URLRow, n)
	for i := range rows {
		host := fmt.Sprintf("h%03d.example.com", i%37)
		rows[i] = URLRow{
			URLSurtKey:              fmt.Sprintf("com,example,h%03d)/page/%06d", i%37, i),
			URL:                     fmt.Sprintf("https://%s/page/%06d", host, i),
			URLHostName:             host,
			URLHostRegisteredDomain: "example.com",
			URLHostTLD:              "com",
			URLProtocol:             "https",
			FetchTime:               time.Unix(1700000000+int64(i), 0).UTC(),
			FetchStatus:             200,
			ContentDigest:           fmt.Sprintf("DIGEST%026d", i),
			ContentMIMEType:         "text/html",
			ContentMIMEDetected:     "text/html",
			ContentCharset:          "UTF-8",
			ContentLanguages:        "eng",
			WARCFilename:            "crawl-data/CC-MAIN-2026-25/segments/x/warc/part.warc.gz",
			WARCRecordOffset:        int32(i * 1024),
			WARCRecordLength:        1024,
		}
	}
	if n > 0 {
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return full
}

// otherRow is a schema that is close enough to be published by mistake and wrong
// enough that the dataset's readers would break on it: two columns are gone, one
// is an extra, and fetch_status is the wrong width.
type otherRow struct {
	URLSurtKey  string `parquet:"url_surtkey"`
	URL         string `parquet:"url"`
	FetchStatus int64  `parquet:"fetch_status"`
	Score       string `parquet:"score"`
}

func writeOtherShard(t *testing.T, s fileStore, path string) {
	t.Helper()
	full := filepath.Join(s.dir, path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(full)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[otherRow](f)
	if _, err := w.Write([]otherRow{{URLSurtKey: "com,example)/", URL: "https://example.com/", FetchStatus: 200, Score: "1"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// cutTail drops the last frac of a file, which is what an upload that died part
// way through leaves behind.
func cutTail(t *testing.T, path string, frac float64) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, int64(float64(fi.Size())*(1-frac))); err != nil {
		t.Fatal(err)
	}
}

func verifyPaths(t *testing.T, s fileStore, paths []string, o VerifyOptions) *VerifyReport {
	t.Helper()
	if o.Schema == nil {
		o.Schema = parquet.SchemaOf(URLRow{})
	}
	rep, err := VerifyShards(context.Background(), NewHTTPClient(Config{}), s, paths, o)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	return rep
}

func TestVerifyShardsPassesAGoodCrawl(t *testing.T) {
	s := newFileStore(t)
	var paths []string
	for i := range 4 {
		p := fmt.Sprintf("data/CC-MAIN-2026-25/part-%05d.parquet", i)
		writeShard(t, s, p, 500)
		paths = append(paths, p)
	}
	rep := verifyPaths(t, s, paths, VerifyOptions{})
	if rep.Failed != 0 || rep.Passed != 4 {
		t.Fatalf("passed %d failed %d, want 4 and 0: %+v", rep.Passed, rep.Failed, rep.Failures())
	}
	if rep.Rows != 2000 {
		t.Fatalf("rows %d, want 2000", rep.Rows)
	}
	if rep.Bytes <= 0 || rep.BytesRead <= 0 {
		t.Fatalf("bytes %d read %d, want both positive", rep.Bytes, rep.BytesRead)
	}
	// The point of the footer check is that it does not download the shard. The
	// reader pulls 256 KiB blocks, so two blocks per shard is the ceiling for a
	// footer plus the metadata behind it.
	if perShard := rep.BytesRead / int64(rep.Shards); perShard > 512<<10 {
		t.Fatalf("read %d bytes per shard, want a footer read rather than a download", perShard)
	}
}

func TestVerifyShardsCatchesBadShards(t *testing.T) {
	s := newFileStore(t)
	good := "data/x/part-00000.parquet"
	writeShard(t, s, good, 500)

	missing := "data/x/part-00001.parquet"

	truncated := "data/x/part-00002.parquet"
	cutTail(t, writeShard(t, s, truncated, 500), 0.25)

	empty := "data/x/part-00003.parquet"
	writeShard(t, s, empty, 0)

	wrong := "data/x/part-00004.parquet"
	writeOtherShard(t, s, wrong)

	paths := []string{good, missing, truncated, empty, wrong}
	rep := verifyPaths(t, s, paths, VerifyOptions{})
	want := []string{VerifyOK, VerifyMissing, VerifyUnreadable, VerifyEmpty, VerifySchema}
	for i, c := range rep.Checks {
		if c.Status != want[i] {
			t.Errorf("%s: status %q detail %q, want %q", c.Path, c.Status, c.Detail, want[i])
		}
		if c.Index != i {
			t.Errorf("%s: index %d, want %d", c.Path, c.Index, i)
		}
	}
	if rep.Passed != 1 || rep.Failed != 4 {
		t.Fatalf("passed %d failed %d, want 1 and 4", rep.Passed, rep.Failed)
	}
	if got := rep.Failures(); len(got) != 4 || got[0].Path != missing {
		t.Fatalf("failures %+v, want the four bad shards in shard order", got)
	}
	// The schema complaint has to name what is wrong, because that is the only
	// thing that tells the operator which writer produced the shard.
	d := rep.Checks[4].Detail
	for _, want := range []string{"missing", "url_host_name", "unexpected score", "fetch_status"} {
		if !strings.Contains(d, want) {
			t.Errorf("schema detail %q does not mention %q", d, want)
		}
	}
}

func TestVerifySampleCatchesACorruptPage(t *testing.T) {
	s := newFileStore(t)
	path := "data/x/part-00000.parquet"
	full := writeShard(t, s, path, 500)

	// Overwrite the pages of one column chunk in place and leave the length, the
	// offsets and the footer alone, which is the failure the footer checks cannot
	// see. The last row group is the one --sample reads.
	fi, err := os.Stat(full)
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(mustOpen(t, full), fi.Size())
	if err != nil {
		t.Fatal(err)
	}
	md := pf.Metadata()
	cm := md.RowGroups[len(md.RowGroups)-1].Columns[0].MetaData
	start := cm.DataPageOffset
	if cm.DictionaryPageOffset > 0 && cm.DictionaryPageOffset < start {
		start = cm.DictionaryPageOffset
	}
	junk := make([]byte, cm.TotalCompressedSize)
	for i := range junk {
		junk[i] = 0xA5
	}
	f, err := os.OpenFile(full, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(junk, start); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if rep := verifyPaths(t, s, []string{path}, VerifyOptions{}); rep.Failed != 0 {
		t.Fatalf("footer checks reported %q, and they cannot see a bad page", rep.Checks[0].Status)
	}
	rep := verifyPaths(t, s, []string{path}, VerifyOptions{Sample: 64})
	if rep.Checks[0].Status != VerifyCorrupt {
		t.Fatalf("with --sample: status %q detail %q, want %q", rep.Checks[0].Status, rep.Checks[0].Detail, VerifyCorrupt)
	}
	if rep.BytesRead <= 0 {
		t.Fatal("sampling read nothing")
	}
}

// TestVerifyKeepsAccessApartFromCorruption covers the shard the store will not
// hand over. Verify reads with plain ranged GETs because the published datasets
// are public, so a private repo looks like a read failure, and calling that a
// bad shard would send an operator rebuilding data that is fine.
func TestVerifyKeepsAccessApartFromCorruption(t *testing.T) {
	s := newFileStore(t)
	path := "data/x/part-00000.parquet"
	writeShard(t, s, path, 100)
	s.deny[path] = true
	rep := verifyPaths(t, s, []string{path}, VerifyOptions{})
	if rep.Checks[0].Status != VerifyNoAccess {
		t.Fatalf("status %q detail %q, want %q", rep.Checks[0].Status, rep.Checks[0].Detail, VerifyNoAccess)
	}
	if !strings.Contains(rep.Checks[0].Detail, "403") {
		t.Errorf("detail %q, want the status spelled out", rep.Checks[0].Detail)
	}
}

func TestVerifySamplePassesAGoodShard(t *testing.T) {
	s := newFileStore(t)
	path := "data/x/part-00000.parquet"
	writeShard(t, s, path, 500)
	rep := verifyPaths(t, s, []string{path}, VerifyOptions{Sample: 64})
	if rep.Failed != 0 {
		t.Fatalf("status %q detail %q, want ok", rep.Checks[0].Status, rep.Checks[0].Detail)
	}
}

// TestCheckShardStructureSpotsAShortObject covers the case where the tail of an
// interrupted upload survived: the footer parses and describes column chunks
// that run past the bytes the store is holding.
func TestCheckShardStructureSpotsAShortObject(t *testing.T) {
	s := newFileStore(t)
	full := writeShard(t, s, "data/x/part-00000.parquet", 500)
	fi, err := os.Stat(full)
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(mustOpen(t, full), fi.Size())
	if err != nil {
		t.Fatal(err)
	}
	if d := checkShardStructure(pf, fi.Size()); d != "" {
		t.Fatalf("intact file: %s", d)
	}
	d := checkShardStructure(pf, fi.Size()/2)
	if !strings.Contains(d, "past the") {
		t.Fatalf("short object: %q, want a complaint about a chunk running past the end", d)
	}
}

func mustOpen(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func TestCheckShardSchema(t *testing.T) {
	want := parquet.SchemaOf(URLRow{})
	if d := checkShardSchema(parquet.SchemaOf(URLRow{}), want); d != "" {
		t.Fatalf("same schema: %q", d)
	}
	d := checkShardSchema(parquet.SchemaOf(otherRow{}), want)
	if !strings.Contains(d, "missing") || !strings.Contains(d, "unexpected score") {
		t.Fatalf("detail %q, want the missing columns and the extra one", d)
	}
	if !strings.Contains(d, "fetch_status is INT(64,true) and should be INT(32,true)") {
		t.Fatalf("detail %q, want the width mismatch spelled out", d)
	}
}

func TestLedgerNotes(t *testing.T) {
	rep := &VerifyReport{
		Rows: 2000, Bytes: 4096,
		LedgerShards: 3, LedgerRows: 2500, LedgerBytes: 4096,
		Checks: []ShardCheck{{Status: VerifyOK}, {Status: VerifyOK}, {Status: VerifyMissing}},
	}
	notes := ledgerNotes(rep, true)
	if len(notes) != 2 {
		t.Fatalf("notes %q, want one about the shard count and one about the rows", notes)
	}
	if !strings.Contains(notes[0], "3 shards") || !strings.Contains(notes[0], "hub has 2") {
		t.Errorf("shard note %q", notes[0])
	}
	if !strings.Contains(notes[1], "rows") {
		t.Errorf("row note %q", notes[1])
	}
	if got := ledgerNotes(&VerifyReport{}, false); len(got) != 1 || !strings.Contains(got[0], "no ledger row") {
		t.Errorf("without a ledger: %q", got)
	}
	clean := &VerifyReport{Rows: 10, Bytes: 20, LedgerShards: 1, LedgerRows: 10, LedgerBytes: 20,
		Checks: []ShardCheck{{Status: VerifyOK}}}
	if notes := ledgerNotes(clean, true); len(notes) != 0 {
		t.Errorf("agreeing ledger: %q", notes)
	}
	clean.Notes = ledgerNotes(clean, true)
	if !clean.Clean() {
		t.Error("a report with no failures and no notes should be clean")
	}
}

func TestMarkRepaired(t *testing.T) {
	rep := &VerifyReport{Checks: []ShardCheck{
		{Path: "a", Status: VerifyOK},
		{Path: "b", Status: VerifyUnreadable},
		{Path: "c", Status: VerifyMissing},
	}}
	markRepaired(rep, []string{"c", "b"})
	if rep.Checks[0].Repaired || !rep.Checks[1].Repaired || !rep.Checks[2].Repaired {
		t.Fatalf("repaired flags %+v", rep.Checks)
	}
}

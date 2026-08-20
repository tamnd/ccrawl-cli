package ccrawl

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

// TestCaptureSchemaMatchesAmi pins the columns and their types.
//
// The promise the schema makes is that a query written against an ami capture
// file reads one of ours and the other way round, and that promise is only worth
// anything if it is checked. A column renamed, retyped, added or dropped here
// breaks a query somebody else wrote, which is exactly the kind of break that
// shows up as an empty result rather than an error.
func TestCaptureSchemaMatchesAmi(t *testing.T) {
	want := []string{
		"url:BYTE_ARRAY",
		"host:BYTE_ARRAY",
		"status:INT32",
		"fetched_at:INT64",
		"content_type:BYTE_ARRAY",
		"body_length:INT64",
		"digest:BYTE_ARRAY",
		"unchanged:BOOLEAN",
		"etag:BYTE_ARRAY",
		"last_modified:BYTE_ARRAY",
		"warc_file:BYTE_ARRAY",
		"warc_offset:INT64",
		"warc_length:INT64",
		"error:BYTE_ARRAY",
		"meta_json:BYTE_ARRAY",
		"markdown:BYTE_ARRAY",
		"markdown_length:INT64",
		"ttfb_ms:INT64",
		"fetch_duration_ms:INT64",
		"final_url:BYTE_ARRAY",
		"ip_address:BYTE_ARRAY",
		"resp_headers:BYTE_ARRAY",
		"req_headers:BYTE_ARRAY",
		"body:BYTE_ARRAY",
	}

	schema := parquet.SchemaOf(Capture{})
	var got []string
	for _, f := range schema.Fields() {
		got = append(got, f.Name()+":"+f.Type().Kind().String())
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("the capture schema drifted from ami's\n got: %v\nwant: %v", got, want)
	}
}

// fakeResult is a fetch with every field populated, so a round trip has
// something to lose.
func fakeResult() *CrawlResult {
	return &CrawlResult{
		URL:            "https://example.com/page",
		FinalURL:       "https://www.example.com/page",
		Status:         200,
		ContentType:    "text/html; charset=utf-8",
		Body:           []byte("<html><body><p>hello \x00 bytes \xff</p></body></html>"),
		Digest:         "sha1:ABCDEF",
		FetchedAt:      time.Unix(1770000000, 0).UTC(),
		RequestHeader:  []byte("GET /page HTTP/1.1\r\nHost: example.com\r\n\r\n"),
		ResponseHeader: []byte("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nETag: \"abc\"\r\n\r\n"),
		RemoteAddr:     "93.184.216.34",
		ETag:           `"abc"`,
		LastModified:   "Wed, 21 Oct 2026 07:28:00 GMT",
		TTFB:           120 * time.Millisecond,
		Duration:       450 * time.Millisecond,
	}
}

// TestCaptureRoundTrips writes a fetch and reads it back, comparing against the
// bytes that went in rather than against another copy of the row.
func TestCaptureRoundTrips(t *testing.T) {
	dir := t.TempDir()
	w, err := NewCaptureWriter(dir, "captures", 0)
	if err != nil {
		t.Fatal(err)
	}
	res := fakeResult()
	if err := w.WriteCapture(res); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	files := w.Files()
	if len(files) != 1 {
		t.Fatalf("wrote %d shards for one capture, want 1", len(files))
	}
	rows, err := ReadCaptures(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("read %d rows, want 1", len(rows))
	}
	c := rows[0]

	if !bytes.Equal(c.Body, res.Body) {
		t.Fatalf("the body came back as %q, want %q", c.Body, res.Body)
	}
	if c.RespHeaders != string(res.ResponseHeader) {
		t.Fatalf("response headers came back as %q", c.RespHeaders)
	}
	if c.ReqHeaders != string(res.RequestHeader) {
		t.Fatalf("request headers came back as %q", c.ReqHeaders)
	}
	if c.URL != res.URL || c.Host != "example.com" {
		t.Fatalf("url %q host %q", c.URL, c.Host)
	}
	if c.FinalURL != res.FinalURL {
		t.Fatalf("final_url came back as %q, want %q", c.FinalURL, res.FinalURL)
	}
	if c.Status != 200 || c.BodyLength != int64(len(res.Body)) || c.Digest != res.Digest {
		t.Fatalf("status %d body_length %d digest %q", c.Status, c.BodyLength, c.Digest)
	}
	if c.FetchedAt != res.FetchedAt.UnixMilli() {
		t.Fatalf("fetched_at came back as %d", c.FetchedAt)
	}
	if c.ContentType != res.ContentType || c.IPAddress != res.RemoteAddr {
		t.Fatalf("content_type %q ip_address %q", c.ContentType, c.IPAddress)
	}
	if c.ETag != res.ETag || c.LastModified != res.LastModified {
		t.Fatalf("validators came back as %q and %q", c.ETag, c.LastModified)
	}
	if c.TTFBMS != 120 || c.FetchDurMS != 450 {
		t.Fatalf("timing came back as ttfb %d duration %d", c.TTFBMS, c.FetchDurMS)
	}
	if c.Unchanged {
		t.Fatal("a 200 came back marked unchanged")
	}
}

func TestCaptureFinalURLIsEmptyWhenNothingMoved(t *testing.T) {
	res := fakeResult()
	res.FinalURL = res.URL
	if c := NewCapture(res); c.FinalURL != "" {
		t.Fatalf("final_url is %q for a fetch that did not redirect", c.FinalURL)
	}
}

func TestCaptureRecordsATruncatedBodyInMeta(t *testing.T) {
	res := fakeResult()
	res.Truncated = true
	c := NewCapture(res)
	if c.Meta()["truncated"] != "length" {
		t.Fatalf("a truncated body came back as meta %q", c.MetaJSON)
	}
	// And an untruncated one says nothing rather than saying no, so the column
	// stays empty on the overwhelming majority of rows and costs nothing.
	res.Truncated = false
	if c := NewCapture(res); c.MetaJSON != "" {
		t.Fatalf("an untruncated body wrote meta %q", c.MetaJSON)
	}
}

func TestCaptureMarksA304Unchanged(t *testing.T) {
	res := fakeResult()
	res.Status = 304
	res.Body = nil
	c := NewCapture(res)
	if !c.Unchanged {
		t.Fatal("a 304 is the page not having moved and the row has to say so")
	}
	if c.BodyLength != 0 {
		t.Fatalf("a 304 carries a body of %d bytes", c.BodyLength)
	}
}

func TestCaptureHostOfAMalformedURL(t *testing.T) {
	res := fakeResult()
	res.URL = "not a url at all"
	if h := NewCapture(res).Host; h != "" {
		t.Fatalf("a malformed URL produced host %q rather than nothing", h)
	}
}

// TestCaptureWriterRotatesBySize is the shard rotation done-when. The publish
// step commits a shard at a time, so the size has to be the thing that decides
// where one ends.
func TestCaptureWriterRotatesBySize(t *testing.T) {
	dir := t.TempDir()
	// A body of 8 KB and a target of 64 KB, so a shard holds a handful of rows
	// and the boundaries are easy to count.
	const body = 8 << 10
	const target = 64 << 10
	w, err := NewCaptureWriter(dir, "captures", target)
	if err != nil {
		t.Fatal(err)
	}
	w.batchRows = 4
	w.buf = make([]Capture, 0, 4)

	// Rotation is decided in Sync, so this writes the way a run does: a batch of
	// work, then a sync, and the sync seals the shard once it has reached its
	// size.
	const rows = 200
	const batch = 4
	for i := range rows {
		res := fakeResult()
		res.URL = fmt.Sprintf("https://example.com/p%03d", i)
		res.Body = bytes.Repeat([]byte("x"), body)
		if err := w.WriteCapture(res); err != nil {
			t.Fatal(err)
		}
		if (i+1)%batch == 0 {
			if _, err := w.Sync(false); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	files := w.Files()
	if len(files) < 10 {
		t.Fatalf("200 rows of 8 KB against a 64 KB target made %d shards, want at least 10", len(files))
	}
	var total int
	for _, f := range files {
		got, err := ReadCaptures(f)
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		if len(got) == 0 {
			t.Fatalf("%s is an empty shard", f)
		}
		total += len(got)
		// Every shard has its own footer, which is what lets the publish step
		// upload one while the run is still going.
		st, err := os.Stat(f)
		if err != nil {
			t.Fatal(err)
		}
		if st.Size() == 0 {
			t.Fatalf("%s is zero bytes", f)
		}
	}
	if total != rows {
		t.Fatalf("the shards hold %d rows between them, want %d", total, rows)
	}
}

// TestCaptureWriterLeavesNoEmptyShard checks the thing the publish step trips
// over. A zero row Parquet file is a perfectly valid file that means nothing,
// and a publish step walking the output directory would upload it. Shards are
// opened by the row that goes into them, so there is never one to upload.
// TestCaptureWriterHidesTheOpenShard is what lets the publisher run as its own
// process against the directory a crawl is still writing into. A Parquet file
// has no footer until it is closed, so an open shard and a truncated one look
// the same from outside, and the only safe rule is that a .parquet is finished
// and nothing else is.
func TestCaptureWriterHidesTheOpenShard(t *testing.T) {
	dir := t.TempDir()
	w, err := NewCaptureWriter(dir, "captures", 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := w.WriteCapture(fakeResult()); err != nil {
			t.Fatal(err)
		}
	}
	// Nothing is sealed yet, so a publisher looking for finished shards must find
	// none, and what is on disk must not be named like one.
	sealed, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) != 0 {
		t.Fatalf("an unsealed shard is on disk as %v, which a publisher would commit half written", sealed)
	}
	tmps, err := filepath.Glob(filepath.Join(dir, "*.parquet.tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(tmps) != 1 {
		t.Fatalf("the open shard is %v, want one .parquet.tmp", tmps)
	}

	if _, err := w.Sync(true); err != nil {
		t.Fatal(err)
	}
	sealed, err = filepath.Glob(filepath.Join(dir, "*.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) != 1 {
		t.Fatalf("after a forced sync the finished shards are %v, want one", sealed)
	}
	rows, err := ReadCaptures(sealed[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("the sealed shard holds %d rows, want 3", len(rows))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Close after everything was already sealed leaves no second file and no
	// leftover temp.
	left, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 1 {
		t.Fatalf("the directory holds %d entries after close, want the one sealed shard", len(left))
	}
}

func TestCaptureWriterLeavesNoEmptyShard(t *testing.T) {
	dir := t.TempDir()
	// A target of one byte, so every row seals its shard and the next row is the
	// only thing that opens another.
	w, err := NewCaptureWriter(dir, "captures", 1)
	if err != nil {
		t.Fatal(err)
	}
	w.batchRows = 1
	w.buf = make([]Capture, 0, 1)
	if err := w.WriteCapture(fakeResult()); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 1 {
		var names []string
		for _, e := range ents {
			names = append(names, e.Name())
		}
		t.Fatalf("one capture left %v behind", names)
	}
	if len(w.Files()) != 1 {
		t.Fatalf("Files reports %v", w.Files())
	}
}

// TestCaptureWriterSyncSealsOnlyWhenForced is the durability contract the
// recrawl checkpoint leans on. A Parquet file is unreadable until its footer is
// written, so a sink that answered yes without sealing would let the checkpoint
// move past rows nobody can read back.
func TestCaptureWriterSyncSealsOnlyWhenForced(t *testing.T) {
	dir := t.TempDir()
	w, err := NewCaptureWriter(dir, "captures", 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	if durable, err := w.Sync(false); err != nil || !durable {
		t.Fatalf("an empty writer said durable=%v err=%v, and there is nothing unwritten", durable, err)
	}
	if err := w.WriteCapture(fakeResult()); err != nil {
		t.Fatal(err)
	}
	if durable, err := w.Sync(false); err != nil || durable {
		t.Fatalf("a shard well short of its target said durable=%v err=%v", durable, err)
	}
	if durable, err := w.Sync(true); err != nil || !durable {
		t.Fatalf("a forced sync said durable=%v err=%v", durable, err)
	}
	rows, err := ReadCaptures(w.Files()[0])
	if err != nil {
		t.Fatalf("the sealed shard does not read back: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("the sealed shard holds %d rows, want 1", len(rows))
	}
	// And a second sync with nothing new is durable without sealing an empty
	// shard on top.
	before := len(w.Files())
	if durable, err := w.Sync(true); err != nil || !durable {
		t.Fatalf("syncing an untouched writer said durable=%v err=%v", durable, err)
	}
	if len(w.Files()) != before {
		t.Fatalf("syncing an untouched writer opened another shard: %v", w.Files())
	}
}

// TestCaptureWriterSyncSealsAShardThatIsFull is the other side of it, and it is
// the one that makes a long run checkpoint at all. An unforced sync on a shard
// that has reached its size has to seal, or a caller syncing between batches
// would never find one at zero rows and would never move its checkpoint.
func TestCaptureWriterSyncSealsAShardThatIsFull(t *testing.T) {
	dir := t.TempDir()
	w, err := NewCaptureWriter(dir, "captures", 16<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	res := fakeResult()
	res.Body = bytes.Repeat([]byte("x"), 20<<10)
	if err := w.WriteCapture(res); err != nil {
		t.Fatal(err)
	}
	if durable, err := w.Sync(false); err != nil || !durable {
		t.Fatalf("a shard past its size said durable=%v err=%v", durable, err)
	}
	if len(w.Files()) != 1 {
		t.Fatalf("the writer holds %v", w.Files())
	}
	rows, err := ReadCaptures(w.Files()[0])
	if err != nil || len(rows) != 1 {
		t.Fatalf("the sealed shard read back %d rows: %v", len(rows), err)
	}
}

// TestWARCSinkIsAlwaysDurable is the other half. A WARC is durable wherever it
// is fsynced, so a run writing WARC checkpoints at every batch the way it always
// did.
func TestWARCSinkIsAlwaysDurable(t *testing.T) {
	dir := t.TempDir()
	w := NewWARCSink(dir, "test", DefaultCrawlWARCSize, WARCInfo{})
	defer func() { _ = w.Close() }()
	if err := w.WriteCapture(fakeResult()); err != nil {
		t.Fatal(err)
	}
	for _, force := range []bool{false, true} {
		durable, err := w.Sync(force)
		if err != nil || !durable {
			t.Fatalf("Sync(%v) said durable=%v err=%v", force, durable, err)
		}
	}
	if len(w.Files()) != 1 {
		t.Fatalf("the WARC sink reports %v", w.Files())
	}
}

func TestCaptureFormatValidate(t *testing.T) {
	for _, f := range []CaptureFormat{FormatWARC, FormatParquet} {
		if err := f.Validate(); err != nil {
			t.Fatalf("%s rejected: %v", f, err)
		}
	}
	for _, f := range []CaptureFormat{"", "jsonl", "WARC"} {
		if err := CaptureFormat(f).Validate(); err == nil {
			t.Fatalf("%q accepted as a format", string(f))
		}
	}
}

func TestNewCaptureSinkPicksTheFormat(t *testing.T) {
	dir := t.TempDir()
	pq, err := NewCaptureSink(FormatParquet, filepath.Join(dir, "pq"), "captures", 1<<20, WARCInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if err := pq.WriteCapture(fakeResult()); err != nil {
		t.Fatal(err)
	}
	if err := pq.Close(); err != nil {
		t.Fatal(err)
	}
	if got := pq.Files(); len(got) != 1 || !strings.HasSuffix(got[0], ".parquet") {
		t.Fatalf("the parquet sink wrote %v", pq.Files())
	}

	wr, err := NewCaptureSink(FormatWARC, filepath.Join(dir, "warc"), "captures", 1<<20, WARCInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if err := wr.WriteCapture(fakeResult()); err != nil {
		t.Fatal(err)
	}
	if err := wr.Close(); err != nil {
		t.Fatal(err)
	}
	if got := wr.Files(); len(got) != 1 || !strings.HasSuffix(got[0], ".warc.gz") {
		t.Fatalf("the warc sink wrote %v", wr.Files())
	}

	if _, err := NewCaptureSink("jsonl", dir, "captures", 0, WARCInfo{}); err == nil {
		t.Fatal("an unknown format opened a sink")
	}
}

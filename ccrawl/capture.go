package ccrawl

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// Writing what a recrawl fetched as Parquet, with the body in the row.
//
// WARC stays the archival format and it is good at being one. It is a poor
// publishing format for the same reasons: it has to be read start to finish, it
// does not project columns, and somebody on the hub who wants to count status
// codes should not have to stream gigabytes to do it.
//
// The schema is ami's captures schema, column for column, because that schema
// has earned its shape over a lot of crawling and a second one helps nobody. A
// query written against an ami capture file reads one of these and the other way
// round.

// DefaultCaptureShardSize is how much uncompressed payload goes into one shard
// before the writer rotates.
//
// Half a gigabyte rather than the WARC convention of one, because these files
// are made to be published: a shard is committed to the hub whole, and half a
// gigabyte of payload lands somewhere around a hundred megabytes on disk after
// zstd, which is the size the rest of our published parts already are.
const DefaultCaptureShardSize int64 = 1 << 29

// Capture is one fetched URL as a published row.
//
// The column names and types are ami's, and the comments say which ones a
// ccrawl recrawl fills. The ones it does not fill are still here, because a
// schema that matches only when both writers happen to populate the same fields
// is not a schema anybody can query across.
type Capture struct {
	URL         string `parquet:"url"`
	Host        string `parquet:"host"`
	Status      int32  `parquet:"status"`
	FetchedAt   int64  `parquet:"fetched_at"` // unix millis
	ContentType string `parquet:"content_type"`
	BodyLength  int64  `parquet:"body_length"`
	Digest      string `parquet:"digest"`
	// Unchanged is a 304, meaning the page is the one the previous pass already
	// has. The body is empty on such a row and that is the point of it: over a
	// corpus where most pages do not move between crawls, storing the ones that
	// did is the difference between a dataset and a copy of one.
	Unchanged bool `parquet:"unchanged"`

	// The response validators, so a published shard doubles as the seed list for
	// the next pass over the same URLs.
	ETag         string `parquet:"etag"`
	LastModified string `parquet:"last_modified"`

	// Where the bytes live when the run wrote WARC instead. Zero here, since a
	// Parquet capture carries its own body.
	WARCFile   string `parquet:"warc_file"`
	WARCOffset int64  `parquet:"warc_offset"`
	WARCLength int64  `parquet:"warc_length"`

	// Error text for a fetch that failed. Empty on success.
	Error string `parquet:"error"`

	// MetaJSON is a JSON object of whatever else the producer knows, so context
	// that has no column survives without anybody inventing one. A recrawl puts
	// the body cap flag here when it trips.
	MetaJSON string `parquet:"meta_json"`

	// Filled by ami's Markdown pass. A recrawl leaves them empty, since ccrawl
	// renders Markdown in its own pipeline against published WARCs.
	Markdown       string `parquet:"markdown"`
	MarkdownLength int64  `parquet:"markdown_length"`

	TTFBMS     int64  `parquet:"ttfb_ms"`
	FetchDurMS int64  `parquet:"fetch_duration_ms"`
	FinalURL   string `parquet:"final_url"` // after redirects, empty if it did not move
	IPAddress  string `parquet:"ip_address"`

	// The exchange itself. The header fields hold the reconstructed HTTP head
	// text, so a reader rebuilds the request and the response without a WARC.
	RespHeaders string `parquet:"resp_headers"`
	ReqHeaders  string `parquet:"req_headers"`
	Body        []byte `parquet:"body"`
}

// NewCapture turns a fetch into the row that describes it.
func NewCapture(res *CrawlResult) Capture {
	c := Capture{
		URL:          res.URL,
		Host:         captureHost(res.URL),
		Status:       int32(res.Status),
		FetchedAt:    res.FetchedAt.UnixMilli(),
		ContentType:  res.ContentType,
		BodyLength:   int64(len(res.Body)),
		Digest:       res.Digest,
		Unchanged:    res.Status == 304,
		ETag:         res.ETag,
		LastModified: res.LastModified,
		TTFBMS:       res.TTFB.Milliseconds(),
		FetchDurMS:   res.Duration.Milliseconds(),
		IPAddress:    res.RemoteAddr,
		RespHeaders:  string(res.ResponseHeader),
		ReqHeaders:   string(res.RequestHeader),
		Body:         res.Body,
	}
	// final_url is what changed rather than what happened, so a reader can find
	// the redirects by asking for the rows that have one.
	if res.FinalURL != res.URL {
		c.FinalURL = res.FinalURL
	}
	if res.Truncated {
		// There is no truncated column in the schema and adding one would break
		// the promise that a query crosses between the two writers. This is what
		// meta_json is for.
		c.MetaJSON = `{"truncated":"length"}`
	}
	return c
}

// captureHost is the host column, which is the registered host and not the
// registered domain. A malformed URL gets an empty host rather than a guess.
func captureHost(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// CaptureSink is where a run's fetches go, whichever format it was asked for.
type CaptureSink interface {
	// WriteCapture stores one fetch.
	WriteCapture(res *CrawlResult) error
	// Sync makes what has been handed over durable and readable, and says
	// whether it managed it for everything so far. force seals a shard that has
	// not filled yet, which is what the end of a run wants and what a checkpoint
	// in the middle of one does not.
	//
	// The report matters because the two formats can promise different things. A
	// WARC file is durable the moment it is fsynced, at any offset. A Parquet
	// file is not readable at all until its footer is written, so the only
	// durable position in one is the end of a shard. A caller moving a checkpoint
	// has to know which it is dealing with, or it moves the checkpoint past rows
	// that are still sitting in a file nobody can open.
	Sync(force bool) (durable bool, err error)
	// Files lists what has been written so far, finished or not.
	Files() []string
	Close() error
}

// WriteCapture makes a WARC writer a CaptureSink.
func (w *WARCWriter) WriteCapture(res *CrawlResult) error {
	return w.Write(NewWARCCapture(res))
}

// warcSink adapts a WARC writer to the sink interface. The only thing it has to
// say differently is that a WARC is durable wherever it is fsynced, so the
// answer is always yes and force has nothing to seal.
type warcSink struct{ *WARCWriter }

func (s warcSink) Sync(bool) (bool, error) { return true, s.WARCWriter.Sync() }

// NewWARCSink builds a WARC writer as a capture sink.
func NewWARCSink(dir, prefix string, maxSize int64, info WARCInfo) CaptureSink {
	return warcSink{NewWARCWriter(dir, prefix, maxSize, info)}
}

// CaptureWriter writes Capture rows into rotating zstd Parquet shards.
//
// Every shard is finalised with its own footer, so it is independently
// readable, can be uploaded and deleted while the run is still going, and a
// crash costs at most the shard that was open. That is what the publish step
// needs: something to commit one file at a time rather than at the end of a
// hundred day run.
//
// Rows are buffered and handed to the encoder in batches, and the row group
// size is bounded, so a run of many millions of rows keeps a steady footprint
// instead of holding the whole shard before the first flush. The body and
// header columns compress columnar, which packs thousands of similar pages far
// tighter than compressing each one on its own, and a crawl is waiting on the
// network anyway so the heavier setting costs nothing that matters.
type CaptureWriter struct {
	dir       string
	prefix    string
	batchRows int
	target    int64
	codec     *zstd.Codec

	seq         int
	f           *os.File
	bw          *bufio.Writer
	w           *parquet.GenericWriter[Capture]
	buf         []Capture
	accumulated int64 // uncompressed payload in the open shard
	shardRows   int   // rows in the open shard, buffered ones included
	rows        int
	files       []string
}

// NewCaptureWriter builds a writer over dir. A non-positive target size means
// one shard that never rotates, which is for a small run and wrong for a large
// one.
func NewCaptureWriter(dir, prefix string, target int64) (*CaptureWriter, error) {
	if prefix == "" {
		prefix = "captures"
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	cw := &CaptureWriter{
		dir:       dir,
		prefix:    strings.TrimSuffix(prefix, ".parquet"),
		batchRows: 500,
		target:    target,
		// BetterCompression is around zstd level 7. It is a large ratio gain over
		// the default and still far faster than the network feeding it.
		codec: &zstd.Codec{Level: zstd.SpeedBetterCompression, Concurrency: 4},
	}
	cw.buf = make([]Capture, 0, cw.batchRows)
	return cw, nil
}

// Rows is how many captures have been handed to the writer.
func (w *CaptureWriter) Rows() int { return w.rows }

// Files lists the shards written so far, the open one included.
func (w *CaptureWriter) Files() []string { return w.files }

// open starts the next shard. Shards are opened on the first row that goes into
// one rather than ahead of it, so sealing never leaves an empty file behind for
// the publish step to find and a run that writes nothing writes nothing.
func (w *CaptureWriter) open() error {
	path := filepath.Join(w.dir, fmt.Sprintf("%s-%05d.parquet", w.prefix, w.seq))
	w.seq++
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w.f = f
	w.bw = bufio.NewWriterSize(f, 256<<10)
	w.w = parquet.NewGenericWriter[Capture](w.bw,
		parquet.Compression(w.codec),
		parquet.MaxRowsPerRowGroup(int64(w.batchRows)*64),
	)
	w.files = append(w.files, path)
	return nil
}

// WriteCapture appends one fetch, flushing and rotating as the shard fills.
func (w *CaptureWriter) WriteCapture(res *CrawlResult) error {
	return w.Write(NewCapture(res))
}

// Write appends one row.
func (w *CaptureWriter) Write(c Capture) error {
	if w.f == nil {
		if err := w.open(); err != nil {
			return err
		}
	}
	w.buf = append(w.buf, c)
	w.rows++
	w.shardRows++
	w.accumulated += approxCaptureBytes(c)
	if len(w.buf) < cap(w.buf) {
		return nil
	}
	return w.flushBatch()
}

// Sync seals the open shard when it has reached its size, or when forced, and
// reports whether everything written so far is readable.
//
// Rotation lives here rather than in Write, and that is deliberate. A Parquet
// file has no half state to flush: it is unreadable until its footer is written,
// so the only position a caller can safely checkpoint at is a sealed shard. If
// shards rotated mid batch, a caller syncing between batches would almost never
// find one open at exactly zero rows, and its checkpoint would never move at
// all. Deciding it here means every sync either seals or is a no-op, and the
// checkpoint advances once per shard.
//
// The price is that a shard overshoots its target by up to whatever was written
// since the last sync, which for a recrawl is one batch. The gain is that a
// crash replays the open shard and never skips a row.
func (w *CaptureWriter) Sync(force bool) (bool, error) {
	if w.shardRows == 0 {
		// Nothing has gone into the open shard, so everything handed over is in a
		// sealed one behind it.
		return true, nil
	}
	if !force && !(w.target > 0 && w.accumulated >= w.target) {
		return false, nil
	}
	if err := w.seal(); err != nil {
		return false, err
	}
	return true, nil
}

// seal writes the footer, flushes and closes the open shard. The next row opens
// the next one.
func (w *CaptureWriter) seal() error {
	if w.f == nil {
		return nil
	}
	if err := w.flushBatch(); err != nil {
		_ = w.w.Close()
		_ = w.f.Close()
		w.f = nil
		return err
	}
	err := w.w.Close()
	if err == nil {
		err = w.bw.Flush()
	}
	if err == nil {
		err = w.f.Sync()
	}
	cerr := w.f.Close()
	w.f, w.bw, w.w = nil, nil, nil
	w.accumulated, w.shardRows = 0, 0
	if err != nil {
		return err
	}
	return cerr
}

// flushBatch hands the buffered rows to the encoder.
func (w *CaptureWriter) flushBatch() error {
	if len(w.buf) == 0 {
		return nil
	}
	if _, err := w.w.Write(w.buf); err != nil {
		return err
	}
	w.buf = w.buf[:0]
	return nil
}

// Close seals the open shard, if there is one.
func (w *CaptureWriter) Close() error { return w.seal() }

// approxCaptureBytes estimates a row's uncompressed footprint, which is what
// the rotation target is counted in. It is dominated by the body, and the point
// of counting payload rather than file size is that file size is not known until
// the footer is written and a rotation decision has to be made before then.
func approxCaptureBytes(c Capture) int64 {
	return int64(len(c.Body)+len(c.RespHeaders)+len(c.ReqHeaders)+len(c.Markdown)+
		len(c.URL)+len(c.FinalURL)+len(c.Host)+len(c.ContentType)+len(c.Digest)+
		len(c.ETag)+len(c.LastModified)+len(c.Error)+len(c.MetaJSON)) + 128
}

// ReadCaptures reads a capture shard back. It is what the round trip check uses
// and what a caller verifying a published shard wants.
func ReadCaptures(path string) ([]Capture, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		return nil, fmt.Errorf("the file %s is not a capture shard: %w", path, err)
	}
	r := parquet.NewGenericReader[Capture](pf)
	defer func() { _ = r.Close() }()

	out := make([]Capture, 0, pf.NumRows())
	buf := make([]Capture, 256)
	for {
		n, err := r.Read(buf)
		out = append(out, buf[:n]...)
		if n == 0 || err != nil {
			if err != nil && err.Error() != "EOF" {
				return out, err
			}
			return out, nil
		}
	}
}

// CaptureMeta is what goes in and comes out of the meta_json column.
type CaptureMeta map[string]string

// Meta decodes meta_json, returning nil when there is nothing in it.
func (c Capture) Meta() CaptureMeta {
	if c.MetaJSON == "" {
		return nil
	}
	var m CaptureMeta
	if err := json.Unmarshal([]byte(c.MetaJSON), &m); err != nil {
		return nil
	}
	return m
}

// CaptureFormat is what a run writes.
type CaptureFormat string

const (
	// FormatWARC is ISO 28500, the archival format. It is what to write when the
	// output is going into an archive somebody will read with archive tools.
	FormatWARC CaptureFormat = "warc"
	// FormatParquet is the publishing format, one row per fetch with the body in
	// the row. It is what to write when the output is going to become a dataset.
	FormatParquet CaptureFormat = "parquet"
)

// Validate reports whether the format is one we write.
func (f CaptureFormat) Validate() error {
	switch f {
	case FormatWARC, FormatParquet:
		return nil
	default:
		return fmt.Errorf("format %q is neither warc nor parquet", string(f))
	}
}

// NewCaptureSink opens the output for a run in the format it asked for.
func NewCaptureSink(f CaptureFormat, dir, prefix string, shardSize int64, info WARCInfo) (CaptureSink, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	if f == FormatWARC {
		return NewWARCSink(dir, prefix, shardSize, info), nil
	}
	return NewCaptureWriter(dir, prefix, shardSize)
}

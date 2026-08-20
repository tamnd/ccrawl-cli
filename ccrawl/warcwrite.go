package ccrawl

import (
	"bytes"
	"crypto/sha1"
	"encoding/base32"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Writing WARC files a crawl produces, to ISO 28500.
//
// What used to be here wrote a response record with five headers and no
// digests. It looked like WARC and it would not survive contact with the tools
// people actually read WARC with: no way to check a payload against its digest,
// no request record to say what was asked, no warcinfo to say who wrote the
// file, and a Content-Length copied off the wire rather than measured against
// the body being stored.

// DefaultCrawlWARCSize is the size a crawl's WARC file grows to before the
// writer rotates. One gigabyte is the convention across the archiving tools and
// it is what the export path already uses.
const DefaultCrawlWARCSize int64 = 1 << 30

// WARCCapture is one fetched URL, in the form a WARC pair needs it.
type WARCCapture struct {
	URL       string
	FetchedAt time.Time
	IPAddress string
	Request   []byte // request line and headers, ending in a blank line
	Response  []byte // status line and headers, ending in a blank line
	Body      []byte
	Truncated string // a WARC-Truncated reason, usually "length", or empty
}

// NewWARCCapture turns a fetch into the pair of records that describes it.
func NewWARCCapture(res *CrawlResult) WARCCapture {
	c := WARCCapture{
		URL:       res.FinalURL,
		FetchedAt: res.FetchedAt,
		IPAddress: res.RemoteAddr,
		Request:   res.RequestHeader,
		Response:  res.ResponseHeader,
		Body:      res.Body,
	}
	if c.URL == "" {
		c.URL = res.URL
	}
	if res.Truncated {
		c.Truncated = "length"
	}
	return c
}

// WARCWriter writes crawl captures into rotating .warc.gz files.
//
// Every file opens with a warcinfo record and every record in it carries that
// record's ID, so a file pulled out of a set still says where it came from.
// Each record is its own gzip member, which is what makes a WARC file seekable
// and what lets a reader pull one record out of a byte range without
// decompressing the file.
type WARCWriter struct {
	dir     string
	prefix  string
	maxSize int64
	info    WARCInfo
	now     func() time.Time

	seq         int
	w           *os.File
	written     int64
	records     int
	fileRecords int
	warcinfoID  string
	files       []string
}

// NewWARCWriter builds a writer over dir. An empty or negative maxSize falls
// back to DefaultCrawlWARCSize.
func NewWARCWriter(dir, prefix string, maxSize int64, info WARCInfo) *WARCWriter {
	if maxSize <= 0 {
		maxSize = DefaultCrawlWARCSize
	}
	if prefix == "" {
		prefix = "ccrawl-crawl"
	}
	if info.Format == "" {
		info.Format = "WARC file version 1.0"
	}
	return &WARCWriter{dir: dir, prefix: prefix, maxSize: maxSize, info: info, now: time.Now}
}

// Records reports how many captures have been written.
func (w *WARCWriter) Records() int { return w.records }

// Files lists the paths written so far, in order.
func (w *WARCWriter) Files() []string { return w.files }

// Write appends the request and response records for one capture.
//
// The pair is written together and the rotation check happens before it, so a
// request and the response it goes with never land in different files. A reader
// that has one and wants the other should not have to go looking.
func (w *WARCWriter) Write(c WARCCapture) error {
	reqID, respID := "urn:uuid:"+newUUID(), "urn:uuid:"+newUUID()
	when := c.FetchedAt
	if when.IsZero() {
		when = w.now()
	}

	// The file has to be open before the records are built, because every record
	// carries the WARC-Warcinfo-ID of the file it lands in. That is also why a
	// rotation rebuilds the pair: it is going into a different file now, under a
	// different warcinfo.
	if w.w == nil {
		if err := w.open(); err != nil {
			return err
		}
	}
	build := func() ([]byte, error) {
		var pair bytes.Buffer
		if len(c.Request) > 0 {
			member, err := gzipMember(w.record(warcRecord{
				Type:         "request",
				ID:           reqID,
				URI:          c.URL,
				Date:         when,
				IPAddress:    c.IPAddress,
				ConcurrentTo: respID,
				ContentType:  "application/http; msgtype=request",
				Block:        c.Request,
			}))
			if err != nil {
				return nil, err
			}
			pair.Write(member)
		}
		block := append(append([]byte{}, c.Response...), c.Body...)
		member, err := gzipMember(w.record(warcRecord{
			Type:         "response",
			ID:           respID,
			URI:          c.URL,
			Date:         when,
			IPAddress:    c.IPAddress,
			ConcurrentTo: reqID,
			ContentType:  "application/http; msgtype=response",
			Truncated:    c.Truncated,
			Block:        block,
			Payload:      c.Body,
			HasPayload:   true,
		}))
		if err != nil {
			return nil, err
		}
		pair.Write(member)
		return pair.Bytes(), nil
	}

	pair, err := build()
	if err != nil {
		return err
	}
	// A capture bigger than maxSize on its own still gets written whole, which is
	// why the check is on whether the file already holds one rather than on size
	// alone. Rotating a file with nothing in it would loop forever and produce an
	// empty file for the trouble.
	if w.fileRecords > 0 && w.written+int64(len(pair)) > w.maxSize {
		if err := w.rotate(); err != nil {
			return err
		}
		if pair, err = build(); err != nil {
			return err
		}
	}
	n, err := w.w.Write(pair)
	w.written += int64(n)
	if err != nil {
		return err
	}
	w.records++
	w.fileRecords++
	return nil
}

// Sync flushes the current file to the platter. It is safe to call when nothing
// was written.
//
// A recrawl calls it before saving a checkpoint, because a checkpoint that
// outlives the bytes it claims are on disk is worse than no checkpoint at all:
// the run resumes past work that a power cut took away, and the gap is silent.
func (w *WARCWriter) Sync() error {
	if w.w == nil {
		return nil
	}
	return w.w.Sync()
}

// Close closes the current file. It is safe to call when nothing was written.
func (w *WARCWriter) Close() error {
	if w.w == nil {
		return nil
	}
	err := w.w.Close()
	w.w = nil
	return err
}

func (w *WARCWriter) filename(seq int) string {
	return fmt.Sprintf("%s-%05d.warc.gz", w.prefix, seq)
}

func (w *WARCWriter) open() error {
	if err := os.MkdirAll(w.dir, 0o755); err != nil {
		return err
	}
	// A second crawl into the same directory starts at sequence zero again, so
	// the writer takes the first name nobody has used rather than truncating an
	// archive somebody already has. O_EXCL rather than a stat because two writers
	// on the same directory would otherwise both see a free name and one of them
	// would lose its records.
	var name string
	var f *os.File
	for {
		name = w.filename(w.seq)
		var err error
		f, err = os.OpenFile(filepath.Join(w.dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err == nil {
			break
		}
		if !os.IsExist(err) {
			return err
		}
		w.seq++
	}
	path := filepath.Join(w.dir, name)
	w.w = f
	w.written = 0
	w.fileRecords = 0
	w.files = append(w.files, path)

	w.warcinfoID = "urn:uuid:" + newUUID()
	member, err := gzipMember(warcinfoRecord(w.info, name, w.warcinfoID, w.now().UTC()))
	if err != nil {
		_ = f.Close()
		return err
	}
	n, err := f.Write(member)
	w.written += int64(n)
	return err
}

func (w *WARCWriter) rotate() error {
	if err := w.w.Close(); err != nil {
		return err
	}
	w.w = nil
	w.seq++
	return w.open()
}

// warcRecord is the set of fields one record needs. It exists so that building
// a record is filling in a struct rather than remembering the order of nine
// arguments.
type warcRecord struct {
	Type         string
	ID           string
	URI          string
	Date         time.Time
	IPAddress    string
	ConcurrentTo string
	ContentType  string
	Truncated    string
	Block        []byte
	Payload      []byte
	HasPayload   bool
}

// record serialises one WARC record, digests and all.
func (w *WARCWriter) record(r warcRecord) []byte {
	var b bytes.Buffer
	b.WriteString("WARC/1.0\r\n")
	fmt.Fprintf(&b, "WARC-Type: %s\r\n", r.Type)
	fmt.Fprintf(&b, "WARC-Record-ID: <%s>\r\n", r.ID)
	fmt.Fprintf(&b, "WARC-Date: %s\r\n", r.Date.UTC().Format("2006-01-02T15:04:05Z"))
	if r.URI != "" {
		fmt.Fprintf(&b, "WARC-Target-URI: %s\r\n", r.URI)
	}
	if r.IPAddress != "" {
		fmt.Fprintf(&b, "WARC-IP-Address: %s\r\n", r.IPAddress)
	}
	if r.ConcurrentTo != "" {
		fmt.Fprintf(&b, "WARC-Concurrent-To: <%s>\r\n", r.ConcurrentTo)
	}
	if w.warcinfoID != "" {
		fmt.Fprintf(&b, "WARC-Warcinfo-ID: <%s>\r\n", w.warcinfoID)
	}
	fmt.Fprintf(&b, "WARC-Block-Digest: %s\r\n", WARCDigest(r.Block))
	if r.HasPayload {
		fmt.Fprintf(&b, "WARC-Payload-Digest: %s\r\n", WARCDigest(r.Payload))
	}
	if r.Truncated != "" {
		fmt.Fprintf(&b, "WARC-Truncated: %s\r\n", r.Truncated)
	}
	fmt.Fprintf(&b, "Content-Type: %s\r\n", r.ContentType)
	fmt.Fprintf(&b, "Content-Length: %d\r\n\r\n", len(r.Block))
	b.Write(r.Block)
	// Two CRLFs close a record, and they are not counted in Content-Length.
	b.WriteString("\r\n\r\n")
	return b.Bytes()
}

// WARCDigest is the sha1:BASE32 form WARC uses for block and payload digests.
// Twenty bytes of SHA-1 encode to exactly 32 base32 characters, so there is no
// padding to strip and nothing to get wrong about the length.
func WARCDigest(b []byte) string {
	sum := sha1.Sum(b)
	return "sha1:" + base32.StdEncoding.EncodeToString(sum[:])
}

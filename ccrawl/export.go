package ccrawl

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// DefaultWARCSize is the target size a single exported WARC file grows to before
// the exporter rotates to the next one. It matches cdx_toolkit's 1 GB default.
const DefaultWARCSize int64 = 1 << 30

// WARCInfo is the provenance written into each exported WARC file's warcinfo
// record. It mirrors the fields cdx_toolkit writes so the output is self
// describing: which tool made it, from what, and with which command.
type WARCInfo struct {
	Software    string // tool and version, e.g. "ccrawl/0.4.0"
	IsPartOf    string // prefix, with the subprefix appended when set
	Description string // the exact command line used
	Format      string // e.g. "WARC file version 1.0"
	Creator     string // optional: a person, organization, or service
	Operator    string // optional: a person, when the creator is an organization
}

// WARCExporter writes captured WARC records into one or more standards
// compliant .warc.gz files. Each file opens with a warcinfo record carrying the
// provenance, and the exporter rotates to a new file once the current one passes
// maxSize bytes. Records are written as their original gzip members, so the
// output round-trips through ccrawl parse unchanged.
type WARCExporter struct {
	dir       string
	prefix    string
	subprefix string
	maxSize   int64
	info      WARCInfo
	now       func() time.Time

	seq     int
	w       *os.File
	written int64
	records int
	Files   []string // paths written, in order
}

// NewWARCExporter builds an exporter that writes into dir. An empty or negative
// maxSize falls back to DefaultWARCSize.
func NewWARCExporter(dir, prefix, subprefix string, maxSize int64, info WARCInfo) *WARCExporter {
	if maxSize <= 0 {
		maxSize = DefaultWARCSize
	}
	if prefix == "" {
		prefix = "ccrawl"
	}
	return &WARCExporter{
		dir: dir, prefix: prefix, subprefix: subprefix,
		maxSize: maxSize, info: info, now: time.Now,
	}
}

// Records reports how many capture records have been written so far.
func (e *WARCExporter) Records() int { return e.records }

func (e *WARCExporter) filename(seq int) string {
	name := e.prefix
	if e.subprefix != "" {
		name += "-" + e.subprefix
	}
	return fmt.Sprintf("%s-%06d.extracted.warc.gz", name, seq)
}

func (e *WARCExporter) open() error {
	if err := os.MkdirAll(e.dir, 0o755); err != nil {
		return err
	}
	name := e.filename(e.seq)
	path := filepath.Join(e.dir, name)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	e.w = f
	e.written = 0
	e.Files = append(e.Files, path)
	member, err := gzipMember(warcinfoRecord(e.info, name, e.now().UTC()))
	if err != nil {
		_ = f.Close()
		return err
	}
	n, err := f.Write(member)
	e.written += int64(n)
	return err
}

// Write appends one raw WARC record (an original gzip member fetched by byte
// range) to the current file, rotating first if it would overflow maxSize.
func (e *WARCExporter) Write(raw []byte) error {
	switch {
	case e.w == nil:
		if err := e.open(); err != nil {
			return err
		}
	case e.written > 0 && e.written+int64(len(raw)) > e.maxSize:
		if err := e.rotate(); err != nil {
			return err
		}
	}
	n, err := e.w.Write(raw)
	e.written += int64(n)
	if err == nil {
		e.records++
	}
	return err
}

func (e *WARCExporter) rotate() error {
	if err := e.w.Close(); err != nil {
		return err
	}
	e.w = nil
	e.seq++
	return e.open()
}

// Close flushes and closes the current file. It is safe to call when nothing was
// written.
func (e *WARCExporter) Close() error {
	if e.w == nil {
		return nil
	}
	err := e.w.Close()
	e.w = nil
	return err
}

func gzipMember(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// warcinfoRecord builds an uncompressed WARC/1.0 warcinfo record holding the
// provenance fields, ready to be written as a gzip member.
func warcinfoRecord(info WARCInfo, filename string, t time.Time) []byte {
	var fields strings.Builder
	add := func(k, v string) {
		if v != "" {
			fmt.Fprintf(&fields, "%s: %s\r\n", k, v)
		}
	}
	add("software", info.Software)
	add("format", info.Format)
	add("isPartOf", info.IsPartOf)
	add("description", info.Description)
	add("creator", info.Creator)
	add("operator", info.Operator)
	body := fields.String()

	var rec bytes.Buffer
	rec.WriteString("WARC/1.0\r\n")
	rec.WriteString("WARC-Type: warcinfo\r\n")
	fmt.Fprintf(&rec, "WARC-Date: %s\r\n", t.Format("2006-01-02T15:04:05Z"))
	fmt.Fprintf(&rec, "WARC-Record-ID: <urn:uuid:%s>\r\n", newUUID())
	fmt.Fprintf(&rec, "WARC-Filename: %s\r\n", filename)
	rec.WriteString("Content-Type: application/warc-fields\r\n")
	fmt.Fprintf(&rec, "Content-Length: %d\r\n\r\n", len(body))
	rec.WriteString(body)
	rec.WriteString("\r\n\r\n")
	return rec.Bytes()
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]), hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]), hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]))
}

// FetchWARCRecordRaw fetches the raw bytes of a single WARC record by byte
// range. The bytes are the original gzip member as stored in the WARC file, so
// they can be written straight into an exported WARC without re-encoding.
func FetchWARCRecordRaw(ctx context.Context, h *HTTPClient, filename string, offset, length int64) ([]byte, error) {
	resp, err := h.GetRange(ctx, FileURL(filename, SourceHTTPS), offset, length)
	if err != nil {
		return nil, fmt.Errorf("range GET: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	return io.ReadAll(resp.Body)
}

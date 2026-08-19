// Package warc reads WARC files the way Common Crawl stores them: a stream of
// gzip members, one record per member. It decodes records without buffering the
// whole file, so a single record can be pulled from an HTTP byte range, and it
// exposes the small helpers needed to split an HTTP response block into its
// headers and body.
package warc

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	kgzip "github.com/klauspost/compress/gzip"
)

// Header holds parsed WARC record headers.
type Header struct {
	Type          string // warcinfo|request|response|metadata|revisit|conversion|resource
	Date          time.Time
	RecordID      string
	TargetURI     string
	IPAddress     string
	ConcurrentTo  string
	WarcinfoID    string
	BlockDigest   string
	PayloadDigest string
	RefersTo      string
	Truncated     string
	ContentType   string
	ContentLength int64
	Language      string // WARC-Identified-Content-Language (WET records)
	// Response records only: extracted HTTP fields.
	HTTPStatus int
	HTTPMIME   string
	// Source location for range-request retrieval.
	WARCFilename string
	WARCOffset   int64
	WARCLength   int64
}

// Record is a parsed WARC record: its header and the raw block bytes. For a
// response record the block is the full HTTP message (status line, headers, body).
type Record struct {
	Header Header
	Block  []byte
}

// Iterate reads a WARC file (a multi-member gzip stream where each member is one
// record) and calls fn for every record.
//
// The whole input is wrapped in one *bufio.Reader and the SAME reader is handed
// to gz.Reset on each member boundary. klauspost/compress/gzip keeps that
// buffered reader (z.r = rb), so read-ahead bytes from the previous member start
// the next member correctly and no full-file buffering is needed. This is what
// makes fetching a single record over an HTTP byte range work.
func Iterate(r io.Reader, fn func(Record) error) error {
	return IterateFrom(r, 0, fn)
}

// IterateFrom is Iterate with each record's byte span in the compressed file
// filled in: Header.WARCOffset is where the record's gzip member starts and
// Header.WARCLength is how many compressed bytes it occupies. base is the
// position in the file the reader is starting from, which is 0 for a whole file
// and the range start when the caller is resuming part way in.
//
// Those two numbers plus the filename are what a CDX row is for, so this is the
// call that lets an index be built for an archive that does not ship one. Fed
// back to a ranged GET they return exactly this record and nothing else.
//
// The offset is tracked rather than accumulated from the decompressed sizes,
// because a gzip member is a header, a deflate stream and an 8 byte trailer, and
// only the file knows how long that came out. The reader counts what it has
// pulled from the source and subtracts what is sitting unread in the buffer, so
// the difference is the exact position of the byte the next member starts on.
func IterateFrom(r io.Reader, base int64, fn func(Record) error) error {
	cr := &countingReader{r: r}
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReaderSize(cr, 1<<20)
	} else {
		// A caller who handed us a *bufio.Reader already owns the buffering and
		// we cannot see underneath it, so offsets are not available. Iterate
		// takes this path and does not ask for them.
		cr = nil
	}

	// pos is the position in the file of the next byte the gzip reader will
	// consume: everything pulled from the source, less what is still buffered.
	pos := func() int64 {
		if cr == nil {
			return 0
		}
		return base + cr.n - int64(br.Buffered())
	}

	gz, err := kgzip.NewReader(br)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open gzip: %w", err)
	}
	defer func() { _ = gz.Close() }()
	gz.Multistream(false)

	start := base
	for {
		data, readErr := io.ReadAll(gz)
		// Taken before Reset, which reads the next member's header and would
		// carry the position past the end of this one.
		end := pos()
		if len(data) > 0 {
			rec, parseErr := parseRecord(bytes.NewReader(data))
			if parseErr == nil {
				if cr != nil {
					rec.Header.WARCOffset = start
					rec.Header.WARCLength = end - start
				}
				if callErr := fn(rec); callErr != nil {
					return callErr
				}
			}
		}
		if readErr != nil && readErr != io.EOF {
			return fmt.Errorf("decompress member: %w", readErr)
		}
		if resetErr := gz.Reset(br); resetErr != nil {
			return nil // io.EOF or trailing garbage: clean stop
		}
		gz.Multistream(false)
		start = end
	}
}

// countingReader counts the bytes read from the source so a member's position in
// the compressed file can be worked out.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// parseRecord parses one WARC record from a single decompressed member.
func parseRecord(r io.Reader) (Record, error) {
	br := bufio.NewReader(r)

	versionLine, err := br.ReadString('\n')
	if err != nil {
		return Record{}, err
	}
	if !strings.HasPrefix(versionLine, "WARC/") {
		return Record{}, fmt.Errorf("expected WARC version line, got %q", strings.TrimSpace(versionLine))
	}

	tp := textproto.NewReader(br)
	mh, err := tp.ReadMIMEHeader()
	if err != nil && err != io.EOF && !strings.Contains(err.Error(), "EOF") {
		return Record{}, fmt.Errorf("read WARC headers: %w", err)
	}

	hdr := Header{
		Type:          mh.Get("Warc-Type"),
		RecordID:      mh.Get("Warc-Record-Id"),
		TargetURI:     TrimURI(mh.Get("Warc-Target-Uri")),
		IPAddress:     mh.Get("Warc-Ip-Address"),
		ConcurrentTo:  mh.Get("Warc-Concurrent-To"),
		WarcinfoID:    mh.Get("Warc-Warcinfo-Id"),
		BlockDigest:   mh.Get("Warc-Block-Digest"),
		PayloadDigest: mh.Get("Warc-Payload-Digest"),
		RefersTo:      mh.Get("Warc-Refers-To"),
		Truncated:     mh.Get("Warc-Truncated"),
		ContentType:   mh.Get("Content-Type"),
		Language:      mh.Get("Warc-Identified-Content-Language"),
	}
	if ds := mh.Get("Warc-Date"); ds != "" {
		if t, err := time.Parse(time.RFC3339, ds); err == nil {
			hdr.Date = t
		}
	}
	if cl := mh.Get("Content-Length"); cl != "" {
		hdr.ContentLength, _ = strconv.ParseInt(cl, 10, 64)
	}

	var block []byte
	if hdr.ContentLength > 0 {
		block = make([]byte, hdr.ContentLength)
		if _, err := io.ReadFull(br, block); err != nil {
			return Record{}, fmt.Errorf("read block: %w", err)
		}
	} else {
		block, _ = io.ReadAll(br)
	}

	if hdr.Type == "response" && len(block) > 0 {
		hdr.HTTPStatus, hdr.HTTPMIME = parseHTTPResponse(block)
	}
	return Record{Header: hdr, Block: block}, nil
}

// TrimURI removes the angle brackets WARC sometimes wraps URIs in.
func TrimURI(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "<")
	s = strings.TrimSuffix(s, ">")
	return s
}

// parseHTTPResponse pulls the status code and Content-Type from a raw HTTP block.
func parseHTTPResponse(block []byte) (status int, mime string) {
	idx := bytes.IndexByte(block, '\n')
	if idx < 0 {
		return 0, ""
	}
	parts := strings.SplitN(strings.TrimSpace(string(block[:idx])), " ", 3)
	if len(parts) >= 2 {
		status, _ = strconv.Atoi(parts[1])
	}
	headerSection := block
	if sep := bytes.Index(block, []byte("\r\n\r\n")); sep >= 0 {
		headerSection = block[:sep]
	} else if sep := bytes.Index(block, []byte("\n\n")); sep >= 0 {
		headerSection = block[:sep]
	}
	for line := range bytes.SplitSeq(headerSection, []byte("\n")) {
		s := strings.TrimSpace(string(line))
		if strings.HasPrefix(strings.ToLower(s), "content-type:") {
			mime = strings.TrimSpace(s[len("content-type:"):])
			if i := strings.Index(mime, ";"); i >= 0 {
				mime = strings.TrimSpace(mime[:i])
			}
			break
		}
	}
	return status, mime
}

// HTTPBody splits a response block at the header/body boundary and returns the
// body. It returns the whole block when no boundary is found.
func HTTPBody(block []byte) []byte {
	if sep := bytes.Index(block, []byte("\r\n\r\n")); sep >= 0 {
		return block[sep+4:]
	}
	if sep := bytes.Index(block, []byte("\n\n")); sep >= 0 {
		return block[sep+2:]
	}
	return block
}

// HTTPHeaders returns the header section (status line + headers) of a response
// block, without the body.
func HTTPHeaders(block []byte) []byte {
	if sep := bytes.Index(block, []byte("\r\n\r\n")); sep >= 0 {
		return block[:sep]
	}
	if sep := bytes.Index(block, []byte("\n\n")); sep >= 0 {
		return block[:sep]
	}
	return block
}

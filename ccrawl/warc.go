package ccrawl

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

// IterateWARC reads a WARC file (a multi-member gzip stream where each member is
// one record) and calls fn for every record.
//
// The whole input is wrapped in one *bufio.Reader and the SAME reader is handed
// to gz.Reset on each member boundary. klauspost/compress/gzip keeps that
// buffered reader (z.r = rb), so read-ahead bytes from the previous member start
// the next member correctly and no full-file buffering is needed. This is what
// makes fetching a single record over an HTTP byte range work.
func IterateWARC(r io.Reader, fn func(WARCRecord) error) error {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReaderSize(r, 1<<20)
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

	for {
		data, readErr := io.ReadAll(gz)
		if len(data) > 0 {
			rec, parseErr := parseWARCRecord(bytes.NewReader(data))
			if parseErr == nil {
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
	}
}

// parseWARCRecord parses one WARC record from a single decompressed member.
func parseWARCRecord(r io.Reader) (WARCRecord, error) {
	br := bufio.NewReader(r)

	versionLine, err := br.ReadString('\n')
	if err != nil {
		return WARCRecord{}, err
	}
	if !strings.HasPrefix(versionLine, "WARC/") {
		return WARCRecord{}, fmt.Errorf("expected WARC version line, got %q", strings.TrimSpace(versionLine))
	}

	tp := textproto.NewReader(br)
	mh, err := tp.ReadMIMEHeader()
	if err != nil && err != io.EOF && !strings.Contains(err.Error(), "EOF") {
		return WARCRecord{}, fmt.Errorf("read WARC headers: %w", err)
	}

	hdr := WARCHeader{
		Type:          mh.Get("Warc-Type"),
		RecordID:      mh.Get("Warc-Record-Id"),
		TargetURI:     trimURI(mh.Get("Warc-Target-Uri")),
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
			return WARCRecord{}, fmt.Errorf("read block: %w", err)
		}
	} else {
		block, _ = io.ReadAll(br)
	}

	if hdr.Type == "response" && len(block) > 0 {
		hdr.HTTPStatus, hdr.HTTPMIME = parseHTTPResponse(block)
	}
	return WARCRecord{Header: hdr, Block: block}, nil
}

// trimURI removes the angle brackets WARC sometimes wraps URIs in.
func trimURI(s string) string {
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

package ccrawl

import (
	"io"

	"github.com/tamnd/ccrawl-cli/pkg/warc"
)

// IterateWARC reads a WARC file (a multi-member gzip stream where each member is
// one record) and calls fn for every record. The parser lives in pkg/warc.
func IterateWARC(r io.Reader, fn func(WARCRecord) error) error {
	return warc.Iterate(r, fn)
}

// IterateWARCFrom is IterateWARC with each record's byte span in the compressed
// file filled in, so a caller can record where a record was and fetch it again
// later without re-reading the archive. base is the file position the reader
// starts at, 0 for a whole file.
func IterateWARCFrom(r io.Reader, base int64, fn func(WARCRecord) error) error {
	return warc.IterateFrom(r, base, fn)
}

// HTTPBody splits a response block at the header/body boundary and returns the
// body. It returns the whole block when no boundary is found.
func HTTPBody(block []byte) []byte { return warc.HTTPBody(block) }

// HTTPHeaders returns the header section (status line + headers) of a response
// block, without the body.
func HTTPHeaders(block []byte) []byte { return warc.HTTPHeaders(block) }

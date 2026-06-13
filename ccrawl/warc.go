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

// HTTPBody splits a response block at the header/body boundary and returns the
// body. It returns the whole block when no boundary is found.
func HTTPBody(block []byte) []byte { return warc.HTTPBody(block) }

// HTTPHeaders returns the header section (status line + headers) of a response
// block, without the body.
func HTTPHeaders(block []byte) []byte { return warc.HTTPHeaders(block) }

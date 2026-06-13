package ccrawl

import (
	"io"

	"github.com/tamnd/ccrawl-cli/pkg/wet"
)

// IterateWET reads a WET file (WARC conversion records holding plain text) and
// calls fn for each record. The parser lives in pkg/wet.
func IterateWET(r io.Reader, crawlID string, fn func(WETRecord) error) error {
	return wet.Iterate(r, crawlID, fn)
}

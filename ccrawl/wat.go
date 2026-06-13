package ccrawl

import (
	"io"

	"github.com/tamnd/ccrawl-cli/pkg/wat"
)

// IterateWAT reads a WAT file and calls fn for each parsed record. The parser
// lives in pkg/wat.
func IterateWAT(r io.Reader, crawlID string, fn func(WATRecord) error) error {
	return wat.Iterate(r, crawlID, fn)
}

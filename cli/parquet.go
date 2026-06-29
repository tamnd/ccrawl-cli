package cli

import (
	"io"

	"github.com/tamnd/any-cli/kit/render"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// init teaches kit how to emit "-o parquet". kit's core render package does not
// carry the Parquet dependency, so a CLI that wants it registers a RowEncoder
// factory. Every command that emits records can then stream Parquet just by
// asking for the format, with no per-command wiring.
func init() {
	render.RegisterEncoder(render.Parquet, func(w io.Writer, _ render.Options) (render.RowEncoder, error) {
		return ccrawl.NewStreamingParquetWriter(w), nil
	})
}

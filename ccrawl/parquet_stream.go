package ccrawl

import (
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// StreamingParquetWriter writes dynamically-shaped rows to a zstd-compressed
// Parquet stream. Unlike ParquetWriter, the schema is not known at compile
// time: it is derived from the first row's column names, so any command's
// projected output can be emitted as Parquet without a hand-written struct.
// Every column is an optional UTF-8 string, matching the string projection the
// render layer hands us. This is the encoder behind "-o parquet".
type StreamingParquetWriter struct {
	dest io.Writer

	w        *parquet.Writer
	colIndex map[string]int // column name to its position in the schema
	width    int            // number of columns, fixed after the first row
}

// NewStreamingParquetWriter returns a writer that emits Parquet to dest. The
// schema is built lazily from the first EmitRow call.
func NewStreamingParquetWriter(dest io.Writer) *StreamingParquetWriter {
	return &StreamingParquetWriter{dest: dest}
}

func (s *StreamingParquetWriter) start(cols []string) {
	group := parquet.Group{}
	for _, c := range cols {
		group[c] = parquet.Optional(parquet.String())
	}
	schema := parquet.NewSchema("ccrawl", group)

	// A Group orders its fields alphabetically, so the schema's column order is
	// not the emit order. Map each name to its column index once.
	s.colIndex = make(map[string]int, len(cols))
	for i, path := range schema.Columns() {
		if len(path) == 1 {
			s.colIndex[path[0]] = i
		}
	}
	s.width = len(s.colIndex)
	s.w = parquet.NewWriter(s.dest, schema, parquet.Compression(&zstd.Codec{}))
}

// EmitRow writes one projected row. The first call defines the schema from cols.
func (s *StreamingParquetWriter) EmitRow(cols, vals []string) error {
	if s.w == nil {
		s.start(cols)
	}
	row := make(parquet.Row, s.width)
	// Default every column to null, then fill the ones this row carries.
	for _, idx := range s.colIndex {
		row[idx] = parquet.NullValue().Level(0, 0, idx)
	}
	for i, name := range cols {
		idx, ok := s.colIndex[name]
		if !ok || i >= len(vals) {
			continue
		}
		row[idx] = parquet.ValueOf(vals[i]).Level(0, 1, idx)
	}
	_, err := s.w.WriteRows([]parquet.Row{row})
	return err
}

// Close flushes and finalizes the Parquet stream. It is safe to call when no
// row was ever emitted.
func (s *StreamingParquetWriter) Close() error {
	if s.w == nil {
		return nil
	}
	return s.w.Close()
}

package ccrawl

import (
	"os"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// WARCParquetRow is the columnar schema for parsed WARC record metadata. When a
// response body is converted, the content fields are populated too.
type WARCParquetRow struct {
	RecordID      string    `parquet:"record_id,dict"`
	CrawlID       string    `parquet:"crawl_id,dict"`
	WARCType      string    `parquet:"warc_type,dict"`
	TargetURI     string    `parquet:"target_uri"`
	Date          time.Time `parquet:"date,timestamp(microsecond)"`
	IPAddress     string    `parquet:"ip_address,dict"`
	PayloadDigest string    `parquet:"payload_digest"`
	ContentType   string    `parquet:"content_type,dict"`
	ContentLength int64     `parquet:"content_length"`
	Truncated     string    `parquet:"truncated,dict"`
	HTTPStatus    int32     `parquet:"http_status"`
	HTTPMIME      string    `parquet:"http_mime,dict"`
	WARCFilename  string    `parquet:"warc_filename,dict"`
	WARCOffset    int64     `parquet:"warc_offset"`
	WARCLength    int64     `parquet:"warc_length"`
	Title         string    `parquet:"title"`
	Language      string    `parquet:"language,dict"`
	Markdown      string    `parquet:"markdown"`
	Text          string    `parquet:"text"`
}

// WATParquetRow is the columnar schema for WAT link and metadata records.
type WATParquetRow struct {
	RecordID    string    `parquet:"record_id,dict"`
	CrawlID     string    `parquet:"crawl_id,dict"`
	URL         string    `parquet:"url"`
	Date        time.Time `parquet:"date,timestamp(microsecond)"`
	HTTPStatus  int32     `parquet:"http_status"`
	ContentType string    `parquet:"content_type,dict"`
	Title       string    `parquet:"title"`
	LinksCount  int32     `parquet:"links_count"`
	Links       string    `parquet:"links"` // JSON
	Metas       string    `parquet:"metas"` // JSON
}

// WETParquetRow is the columnar schema for WET plain-text records.
type WETParquetRow struct {
	RecordID        string    `parquet:"record_id,dict"`
	CrawlID         string    `parquet:"crawl_id,dict"`
	URL             string    `parquet:"url"`
	Date            time.Time `parquet:"date,timestamp(microsecond)"`
	ContentLanguage string    `parquet:"content_language,dict"`
	TextLength      int32     `parquet:"text_length"`
	Text            string    `parquet:"text"`
}

// ParquetWriter writes rows of type T to a zstd-compressed Parquet file.
type ParquetWriter[T any] struct {
	f *os.File
	w *parquet.GenericWriter[T]
	n int64
}

// NewParquetWriter creates a Parquet writer for path.
func NewParquetWriter[T any](path string) (*ParquetWriter[T], error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := parquet.NewGenericWriter[T](f, parquet.Compression(&zstd.Codec{}))
	return &ParquetWriter[T]{f: f, w: w}, nil
}

// Write appends one row.
func (p *ParquetWriter[T]) Write(row T) error {
	_, err := p.w.Write([]T{row})
	if err == nil {
		p.n++
	}
	return err
}

// WriteRows appends a batch of rows in one call. Batching amortizes the
// per-call overhead of Write, which matters when a shard holds millions of
// rows: transcoding a full-crawl seed touches billions of URLs, and a row at a
// time is dominated by the generic writer's per-call bookkeeping.
func (p *ParquetWriter[T]) WriteRows(rows []T) error {
	n, err := p.w.Write(rows)
	p.n += int64(n)
	return err
}

// Rows returns the number of rows written.
func (p *ParquetWriter[T]) Rows() int64 { return p.n }

// Close flushes and closes the file.
func (p *ParquetWriter[T]) Close() error {
	if err := p.w.Close(); err != nil {
		_ = p.f.Close()
		return err
	}
	return p.f.Close()
}

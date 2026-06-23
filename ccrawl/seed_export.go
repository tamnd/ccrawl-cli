package ccrawl

import (
	"context"
	"strings"
)

// SeedRow is the generic seed projection of one CDX capture: a URL plus the few
// hints a downstream recrawler can use. The column names are deliberately plain
// (url, digest, host, ...) so the file is a generic seed and carries nothing
// specific to its origin. A recrawler reads "url" and "digest" and treats every
// other string column as opaque metadata.
type SeedRow struct {
	URL    string `parquet:"url" json:"url"`
	Digest string `parquet:"digest" json:"digest,omitempty"`
	Host   string `parquet:"host" json:"host,omitempty"`
	MIME   string `parquet:"mime" json:"mime,omitempty"`
	Lang   string `parquet:"lang" json:"lang,omitempty"`
	Status int32  `parquet:"status" json:"status,omitempty"`
	Time   string `parquet:"time" json:"time,omitempty"`
}

// SeedExportOptions filters which captures become seed rows.
type SeedExportOptions struct {
	// Status keeps only captures with this fetch_status. Zero means any status.
	Status int32
	// MIME, when set, keeps only captures whose detected MIME contains it.
	MIME string
	// Lang, when set, keeps only captures whose content languages contain it.
	Lang string
	// Limit stops after this many written rows. Zero means no limit.
	Limit int64
}

// DefaultSeedExportOptions keeps successful captures only, which is almost
// always what a recrawl wants as a starting list.
func DefaultSeedExportOptions() SeedExportOptions {
	return SeedExportOptions{Status: 200}
}

// SeedExportStats reports how a shard projected into seed rows.
type SeedExportStats struct {
	Scanned int64
	Written int64
}

// ExportSeedFromCDX streams a CDX URL-index shard at shardPath, projects each
// matching capture to a SeedRow, and hands it to write. It performs no network
// and no aggregation: one capture becomes at most one seed row, in file order,
// so the output streams in constant memory regardless of shard size.
func ExportSeedFromCDX(ctx context.Context, shardPath string, opt SeedExportOptions, write func(SeedRow) error) (SeedExportStats, error) {
	var st SeedExportStats
	err := streamParquetCDX(shardPath, func(row CDXRawRow) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		st.Scanned++
		if opt.Status != 0 && row.FetchStatus != opt.Status {
			return nil
		}
		if row.URL == "" {
			return nil
		}
		mime := row.ContentMIMEDetected
		if mime == "" {
			mime = row.ContentMIMEType
		}
		if opt.MIME != "" && !strings.Contains(mime, opt.MIME) {
			return nil
		}
		if opt.Lang != "" && !strings.Contains(row.ContentLanguages, opt.Lang) {
			return nil
		}
		if err := write(SeedRow{
			URL:    row.URL,
			Digest: row.Digest,
			Host:   row.URLHostName,
			MIME:   mime,
			Lang:   row.ContentLanguages,
			Status: row.FetchStatus,
			Time:   row.FetchTime,
		}); err != nil {
			return err
		}
		st.Written++
		if opt.Limit > 0 && st.Written >= opt.Limit {
			return errStopSeed
		}
		return nil
	})
	if err == errStopSeed {
		err = nil
	}
	return st, err
}

// errStopSeed unwinds streamParquetCDX once the export limit is reached.
var errStopSeed = stopSeed{}

type stopSeed struct{}

func (stopSeed) Error() string { return "seed export limit reached" }

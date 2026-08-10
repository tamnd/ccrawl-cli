package ccrawl

import (
	"context"
	"time"
)

// DefaultLocationPartSize is how many index locations go into one parquet part.
//
// It is a compromise between two costs that pull opposite ways. Small parts mean
// more files, more HuggingFace commits, and a worse compression ratio, because
// parquet earns its keep over a column with many values in it. Large parts mean
// a resumed run redoes more, and the digest dedup set for a part sits in memory
// for as long as the part takes. Fifty thousand pages is a few hundred megabytes
// of Markdown, which is a sane parquet file and a few minutes of work.
const DefaultLocationPartSize = 50000

// packLocations converts exactly the records a set of index locations points at,
// instead of a whole shard.
//
// This is the recovery pass shape. A columnar query over the crawl index picks
// out a few thousand pages that are scattered across a few hundred WARC files,
// and reading those files whole would move something like a thousand times the
// bytes the pages are worth. Ranged reads with the neighbours coalesced move
// what the pages are worth plus the holes between them.
//
// Everything after the fetch is the pipeline every other pack uses: the same
// extractor, the same language filter, the same digest dedup, the same schema.
// Only the source changes, which is the point of recordSource existing.
func packLocations(ctx context.Context, h *HTTPClient, cfg MarkdownPackConfig, stats MarkdownStats, t0 time.Time) (MarkdownStats, error) {
	gap := cfg.FetchGap
	if gap <= 0 {
		gap = DefaultFetchGap
	}
	maxSpan := cfg.FetchMaxSpan
	if maxSpan <= 0 {
		maxSpan = DefaultFetchMaxSpan
	}
	workers := cfg.FetchWorkers
	if workers <= 0 {
		workers = cfg.Workers
	}
	if workers <= 0 {
		workers = 8
	}

	var fetched BatchFetchStats
	src := func(ctx context.Context, emit func(htmlRecord) error) error {
		var err error
		fetched, err = RunBatchFetch(ctx, h, BatchFetchConfig{
			Locations: cfg.Locations,
			Gap:       gap,
			MaxSpan:   maxSpan,
			Workers:   workers,
			Window:    workers * 2,
			OnRecord: func(_ Location, rec WARCRecord) error {
				page, ok := htmlPageOf(rec)
				if !ok {
					return nil
				}
				return emit(page)
			},
			// A location that will not fetch is one page missing out of many, not
			// a reason to throw away the part. A recovery pass runs over an index
			// that can disagree with the archive, and a run that dies on the first
			// disagreement never finishes.
			OnError: func(Location, error) {},
		})
		return err
	}

	out, err := packSource(ctx, src, cfg, stats, t0)
	// WARCBytes is what the fetch actually pulled off the wire, holes included,
	// rather than a shard size. That is the number worth publishing here: it is
	// what the ranged reads cost, and comparing it against the shards these
	// records live in is the whole argument for doing it this way.
	out.WARCBytes = fetched.Bytes
	return out, err
}

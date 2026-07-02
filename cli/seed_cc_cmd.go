package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
	"github.com/tamnd/meguri/seed"
)

// seedCCCmd holds the flags for `ccrawl seed cc`, the max-speed bulk URL puller.
// It reads a whole Common Crawl columnar index (300 or so Parquet files per
// subset), projects only the url column so the network pull is a fraction of the
// full index, and routes every url into a hostkey-sharded .seed directory that
// `meguri shard build` turns into a partitioned frontier with no reparsing.
type seedCCCmd struct {
	crawl     string
	subset    string
	out       string
	shards    int
	codec     string
	blockSize int
	limit     int64
	workers   int
	skipFiles int
	maxFiles  int
	whole     bool
	polite    bool
}

func newSeedCCCmd() kit.Command {
	v := &seedCCCmd{}
	return kit.Command{
		Use:   "cc",
		Short: "Build a sharded .seed straight from a Common Crawl columnar index",
		Long: `Pull a whole Common Crawl columnar URL index and write a hostkey-sharded
.seed directory, the native seed format meguri ingests.

Only the url column is read. parquet-go decodes just the columns a reader's
schema names, so each index file's url column chunks are fetched over ranged
GETs and nothing else downloads, which is why a 155 GiB index costs a fraction
of that on the wire. Every url is routed into one of --shards shards by its
meguri hostkey, the same key meguri assigns on ingest, so the seed this writes
is byte-for-byte a seed meguri would build from the same URLs. BulkLoad external
-sorts on ingest, so the shards do not need to be pre-sorted here.

The default is max speed: the request throttle is off and workers run wide.
Pass --polite to keep the configured --rate delay when Common Crawl asks for a
lighter touch (the retry/backoff on 403/429/503 stays on either way).

The output feeds meguri directly:

  ccrawl seed cc --crawl latest --shards 256 -O /data/cc.seed
  meguri shard build --seed /data/cc.seed --out /data/store

Ranged column reads are the default. --whole downloads each index file first
and reads it locally, which is faster per file on a fat pipe but pulls the whole
index. Examples:

  ccrawl seed cc --crawl CC-MAIN-2026-25 --shards 256 -O /data/cc.seed
  ccrawl seed cc --limit 1000000 --shards 16 -O /tmp/1m.seed        # a ladder rung
  ccrawl seed cc --max-files 4 --workers 8 -O /tmp/sample.seed      # a quick sample`,
		Flags: v.flags,
		Run:   v.run,
	}
}

func (v *seedCCCmd) flags(f *kit.FlagSet) {
	f.StringVar(&v.crawl, "crawl", "latest", "crawl to pull (latest, or an ID like CC-MAIN-2026-25)")
	f.StringVar(&v.subset, "subset", "warc", "columnar index subset: warc|crawldiagnostics|robotstxt")
	f.StringVarP(&v.out, "out", "O", "", "output .seed directory (required)")
	f.IntVar(&v.shards, "shards", 256, "hostkey-range shards (rounded up to a power of two)")
	f.StringVar(&v.codec, "codec", "zstd", "seed block codec: zstd|raw")
	f.IntVar(&v.blockSize, "block-size", 0, "seed block size in bytes (0 = 1 MiB default)")
	f.Int64Var(&v.limit, "limit", 0, "stop after this many URLs (0 = all)")
	f.IntVar(&v.workers, "workers", 16, "concurrent index files in flight")
	f.IntVar(&v.skipFiles, "skip-files", 0, "skip the first N index files (coarse resume)")
	f.IntVar(&v.maxFiles, "max-files", 0, "process at most this many index files (0 = all)")
	f.BoolVar(&v.whole, "whole", false, "download each index file whole, then read locally")
	f.BoolVar(&v.polite, "polite", false, "keep the configured --rate delay (default: no throttle)")
}

func (v *seedCCCmd) run(ctx context.Context, _ []string) error {
	if v.out == "" {
		return fmt.Errorf("seed cc needs an output directory: -O/--out")
	}
	codec, err := parseSeedCodec(v.codec)
	if err != nil {
		return err
	}

	app := appFromCtx(ctx)

	// Max speed by default: clone the config with the throttle off so the bulk
	// pull is bounded by the network and the shard writers, not a politeness
	// timer. --polite reuses the shared, rate-limited client instead. The retry
	// and backoff on 403/429/503 is a property of the client either way, so a
	// throttle-free run still yields under real pushback.
	h := app.HTTP
	if !v.polite {
		cfg := app.Cfg
		cfg.Delay = 0
		h = ccrawl.NewHTTPClient(cfg)
	}

	start := time.Now()
	logf := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "[%s] "+format+"\n", append([]any{time.Since(start).Round(time.Second)}, args...)...)
	}

	opt := ccrawl.CCSeedOptions{
		Crawl:     v.crawl,
		Subset:    v.subset,
		Shards:    v.shards,
		BlockSize: v.blockSize,
		Codec:     codec,
		OutDir:    v.out,
		Limit:     v.limit,
		SkipFiles: v.skipFiles,
		MaxFiles:  v.maxFiles,
		Workers:   v.workers,
		Whole:     v.whole,
		Progress: func(p ccrawl.CCSeedProgress) {
			rate := float64(p.URLs) / p.Elapsed.Seconds()
			logf("file %d/%d: %s URLs, %s fetched, %.0f URLs/s",
				p.FilesDone, p.FilesTotal, humanCount(p.URLs), humanBytes(p.BytesFetched), rate)
		},
	}

	logf("seed cc: crawl=%s subset=%s shards=%d codec=%s workers=%d polite=%v -> %s",
		v.crawl, v.subset, v.shards, v.codec, v.workers, v.polite, v.out)

	st, err := ccrawl.BuildCCSeed(ctx, h, app.Cache, app.Cfg.Source, opt)
	if err != nil {
		return err
	}

	logf("done: crawl=%s files=%d scanned=%s URLs=%s shards=%d",
		st.Crawl, st.Files, humanCount(st.Scanned), humanCount(st.Written), st.Shards)
	logf("network: fetched %s of index; seed on disk: %s in %s",
		humanBytes(st.BytesFetched), humanBytes(st.SeedBytes), st.Elapsed.Round(time.Second))
	if st.Elapsed.Seconds() > 0 {
		logf("throughput: %.0f URLs/s, %.1f MiB/s off the wire",
			float64(st.Written)/st.Elapsed.Seconds(),
			float64(st.BytesFetched)/st.Elapsed.Seconds()/(1<<20))
	}
	return nil
}

// parseSeedCodec turns a codec flag into a seed.Codec.
func parseSeedCodec(s string) (seed.Codec, error) {
	switch s {
	case "zstd", "":
		return seed.CodecZstd, nil
	case "raw", "none":
		return seed.CodecRaw, nil
	default:
		return 0, fmt.Errorf("unknown codec %q (want zstd or raw)", s)
	}
}

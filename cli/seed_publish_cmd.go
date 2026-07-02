package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
	"github.com/tamnd/meguri/seed"
)

// seedPublishCmd holds the flags for `ccrawl seed publish`, which offloads a
// sharded .seed to a HuggingFace dataset repo as partitioned Parquet so the URL
// corpus never has to be pulled from Common Crawl twice.
type seedPublishCmd struct {
	seedDir     string
	crawl       string
	subset      string
	repo        string
	out         string
	parallel    int
	commitBatch int
	keepParquet bool
	noPush      bool
	minFreeGB   int
}

func newSeedPublishCmd() kit.Command {
	v := &seedPublishCmd{}
	return kit.Command{
		Use:   "publish <seed-dir>",
		Short: "Publish a sharded .seed to a HuggingFace dataset repo as partitioned Parquet",
		Long: `Transcode a sharded .seed directory (as written by ` + "`ccrawl seed cc`" + `) into
partitioned Parquet and push it to a HuggingFace dataset repo. Each hostkey
range shard becomes one data/crawl=<id>/shard-NNNNN.parquet file with a url
column and a derived host column. The seed's manifest.json is pushed alongside
so a puller can rebuild the exact same crawl-frontier partitions.

This offloads the URL corpus from the box: once it is on HuggingFace it need
not be redownloaded from Common Crawl, and the local .seed and store can be
removed. Shards are committed in batches off the transcode critical path, a
ledger skips shards already on HF so a killed run resumes, and each committed
Parquet is deleted locally after it is safe on HF (unless --keep-parquet), so a
full seed publishes from a box that cannot hold the whole Parquet copy at once.

HF_TOKEN (or HUGGINGFACE_TOKEN) must be set. Examples:

  ccrawl seed publish /data/cc.seed --crawl CC-MAIN-2026-25 --repo open-index/commoncrawl-urls
  ccrawl seed publish /data/cc.seed --crawl CC-MAIN-2026-25 --commit-batch 8 --parallel 8
  ccrawl seed publish /data/cc.seed --no-push --out /tmp/pq   # transcode locally, inspect first`,
		Args:  kit.ExactArgs(1),
		Flags: v.flags,
		Run:   v.run,
	}
}

func (v *seedPublishCmd) flags(f *kit.FlagSet) {
	f.StringVar(&v.crawl, "crawl", "", "crawl ID for the Hive partition (default: the seed dir name)")
	f.StringVar(&v.subset, "subset", "warc", "index subset the seed came from, for the dataset card")
	f.StringVar(&v.repo, "repo", "open-index/commoncrawl-urls", "HuggingFace dataset repo (org/name)")
	f.StringVarP(&v.out, "out", "O", "", "staging dir for transcoded Parquet (default: <seed-dir>/.publish)")
	f.IntVar(&v.parallel, "parallel", 4, "shards transcoded at once")
	f.IntVar(&v.commitBatch, "commit-batch", 4, "shards per HuggingFace commit")
	f.BoolVar(&v.keepParquet, "keep-parquet", false, "keep staged Parquet after commit (default: delete once on HF)")
	f.BoolVar(&v.noPush, "no-push", false, "transcode locally and skip the upload")
	f.IntVar(&v.minFreeGB, "min-free-gb", 2, "pause transcoding while free disk is below this many GiB")
}

func (v *seedPublishCmd) run(ctx context.Context, args []string) error {
	v.seedDir = args[0]
	if _, err := os.Stat(filepath.Join(v.seedDir, seed.ManifestName)); err != nil {
		return fmt.Errorf("%s is not a .seed directory (no %s): %w", v.seedDir, seed.ManifestName, err)
	}

	crawlID := v.crawl
	if crawlID == "" {
		crawlID = seedDirCrawlID(v.seedDir)
	}
	out := v.out
	if out == "" {
		out = filepath.Join(v.seedDir, ".publish")
	}

	push := !v.noPush
	hf := ccrawl.NewHFClient("")
	if push {
		if !hf.Valid() {
			return fmt.Errorf("HF_TOKEN (or HUGGINGFACE_TOKEN) is not set; set it or pass --no-push")
		}
		if err := hf.CreateDatasetRepo(ctx, v.repo, false); err != nil {
			return fmt.Errorf("create dataset repo %s: %w", v.repo, err)
		}
		fmt.Fprintf(os.Stderr, "publishing %s to https://huggingface.co/datasets/%s\n", v.seedDir, v.repo)
	} else {
		fmt.Fprintf(os.Stderr, "transcoding %s to %s (no push)\n", v.seedDir, out)
	}

	ledger, err := ccrawl.OpenLedger(filepath.Join(out, "published.ledger"))
	if err != nil {
		// The ledger lives under out, which may not exist yet; make it and retry.
		if mkErr := os.MkdirAll(out, 0o755); mkErr != nil {
			return mkErr
		}
		ledger, err = ccrawl.OpenLedger(filepath.Join(out, "published.ledger"))
		if err != nil {
			return err
		}
	}
	defer func() { _ = ledger.Close() }()

	cfg := ccrawl.SeedPublishConfig{
		SeedDir:       v.seedDir,
		CrawlID:       crawlID,
		Subset:        v.subset,
		OutDir:        out,
		Repo:          v.repo,
		Push:          push,
		ShardParallel: v.parallel,
		CommitBatch:   v.commitBatch,
		KeepParquet:   v.keepParquet,
		MinFreeBytes:  int64(v.minFreeGB) << 30,
		Ledger:        ledger,
	}

	st, err := ccrawl.RunSeedPublish(ctx, hf, cfg)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr,
		"done: %d published, %d skipped, %d failed | %s URLs | Parquet %s | transcode %ds, publish %ds in %s\n",
		st.Published, st.Skipped, st.Failed, humanCount(st.Rows),
		humanBytes(st.ParquetBytes), st.TranscodeS, st.PublishS, st.Elapsed.Round(1e9))
	if push {
		fmt.Fprintf(os.Stderr, "dataset: https://huggingface.co/datasets/%s\n", v.repo)
	}
	if st.Failed > 0 {
		return fmt.Errorf("%d shards failed to publish", st.Failed)
	}
	return nil
}

// seedDirCrawlID derives a crawl label from a seed directory name when --crawl
// is not given. A dir like /data/CC-MAIN-2026-25.seed yields CC-MAIN-2026-25.
func seedDirCrawlID(dir string) string {
	base := strings.TrimSuffix(filepath.Base(filepath.Clean(dir)), ".seed")
	if base == "" || base == "." || base == "/" {
		return "unknown"
	}
	return base
}

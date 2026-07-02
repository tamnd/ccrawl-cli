package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// registerDataset attaches the dataset command group, which caches a processed
// Parquet corpus on HuggingFace and restores it later. The pair lets an
// expensive WET-to-Parquet conversion be run once and reused: publish uploads
// the local Parquet directory, pull brings it back, so a rebuild on any box
// never has to redownload and reconvert the crawl.
func registerDataset(app *kit.App) {
	app.CommandGroup("dataset", "Cache a processed Parquet corpus on HuggingFace and restore it")
	app.AddCommandUnder("dataset", newDatasetPublishCmd())
	app.AddCommandUnder("dataset", newDatasetPullCmd())
}

// datasetPublishCmd holds the flags for `ccrawl dataset publish`.
type datasetPublishCmd struct {
	crawl       string
	subset      string
	repo        string
	commitBatch int
	private     bool
	noPush      bool
}

func newDatasetPublishCmd() kit.Command {
	v := &datasetPublishCmd{}
	return kit.Command{
		Use:   "publish <parquet-dir>",
		Short: "Publish a directory of processed Parquet files to a HuggingFace dataset repo",
		Long: `Upload a directory of already-converted Parquet files to a HuggingFace dataset
repo, Hive-partitioned by crawl under data/crawl=<id>/. The files are the output
of ` + "`ccrawl convert ... --to parquet`" + ` and are pushed as is; there is no
transcode, and nothing local is deleted.

The point is a cache. Converting a crawl's WET archives to Parquet is the slow
part of a build; once the result is on HuggingFace, a later run on any box calls
` + "`ccrawl dataset pull`" + ` and skips the download and the conversion entirely.

Files already on HuggingFace are skipped, so a killed run resumes, and the
dataset card is refreshed each batch. HF_TOKEN (or HUGGINGFACE_TOKEN) must be
set. Examples:

  ccrawl dataset publish ./wet-parquet --crawl CC-MAIN-2026-25 --repo open-index/commoncrawl-2026-25-text
  ccrawl dataset publish ./wet-parquet -c 2026-25 --commit-batch 16
  ccrawl dataset publish ./wet-parquet --no-push   # scan and report, upload nothing`,
		Args:  kit.ExactArgs(1),
		Flags: v.flags,
		Run:   v.run,
	}
}

func (v *datasetPublishCmd) flags(f *kit.FlagSet) {
	f.StringVar(&v.crawl, "crawl", "", "crawl ID for the Hive partition (default: the resolved -c crawl)")
	f.StringVar(&v.subset, "subset", "wet", "archive kind the corpus came from (wet, warc, wat), for the card")
	f.StringVar(&v.repo, "repo", "", "HuggingFace dataset repo (org/name), required to push")
	f.IntVar(&v.commitBatch, "commit-batch", 8, "files per HuggingFace commit")
	f.BoolVar(&v.private, "private", false, "create the dataset repo private")
	f.BoolVar(&v.noPush, "no-push", false, "scan and report but skip the upload")
}

func (v *datasetPublishCmd) run(ctx context.Context, args []string) error {
	app := appFromCtx(ctx)
	srcDir := args[0]
	if fi, err := os.Stat(srcDir); err != nil || !fi.IsDir() {
		return fmt.Errorf("%s is not a directory", srcDir)
	}

	crawlID := v.crawl
	if crawlID == "" {
		id, err := app.Crawl(ctx)
		if err != nil {
			return fmt.Errorf("resolve crawl (pass --crawl): %w", err)
		}
		crawlID = id
	}

	push := !v.noPush
	if push && v.repo == "" {
		return usageErr("--repo is required to publish (or pass --no-push)")
	}

	hf := ccrawl.NewHFClient("")
	if push {
		if !hf.Valid() {
			return fmt.Errorf("HF_TOKEN (or HUGGINGFACE_TOKEN) is not set; set it or pass --no-push")
		}
		if err := hf.CreateDatasetRepo(ctx, v.repo, v.private); err != nil {
			return fmt.Errorf("create dataset repo %s: %w", v.repo, err)
		}
		fmt.Fprintf(os.Stderr, "publishing %s to https://huggingface.co/datasets/%s\n", srcDir, v.repo)
	} else {
		fmt.Fprintf(os.Stderr, "scanning %s (no push)\n", srcDir)
	}

	st, err := ccrawl.RunDatasetPublish(ctx, hf, ccrawl.DatasetPublishConfig{
		SrcDir:      srcDir,
		CrawlID:     crawlID,
		Subset:      v.subset,
		Repo:        v.repo,
		Push:        push,
		Private:     v.private,
		CommitBatch: v.commitBatch,
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr,
		"done: %d published, %d skipped, %d failed | %s docs | Parquet %s | publish %ds in %s\n",
		st.Published, st.Skipped, st.Failed, humanCount(st.Rows),
		humanBytes(st.ParquetBytes), st.PublishS, st.Elapsed.Round(1e9))
	if push {
		fmt.Fprintf(os.Stderr, "dataset: https://huggingface.co/datasets/%s\n", v.repo)
	}
	if st.Failed > 0 {
		return fmt.Errorf("%d files failed to publish", st.Failed)
	}
	return nil
}

// datasetPullCmd holds the flags for `ccrawl dataset pull`.
type datasetPullCmd struct {
	crawl   string
	repo    string
	outDir  string
	workers int
	tree    bool
}

func newDatasetPullCmd() kit.Command {
	v := &datasetPullCmd{}
	return kit.Command{
		Use:   "pull",
		Short: "Restore a processed Parquet corpus from a HuggingFace dataset repo",
		Long: `Download the Parquet files of a published corpus from a HuggingFace dataset
repo into a local directory, the inverse of ` + "`ccrawl dataset publish`" + `. Files
already present with the expected size are skipped, so a killed pull resumes.

By default the files land flat (basenames) in the output directory, which is
what an indexer reads directly. Pass --tree to keep the repo's
data/crawl=<id>/ path layout instead.

A public dataset needs no token; a private one uses HF_TOKEN. Examples:

  ccrawl dataset pull --repo open-index/commoncrawl-2026-25-text --crawl CC-MAIN-2026-25 --out ./corpus
  ccrawl dataset pull --repo open-index/commoncrawl-2026-25-text --out ./corpus --workers 8
  ccrawl dataset pull --repo open-index/commoncrawl-2026-25-text --out ./corpus --tree`,
		Args:  kit.NoArgs,
		Flags: v.flags,
		Run:   v.run,
	}
}

func (v *datasetPullCmd) flags(f *kit.FlagSet) {
	f.StringVar(&v.crawl, "crawl", "", "restore only this crawl's partition (default: every crawl in the repo)")
	f.StringVar(&v.repo, "repo", "", "HuggingFace dataset repo (org/name), required")
	f.StringVarP(&v.outDir, "out", "O", "", "output directory (required)")
	f.IntVar(&v.workers, "workers", 4, "concurrent downloads")
	f.BoolVar(&v.tree, "tree", false, "keep the repo path tree instead of flattening to basenames")
}

func (v *datasetPullCmd) run(ctx context.Context, args []string) error {
	if v.repo == "" {
		return usageErr("--repo is required")
	}
	if v.outDir == "" {
		return usageErr("--out is required")
	}

	hf := ccrawl.NewHFClient("")
	fmt.Fprintf(os.Stderr, "pulling https://huggingface.co/datasets/%s into %s\n", v.repo, v.outDir)

	var pulled, skipped, bytes int64
	n, err := ccrawl.RunDatasetPull(ctx, hf, ccrawl.DatasetPullConfig{
		Repo:    v.repo,
		CrawlID: v.crawl,
		OutDir:  v.outDir,
		Workers: v.workers,
		Flat:    !v.tree,
		Progress: func(r ccrawl.DatasetPullResult) {
			if r.Err != nil {
				fmt.Fprintf(os.Stderr, "  %s: %v\n", r.Path, r.Err)
				return
			}
			bytes += r.Bytes
			if r.Skipped {
				skipped++
			} else {
				pulled++
			}
		},
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "done: %d files, %d downloaded, %d skipped | %s\n",
		n, pulled, skipped, humanBytes(bytes))
	return nil
}

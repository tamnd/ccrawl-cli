package ccrawl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

// HFDatasetDocPath returns the HF repo path for one published document-corpus
// Parquet file. Every file for a crawl lands under data/crawl=CC-MAIN-YYYY-WW/
// so HF's partition tooling can filter by crawl without a full scan and several
// snapshots can share one repo.
func HFDatasetDocPath(crawlID, name string) string {
	return fmt.Sprintf("data/crawl=%s/%s", crawlID, name)
}

// DatasetPublishConfig drives RunDatasetPublish: which directory of Parquet
// files to push and which HF dataset repo to push it to. The files are already
// transcoded (as `ccrawl convert ... --to parquet` writes them), so publish only
// uploads; it never re-reads a WET archive.
type DatasetPublishConfig struct {
	SrcDir  string // directory of *.parquet files to publish
	CrawlID string // crawl label for the Hive partition (data/crawl=<id>/)
	Subset  string // archive kind the corpus came from (wet, warc, wat), for the card
	Repo    string // HF dataset repo, org/name
	Push    bool   // when false, scan and report but skip the upload
	Private bool   // create the repo private

	// CommitBatch is how many files go into one HF commit. Batching keeps commit
	// round trips from dominating a run of hundreds of files. 0 means 8.
	CommitBatch int

	// Progress is called once per committed batch with a snapshot of the run.
	Progress func(DatasetPublishStats)
}

// DatasetPublishStats is a live snapshot of a publish run.
type DatasetPublishStats struct {
	Total        int   // files found in the source directory
	Skipped      int   // files already on HF, skipped
	Published    int   // files committed so far
	Failed       int   // files that errored
	Rows         int64 // cumulative document rows across published files
	ParquetBytes int64 // cumulative Parquet bytes pushed
	PublishS     int64 // cumulative HF commit wall-clock, seconds
	Elapsed      time.Duration
}

// datasetFile is one local Parquet file staged for publish, with the row count
// and byte size read from its footer for the dataset card.
type datasetFile struct {
	localPath  string
	pathInRepo string
	name       string
	rows       int64
	bytes      int64
}

// RunDatasetPublish uploads a directory of Parquet files to a HuggingFace
// dataset repo under data/crawl=<id>/, in batched commits, skipping any file
// already on HF so a killed run resumes. It refreshes the dataset card each
// batch so the counts on HF track what has actually landed.
//
// Unlike the seed publish, the source files are already columnar, so there is no
// transcode step and nothing is deleted locally: the point is to leave a cached
// copy on HF that a later `ccrawl dataset pull` restores instead of redownloading
// and reconverting the whole crawl.
func RunDatasetPublish(ctx context.Context, hf *HFClient, cfg DatasetPublishConfig) (DatasetPublishStats, error) {
	k := cfg.CommitBatch
	if k <= 0 {
		k = 8
	}

	files, err := scanDatasetFiles(cfg.SrcDir, cfg.CrawlID)
	if err != nil {
		return DatasetPublishStats{}, err
	}
	if len(files) == 0 {
		return DatasetPublishStats{}, fmt.Errorf("no .parquet files in %s", cfg.SrcDir)
	}

	start := time.Now()
	run := DatasetPublishStats{Total: len(files)}

	// Skip files already on HF. paths-info answers in one round trip per 100, so
	// a resumed run does not re-upload what is safe.
	todo := files
	if cfg.Push {
		paths := make([]string, len(files))
		for i, f := range files {
			paths[i] = f.pathInRepo
		}
		existing, perr := hf.PathsExist(ctx, cfg.Repo, paths)
		if perr != nil {
			return run, fmt.Errorf("check existing paths: %w", perr)
		}
		todo = todo[:0]
		for _, f := range files {
			if existing[f.pathInRepo] {
				run.Skipped++
				continue
			}
			todo = append(todo, f)
		}
	}

	manifestSubset := cfg.Subset
	if manifestSubset == "" {
		manifestSubset = "wet"
	}

	for batchStart := 0; batchStart < len(todo); batchStart += k {
		if ctx.Err() != nil {
			return run, ctx.Err()
		}
		end := min(batchStart+k, len(todo))
		batch := todo[batchStart:end]

		ops := make([]HFOperation, 0, len(batch)+1)
		for _, f := range batch {
			ops = append(ops, HFOperation{LocalPath: f.localPath, PathInRepo: f.pathInRepo})
		}

		var batchRows, batchBytes int64
		for _, f := range batch {
			batchRows += f.rows
			batchBytes += f.bytes
		}

		if cfg.Push {
			card, cerr := writeTempDatasetREADME(CorpusCardStats{
				Repo:            cfg.Repo,
				CrawlID:         cfg.CrawlID,
				Subset:          manifestSubset,
				PublishedFiles:  run.Published + len(batch),
				TotalFiles:      len(files),
				Rows:            run.Rows + batchRows,
				ParquetBytes:    run.ParquetBytes + batchBytes,
				PartialProgress: run.Published+len(batch) < len(files),
			})
			if cerr != nil {
				return run, cerr
			}
			ops = append(ops, HFOperation{LocalPath: card, PathInRepo: "README.md"})

			lo, hi := batch[0].name, batch[len(batch)-1].name
			msg := fmt.Sprintf("Add %s corpus %s..%s (%d files)", cfg.CrawlID, lo, hi, len(batch))
			tPush := time.Now()
			_, cErr := hf.CommitWithRetry(ctx, cfg.Repo, msg, ops, 5)
			_ = os.Remove(card)
			if cErr != nil {
				run.Failed += len(batch)
				fmt.Fprintf(os.Stderr, "dataset publish: batch %s..%s failed: %v\n", lo, hi, cErr)
				return run, fmt.Errorf("commit %s..%s: %w", lo, hi, cErr)
			}
			run.PublishS += int64(time.Since(tPush).Seconds())
		}

		run.Published += len(batch)
		run.Rows += batchRows
		run.ParquetBytes += batchBytes
		run.Elapsed = time.Since(start)
		logDatasetProgress(&run)
		if cfg.Progress != nil {
			cfg.Progress(run)
		}
	}

	run.Elapsed = time.Since(start)
	return run, ctx.Err()
}

// scanDatasetFiles lists the *.parquet files in dir, sorts them by name for a
// stable commit order, and reads each footer for its row count and byte size.
func scanDatasetFiles(dir, crawlID string) ([]datasetFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".parquet") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	files := make([]datasetFile, 0, len(names))
	for _, name := range names {
		local := filepath.Join(dir, name)
		fi, serr := os.Stat(local)
		if serr != nil {
			return nil, serr
		}
		files = append(files, datasetFile{
			localPath:  local,
			pathInRepo: HFDatasetDocPath(crawlID, name),
			name:       name,
			rows:       parquetRowCount(local),
			bytes:      fi.Size(),
		})
	}
	return files, nil
}

// parquetRowCount reads a Parquet file's footer for its row count. It returns 0
// on any error, since the count only feeds the dataset card and a bad footer
// should not stop a publish.
func parquetRowCount(path string) int64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return 0
	}
	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		return 0
	}
	return pf.NumRows()
}

// logDatasetProgress prints a one-line status after each committed batch.
func logDatasetProgress(run *DatasetPublishStats) {
	done := run.Published + run.Skipped + run.Failed
	pct := 0.0
	if run.Total > 0 {
		pct = float64(done) / float64(run.Total) * 100
	}
	fmt.Fprintf(os.Stderr,
		"dataset publish: %d/%d files (%.1f%%) | %s docs | Parquet %s | %s elapsed\n",
		done, run.Total, pct, fmtInt(run.Rows), fmtBytes(run.ParquetBytes),
		run.Elapsed.Round(time.Second))
}

// writeTempDatasetREADME renders the document-corpus dataset card to a temp file.
func writeTempDatasetREADME(s CorpusCardStats) (string, error) {
	f, err := os.CreateTemp("", "cc-corpus-readme-*.md")
	if err != nil {
		return "", err
	}
	if _, err := f.WriteString(GenerateCorpusREADME(s)); err != nil {
		_ = f.Close()
		return "", err
	}
	return f.Name(), f.Close()
}

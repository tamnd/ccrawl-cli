package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// registerMarkdown attaches the markdown command group.
func registerMarkdown(app *kit.App) {
	app.CommandGroup("markdown", "Build open-index/open-markdown-style Markdown-parquet datasets from CC WARCs")
	app.AddCommandUnder("markdown", newMarkdownExportCmd())
}

// markdownExportCmd holds the flags for `ccrawl markdown export`.
type markdownExportCmd struct {
	shards  string // "0", "0-49", "1,3,5", "0-2,5" or "all"
	outDir  string
	repo    string
	workers int
	skip    bool // --skip-errors: continue on per-shard failure
	push    bool // push to HF after each shard (default true)
	limit   int  // stop after this many shards (0 = all)
}

func newMarkdownExportCmd() kit.Command {
	v := &markdownExportCmd{push: true}
	return kit.Command{
		Use:   "export",
		Short: "Stream CC WARCs → Markdown → Parquet → HuggingFace",
		Long: `Download one or more Common Crawl WARC files, extract HTML bodies, convert to
Markdown with readability + mdconv (GFM tables, cleaned prose, absolute links),
and write each shard to a zstd-compressed Parquet file. After each shard the
Parquet is committed to a HuggingFace dataset repo.

Output schema (open-markdown-v2):
  doc_id          stable SHA-256 URL hash (16 bytes hex)
  url             original page URL
  host            hostname
  crawl_date      WARC-Date YYYY-MM-DD
  warc_record_id  WARC record ID
  html_length     raw HTML body bytes before conversion
  markdown_length converted Markdown bytes
  markdown        converted Markdown text

HF path layout:
  data/crawl=CC-MAIN-YYYY-WW/NNNNNN.parquet

Examples:
  ccrawl markdown export --shards 0 --repo open-index/open-markdown-v2
  ccrawl markdown export --shards 0-9 --repo open-index/open-markdown-v2
  ccrawl markdown export --shards 0,5,10 --repo open-index/open-markdown-v2
  ccrawl markdown export --shards 0-99 --no-push --out ~/data/md
  HF_TOKEN=hf_... ccrawl markdown export --shards 0 -c 2026-25`,
		Flags: v.flags,
		Run:   v.run,
	}
}

func (v *markdownExportCmd) flags(f *kit.FlagSet) {
	f.StringVar(&v.shards, "shards", "0", "shard range: N, N-M, N,M, or all")
	f.StringVar(&v.outDir, "out", "", "directory for parquet files (default: <data-dir>/markdown)")
	f.StringVar(&v.repo, "repo", "open-index/open-markdown-v2", "HuggingFace dataset repo (org/name)")
	f.IntVar(&v.workers, "workers", 0, "conversion workers per shard (0 = NumCPU)")
	f.IntVar(&v.limit, "limit", 0, "process at most this many shards (0 = all)")
	f.BoolVar(&v.skip, "skip-errors", false, "continue to next shard on failure instead of aborting")
	f.BoolVar(&v.push, "push", true, "commit each parquet shard to HuggingFace after writing")
}

func (v *markdownExportCmd) run(ctx context.Context, _ []string) error {
	app := appFromCtx(ctx)

	crawlID, err := app.Crawl(ctx)
	if err != nil {
		return err
	}

	outDir := v.outDir
	if outDir == "" {
		outDir = filepath.Join(app.Cfg.DataDir, "markdown", crawlID)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	// Resolve the WARC manifest for this crawl (cached after first fetch).
	fmt.Fprintf(os.Stderr, "markdown: fetching WARC manifest for %s …\n", crawlID)
	paths, err := ccrawl.FetchPaths(ctx, app.HTTP, app.Cache, crawlID, "warc")
	if err != nil {
		return fmt.Errorf("fetch WARC manifest: %w", err)
	}
	fmt.Fprintf(os.Stderr, "markdown: manifest has %d WARC files\n", len(paths))

	indices, err := parseShardRange(v.shards, len(paths))
	if err != nil {
		return err
	}
	if v.limit > 0 && len(indices) > v.limit {
		indices = indices[:v.limit]
	}

	hf := ccrawl.NewHFClient("")
	if v.push {
		if !hf.Valid() {
			return fmt.Errorf("HF_TOKEN not set — set it or pass --push=false")
		}
		if err := hf.CreateDatasetRepo(ctx, v.repo, false); err != nil {
			return fmt.Errorf("create HF repo: %w", err)
		}
	}

	var (
		totalRows    int64
		totalHTML    int64
		totalMD      int64
		totalParquet int64
		failedShards int
	)

	for n, idx := range indices {
		if ctx.Err() != nil {
			break
		}
		warcPath := paths[idx]
		outPath := filepath.Join(outDir, fmt.Sprintf("%06d.parquet", idx))

		// Skip shards whose parquet already exists and is non-empty (idempotent).
		if fi, serr := os.Stat(outPath); serr == nil && fi.Size() > 0 {
			fmt.Fprintf(os.Stderr, "markdown: [%d/%d] shard %d: already exists (%s), skipping\n",
				n+1, len(indices), idx, humanBytes(fi.Size()))
			if v.push {
				if err := pushShard(ctx, hf, v.repo, crawlID, idx, outPath); err != nil {
					fmt.Fprintf(os.Stderr, "markdown: push shard %d: %v\n", idx, err)
				}
			}
			continue
		}

		fmt.Fprintf(os.Stderr, "markdown: [%d/%d] shard %06d: %s\n", n+1, len(indices), idx, warcPath)

		t0 := time.Now()
		stats, packErr := ccrawl.PackMarkdownShard(ctx, app.HTTP, ccrawl.MarkdownPackConfig{
			CrawlID:  crawlID,
			ShardIdx: idx,
			WARCPath: warcPath,
			OutPath:  outPath,
			Workers:  v.workers,
			Progress: func(s ccrawl.MarkdownStats) {
				if s.Rows%5000 == 0 {
					fmt.Fprintf(os.Stderr, "  … %d rows, %s html, %s md\n",
						s.Rows, humanBytes(s.HTMLBytes), humanBytes(s.MDBytes))
				}
			},
		})
		elapsed := time.Since(t0)

		if packErr != nil {
			fmt.Fprintf(os.Stderr, "markdown: shard %d failed: %v\n", idx, packErr)
			_ = os.Remove(outPath)
			failedShards++
			if !v.skip {
				return packErr
			}
			continue
		}

		totalRows += stats.Rows
		totalHTML += stats.HTMLBytes
		totalMD += stats.MDBytes
		totalParquet += stats.ParquetBytes

		ratio := float64(0)
		if stats.HTMLBytes > 0 {
			ratio = float64(stats.MDBytes) / float64(stats.HTMLBytes) * 100
		}
		fmt.Fprintf(os.Stderr,
			"markdown: shard %06d done in %s | rows=%d html=%s md=%s (%.0f%%) parquet=%s dl=%s conv=%s\n",
			idx, elapsed.Round(time.Second),
			stats.Rows,
			humanBytes(stats.HTMLBytes), humanBytes(stats.MDBytes), ratio,
			humanBytes(stats.ParquetBytes),
			stats.DurDownload.Round(time.Second),
			stats.DurConvert.Round(time.Second),
		)

		if v.push {
			if err := pushShard(ctx, hf, v.repo, crawlID, idx, outPath); err != nil {
				fmt.Fprintf(os.Stderr, "markdown: push shard %d: %v\n", idx, err)
				if !v.skip {
					return err
				}
			}
		}
	}

	fmt.Fprintf(os.Stderr, "\nmarkdown: %d shards complete | %d rows | html=%s md=%s parquet=%s | %d failed\n",
		len(indices)-failedShards, totalRows,
		humanBytes(totalHTML), humanBytes(totalMD), humanBytes(totalParquet),
		failedShards)
	return nil
}

// pushShard commits one finished parquet file to the HuggingFace dataset repo.
func pushShard(ctx context.Context, hf *ccrawl.HFClient, repo, crawlID string, shardIdx int, localPath string) error {
	hfPath := ccrawl.HFMarkdownPath(crawlID, shardIdx)
	commitMsg := fmt.Sprintf("add %s shard %06d", crawlID, shardIdx)
	fmt.Fprintf(os.Stderr, "  → pushing to %s/%s\n", repo, hfPath)
	t0 := time.Now()
	url, err := hf.CommitWithRetry(ctx, repo, commitMsg, []ccrawl.HFOperation{
		{LocalPath: localPath, PathInRepo: hfPath},
	}, 5)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "  ✓ committed in %s: %s\n", time.Since(t0).Round(time.Second), url)
	return nil
}

// parseShardRange turns a shard spec into a sorted list of 0-based indices. It
// accepts a single number ("5"), an inclusive range ("0-49"), a comma list
// ("1,3,5"), combinations ("0-9,20"), or "all" for every shard in the manifest.
func parseShardRange(spec string, total int) ([]int, error) {
	if strings.EqualFold(strings.TrimSpace(spec), "all") {
		out := make([]int, total)
		for i := range out {
			out[i] = i
		}
		return out, nil
	}
	seen := make(map[int]bool)
	var out []int
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if dash := strings.Index(part, "-"); dash >= 0 {
			lo, err1 := strconv.Atoi(part[:dash])
			hi, err2 := strconv.Atoi(part[dash+1:])
			if err1 != nil || err2 != nil {
				return nil, fmt.Errorf("invalid shard range %q", part)
			}
			if lo > hi || lo < 0 || hi >= total {
				return nil, fmt.Errorf("shard range %d-%d out of bounds [0, %d)", lo, hi, total)
			}
			for i := lo; i <= hi; i++ {
				if !seen[i] {
					seen[i] = true
					out = append(out, i)
				}
			}
		} else {
			n, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("invalid shard index %q", part)
			}
			if n < 0 || n >= total {
				return nil, fmt.Errorf("shard %d out of bounds [0, %d)", n, total)
			}
			if !seen[n] {
				seen[n] = true
				out = append(out, n)
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("shard spec %q matched no shards", spec)
	}
	return out, nil
}

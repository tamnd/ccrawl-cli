package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
	registerMarkdownRefetch(app)
}

// markdownExportCmd holds the flags for `ccrawl markdown export`.
type markdownExportCmd struct {
	shards      string // "0", "0-49", "1,3,5", "0-2,5" or "all"
	outDir      string
	repo        string
	workers     int
	skip        bool // --skip-errors: continue on per-shard failure
	push        bool // push to HF after each shard (default true)
	limit       int  // stop after this many shards (0 = all)
	parallel    int  // shards in flight at once (P)
	commitBatch int  // parquets per HF commit (K)
	keepParquet bool // keep local parquet after commit
	minFreeGB   int  // pause downloads below this much free disk
	ledger      string

	lang    string
	minConf float64

	extractor  string
	sourceKind string

	dedupDigest bool
}

func newMarkdownExportCmd() kit.Command {
	v := &markdownExportCmd{push: true}
	return kit.Command{
		Use:   "export",
		Short: "Stream CC WARCs → Markdown → Parquet → HuggingFace",
		Long: `Download one or more Common Crawl shards, turn each captured page into
Markdown, and write each shard to a zstd-compressed Parquet file. After each
shard the Parquet is committed to a HuggingFace dataset repo.

Output schema (open-markdown-v3):
  doc_id              stable SHA-256 URL hash (16 bytes hex)
  url                 original page URL
  host                hostname
  crawl_date          WARC-Date YYYY-MM-DD
  warc_record_id      WARC record ID
  html_length         raw HTML body bytes before conversion
  markdown_length     converted Markdown bytes
  markdown            converted Markdown text
  language            ISO 639-3 code detected in the Markdown, "" if too short
  language_confidence 0 to 1, how sure the identifier is
  extractor           engine that produced the text, as name@version
  simhash             64-bit near-duplicate fingerprint, 0 if too short

v3 appends the last four columns and changes nothing else, so a v2 reader that
projects the columns it knows about reads a v3 file unchanged.

Extractors (--extractor):
  h2m          go-trafilatura tuned for recall, rendered as GFM (default)
  readability  go-readability extraction, the engine open-markdown-v2 shipped
  raw          the whole document as Markdown, no boilerplate removal
  wet          the plain text Common Crawl already extracted, needs --source-kind wet

Which extractor you use is a corpus quality decision, so it is a flag and not a
build time constant. The same shard run through two engines is two different
corpora, and the version is recorded next to the name because extraction changes
between releases.

--source-kind picks the manifest: warc reads warc.paths.gz and extracts the HTML
yourself, wet reads wet.paths.gz and takes the text Common Crawl already
extracted. They are not interchangeable, so wet pairs with --extractor wet and
nothing else.

--dedup-digest drops a page whose bytes are identical to one already seen in the
same shard, before it is converted, so a duplicate costs a hash instead of an
extraction. The scope is one shard and not the run: shards are processed in
parallel, so a run-wide set would make which copy survives depend on scheduling,
and it would grow without a bound over --shards all. Near duplicates are not
dropped at all. Every row carries a simhash instead, and "ccrawl dedup" reports
the clusters, because collapsing them is a judgement about what the corpus is for
and belongs in a query rather than in a pipeline that already threw the evidence
away.

--lang keeps only the documents identified as that language, which is a document
level check on the text actually extracted, not the CLD2 label Common Crawl
computed over the raw HTML. It is a coarse pre-filter: a trigram identifier that
answers "this looks like Vietnamese" well enough to cut a corpus down, and not a
substitute for a language specific classifier if you need one.

HF path layout:
  data/crawl=CC-MAIN-YYYY-WW/NNNNNN.parquet

Shards stream in parallel: several downloads run at once (--parallel) to hide
network latency, while a single CPU-sized convert pool (--workers) is shared
across them so the cores never oversubscribe. A background committer batches
finished parquets into one HuggingFace commit (--commit-batch), then deletes the
local files, so the slow commit round trip stays off the per-shard critical path.
A ledger file records committed shards, so a killed run resumes where it stopped.

Examples:
  ccrawl markdown export --shards 0 --repo open-index/open-markdown-v3
  ccrawl markdown export --shards 0-9 --repo open-index/open-markdown-v3
  ccrawl markdown export --shards all --parallel 4 --commit-batch 10
  ccrawl markdown export --shards 0-99 --push=false --out ~/data/md
  ccrawl markdown export --shards 0-9 --lang vie --min-lang-confidence 0.8
  ccrawl markdown export --shards 0 --extractor readability --push=false
  ccrawl markdown export --shards 0 --source-kind wet --extractor wet --push=false
  HF_TOKEN=hf_... ccrawl markdown export --shards 0 -c 2026-25`,
		Flags: v.flags,
		Run:   v.run,
	}
}

func (v *markdownExportCmd) flags(f *kit.FlagSet) {
	f.StringVar(&v.shards, "shards", "0", "shard range: N, N-M, N,M, or all")
	f.StringVar(&v.outDir, "out", "", "directory for parquet files (default: <data-dir>/markdown)")
	f.StringVar(&v.repo, "repo", "open-index/open-markdown-v3", "HuggingFace dataset repo (org/name)")
	f.IntVar(&v.workers, "workers", 0, "total conversion workers shared across shards (0 = NumCPU)")
	f.IntVar(&v.limit, "limit", 0, "process at most this many shards (0 = all)")
	f.BoolVar(&v.skip, "skip-errors", false, "continue past per-shard failures instead of aborting")
	f.BoolVar(&v.push, "push", true, "commit each parquet shard to HuggingFace after writing")
	f.IntVar(&v.parallel, "parallel", 3, "shards downloaded/converted concurrently")
	f.IntVar(&v.commitBatch, "commit-batch", 1, "parquet files per HuggingFace commit")
	f.BoolVar(&v.keepParquet, "keep-parquet", false, "keep local parquet files after they are committed")
	f.IntVar(&v.minFreeGB, "min-free-gb", 2, "pause new downloads when free disk drops below this many GiB")
	f.StringVar(&v.ledger, "ledger", "", "resume ledger file (default: <out>/.committed)")
	f.StringVar(&v.lang, "lang", "", "keep only documents detected as this ISO 639-3 language (e.g. vie)")
	f.Float64Var(&v.minConf, "min-lang-confidence", ccrawl.DefaultMinLangConfidence, "confidence a document must clear for --lang")
	f.StringVar(&v.extractor, "extractor", ccrawl.DefaultExtractor, "conversion engine: "+strings.Join(ccrawl.ExtractorNames(), "|"))
	f.StringVar(&v.sourceKind, "source-kind", "", "shard manifest to read: warc|wet (default: whatever the extractor needs)")
	f.BoolVar(&v.dedupDigest, "dedup-digest", false, "skip a page whose bytes are identical to one already seen in the same shard")
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

	ex, err := ccrawl.LookupExtractor(v.extractor)
	if err != nil {
		return usageErr(err.Error())
	}
	// The extractor decides which manifest makes sense, so --source-kind only
	// has to be given when you want to be explicit, and disagreeing with the
	// extractor is an error rather than a silent reinterpretation.
	kind := v.sourceKind
	if kind == "" {
		kind = ex.SourceKind
	}
	if kind != ex.SourceKind {
		return usageErr(fmt.Sprintf("extractor %s reads %s shards, so it cannot be used with --source-kind %s", ex.Name, ex.SourceKind, kind))
	}

	// Resolve the shard manifest for this crawl (cached after first fetch).
	fmt.Fprintf(os.Stderr, "markdown: fetching %s manifest for %s …\n", strings.ToUpper(kind), crawlID)
	paths, err := ccrawl.FetchPaths(ctx, app.HTTP, app.Cache, crawlID, kind)
	if err != nil {
		return fmt.Errorf("fetch %s manifest: %w", kind, err)
	}
	fmt.Fprintf(os.Stderr, "markdown: manifest has %d %s files, extracting with %s\n", len(paths), strings.ToUpper(kind), ex.ID(crawlID))

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
			return fmt.Errorf("HF_TOKEN not set, set it or pass --push=false")
		}
		if err := hf.CreateDatasetRepo(ctx, v.repo, false); err != nil {
			return fmt.Errorf("create HF repo: %w", err)
		}
	}

	ledgerPath := v.ledger
	if ledgerPath == "" {
		ledgerPath = filepath.Join(outDir, ".committed")
	}
	ledger, err := ccrawl.OpenLedger(ledgerPath)
	if err != nil {
		return fmt.Errorf("open ledger: %w", err)
	}
	defer func() { _ = ledger.Close() }()
	if done := ledger.Count(); done > 0 {
		fmt.Fprintf(os.Stderr, "markdown: ledger %s already records %d committed shards\n", ledgerPath, done)
	}

	// The journal lands beside the ledger, so a resumed run's events sit next to
	// the record of what it resumed from.
	rep, stopRun, err := app.StartRun("markdown export", filepath.Join(outDir, "run.jsonl"))
	if err != nil {
		return err
	}
	defer stopRun()
	if p := rep.JournalPath(); p != "" {
		fmt.Fprintf(os.Stderr, "markdown: run journal %s (run %s)\n", p, rep.RunID())
	}

	run, runErr := ccrawl.RunMarkdownExport(ctx, app.HTTP, hf, ccrawl.MarkdownExportConfig{
		CrawlID:        crawlID,
		Indices:        indices,
		WARCPaths:      paths,
		OutDir:         outDir,
		Repo:           v.repo,
		Push:           v.push,
		ShardParallel:  v.parallel,
		ConvertWorkers: v.workers,
		CommitBatch:    v.commitBatch,
		KeepParquet:    v.keepParquet,
		MinFreeBytes:   int64(v.minFreeGB) << 30,
		Ledger:         ledger,
		Reporter:       rep,

		Lang:              v.lang,
		MinLangConfidence: v.minConf,
		Extractor:         ex,
		DedupDigest:       v.dedupDigest,
	})

	rep.Textf(
		"\nmarkdown: %d committed, %d skipped, %d failed of %d | %d rows | html=%s md=%s parquet=%s | %s elapsed (%.1f shards/hour)\n",
		run.Committed, run.Skipped, run.Failed, run.Total, run.Rows,
		humanBytes(run.HTMLBytes), humanBytes(run.MDBytes), humanBytes(run.ParquetBytes),
		run.Elapsed.Round(time.Second), run.ShardsPerHour)
	rep.Textf("%s", dedupSummary(v.dedupDigest, run.DigestDropped, run.Rows))
	rep.Textf("%s", langSummary(v.lang, run.LangDropped, run.LangCounts))

	// Per-shard conversion failures never abort the run; they are logged and
	// counted (the committer keeps draining). runErr is only set for a fatal
	// commit failure or a cancelled context, which always propagates.
	return runErr
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

// dedupSummary reports what --dedup-digest threw away. A drop count is the only
// way to tell a filter that worked from a filter that did nothing, and the two
// look identical in a row count you have nothing to compare against.
//
// The drop count is payloads, not rows, and the two do not have to move
// together: a duplicate of a page that extracts to nothing was never going to be
// a row, so dropping it costs the parquet nothing. Reporting both numbers rather
// than a percentage of one over the other keeps that visible.
func dedupSummary(on bool, dropped, rows int64) string {
	if !on {
		return ""
	}
	if dropped == 0 {
		return fmt.Sprintf("dedup: --dedup-digest dropped nothing, all %d payloads were distinct\n", rows)
	}
	return fmt.Sprintf("dedup: --dedup-digest dropped %d duplicate payloads before conversion, %d rows written\n",
		dropped, rows)
}

// langSummary renders the language breakdown for the end of a run. Without a
// --lang filter it is still worth printing: it says what the shard turned out to
// be, which is the thing you want before deciding what to filter on next time.
// The empty code covers documents with no text to identify, and it is named
// rather than left as a blank row.
func langSummary(want string, dropped int64, counts map[string]int64) string {
	if len(counts) == 0 {
		return ""
	}
	type pair struct {
		code string
		n    int64
	}
	var total int64
	pairs := make([]pair, 0, len(counts))
	for code, n := range counts {
		total += n
		if code == "" {
			code = "unknown"
		}
		pairs = append(pairs, pair{code, n})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].n != pairs[j].n {
			return pairs[i].n > pairs[j].n
		}
		return pairs[i].code < pairs[j].code
	})
	if len(pairs) > 8 {
		pairs = pairs[:8]
	}
	var b strings.Builder
	if want != "" {
		fmt.Fprintf(&b, "language: --lang %s kept %d of %d documents, dropped %d (%.1f%%)\n",
			want, total-dropped, total, dropped, 100*float64(dropped)/float64(total))
	}
	b.WriteString("language: detected")
	for _, p := range pairs {
		fmt.Fprintf(&b, " %s=%d", p.code, p.n)
	}
	b.WriteString("\n")
	return b.String()
}

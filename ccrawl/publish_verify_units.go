package ccrawl

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/parquet-go/parquet-go"
)

// The two published datasets differ in what a shard is, and that decides what
// can be repaired. A ccrawl-urls shard is the projection of exactly one source
// part, so a bad one can be rebuilt on its own from the source that made it. A
// ccrawl-domains shard is a cut of one sequential stream, so rebuilding one
// means reading the source up to it, which is the publish path's job and not the
// verifier's. Verify checks both. Repair rebuilds urls shards, and for a graph
// it says what to run.

// VerifyURLCrawl checks every published shard of one crawl in the ccrawl-urls
// dataset against the source part list and the ledger. With o.Repair set it
// re-projects the shards that failed and commits them over what the hub has.
func VerifyURLCrawl(ctx context.Context, h *HTTPClient, cache *Cache, hf *HFClient, o URLPublishOptions, crawl string, vo VerifyOptions) (*VerifyReport, error) {
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
	if o.Subset == "" {
		o.Subset = "warc"
	}
	if vo.Logf == nil {
		vo.Logf = o.Logf
	}
	if vo.Schema == nil {
		vo.Schema = parquet.SchemaOf(URLRow{})
	}

	// The source part list is the truth about how many shards this crawl has, so
	// a part with no shard on the hub is a missing shard rather than a shard the
	// verifier never heard of.
	urls, err := ColumnarParquetURLs(ctx, h, cache, crawl, o.Subset, o.Source)
	if err != nil {
		return nil, err
	}
	crawlDir := filepath.Join(o.StageDir, "data", crawl)
	jobs := make([]urlPartJob, 0, len(urls))
	paths := make([]string, 0, len(urls))
	for _, u := range urls {
		idx, ok := partIndexFromURL(u)
		if !ok {
			return nil, fmt.Errorf("cannot parse part index from %q", u)
		}
		repoPath := fmt.Sprintf("data/%s/part-%05d.parquet", crawl, idx)
		paths = append(paths, repoPath)
		jobs = append(jobs, urlPartJob{
			index:     idx,
			sourceURL: u,
			repoPath:  repoPath,
			tmpPath:   filepath.Join(crawlDir, fmt.Sprintf("part-%05d.parquet.tmp", idx)),
			outPath:   filepath.Join(crawlDir, fmt.Sprintf("part-%05d.parquet", idx)),
		})
	}

	rep, err := VerifyShards(ctx, h, HFShardStore{HF: hf, Repo: o.Repo}, paths, vo)
	if err != nil {
		return nil, err
	}
	rep.Repo, rep.Scope = o.Repo, crawl

	statsPath, err := seedStats(ctx, hf, o.Repo, o.StageDir, o.Logf)
	if err != nil {
		return nil, err
	}
	base := findURLStat(statsPath, crawl)
	rep.LedgerShards, rep.LedgerRows, rep.LedgerBytes = base.Shards, base.Rows, base.ParquetBytes
	rep.Notes = ledgerNotes(rep, base.Shards > 0 || base.Rows > 0)

	if !vo.Repair || rep.Failed == 0 {
		return rep, nil
	}
	if err := os.MkdirAll(crawlDir, 0o755); err != nil {
		return nil, err
	}
	// Check.Index is the position in the path list, which is the position in
	// the job list, because both were built from the same source parts.
	var work []urlPartJob
	for _, c := range rep.Failures() {
		if c.Index < 0 || c.Index >= len(jobs) {
			return nil, fmt.Errorf("no source part for %s", c.Path)
		}
		work = append(work, jobs[c.Index])
	}
	repaired, err := repairURLShards(ctx, h, hf, o, crawl, work)
	if err != nil {
		return rep, err
	}
	markRepaired(rep, repaired)
	return rep, nil
}

// repairURLShards re-projects the given parts from the source and commits them
// over whatever the hub is holding, which is what fixes a truncated upload. It
// returns the repo paths that were rewritten.
func repairURLShards(ctx context.Context, h *HTTPClient, hf *HFClient, o URLPublishOptions, crawl string, work []urlPartJob) ([]string, error) {
	if o.DoCommit && !hf.Valid() {
		return nil, errors.New("no HF token: set HF_TOKEN to repair")
	}
	commitEvery := o.CommitEvery
	if commitEvery <= 0 {
		commitEvery = 16
	}
	c := &committer{
		hf:          hf,
		repo:        o.Repo,
		scope:       crawl,
		kind:        "url",
		width:       5,
		commitEvery: commitEvery,
		keep:        o.Keep,
		doCommit:    o.DoCommit,
		logf:        o.Logf,
	}
	var done []string
	for _, j := range work {
		o.Logf("repair %s: re-projecting %s", j.repoPath, j.sourceURL)
		rows, bytes, err := projectURLPart(ctx, h, j, o.Whole)
		if err != nil {
			return done, fmt.Errorf("project %s again: %w", j.repoPath, err)
		}
		if err := c.add(ctx, shard{Index: j.index, RepoPath: j.repoPath, Local: j.outPath, Rows: rows, Bytes: bytes}); err != nil {
			return done, err
		}
		done = append(done, j.repoPath)
	}
	if err := c.flush(ctx); err != nil {
		return done, err
	}
	return done, nil
}

// VerifyDomainGraph checks every published shard of one web-graph release in the
// ccrawl-domains dataset. The source is a single stream with no part list, so
// the shard count comes from the ledger, and from a probe of the hub when the
// ledger has nothing to say.
func VerifyDomainGraph(ctx context.Context, h *HTTPClient, hf *HFClient, o DomainPublishOptions, vo VerifyOptions) (*VerifyReport, error) {
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
	if vo.Logf == nil {
		vo.Logf = o.Logf
	}
	if vo.Schema == nil {
		vo.Schema = parquet.SchemaOf(DomainRow{})
	}
	graph := o.Graph.ID

	statsPath, err := seedStats(ctx, hf, o.Repo, o.StageDir, o.Logf)
	if err != nil {
		return nil, err
	}
	base := findDomainStat(statsPath, graph)

	var paths []string
	if base.Shards > 0 {
		for i := 0; i < base.Shards; i++ {
			paths = append(paths, fmt.Sprintf("data/%s/part-%03d.parquet", graph, i))
		}
	} else {
		// No ledger row: ask the hub what it has and check that, which cannot
		// report a missing shard but can still report a bad one.
		probe := make([]string, 0, 512)
		for i := range 512 {
			probe = append(probe, fmt.Sprintf("data/%s/part-%03d.parquet", graph, i))
		}
		have, err := hf.PathsExist(ctx, o.Repo, probe)
		if err != nil {
			return nil, err
		}
		for _, p := range probe {
			if have[p] {
				paths = append(paths, p)
			}
		}
		o.Logf("graph %s: no ledger row, checking the %d shards the hub has", graph, len(paths))
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("graph %s: nothing published to verify", graph)
	}

	rep, err := VerifyShards(ctx, h, HFShardStore{HF: hf, Repo: o.Repo}, paths, vo)
	if err != nil {
		return nil, err
	}
	rep.Repo, rep.Scope = o.Repo, graph
	rep.LedgerShards, rep.LedgerRows, rep.LedgerBytes = base.Shards, base.Domains, base.ParquetBytes
	rep.Notes = ledgerNotes(rep, base.Shards > 0)
	return rep, nil
}

// seedStats makes sure a local stats.csv exists to compare against, pulling the
// hub's copy when there is none. A ledger that cannot be fetched is not fatal:
// the shard checks still stand on their own.
func seedStats(ctx context.Context, hf *HFClient, repo, stageDir string, logf func(string, ...any)) (string, error) {
	if err := os.MkdirAll(stageDir, 0o755); err != nil {
		return "", err
	}
	statsPath := filepath.Join(stageDir, "stats.csv")
	if _, err := os.Stat(statsPath); os.IsNotExist(err) {
		if _, err := hf.DownloadRepoFile(ctx, repo, "stats.csv", statsPath); err != nil {
			logf("warning: could not seed stats.csv from hub: %v", err)
		}
	}
	return statsPath, nil
}

// ledgerNotes is where the shards and the ledger are compared. The ledger is
// what the dataset card reports, so a disagreement is a real finding even when
// every shard passes: it means the numbers the dataset advertises are not the
// numbers it holds.
func ledgerNotes(rep *VerifyReport, haveLedger bool) []string {
	if !haveLedger {
		return []string{"no ledger row for this unit, so there is nothing to reconcile the shards against"}
	}
	present := 0
	for _, c := range rep.Checks {
		if c.Status != VerifyMissing {
			present++
		}
	}
	var notes []string
	if rep.LedgerShards != present {
		notes = append(notes, fmt.Sprintf("the ledger says %d shards and the hub has %d", rep.LedgerShards, present))
	}
	if rep.LedgerRows != rep.Rows {
		notes = append(notes, fmt.Sprintf("the ledger says %s rows and the shards hold %s",
			humanCountShort(rep.LedgerRows), humanCountShort(rep.Rows)))
	}
	if rep.LedgerBytes != rep.Bytes {
		notes = append(notes, fmt.Sprintf("the ledger says %s and the hub is holding %s",
			humanBytes(rep.LedgerBytes), humanBytes(rep.Bytes)))
	}
	return notes
}

// markRepaired ticks the checks whose shards were rewritten.
func markRepaired(rep *VerifyReport, repaired []string) {
	if len(repaired) == 0 {
		return
	}
	sort.Strings(repaired)
	done := make(map[string]bool, len(repaired))
	for _, p := range repaired {
		done[p] = true
	}
	for i := range rep.Checks {
		if done[rep.Checks[i].Path] {
			rep.Checks[i].Repaired = true
		}
	}
}

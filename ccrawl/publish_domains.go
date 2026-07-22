package ccrawl

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// DomainPublishOptions configures a ccrawl-domains publish run.
type DomainPublishOptions struct {
	Repo        string    // target dataset repo, org/name
	Graph       WebGraph  // web-graph release to publish
	ShardRows   int       // rows per output shard
	StageDir    string    // local staging root
	CommitEvery int       // shards per commit
	Private     bool      // create the repo private
	Keep        bool      // keep local shards after commit
	DoCommit    bool      // false is a dry run
	MinFreeGB   int       // free-disk floor
	MaxStall    time.Duration
	Logf        func(string, ...any)
}

// DefaultShardRows is the row count that cuts the single pre-sorted ranks stream
// into readable, evenly sized shards.
const DefaultShardRows = 5_000_000

// PublishDomains runs the ccrawl-domains pipeline. The source is one gzipped TSV
// per release, pre-sorted by harmonic centrality, so it streams top-to-bottom
// and cuts fixed-size shards without buffering the whole table. Rank order is
// preserved: part-000 holds the highest-centrality domains. It resumes by
// skipping shards already present on the hub.
func PublishDomains(ctx context.Context, h *HTTPClient, hf *HFClient, o DomainPublishOptions) error {
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
	if o.ShardRows <= 0 {
		o.ShardRows = DefaultShardRows
	}
	if o.CommitEvery <= 0 {
		o.CommitEvery = 4
	}
	if err := os.MkdirAll(o.StageDir, 0o755); err != nil {
		return err
	}
	sweepTemps(o.StageDir)

	statsPath := filepath.Join(o.StageDir, "stats.csv")
	progressPath := filepath.Join(o.StageDir, "publish-progress.json")
	graph := o.Graph.ID

	if o.DoCommit {
		if !hf.Valid() {
			return errors.New("no HF token: set HF_TOKEN to publish")
		}
		if err := hf.CreateDatasetRepo(ctx, o.Repo, o.Private); err != nil {
			return err
		}
		if _, err := os.Stat(statsPath); os.IsNotExist(err) {
			if _, err := hf.DownloadRepoFile(ctx, o.Repo, "stats.csv", statsPath); err != nil {
				o.Logf("warning: could not seed stats.csv from hub: %v", err)
			}
		}
	}

	// A committed release is already whole: its ledger row is complete. The
	// source is a single object, so there is no cheap way to know the shard
	// count ahead of the stream; the ledger is the resume signal.
	if !o.DoCommit {
		// Dry runs always re-stream.
	} else if base := findDomainStat(statsPath, graph); base.Shards > 0 && base.CommittedAt != "" {
		// Confirm the last shard is really on the hub before declaring done.
		last := fmt.Sprintf("data/%s/part-%03d.parquet", graph, base.Shards-1)
		exist, err := hf.PathsExist(ctx, o.Repo, []string{last})
		if err != nil {
			return err
		}
		if exist[last] {
			o.Logf("graph %s: already complete (%d shards, %s domains)", graph, base.Shards, humanCountShort(base.Domains))
			return nil
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	clock := newStallClock(o.MaxStall, cancel)
	go clock.watch(runCtx)

	if err := os.MkdirAll(filepath.Join(o.StageDir, "data", graph), 0o755); err != nil {
		return err
	}

	// Seed byte rollup from the ledger so a resume that skips already-committed
	// shards still reports the full release size. Skipped shards are counted
	// into the shard total in closeShard.
	base := findDomainStat(statsPath, graph)
	c := &committer{
		hf:           hf,
		repo:         o.Repo,
		scope:        graph,
		kind:         "domain",
		width:        3,
		commitEvery:  o.CommitEvery,
		keep:         o.Keep,
		doCommit:     o.DoCommit,
		progressKey:  graph,
		progressPath: progressPath,
		clock:        clock,
		logf:         o.Logf,
		bytes:        base.ParquetBytes,
	}

	url := o.Graph.DomainRankURL()
	srcBytes, domains, err := streamDomainShards(runCtx, h, hf, o, url, c)
	if err != nil {
		if clock.stalled() {
			return ErrCommitStall
		}
		return err
	}
	if err := c.flush(runCtx); err != nil {
		if clock.stalled() {
			return ErrCommitStall
		}
		return err
	}
	if clock.stalled() {
		return ErrCommitStall
	}

	return finalizeDomainGraph(runCtx, hf, o, graph, c, statsPath, srcBytes, domains)
}

// streamDomainShards reads the gzipped ranks table line by line, writing a new
// Parquet shard every ShardRows rows and handing each finished shard to the
// committer. Shards already on the hub are skipped without re-downloading their
// rows, but the stream is still read through so ordering stays exact.
func streamDomainShards(ctx context.Context, h *HTTPClient, hf *HFClient, o DomainPublishOptions, url string, c *committer) (srcBytes, domains int64, err error) {
	graph := o.Graph.ID

	// Preflight the done-set for the shards we expect, so a resume skips whole
	// committed shards. We do not know the final count, so probe generously and
	// treat missing as not-done.
	done := map[string]bool{}
	if o.DoCommit {
		probe := make([]string, 0, 512)
		for i := range 512 {
			probe = append(probe, fmt.Sprintf("data/%s/part-%03d.parquet", graph, i))
		}
		if done, err = hf.PathsExist(ctx, o.Repo, probe); err != nil {
			return 0, 0, err
		}
	}

	resp, err := h.GetDownload(ctx, url)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return 0, 0, fmt.Errorf("domain ranks HTTP %d (%s)", resp.StatusCode, url)
	}
	srcBytes = resp.ContentLength

	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = gz.Close() }()

	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 1<<20), 8<<20)

	shardIdx := 0
	var w *ParquetWriter[DomainRow]
	var outPath, tmpPath, repoPath string
	shardActive := false // false when the current shard is skipped (already done)
	var rowsInShard int64

	openShard := func() error {
		repoPath = fmt.Sprintf("data/%s/part-%03d.parquet", graph, shardIdx)
		outPath = filepath.Join(o.StageDir, "data", graph, fmt.Sprintf("part-%03d.parquet", shardIdx))
		tmpPath = outPath + ".tmp"
		rowsInShard = 0
		if done[repoPath] {
			shardActive = false
			return nil
		}
		if err := waitForDiskFloor(ctx, o.StageDir, o.MinFreeGB, c.clock); err != nil {
			return err
		}
		pw, err := NewParquetWriter[DomainRow](tmpPath)
		if err != nil {
			return err
		}
		w = pw
		shardActive = true
		return nil
	}

	closeShard := func() error {
		if !shardActive {
			// Shard was already on the hub. Count it toward the total so the
			// finalize rollup is right, then move on without a re-commit.
			c.committed++
			shardIdx++
			return nil
		}
		if err := w.Close(); err != nil {
			_ = os.Remove(tmpPath)
			return err
		}
		if err := os.Rename(tmpPath, outPath); err != nil {
			return err
		}
		fi, err := os.Stat(outPath)
		if err != nil {
			return err
		}
		if err := c.add(ctx, shard{Index: shardIdx, RepoPath: repoPath, Local: outPath, Rows: w.Rows(), Bytes: fi.Size()}); err != nil {
			return err
		}
		shardIdx++
		w = nil
		return nil
	}

	if err := openShard(); err != nil {
		return srcBytes, domains, err
	}

	for sc.Scan() {
		if err := ctx.Err(); err != nil {
			return srcBytes, domains, err
		}
		line := sc.Text()
		if line == "" || rankComment(line) {
			continue
		}
		row, ok := parseDomainLine(line)
		if !ok {
			continue
		}
		domains++
		if shardActive {
			if err := w.Write(row); err != nil {
				return srcBytes, domains, err
			}
		}
		rowsInShard++
		if rowsInShard >= int64(o.ShardRows) {
			if err := closeShard(); err != nil {
				return srcBytes, domains, err
			}
			if err := openShard(); err != nil {
				return srcBytes, domains, err
			}
		}
	}
	if err := sc.Err(); err != nil {
		return srcBytes, domains, err
	}
	// Flush the trailing partial shard when it holds rows.
	if rowsInShard > 0 {
		if err := closeShard(); err != nil {
			return srcBytes, domains, err
		}
	} else if shardActive && w != nil {
		// Empty trailing shard: discard the staged temp.
		_ = w.Close()
		_ = os.Remove(tmpPath)
	}
	return srcBytes, domains, nil
}

// parseDomainLine parses one tab-separated ranks row into a DomainRow. The
// source columns are harmonicc_pos, harmonicc_val, pr_pos, pr_val, host_rev,
// n_hosts; host_rev is un-reversed into a plain domain.
func parseDomainLine(line string) (DomainRow, bool) {
	f := strings.Split(line, "\t")
	if len(f) < 5 {
		return DomainRow{}, false
	}
	hpos, err1 := strconv.ParseInt(f[0], 10, 64)
	hval, err2 := strconv.ParseFloat(f[1], 64)
	ppos, err3 := strconv.ParseInt(f[2], 10, 64)
	pval, err4 := strconv.ParseFloat(f[3], 64)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
		return DomainRow{}, false
	}
	var nhosts int64
	if len(f) >= 6 {
		nhosts, _ = strconv.ParseInt(f[5], 10, 64)
	}
	return DomainRow{
		Domain:      reverseHost(f[hostRevField]),
		HarmonicPos: hpos,
		HarmonicVal: hval,
		PagerankPos: ppos,
		PagerankVal: pval,
		NHosts:      nhosts,
	}, true
}

// finalizeDomainGraph upserts the release ledger row, regenerates the card, and
// commits both.
func finalizeDomainGraph(ctx context.Context, hf *HFClient, o DomainPublishOptions, graph string, c *committer, statsPath string, srcBytes, domains int64) error {
	now := time.Now().UTC().Format(time.RFC3339)
	stat := DomainGraphStat{
		Graph:        graph,
		Shards:       c.committed,
		Domains:      domains,
		ParquetBytes: c.bytes,
		SourceBytes:  srcBytes,
		ShardRows:    o.ShardRows,
		CommittedAt:  now,
	}

	rows, err := ReadDomainStats(statsPath)
	if err != nil {
		return err
	}
	rows = UpsertDomainStat(rows, stat)
	if err := WriteDomainStats(statsPath, rows); err != nil {
		return err
	}

	readmePath := filepath.Join(o.StageDir, "README.md")
	if err := os.WriteFile(readmePath, []byte(GenerateDomainsREADME(o.Repo, rows)), 0o644); err != nil {
		return err
	}

	o.Logf("graph %s: %d shards, %s domains, %s", graph, stat.Shards, humanCountShort(stat.Domains), humanBytes(stat.ParquetBytes))

	if !o.DoCommit {
		o.Logf("[dry-run] would update ledger and card for %s", graph)
		return nil
	}
	ops := []HFOperation{
		{LocalPath: statsPath, PathInRepo: "stats.csv"},
		{LocalPath: readmePath, PathInRepo: "README.md"},
	}
	if _, err := hf.CommitWithRetry(ctx, o.Repo, finalizeDomainMessage(stat), ops, 5); err != nil {
		return err
	}
	c.clock.mark()
	return nil
}

func findDomainStat(statsPath, graph string) DomainGraphStat {
	rows, err := ReadDomainStats(statsPath)
	if err != nil {
		return DomainGraphStat{Graph: graph}
	}
	for _, r := range rows {
		if r.Graph == graph {
			return r
		}
	}
	return DomainGraphStat{Graph: graph}
}

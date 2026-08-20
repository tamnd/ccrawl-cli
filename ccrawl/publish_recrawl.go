package ccrawl

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

// The recrawl publisher is a second process that watches the crawl's output
// directory and commits shards as they close.
//
// It is a separate process rather than a stage inside the crawl for one
// practical reason: a run measured in months has to survive the publisher being
// restarted, reconfigured, or pointed at a different repo, without stopping the
// fetch. The crawl writes files and the publisher moves them, and the only thing
// they share is a directory and the rule that a .parquet in it is a whole shard.
// The crawl writes to .parquet.tmp and renames on seal, so there is no window
// where the publisher can pick up a file whose footer is not on the platter yet.

// DefaultRecrawlCommitEvery is how many shards go into one commit. Shards here
// are large and slow to fill, so batching four keeps commit overhead low without
// letting the hub lag far behind what has been fetched.
const DefaultRecrawlCommitEvery = 4

// DefaultRecrawlPoll is how often a watching publisher looks for new shards.
const DefaultRecrawlPoll = 30 * time.Second

// RecrawlPublishConfig is one server publishing its slice of a recrawl into one
// dataset repo.
type RecrawlPublishConfig struct {
	// Dir is the crawl's output directory, the same one recrawl run writes into.
	Dir string
	// Repo is the dataset repo, for example open-index/ccrawl-recrawl-domains.
	Repo string
	// Kind is "domains" or "urls" and picks the prose on the card.
	Kind string
	// Server names the machine, and is what makes this server's shards and its
	// ledger file distinct from the other two.
	Server string
	// Shard and Shards are the slice of the work list this server took, copied
	// from the recrawl run it is publishing for.
	Shard, Shards int
	// StatePath is the crawl's checkpoint file. The publisher reads it, never
	// writes it, so the card can report how far into the work list this server
	// has got rather than only how many files it has sent.
	StatePath string

	CommitEvery int
	// Poll turns the publisher into a watcher. Zero makes one pass over whatever
	// is in the directory and returns.
	Poll time.Duration
	// Keep leaves committed shards on disk. Off by default, because the point of
	// committing as shards close is that disk stays flat over a hundred days.
	Keep bool
	// DoCommit false stages and prints without touching the hub.
	DoCommit bool
	Private  bool

	Logf func(string, ...any)
}

func (c *RecrawlPublishConfig) applyDefaults() {
	if c.CommitEvery <= 0 {
		c.CommitEvery = DefaultRecrawlCommitEvery
	}
	if c.Kind == "" {
		c.Kind = "domains"
	}
	if c.Logf == nil {
		c.Logf = func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "recrawl publish: "+format+"\n", args...)
		}
	}
}

// Validate rejects a config that would produce a repo nobody can read.
func (c RecrawlPublishConfig) Validate() error {
	if strings.TrimSpace(c.Dir) == "" {
		return fmt.Errorf("name the capture directory to publish from with --dir")
	}
	if !strings.Contains(c.Repo, "/") {
		return fmt.Errorf("the repo %q is not an org/name dataset ID", c.Repo)
	}
	if strings.TrimSpace(c.Server) == "" {
		return fmt.Errorf("name this machine with --server, because it is what keeps its ledger apart from the rest of the fleet")
	}
	switch c.Kind {
	case "domains", "urls":
	default:
		return fmt.Errorf("the kind %q is neither domains nor urls", c.Kind)
	}
	return (Shard{Index: c.Shard, Count: c.Shards}).Validate()
}

// PublishRecrawl commits closed capture shards to a dataset repo and keeps the
// ledger and card in step with them, returning this server's ledger row.
func PublishRecrawl(ctx context.Context, hf *HFClient, cfg RecrawlPublishConfig) (RecrawlStat, error) {
	cfg.applyDefaults()
	var stat RecrawlStat
	if err := cfg.Validate(); err != nil {
		return stat, err
	}
	if cfg.DoCommit && !hf.Valid() {
		return stat, fmt.Errorf("publishing needs a HuggingFace token, set HF_TOKEN or pass --no-push to rehearse")
	}

	work := filepath.Join(cfg.Dir, ".publish")
	if err := os.MkdirAll(work, 0o755); err != nil {
		return stat, err
	}
	ledgerPath := RecrawlLedgerPath(cfg.Server, cfg.Shard, cfg.Shards)

	if cfg.DoCommit {
		if err := hf.CreateDatasetRepo(ctx, cfg.Repo, cfg.Private); err != nil {
			return stat, err
		}
	}

	// Resume from the hub rather than from anything local. A publisher that has
	// been moved to a new disk, or whose local staging was wiped, has to carry on
	// the same row it was writing before, or the file numbering restarts and the
	// card starts counting from zero halfway through a crawl.
	stat = RecrawlStat{Server: cfg.Server, Shard: cfg.Shard, Shards: cfg.Shards}
	if mine, err := fetchRecrawlLedger(ctx, hf, cfg.Repo, ledgerPath, filepath.Join(work, "mine.csv")); err != nil {
		return stat, err
	} else if len(mine) > 0 {
		stat = mine[0]
		stat.Shard, stat.Shards = cfg.Shard, cfg.Shards
		cfg.Logf("resuming from the hub at %d files, %s rows", stat.Files, fmtInt(stat.Rows))
	}

	p := &recrawlPublisher{cfg: cfg, hf: hf, work: work, ledgerPath: ledgerPath, stat: stat}
	c := &committer{
		hf:          hf,
		repo:        cfg.Repo,
		scope:       recrawlSlug(cfg.Server),
		kind:        "capture",
		width:       5,
		commitEvery: cfg.CommitEvery,
		keep:        cfg.Keep,
		doCommit:    cfg.DoCommit,
		logf:        cfg.Logf,
		sidecar: func(shards int, rows, bytes int64, _ []shard) ([]HFOperation, error) {
			return p.sidecar(ctx, shards, rows, bytes)
		},
	}
	c.committed = stat.Files
	c.rows, c.bytes = stat.Rows, stat.Bytes

	for {
		n, err := p.pass(ctx, c)
		if err != nil {
			return p.stat, err
		}
		if cfg.Poll <= 0 {
			break
		}
		// A watching publisher stops on its own when the crawl says the work list
		// is walked out and the directory it was draining is empty. Without that
		// the fleet would need a second signal to shut the publisher down, and a
		// forgotten publisher polling an empty directory for a week is exactly the
		// kind of thing nobody notices.
		if n == 0 && p.crawlDone() {
			cfg.Logf("the crawl is finished and the directory is drained, stopping")
			break
		}
		select {
		case <-ctx.Done():
			return p.stat, nil
		case <-time.After(cfg.Poll):
		}
	}
	return p.stat, nil
}

type recrawlPublisher struct {
	cfg        RecrawlPublishConfig
	hf         *HFClient
	work       string
	ledgerPath string
	stat       RecrawlStat
}

// pass commits everything closed in the directory right now and reports how many
// shards it moved.
func (p *recrawlPublisher) pass(ctx context.Context, c *committer) (int, error) {
	files, err := closedShards(p.cfg.Dir)
	if err != nil {
		return 0, err
	}
	if len(files) == 0 {
		return 0, nil
	}
	// Ask the hub which of these are already there. A publisher killed between
	// the commit landing and the local delete sees the same file again on the
	// next pass, and the path is derived from the content, so the answer is exact
	// rather than a guess from a counter that restarted.
	staged := make([]shard, 0, len(files))
	paths := make([]string, 0, len(files))
	for _, f := range files {
		s, err := p.inspect(f)
		if err != nil {
			return 0, err
		}
		staged = append(staged, s)
		paths = append(paths, s.RepoPath)
	}
	exists := map[string]bool{}
	if p.cfg.DoCommit {
		if exists, err = p.hf.PathsExist(ctx, p.cfg.Repo, paths); err != nil {
			return 0, err
		}
	}

	moved := 0
	for _, s := range staged {
		if exists[s.RepoPath] {
			p.cfg.Logf("%s is already published as %s, dropping the local copy", filepath.Base(s.Local), s.RepoPath)
			if !p.cfg.Keep {
				_ = os.Remove(s.Local)
			}
			continue
		}
		// The index is only used for the commit message, and skipping duplicates
		// leaves gaps in it, so renumber against what is actually being sent.
		s.Index = c.committed + len(c.batch)
		if err := c.add(ctx, s); err != nil {
			return moved, err
		}
		moved++
	}
	if err := c.flush(ctx); err != nil {
		return moved, err
	}
	return moved, nil
}

// inspect reads a closed shard's row count, size, and content hash. Index is
// left at zero and set when the shard is added to a batch, because duplicates
// are dropped in between and the number is only there to make a commit message
// readable.
func (p *recrawlPublisher) inspect(local string) (shard, error) {
	f, err := os.Open(local)
	if err != nil {
		return shard{}, err
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return shard{}, err
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return shard{}, err
	}
	sum := hex.EncodeToString(h.Sum(nil))[:12]
	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		return shard{}, fmt.Errorf("the shard %s is not readable Parquet: %w", local, err)
	}
	return shard{
		Local:    local,
		RepoPath: RecrawlShardPath(p.cfg.Server, p.cfg.Shard, p.cfg.Shards, sum),
		Rows:     pf.NumRows(),
		Bytes:    fi.Size(),
	}, nil
}

// RecrawlShardPath is where one server's shard lives in the repo.
//
// The name carries the content hash rather than a counter because the crawl's
// own file numbering restarts at zero on every run and the files are deleted
// once they are committed, so a counter would either collide with a shard
// already on the hub or publish the same rows twice under two names. Hashing the
// bytes makes republishing the same file a no-op that the existence check
// catches before any of it is uploaded.
func RecrawlShardPath(server string, shard, shards int, sum string) string {
	return fmt.Sprintf("data/%s-shard%dof%d-%s.parquet", recrawlSlug(server), shard, shards, sum)
}

// closedShards lists the sealed shards in the capture directory, oldest name
// first. Files still being written end in .parquet.tmp and are invisible here,
// which is the whole reason the writer renames on seal.
func closedShards(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".parquet") {
			continue
		}
		out = append(out, filepath.Join(dir, e.Name()))
	}
	sort.Strings(out)
	return out, nil
}

// crawlDone reports whether the crawl this publisher is draining has walked its
// slice of the work list out. No state file means no answer, and the publisher
// keeps watching.
func (p *recrawlPublisher) crawlDone() bool {
	if p.cfg.StatePath == "" {
		return false
	}
	ck, err := LoadCheckpoint(p.cfg.StatePath)
	if err != nil {
		return false
	}
	return ck.Done
}

// sidecar brings the ledger and the card up to the state this batch will leave
// the repo in, and hands both back to be committed with the shards.
func (p *recrawlPublisher) sidecar(ctx context.Context, shards int, rows, bytes int64) ([]HFOperation, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	p.stat.Files, p.stat.Rows, p.stat.Bytes = shards, rows, bytes
	if p.stat.FirstCommitted == "" {
		p.stat.FirstCommitted = now
	}
	p.stat.LastCommitted = now
	if p.cfg.StatePath != "" {
		if ck, err := LoadCheckpoint(p.cfg.StatePath); err == nil {
			p.stat.Part, p.stat.Row, p.stat.Done = ck.Part, ck.Row, ck.Done
		}
	}

	local := filepath.Join(p.work, "ledger.csv")
	if err := WriteRecrawlStats(local, []RecrawlStat{p.stat}); err != nil {
		return nil, err
	}

	// The card is generated from the union of every ledger file on the hub with
	// our own row swapped in fresh, so it reflects the whole fleet without any
	// server ever writing another server's file. A peer row that lands between
	// the listing and the commit is missed for one batch and picked up by the
	// next commit from any machine, which is why the card undercounts at worst.
	peers, err := p.peerLedgers(ctx)
	if err != nil {
		return nil, err
	}
	merged := MergeRecrawlStats(peers, []RecrawlStat{p.stat})
	readme := filepath.Join(p.work, "README.md")
	if err := os.WriteFile(readme, []byte(GenerateRecrawlREADME(p.cfg.Repo, p.cfg.Kind, merged)), 0o644); err != nil {
		return nil, err
	}
	return []HFOperation{
		{LocalPath: local, PathInRepo: p.ledgerPath},
		{LocalPath: readme, PathInRepo: "README.md"},
	}, nil
}

// peerLedgers reads every ledger file on the hub except our own.
func (p *recrawlPublisher) peerLedgers(ctx context.Context) ([]RecrawlStat, error) {
	if !p.cfg.DoCommit {
		return nil, nil
	}
	files, err := p.hf.ListRepoFiles(ctx, p.cfg.Repo)
	if err != nil {
		return nil, err
	}
	var out []RecrawlStat
	for _, f := range files {
		if f == p.ledgerPath || path.Dir(f) != "ledger" || !strings.HasSuffix(f, ".csv") {
			continue
		}
		rows, err := fetchRecrawlLedger(ctx, p.hf, p.cfg.Repo, f, filepath.Join(p.work, "peer.csv"))
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}
	return out, nil
}

// fetchRecrawlLedger downloads one ledger file and parses it. A file that is not
// there yet is an empty ledger, which is what a fresh repo looks like.
func fetchRecrawlLedger(ctx context.Context, hf *HFClient, repo, repoPath, local string) ([]RecrawlStat, error) {
	if !hf.Valid() {
		return nil, nil
	}
	ok, err := hf.DownloadRepoFile(ctx, repo, repoPath, local)
	if err != nil || !ok {
		return nil, err
	}
	defer func() { _ = os.Remove(local) }()
	return ReadRecrawlStats(local)
}

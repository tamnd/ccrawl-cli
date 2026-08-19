package ccrawl

import (
	"context"
	"testing"
)

// TestCommitterSidecarRunsOnDryRun pins what --no-push is for. It is a
// rehearsal, and the part of a publish most likely to be wrong is the rollup
// the sidecar folds and the card it writes, not the upload. A dry run that
// skipped the sidecar staged shards next to a card describing none of them, and
// the news language and byte counts came out zero because nothing folded them.
func TestCommitterSidecarRunsOnDryRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var gotShards int
	var gotRows, gotBytes int64
	var gotBatch []shard

	c := &committer{
		commitEvery: 2,
		doCommit:    false,
		clock:       newStallClock(0, cancel),
		logf:        func(string, ...any) {},
		sidecar: func(shards int, rows, bytes int64, batch []shard) ([]HFOperation, error) {
			gotShards, gotRows, gotBytes = shards, rows, bytes
			gotBatch = batch
			return []HFOperation{{LocalPath: "README.md", PathInRepo: "README.md"}}, nil
		},
	}

	if err := c.add(ctx, shard{Index: 0, RepoPath: "data/a.parquet", Rows: 10, Bytes: 100}); err != nil {
		t.Fatal(err)
	}
	if err := c.add(ctx, shard{Index: 1, RepoPath: "data/b.parquet", Rows: 5, Bytes: 50}); err != nil {
		t.Fatal(err)
	}

	if gotShards != 2 {
		t.Errorf("sidecar saw %d shards, want 2", gotShards)
	}
	if gotRows != 15 || gotBytes != 150 {
		t.Errorf("sidecar saw %d rows and %d bytes, want 15 and 150", gotRows, gotBytes)
	}
	if len(gotBatch) != 2 {
		t.Fatalf("sidecar saw a batch of %d, want 2", len(gotBatch))
	}
	// The batch is what a rollup keyed by repo path folds against, so the paths
	// have to arrive intact rather than as counts.
	if gotBatch[0].RepoPath != "data/a.parquet" || gotBatch[1].RepoPath != "data/b.parquet" {
		t.Errorf("batch paths = %q,%q", gotBatch[0].RepoPath, gotBatch[1].RepoPath)
	}
	// A dry run stages the local shards for inspection instead of deleting them,
	// and it does not touch the hub.
	if c.flushes != 0 {
		t.Errorf("dry run recorded %d flushes, want 0", c.flushes)
	}
	if c.committed != 2 || c.rows != 15 || c.bytes != 150 {
		t.Errorf("after flush: %d shards, %d rows, %d bytes", c.committed, c.rows, c.bytes)
	}
}

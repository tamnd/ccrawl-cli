package ccrawl

// A live test of three servers publishing into one real repo at the same time.
//
// The reason this exists rather than a unit test with a fake hub is that the
// failure it is guarding against is a property of the hub, not of our code. The
// hub has no compare-and-set on a path, so if the fleet shared one stats file,
// two servers reading it and writing it back would silently drop one another's
// numbers, and no amount of local testing would show that. The only honest way
// to check that one file per server fixes it is to point three publishers at a
// real repo, make them commit at once, and read back what is actually there.
//
// It is skipped unless you opt in, because it needs a token and it writes to a
// real repo:
//
//	HF_TOKEN=hf_... CCRAWL_RECRAWL_LIVE_REPO=your-org/ccrawl-recrawl-live \
//	go test ./ccrawl/ -run TestLiveRecrawlPublish -v -timeout 20m

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
)

func liveRecrawlRepo(t *testing.T) (*HFClient, string) {
	t.Helper()
	repo := os.Getenv("CCRAWL_RECRAWL_LIVE_REPO")
	if repo == "" {
		t.Skip("set CCRAWL_RECRAWL_LIVE_REPO to a scratch dataset repo to run this")
	}
	hf := NewHFClient("")
	if !hf.Valid() {
		t.Skip("set HF_TOKEN to run this")
	}
	return hf, repo
}

// TestLiveRecrawlPublishConcurrentServers runs three publishers into one repo
// with no coordination between them and checks that all three ledger rows
// survive and that every shard landed.
func TestLiveRecrawlPublishConcurrentServers(t *testing.T) {
	hf, repo := liveRecrawlRepo(t)
	ctx := context.Background()
	const servers = 3

	dirs := make([]string, servers)
	for i := range dirs {
		dirs[i] = t.TempDir()
		writeShards(t, dirs[i], fmt.Sprintf("live%d", i), 2, 20)
	}

	// Start all three at once. There is no gate here on purpose: on the real hub
	// the commits interleave however the network decides, which is the case the
	// design has to hold up under.
	var wg sync.WaitGroup
	stats := make([]RecrawlStat, servers)
	errs := make([]error, servers)
	for i := range servers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats[i], errs[i] = PublishRecrawl(ctx, hf, RecrawlPublishConfig{
				Dir:         dirs[i],
				Repo:        repo,
				Kind:        "domains",
				Server:      fmt.Sprintf("live-server%d", i+1),
				Shard:       i,
				Shards:      servers,
				CommitEvery: 1,
				DoCommit:    true,
				Private:     true,
				Logf:        func(f string, a ...any) { t.Logf(f, a...) },
			})
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("live-server%d: %v", i+1, err)
		}
	}

	// Read the fleet back off the hub the way the card generator does.
	files, err := hf.ListRepoFiles(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	var merged []RecrawlStat
	shards := 0
	for _, f := range files {
		switch {
		case len(f) > 5 && f[:5] == "data/":
			shards++
		case len(f) > 7 && f[:7] == "ledger/":
			local := t.TempDir() + "/l.csv"
			rows, err := fetchRecrawlLedger(ctx, hf, repo, f, local)
			if err != nil {
				t.Fatal(err)
			}
			merged = append(merged, rows...)
		}
	}
	total := TotalRecrawlStats(MergeRecrawlStats(merged))
	if total.Servers < servers {
		t.Fatalf("the hub reports %d servers after three published at once, want at least %d, so a ledger row was lost", total.Servers, servers)
	}
	if shards < servers*2 {
		t.Fatalf("the hub holds %d shards, want at least %d, so a concurrent commit dropped one", shards, servers*2)
	}
	for i, s := range stats {
		if s.Files != 2 || s.Rows != 40 {
			t.Errorf("live-server%d published %d files and %d rows, want 2 and 40", i+1, s.Files, s.Rows)
		}
	}
	t.Logf("live fleet: %d servers, %d shards, %d rows, %s on the hub",
		total.Servers, total.Files, total.Rows, humanBytes(total.Bytes))
}

// TestLiveRecrawlPublishIsIdempotent republishes the same shards and checks the
// repo does not grow, which is what a supervisor restarting the publisher mid
// commit looks like from the hub's side.
func TestLiveRecrawlPublishIsIdempotent(t *testing.T) {
	hf, repo := liveRecrawlRepo(t)
	ctx := context.Background()
	dir := t.TempDir()
	writeShards(t, dir, "idem", 2, 10)

	cfg := RecrawlPublishConfig{
		Dir: dir, Repo: repo, Kind: "domains", Server: "live-idem",
		Shard: 0, Shards: 1, CommitEvery: 2, DoCommit: true, Private: true,
		Keep: true,
		Logf: func(f string, a ...any) { t.Logf(f, a...) },
	}
	first, err := PublishRecrawl(ctx, hf, cfg)
	if err != nil {
		t.Fatal(err)
	}
	before, err := hf.ListRepoFiles(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}

	cfg.Keep = false
	second, err := PublishRecrawl(ctx, hf, cfg)
	if err != nil {
		t.Fatal(err)
	}
	after, err := hf.ListRepoFiles(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("the repo grew from %d files to %d on a replay, so a shard was published twice", len(before), len(after))
	}
	if second.Files != first.Files || second.Rows != first.Rows {
		t.Fatalf("the replay counted %d files and %d rows, want the same %d and %d", second.Files, second.Rows, first.Files, first.Rows)
	}
}

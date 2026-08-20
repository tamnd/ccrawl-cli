package cli

import (
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// TestCrawlRunShardTakesOnlyItsPartition walks the flag all the way through the
// command, which is the part unit tests on Shard cannot reach.
//
// Every loopback target shares the host 127.0.0.1 and so shares a partition key,
// which is the behaviour we want in production and inconvenient here: the whole
// seed list lands in one shard. That makes the test sharper rather than weaker.
// Ask which shard owns the key, run all three, and exactly one of them may crawl.
func TestCrawlRunShardTakesOnlyItsPartition(t *testing.T) {
	const shards = 3

	srv := crawlTarget(t)
	seeds := seedFile(t, []string{srv.URL + "/a", srv.URL + "/b", srv.URL + "/c"})

	owner := -1
	for i := range shards {
		if (ccrawl.Shard{Index: i, Count: shards}).Owns(srv.URL + "/a") {
			owner = i
		}
	}
	if owner < 0 {
		t.Fatal("no shard owns the loopback target, so the partition drops URLs")
	}

	for i := range shards {
		state := filepath.Join(t.TempDir(), "crawl.db")
		code, _, errOut := invoke(t, "", []string{
			"ccrawl", "crawl", "run",
			"--seeds", seeds, "--state", state,
			"--shard", strconv.Itoa(i), "--shards", strconv.Itoa(shards),
			"--delay", "0", "--no-robots", "-q",
		})
		if i == owner {
			if code != 0 {
				t.Fatalf("shard %d owns the seeds and exited %d: %s", i, code, errOut)
			}
			if !strings.Contains(errOut, "3 fetched") {
				t.Errorf("shard %d owns all three seeds, want 3 fetched, got: %s", i, errOut)
			}
			continue
		}
		if code == 0 {
			t.Errorf("shard %d owns none of the seeds and exited 0, so the filter is not being applied: %s", i, errOut)
		}
		if !strings.Contains(strings.ToLower(errOut), "shard") {
			t.Errorf("shard %d found no work and did not say why: %s", i, errOut)
		}
	}
}

// TestCrawlRunRejectsAShardThatDoesNotExist catches the off-by-one an operator
// makes once per fleet, writing --shard 3 --shards 3 for the third machine. The
// numbering is 0-based and the run has to say so rather than crawl nothing.
func TestCrawlRunRejectsAShardThatDoesNotExist(t *testing.T) {
	srv := crawlTarget(t)
	seeds := seedFile(t, []string{srv.URL + "/a"})

	for _, args := range [][]string{
		{"--shard", "3", "--shards", "3"},
		{"--shard", "-1", "--shards", "3"},
		{"--shards", "0"},
	} {
		state := filepath.Join(t.TempDir(), "crawl.db")
		argv := append([]string{
			"ccrawl", "crawl", "run", "--seeds", seeds, "--state", state, "--no-robots", "-q",
		}, args...)
		code, _, errOut := invoke(t, "", argv)
		if code == 0 {
			t.Errorf("crawl run %v exited 0, and that partition does not exist", args)
		}
		if !strings.Contains(strings.ToLower(errOut), "shard") {
			t.Errorf("crawl run %v failed without naming the shard: %s", args, errOut)
		}
	}
}

// TestCrawlRunUnshardedTakesEverything holds the default steady. A run with no
// shard flags has to behave exactly as it did before shards existed.
func TestCrawlRunUnshardedTakesEverything(t *testing.T) {
	srv := crawlTarget(t)
	seeds := seedFile(t, []string{srv.URL + "/a", srv.URL + "/b"})
	state := filepath.Join(t.TempDir(), "crawl.db")

	code, _, errOut := invoke(t, "", []string{
		"ccrawl", "crawl", "run",
		"--seeds", seeds, "--state", state, "--delay", "0", "--no-robots", "-q",
	})
	if code != 0 {
		t.Fatalf("crawl run exited %d: %s", code, errOut)
	}
	if !strings.Contains(errOut, "2 fetched") {
		t.Errorf("an unsharded run fetched something other than both seeds: %s", errOut)
	}
}

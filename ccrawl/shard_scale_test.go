package ccrawl

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

// domainRow is the one column the evenness check needs out of the released
// domain table.
type domainRow struct {
	Domain string `parquet:"domain"`
}

const domainPartsURL = "https://huggingface.co/datasets/open-index/ccrawl-domains/resolve/main/data/cc-main-2026-apr-may-jun/part-%03d.parquet"

// TestShardEvennessOnRealDomains measures how evenly the partition splits the
// real corpus, which is the only measurement that settles the question.
//
// Synthetic hosts prove nothing here. h0.example through h2000000.example are
// uniform by construction, so any hash at all splits them evenly, and the thing
// that could go wrong is exactly what synthetic names cannot show: real domains
// clump, a long tail of them share a handful of public suffixes, and a key that
// leans on the suffix rather than the name would pile a third of the web onto
// one machine while the harness said everything was fine.
//
// Off by default because it pulls the release over the network.
//
//	CCRAWL_SHARD_PARTS=100 go test ./ccrawl -run TestShardEvenness -v -timeout 2h
//
// The count is a ceiling rather than the real number of parts, since the
// harness stops at the first part the release does not have. Each part is about
// 105 MB of five million domains and is deleted as soon as it is counted, so
// the whole release costs one part of disk at a time however many there are.
// Set CCRAWL_SHARD_DIR to choose where that part lands.
func TestShardEvennessOnRealDomains(t *testing.T) {
	raw := os.Getenv("CCRAWL_SHARD_PARTS")
	if raw == "" {
		t.Skip("set CCRAWL_SHARD_PARTS to run the evenness harness against the released domain table")
	}
	parts, err := strconv.Atoi(raw)
	if err != nil || parts <= 0 {
		t.Fatalf("CCRAWL_SHARD_PARTS=%q is not a positive count", raw)
	}
	dir := os.Getenv("CCRAWL_SHARD_DIR")
	if dir == "" {
		dir = t.TempDir()
	}

	// Three is the fleet we are building. Seven and thirty two are here because
	// an evenness bug that hides at three shards rarely hides at thirty two.
	counts := map[int][]int{3: make([]int, 3), 7: make([]int, 7), 32: make([]int, 32)}
	var total int
	start := time.Now()

	for p := 0; p < parts; p++ {
		path := filepath.Join(dir, fmt.Sprintf("part-%03d.parquet", p))
		err := download(fmt.Sprintf(domainPartsURL, p), path)
		// A 404 is the end of the release rather than a failure, so the count is
		// a ceiling and not something anyone has to keep in step with the next
		// publish. Set it high and the harness reads whatever is there.
		if errors.Is(err, errNoSuchPart) {
			t.Logf("part %03d is not published, so the release ends at %d parts", p, p)
			break
		}
		if err != nil {
			t.Fatalf("part %d: %v", p, err)
		}
		rows, err := parquet.ReadFile[domainRow](path)
		if err != nil {
			t.Fatalf("part %d: %v", p, err)
		}
		for _, r := range rows {
			if r.Domain == "" {
				continue
			}
			// Through the same call the seed loader makes, scheme and all, rather
			// than hashing the bare string. A measurement of a different code path
			// than the fleet runs is not a measurement.
			//
			// The key is derived once and the three shard counts are asked about
			// it with OwnsKey, which is the split Owns makes internally. Deriving
			// it inside the loop instead would put the public suffix lookup, the
			// expensive half, through forty two times per domain and turn an hour
			// of measurement into a day of it.
			key := ShardKey("https://" + r.Domain + "/")
			for n, bucket := range counts {
				// An empty key is a domain with no host in it, which Owns puts in
				// shard 0 rather than dropping.
				idx := 0
				if key != "" {
					for i := range bucket {
						if (Shard{Index: i, Count: n}).OwnsKey(key) {
							idx = i
							break
						}
					}
				}
				bucket[idx]++
			}
			total++
		}
		if err := os.Remove(path); err != nil {
			t.Fatalf("part %d: %v", p, err)
		}
		t.Logf("part %03d done, %d domains, %.0f/s", p, total, float64(total)/time.Since(start).Seconds())
	}
	if total == 0 {
		t.Fatal("read no domains out of the release")
	}

	for _, n := range []int{3, 7, 32} {
		bucket := counts[n]
		var sum, worst int
		ideal := float64(total) / float64(n)
		var worstDrift float64
		for i, c := range bucket {
			sum += c
			if d := (float64(c) - ideal) / ideal * 100; d > worstDrift || -d > worstDrift {
				if d < 0 {
					d = -d
				}
				worstDrift, worst = d, i
			}
		}
		t.Logf("%2d shards: %d domains, worst shard %d is %.3f%% off even, counts %v", n, sum, worst, worstDrift, bucket)
		if sum != total {
			t.Errorf("%d shards accounted for %d of %d domains, so the partition loses or duplicates work", n, sum, total)
		}
		// A few percent is the bar from the issue. The real number is far under
		// it, and the floor is here to catch a key that starts clumping rather
		// than to describe what we measured.
		if worstDrift > 2 {
			t.Errorf("%d shards: shard %d is %.2f%% off even, want under 2%%", n, worst, worstDrift)
		}
	}
}

// errNoSuchPart says a part number is past the end of what is published, which
// is how the harness finds the edge of the release without being told.
var errNoSuchPart = errors.New("no such part")

// download fetches one part to disk, streaming so the part never has to be held
// in memory alongside the rows read out of it.
func download(url, path string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return errNoSuchPart
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", url, resp.Status)
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

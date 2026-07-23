package ccrawl

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/parquet-go/parquet-go"
)

// DomainDiff is the set comparison of two published web-graph domain releases.
// From is the older release, To the newer one. Added counts domains present in
// To but not in From, Removed counts domains in From but not in To, and Shared
// counts domains in both. The totals are the raw domain counts of each release.
type DomainDiff struct {
	From      string
	To        string
	FromTotal int64
	ToTotal   int64
	Added     int64
	Removed   int64
	Shared    int64
}

// Summary renders the diff as a short human-readable block: the two release ids,
// each release's domain total, and how many domains were added, removed, and
// shared. Counts are exact and comma-grouped so the deltas read at a glance.
func (d DomainDiff) Summary() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s -> %s\n", d.From, d.To)
	fmt.Fprintf(&b, "  from     %15s domains\n", commaInt(d.FromTotal))
	fmt.Fprintf(&b, "  to       %15s domains\n", commaInt(d.ToTotal))
	fmt.Fprintf(&b, "  added    %15s  (new in %s)\n", commaInt(d.Added), d.To)
	fmt.Fprintf(&b, "  removed  %15s  (gone from %s)\n", commaInt(d.Removed), d.From)
	fmt.Fprintf(&b, "  shared   %15s\n", commaInt(d.Shared))
	return b.String()
}

// commaInt formats an integer with thousands separators, so 121091933 reads as
// 121,091,933.
func commaInt(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}

// DomainDiffOptions configures a diff run.
type DomainDiffOptions struct {
	Repo    string // dataset repo holding both releases, org/name
	From    string // older graph id
	To      string // newer graph id
	Workers int    // concurrent shard readers (0 picks a default from CPU count)
	Logf    func(string, ...any)

	// Collect, when non-nil, receives every domain that is new in To (present in
	// To, absent from From). It lets a caller stream the added domains to a file
	// without the diff holding them all in memory. It is called under a lock, so
	// its callee sees one serialized stream.
	Collect func(string)
}

// domainOnly projects a domain shard to just its domain column, so parquet-go
// fetches only that column's chunks over the network, not the rank fields.
type domainOnly struct {
	Domain string `parquet:"domain"`
}

// hashDomain maps a domain to a 64-bit key. The diff compares releases by these
// keys rather than the strings themselves to keep the From set near a gigabyte
// instead of several: at 120M domains a 64-bit space makes a within-set
// collision vanishingly unlikely, so the counts are exact for all practical use.
func hashDomain(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// DiffDomainReleases compares two published domain releases in the same repo and
// returns how many domains were added, removed, and shared between them. It reads
// only the domain column of each shard straight from the hub, builds a sorted key
// set from the older release, then scans the newer release against it. Both
// releases must already be published.
func DiffDomainReleases(ctx context.Context, h *HTTPClient, hf *HFClient, o DomainDiffOptions) (DomainDiff, error) {
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
	if o.From == "" || o.To == "" {
		return DomainDiff{}, fmt.Errorf("both --from and --to graph ids are required")
	}
	if o.Workers <= 0 {
		o.Workers = budgetProcess(0)
	}

	fromPaths, fromSizes, err := publishedShardPaths(ctx, hf, o.Repo, o.From)
	if err != nil {
		return DomainDiff{}, fmt.Errorf("list %s shards: %w", o.From, err)
	}
	if len(fromPaths) == 0 {
		return DomainDiff{}, fmt.Errorf("release %s has no published shards in %s", o.From, o.Repo)
	}
	toPaths, toSizes, err := publishedShardPaths(ctx, hf, o.Repo, o.To)
	if err != nil {
		return DomainDiff{}, fmt.Errorf("list %s shards: %w", o.To, err)
	}
	if len(toPaths) == 0 {
		return DomainDiff{}, fmt.Errorf("release %s has no published shards in %s", o.To, o.Repo)
	}

	o.Logf("reading %s: %d shards", o.From, len(fromPaths))
	fromKeys, fromTotal, err := readDomainKeys(ctx, h, o.Repo, fromPaths, fromSizes, o.Workers)
	if err != nil {
		return DomainDiff{}, fmt.Errorf("read %s: %w", o.From, err)
	}
	sort.Slice(fromKeys, func(i, j int) bool { return fromKeys[i] < fromKeys[j] })

	o.Logf("reading %s: %d shards, diffing against %s domains", o.To, len(toPaths), humanCountShort(fromTotal))
	toTotal, shared, err := scanAgainstKeys(ctx, h, o.Repo, toPaths, toSizes, fromKeys, o.Collect)
	if err != nil {
		return DomainDiff{}, fmt.Errorf("scan %s: %w", o.To, err)
	}

	return DomainDiff{
		From:      o.From,
		To:        o.To,
		FromTotal: fromTotal,
		ToTotal:   toTotal,
		Added:     toTotal - shared,
		Removed:   fromTotal - shared,
		Shared:    shared,
	}, nil
}

// TwoNewestDomainReleases picks the two most recent releases in a domains ledger,
// the newest as To and the next-newest as From, considering only releases that
// were streamed to completion. It backs the diff command's default when neither
// graph id is named.
func TwoNewestDomainReleases(ledger []DomainGraphStat) (from, to string, err error) {
	ids := make([]string, 0, len(ledger))
	for _, r := range ledger {
		if r.Complete && r.Shards > 0 {
			ids = append(ids, r.Graph)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return graphRank(ids[i]) > graphRank(ids[j]) })
	if len(ids) < 2 {
		return "", "", fmt.Errorf("need two complete releases to diff, %s has %d", "the dataset", len(ids))
	}
	return ids[1], ids[0], nil
}

// publishedShardPaths returns the repo paths and byte sizes of every shard a
// release has on the hub. The final shard count is not known ahead of time, so it
// probes a generous range and keeps the ones that exist.
func publishedShardPaths(ctx context.Context, hf *HFClient, repo, graph string) ([]string, map[string]int64, error) {
	probe := make([]string, 0, 512)
	for i := range 512 {
		probe = append(probe, fmt.Sprintf("data/%s/part-%03d.parquet", graph, i))
	}
	done, err := hf.PathsExist(ctx, repo, probe)
	if err != nil {
		return nil, nil, err
	}
	published := make([]string, 0, len(done))
	for _, p := range probe {
		if done[p] {
			published = append(published, p)
		}
	}
	if len(published) == 0 {
		return nil, nil, nil
	}
	sizes, err := hf.PathsInfo(ctx, repo, published)
	if err != nil {
		return nil, nil, err
	}
	return published, sizes, nil
}

// readDomainKeys reads the domain column of every shard and returns the hashed
// keys of all domains plus the total count. Shards are read concurrently; each
// worker builds a local key slice so only the final merge is serialized.
func readDomainKeys(ctx context.Context, h *HTTPClient, repo string, paths []string, sizes map[string]int64, workers int) ([]uint64, int64, error) {
	type result struct {
		keys []uint64
		err  error
	}
	jobs := make(chan string)
	results := make(chan result)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				local := make([]uint64, 0, 1<<20)
				err := streamDomainColumn(ctx, h, hfResolveURL(repo, p), sizes[p], func(d string) error {
					local = append(local, hashDomain(d))
					return nil
				})
				results <- result{keys: local, err: err}
			}
		}()
	}
	go func() {
		for _, p := range paths {
			select {
			case jobs <- p:
			case <-ctx.Done():
				close(jobs)
				return
			}
		}
		close(jobs)
	}()
	go func() { wg.Wait(); close(results) }()

	keys := make([]uint64, 0, len(paths)*(1<<20))
	var firstErr error
	for r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		keys = append(keys, r.keys...)
	}
	if firstErr != nil {
		return nil, 0, firstErr
	}
	return keys, int64(len(keys)), nil
}

// scanAgainstKeys reads the domain column of every To shard and counts how many
// of its domains are present in the sorted From key set (shared) and the total
// scanned. When collect is non-nil, added domains are sent to it. Shards are read
// concurrently, but collect is invoked under a lock so its callee sees one stream.
func scanAgainstKeys(ctx context.Context, h *HTTPClient, repo string, paths []string, sizes map[string]int64, fromKeys []uint64, collect func(string)) (total, shared int64, err error) {
	var mu sync.Mutex
	var firstErr error
	jobs := make(chan string)
	var wg sync.WaitGroup
	workers := len(paths)
	if workers > 16 {
		workers = 16
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				var localTotal, localShared int64
				err := streamDomainColumn(ctx, h, hfResolveURL(repo, p), sizes[p], func(d string) error {
					localTotal++
					if keyInSet(fromKeys, hashDomain(d)) {
						localShared++
						return nil
					}
					if collect != nil {
						mu.Lock()
						collect(d)
						mu.Unlock()
					}
					return nil
				})
				mu.Lock()
				if err != nil && firstErr == nil {
					firstErr = err
				}
				total += localTotal
				shared += localShared
				mu.Unlock()
			}
		}()
	}
	for _, p := range paths {
		select {
		case jobs <- p:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return 0, 0, ctx.Err()
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return 0, 0, firstErr
	}
	return total, shared, nil
}

// keyInSet reports whether x is in the sorted key slice.
func keyInSet(sorted []uint64, x uint64) bool {
	i := sort.Search(len(sorted), func(i int) bool { return sorted[i] >= x })
	return i < len(sorted) && sorted[i] == x
}

// streamDomainColumn reads the domain column of one remote Parquet shard, calling
// fn for each domain. It uses the same ranged reader as the recount path, so only
// the domain column's chunks are fetched, not the whole shard.
func streamDomainColumn(ctx context.Context, h *HTTPClient, url string, size int64, fn func(string) error) error {
	if size <= 0 {
		n, err := h.ContentLength(ctx, url)
		if err != nil {
			return err
		}
		size = n
	}
	ra := newHTTPReaderAt(ctx, h, url, size, 8<<20, 4)
	return streamDomainColumnAt(ra, size, fn)
}

// streamDomainColumnAt reads the domain column from any ReaderAt over a Parquet
// file. It backs streamDomainColumn and keeps the read loop testable with a local
// file, no HTTP.
func streamDomainColumnAt(ra io.ReaderAt, size int64, fn func(string) error) error {
	pf, err := parquet.OpenFile(ra, size)
	if err != nil {
		return err
	}
	r := parquet.NewGenericReader[domainOnly](pf)
	defer func() { _ = r.Close() }()
	buf := make([]domainOnly, 4096)
	for {
		n, err := r.Read(buf)
		for i := 0; i < n; i++ {
			if ferr := fn(buf[i].Domain); ferr != nil {
				return ferr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

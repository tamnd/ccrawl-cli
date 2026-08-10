package ccrawl

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// DefaultNearDistance is the Hamming distance below which two simhashes are
// called near duplicates. Three of 64 bits is the usual working threshold: it
// catches the same article with different navigation or a changed date line, and
// it stays clear of the distance-10-and-up range where unrelated documents on
// the same topic start colliding.
const DefaultNearDistance = 3

// minNearBytes is the shortest document the near duplicate pass will judge.
//
// A 64 bit fingerprint is decided by a vote per distinct three word shingle, so
// a document with fewer features than it has bits has a fingerprint decided by
// noise, and two short pages that share a phrase land a bit or two apart while
// having nothing to do with each other. Sixty-odd shingles needs roughly sixty
// tokens, which is about 500 bytes of prose; this is that number as a byte count
// so the pass stays a scan of the stored column rather than a re-tokenization of
// the whole corpus. Shorter documents still get a fingerprint and still cluster
// as exact duplicates, which is the only claim worth making about them.
const minNearBytes = 512

// dedupRow is the projection the report needs. Parquet reads by name, so this
// loads three columns out of a file that has eleven, and it works unchanged on
// both the export and the refetch schema.
type dedupRow struct {
	URL      string `parquet:"url"`
	Markdown string `parquet:"markdown"`
	Simhash  uint64 `parquet:"simhash"`
}

// DedupCluster is a group of documents the report considers the same.
type DedupCluster struct {
	Kind  string   `json:"kind"` // "exact" or "near"
	Size  int      `json:"size"`
	URLs  []string `json:"urls"`
	Bytes int64    `json:"bytes"` // Markdown bytes the duplicates beyond the first hold
}

// DedupReport is what `ccrawl dedup` prints.
type DedupReport struct {
	Files int   `json:"files"`
	Rows  int64 `json:"rows"`
	// ExactDuplicates counts rows whose Markdown is byte identical to an earlier
	// row, so a cluster of three contributes two.
	ExactClusters   int   `json:"exact_clusters"`
	ExactDuplicates int64 `json:"exact_duplicates"`
	ExactBytes      int64 `json:"exact_bytes"`
	// NearDuplicates counts rows within NearDistance of an earlier row and not
	// already an exact duplicate of one.
	NearClusters   int   `json:"near_clusters"`
	NearDuplicates int64 `json:"near_duplicates"`
	NearBytes      int64 `json:"near_bytes"`
	NearDistance   int   `json:"near_distance"`
	// NoFingerprint counts rows too short for a simhash. They are reported rather
	// than folded into a cluster, because "no fingerprint" is not "identical".
	NoFingerprint int64          `json:"no_fingerprint"`
	Clusters      []DedupCluster `json:"clusters"`
}

// Summary renders the report for a terminal.
func (r DedupReport) Summary() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s rows in %d files\n", commaInt(r.Rows), r.Files)
	fmt.Fprintf(&b, "  exact duplicates %12s in %s clusters, %s\n",
		commaInt(r.ExactDuplicates), commaInt(int64(r.ExactClusters)), humanBytes(r.ExactBytes))
	fmt.Fprintf(&b, "  near duplicates  %12s in %s clusters, %s  (distance <= %d)\n",
		commaInt(r.NearDuplicates), commaInt(int64(r.NearClusters)), humanBytes(r.NearBytes), r.NearDistance)
	if r.Rows > 0 {
		dup := r.ExactDuplicates + r.NearDuplicates
		fmt.Fprintf(&b, "  redundant        %12s  (%.1f%% of rows)\n", commaInt(dup), 100*float64(dup)/float64(r.Rows))
	}
	if r.NoFingerprint > 0 {
		fmt.Fprintf(&b, "  no fingerprint   %12s  (too short to hash, left alone)\n", commaInt(r.NoFingerprint))
	}
	if len(r.Clusters) > 0 {
		fmt.Fprintf(&b, "\nlargest clusters\n")
		for _, c := range r.Clusters {
			fmt.Fprintf(&b, "  %-5s %4d docs  %s\n", c.Kind, c.Size, humanBytes(c.Bytes))
			for _, u := range c.URLs {
				fmt.Fprintf(&b, "        %s\n", u)
			}
		}
	}
	return b.String()
}

// dedupEntry is one row reduced to what clustering needs, so a directory of
// parquet can be analysed without holding the text in memory.
type dedupEntry struct {
	url     string
	sum     [16]byte
	simhash uint64
	bytes   int64
}

// AnalyzeDedup reads every parquet file under the given paths and reports the
// duplicate structure of the corpus.
//
// Exact duplicates are found by hashing the Markdown. Near duplicates are found
// by banding the simhash: the 64 bits are split into four 16 bit bands, and two
// documents within distance 3 must agree on at least one band by the pigeonhole
// principle, so candidates come out of four hash tables instead of out of an
// N squared scan. Every candidate pair is then verified by counting bits, so the
// banding never widens the answer, only narrows the search.
//
// Rows with no simhash column are fingerprinted on the spot from their Markdown,
// which is what lets the report run over a dataset built before the column
// existed.
func AnalyzeDedup(paths []string, distance, top int) (DedupReport, error) {
	if distance < 0 {
		distance = DefaultNearDistance
	}
	rep := DedupReport{NearDistance: distance}

	files, err := parquetFilesUnder(paths)
	if err != nil {
		return rep, err
	}
	if len(files) == 0 {
		return rep, fmt.Errorf("no parquet files under %s", strings.Join(paths, ", "))
	}
	rep.Files = len(files)

	var entries []dedupEntry
	for _, f := range files {
		if err := readDedupRows(f, func(r dedupRow) {
			rep.Rows++
			if r.Markdown == "" {
				return
			}
			sh := r.Simhash
			if sh == 0 {
				sh = Simhash(r.Markdown)
			}
			if sh == 0 {
				rep.NoFingerprint++
				return
			}
			sum := sha256.Sum256([]byte(r.Markdown))
			entries = append(entries, dedupEntry{
				url:     r.URL,
				sum:     [16]byte(sum[:16]),
				simhash: sh,
				bytes:   int64(len(r.Markdown)),
			})
		}); err != nil {
			return rep, fmt.Errorf("read %s: %w", f, err)
		}
	}

	exact := groupExact(entries)
	for _, c := range exact {
		rep.ExactClusters++
		rep.ExactDuplicates += int64(len(c.URLs) - 1)
		rep.ExactBytes += c.Bytes
	}

	near := groupNear(entries, distance)
	for _, c := range near {
		rep.NearClusters++
		rep.NearDuplicates += int64(len(c.URLs) - 1)
		rep.NearBytes += c.Bytes
	}

	all := append(exact, near...)
	sort.Slice(all, func(i, j int) bool {
		if all[i].Size != all[j].Size {
			return all[i].Size > all[j].Size
		}
		return all[i].URLs[0] < all[j].URLs[0]
	})
	if top > 0 && len(all) > top {
		all = all[:top]
	}
	rep.Clusters = all
	return rep, nil
}

// groupExact buckets entries by content hash and returns the buckets holding
// more than one document.
func groupExact(entries []dedupEntry) []DedupCluster {
	byHash := map[[16]byte][]int{}
	for i, e := range entries {
		byHash[e.sum] = append(byHash[e.sum], i)
	}
	var out []DedupCluster
	for _, idx := range byHash {
		if len(idx) < 2 {
			continue
		}
		out = append(out, buildCluster("exact", entries, idx))
	}
	return out
}

// groupNear finds documents within distance of each other by simhash and unions
// them into clusters, skipping pairs that are already exact duplicates so the
// two numbers in the report do not count the same rows twice.
func groupNear(entries []dedupEntry, distance int) []DedupCluster {
	// One entry per distinct content hash. A set of exact copies is one document
	// as far as near duplicate structure is concerned.
	rep := map[[16]byte]int{}
	var reps []int
	for i, e := range entries {
		if e.bytes < minNearBytes {
			continue
		}
		if _, ok := rep[e.sum]; ok {
			continue
		}
		rep[e.sum] = i
		reps = append(reps, i)
	}

	uf := newUnionFind(len(reps))

	// Four 16 bit bands. Two fingerprints at distance <= 3 differ in at most 3
	// bits, which cannot touch all four bands, so they share at least one band
	// value and land in the same bucket at least once.
	const bands = 4
	for b := range bands {
		shift := uint(b * 16)
		bucket := map[uint16][]int{}
		for p, i := range reps {
			key := uint16(entries[i].simhash >> shift)
			bucket[key] = append(bucket[key], p)
		}
		for _, group := range bucket {
			if len(group) < 2 {
				continue
			}
			// Buckets are small in practice. A pathological one (thousands of
			// documents sharing 16 bits) is capped so one degenerate band cannot
			// turn the report into an N squared scan.
			if len(group) > 512 {
				group = group[:512]
			}
			for x := range group {
				for y := x + 1; y < len(group); y++ {
					i, j := reps[group[x]], reps[group[y]]
					if !similarLength(entries[i].bytes, entries[j].bytes) {
						continue
					}
					if SimhashDistance(entries[i].simhash, entries[j].simhash) <= distance {
						uf.union(group[x], group[y])
					}
				}
			}
		}
	}

	members := map[int][]int{}
	for p, i := range reps {
		root := uf.find(p)
		members[root] = append(members[root], i)
	}
	var out []DedupCluster
	for _, idx := range members {
		if len(idx) < 2 {
			continue
		}
		out = append(out, buildCluster("near", entries, idx))
	}
	return out
}

// similarLength is the guard that keeps the fingerprint honest. A simhash says
// two documents draw on the same vocabulary, which is not the same as saying
// they are the same document, and the case where the difference shows is
// mojibake: a page served as UTF-8 and labelled Latin-1 extracts to a small
// vocabulary of replacement sequences, so two unrelated mojibake pages can land
// one bit apart. They are rarely the same size. Requiring the shorter document
// to be at least half the longer one costs nothing on real near duplicates,
// which differ by a nav bar and not by a factor of eight.
func similarLength(a, b int64) bool {
	if a > b {
		a, b = b, a
	}
	if b == 0 {
		return true
	}
	return a*2 >= b
}

// buildCluster turns a set of entry indices into a reported cluster. Bytes counts
// everything past the first document, which is what deduplicating would save.
func buildCluster(kind string, entries []dedupEntry, idx []int) DedupCluster {
	sort.Slice(idx, func(a, b int) bool { return entries[idx[a]].url < entries[idx[b]].url })
	c := DedupCluster{Kind: kind, Size: len(idx)}
	for n, i := range idx {
		if n < 5 {
			c.URLs = append(c.URLs, entries[i].url)
		}
		if n > 0 {
			c.Bytes += entries[i].bytes
		}
	}
	// Size is the true size; URLs is a sample of it, and a cluster that was cut
	// short says so rather than looking like it had five members.
	if len(idx) > len(c.URLs) {
		c.URLs = append(c.URLs, fmt.Sprintf("... and %d more", len(idx)-len(c.URLs)))
	}
	return c
}

// unionFind is the usual disjoint set with path halving, used to turn pairwise
// near duplicate matches into clusters.
type unionFind struct{ parent []int }

func newUnionFind(n int) *unionFind {
	p := make([]int, n)
	for i := range p {
		p[i] = i
	}
	return &unionFind{parent: p}
}

func (u *unionFind) find(x int) int {
	for u.parent[x] != x {
		u.parent[x] = u.parent[u.parent[x]]
		x = u.parent[x]
	}
	return x
}

func (u *unionFind) union(a, b int) {
	ra, rb := u.find(a), u.find(b)
	if ra != rb {
		u.parent[rb] = ra
	}
}

// parquetFilesUnder expands each path into the parquet files it names: a file
// stays as it is, a directory is walked.
func parquetFilesUnder(paths []string) ([]string, error) {
	var out []string
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			out = append(out, p)
			continue
		}
		err = filepath.WalkDir(p, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && strings.HasSuffix(path, ".parquet") {
				out = append(out, path)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	sort.Strings(out)
	return out, nil
}

// readDedupRows streams one parquet file, calling fn for each row.
func readDedupRows(path string, fn func(dedupRow)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		return err
	}
	r := parquet.NewGenericReader[dedupRow](pf)
	defer func() { _ = r.Close() }()
	buf := make([]dedupRow, 1024)
	for {
		n, err := r.Read(buf)
		for i := range n {
			fn(buf[i])
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

package ccrawl

import (
	"fmt"
	"sort"
	"strings"
)

// shard is one output file staged for a commit: its shard index, its path in the
// repo, its local staged path, and its size.
type shard struct {
	Index    int
	RepoPath string
	Local    string
	Rows     int64
	Bytes    int64
}

// commitSummary builds the one-line commit summary for a batch of add operations,
// for example "Add CC-MAIN-2026-25 url shards 000-007 (8 files, 1.4 GB)". scope is
// the crawl or graph id when every shard shares it, else empty. kind is "url" or
// "domain". width is the zero-pad width of the shard index (5 for urls, 3 for
// domains).
func commitSummary(scope, kind string, width int, shards []shard) string {
	var b strings.Builder
	b.WriteString("Add ")
	if scope != "" {
		b.WriteString(scope)
		b.WriteByte(' ')
	}
	b.WriteString(kind)
	b.WriteByte(' ')
	b.WriteString(shardRange(width, shards))
	var bytes int64
	for _, s := range shards {
		bytes += s.Bytes
	}
	fmt.Fprintf(&b, " (%s, %s)", plural(len(shards), "file"), humanBytes(bytes))
	return b.String()
}

// shardRange renders "shard NNN" for one shard, "shards NNN-MMM" for a contiguous
// run, or "shards NNN,MMM,..." when there is a gap.
func shardRange(width int, shards []shard) string {
	idx := make([]int, len(shards))
	for i, s := range shards {
		idx[i] = s.Index
	}
	sort.Ints(idx)
	noun := "shards"
	if len(idx) == 1 {
		noun = "shard"
		return fmt.Sprintf("%s %0*d", noun, width, idx[0])
	}
	contiguous := true
	for i := 1; i < len(idx); i++ {
		if idx[i] != idx[i-1]+1 {
			contiguous = false
			break
		}
	}
	if contiguous {
		return fmt.Sprintf("%s %0*d-%0*d", noun, width, idx[0], width, idx[len(idx)-1])
	}
	parts := make([]string, len(idx))
	for i, v := range idx {
		parts[i] = fmt.Sprintf("%0*d", width, v)
	}
	return noun + " " + strings.Join(parts, ",")
}

// commitBody lists each file in the batch with its human size, so a reader sees
// the exact files without opening the tree.
func commitBody(shards []shard) string {
	sorted := append([]shard(nil), shards...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Index < sorted[j].Index })
	var b strings.Builder
	for _, s := range sorted {
		fmt.Fprintf(&b, "%s (%s)\n", s.RepoPath, humanBytes(s.Bytes))
	}
	return strings.TrimRight(b.String(), "\n")
}

// finalizeURLMessage is the message for the stats+README commit that closes a crawl.
func finalizeURLMessage(s URLCrawlStat) string {
	state := "progress"
	if s.Complete {
		state = "complete"
	}
	return fmt.Sprintf("Update index: crawl %s %s (%d shards, %s rows, %s)",
		s.Crawl, state, s.Shards, humanCountShort(s.Rows), humanBytes(s.ParquetBytes))
}

// finalizeDomainMessage is the message for the stats+README commit that closes a release.
func finalizeDomainMessage(s DomainGraphStat) string {
	return fmt.Sprintf("Update index: graph %s complete (%d shards, %s domains)",
		s.Graph, s.Shards, humanCountShort(s.Domains))
}

// humanBytes renders a byte count as a human size such as "1.4 GB".
func humanBytes(n int64) string {
	const unit = 1000
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "kMGTPE"[exp])
}

// humanCountShort renders a row count compactly, such as "2.8B" or "130.4M".
func humanCountShort(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.1fB", float64(n)/1e9)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%.1fK", float64(n)/1e3)
	default:
		return fmt.Sprintf("%d", n)
	}
}

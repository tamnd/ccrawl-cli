package ccrawl

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// The recrawl ledger is one file per server rather than one file for the
// dataset, and that is the whole design.
//
// Three machines publish into one repo at the same time for about a hundred
// days. A single stats.csv is a file all three want to rewrite, and the hub has
// no compare-and-set on a path, so the sequence read, add my rows, write is a
// lost update waiting to happen: whoever commits second writes back a copy of
// the ledger that does not have the first one's numbers in it. Nothing errors,
// nothing retries, the counts just quietly go backwards.
//
// Merging on read fixes it by construction. Each server owns exactly one path,
// ledger/<server>-shard<i>of<n>.csv, and never writes another server's. Two
// servers committing at the same instant touch different files, so there is
// nothing to lose. The dataset card is generated from the union of whatever
// ledger files are on the hub at the time, so it is derived rather than
// authoritative: a card written from a snapshot that missed a row someone else
// had just committed is corrected by the next commit from any server, and until
// then it undercounts rather than overcounts.

// RecrawlStat is one server's contribution to a recrawl dataset. It is the whole
// contents of that server's ledger file, one row.
type RecrawlStat struct {
	// Server is the machine that fetched these pages, for example server1.
	Server string
	// Shard and Shards are its slice of the work list, so a reader can see
	// whether the fleet covered the whole thing or two thirds of it.
	Shard  int
	Shards int
	// Files, Rows and Bytes are what this server has published, not what it has
	// fetched. Coverage on the card has to be what is on the hub.
	Files int
	Rows  int64
	Bytes int64
	// Part and Row are how far into the work list this server has got, which is
	// the only honest way to state progress against a work list whose total is
	// known but whose end is a hundred days away.
	Part int
	Row  int64
	// Done is set when this server walked its slice of the work list out.
	Done           bool
	FirstCommitted string
	LastCommitted  string
}

var recrawlStatsHeader = []string{
	"server", "shard", "shards", "files", "rows", "bytes",
	"part", "row", "done", "first_committed", "last_committed",
}

// RecrawlLedgerPath is where a server's ledger file lives in the repo.
func RecrawlLedgerPath(server string, shard, shards int) string {
	return fmt.Sprintf("ledger/%s-shard%dof%d.csv", recrawlSlug(server), shard, shards)
}

// recrawlSlug reduces a server name to something safe in a path. An operator
// naming a machine with a slash or a space should get a usable path rather than
// a commit that fails an hour into a run.
func recrawlSlug(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "server"
	}
	return out
}

// ReadRecrawlStats reads a ledger file. A missing file is an empty ledger.
func ReadRecrawlStats(path string) ([]RecrawlStat, error) {
	recs, err := readCSV(path)
	if err != nil {
		return nil, err
	}
	var rows []RecrawlStat
	for _, r := range recs {
		if len(r) < len(recrawlStatsHeader) {
			continue
		}
		rows = append(rows, RecrawlStat{
			Server:         r[0],
			Shard:          atoi(r[1]),
			Shards:         atoi(r[2]),
			Files:          atoi(r[3]),
			Rows:           atoi64(r[4]),
			Bytes:          atoi64(r[5]),
			Part:           atoi(r[6]),
			Row:            atoi64(r[7]),
			Done:           r[8] == "true",
			FirstCommitted: r[9],
			LastCommitted:  r[10],
		})
	}
	return rows, nil
}

// WriteRecrawlStats writes a ledger file, sorted so a diff between two commits
// reads as a change in the numbers rather than a reshuffle.
func WriteRecrawlStats(path string, rows []RecrawlStat) error {
	SortRecrawlStats(rows)
	recs := [][]string{recrawlStatsHeader}
	for _, r := range rows {
		recs = append(recs, []string{
			r.Server,
			strconv.Itoa(r.Shard),
			strconv.Itoa(r.Shards),
			strconv.Itoa(r.Files),
			strconv.FormatInt(r.Rows, 10),
			strconv.FormatInt(r.Bytes, 10),
			strconv.Itoa(r.Part),
			strconv.FormatInt(r.Row, 10),
			strconv.FormatBool(r.Done),
			r.FirstCommitted,
			r.LastCommitted,
		})
	}
	return writeCSV(path, recs)
}

// SortRecrawlStats orders rows by shard index, then by server name.
func SortRecrawlStats(rows []RecrawlStat) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Shard != rows[j].Shard {
			return rows[i].Shard < rows[j].Shard
		}
		return rows[i].Server < rows[j].Server
	})
}

// MergeRecrawlStats folds ledger rows from every server into one list, keeping
// the newest row for a server and shard.
//
// It is a merge and not a concatenation because a resumed server rewrites its
// own row rather than adding one, and because two ledger files can name the same
// server if an operator moves a shard between machines. Last committed wins,
// which is the only ordering available: the hub does not tell us commit times we
// can trust across three clocks, but each row carries the time its own server
// wrote it, and that server is the only writer of that row.
func MergeRecrawlStats(sets ...[]RecrawlStat) []RecrawlStat {
	best := map[string]RecrawlStat{}
	for _, set := range sets {
		for _, r := range set {
			key := fmt.Sprintf("%s/%d", r.Server, r.Shard)
			cur, seen := best[key]
			if !seen || r.LastCommitted > cur.LastCommitted {
				best[key] = r
			}
		}
	}
	out := make([]RecrawlStat, 0, len(best))
	for _, r := range best {
		out = append(out, r)
	}
	SortRecrawlStats(out)
	return out
}

// RecrawlTotals is the fleet-wide rollup the dataset card reports.
type RecrawlTotals struct {
	Servers int
	Shards  int // distinct work list shards covered
	Slices  int // the shard count the fleet was configured with
	Files   int
	Rows    int64
	Bytes   int64
	Done    int // servers that walked their slice out
}

// TotalRecrawlStats rolls the merged ledger up for the card.
func TotalRecrawlStats(rows []RecrawlStat) RecrawlTotals {
	var t RecrawlTotals
	shards := map[int]bool{}
	servers := map[string]bool{}
	for _, r := range rows {
		servers[r.Server] = true
		shards[r.Shard] = true
		t.Slices = max(t.Slices, r.Shards)
		t.Files += r.Files
		t.Rows += r.Rows
		t.Bytes += r.Bytes
		if r.Done {
			t.Done++
		}
	}
	t.Servers = len(servers)
	t.Shards = len(shards)
	return t
}

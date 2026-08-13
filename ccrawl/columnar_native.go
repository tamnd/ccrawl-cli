package ccrawl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"

	"strings"
	"sync"

	"github.com/parquet-go/parquet-go"
)

// The native columnar engine answers the projection and aggregation subset of
// the columnar commands without duckdb, by reading the Common Crawl Parquet
// files directly over ranged HTTP.
//
// It is the same trick the URL publisher already uses in production: open the
// remote file through httpReaderAt, read the footer, and pull only the column
// chunks the query needs. On top of that it prunes whole row groups whose
// statistics say they cannot match, so a filter on url_host_tld or
// url_surtkey touches a fraction of a 530 MiB shard.
//
// What it does not do is SQL. `columnar query` stays duckdb only, permanently:
// a hand written engine that accepts arbitrary SQL is a database, and this is
// a CLI.

// NativeAggregate selects what a native scan produces.
type NativeAggregate int

const (
	// NativeRows emits one record per matching row, projecting Select.
	NativeRows NativeAggregate = iota
	// NativeCount emits a single row holding the number of matches.
	NativeCount
	// NativeGroupCount emits one row per distinct value of GroupBy, with its
	// count, ordered by count descending.
	NativeGroupCount
)

// NativeScan is one native query: which files to read, what to keep, and what
// to produce.
type NativeScan struct {
	URLs      []string // parquet part URLs, from the crawl's manifest
	Query     ColumnarQuery
	Aggregate NativeAggregate
	Select    []string // output columns, for NativeRows
	GroupBy   string   // grouping column, for NativeGroupCount
	Limit     int      // stop after this many output rows (0 = all)
	Workers   int      // files read concurrently
}

// nativeColumns are the columns the engine knows how to filter on. Anything
// else in a query means the query is not expressible natively and belongs to
// duckdb.
var nativeColumns = map[string]bool{
	"url":                        true,
	"url_surtkey":                true,
	"url_host_name":              true,
	"url_host_tld":               true,
	"url_host_registered_domain": true,
	"url_path":                   true,
	"fetch_status":               true,
	"content_mime_detected":      true,
	"content_languages":          true,
	"warc_filename":              true,
	"warc_record_offset":         true,
	"warc_record_length":         true,
}

// NativeExpressible reports whether the native engine can answer this scan. A
// false here is not a failure: --engine auto falls back to duckdb, which can.
func NativeExpressible(s NativeScan) bool {
	for _, c := range s.Select {
		if !nativeColumns[c] {
			return false
		}
	}
	if s.GroupBy != "" && !nativeColumns[s.GroupBy] {
		return false
	}
	return true
}

// predicate is one filter on one column. The kinds are exactly what the
// columnar flags produce, which is why there is no expression tree here.
type predicate struct {
	col      string
	equals   string          // exact match on a string column
	prefixes []string        // string column starts with any one of these
	contains string          // string column contains this
	oneOf    map[string]bool // string column is one of this set
	status   int32           // fetch_status equality, when col is fetch_status
	isStatus bool            // this predicate compares the int32 fetch_status column

	// negate inverts the test, and changes what a null means. Unnegated, a
	// null matches nothing, the same as SQL equality. Negated, a null matches,
	// because "not Vietnamese" has to include the rows Common Crawl never
	// labelled at all. Those rows are the entire point of the flag.
	negate bool

	// setLo and setHi are the smallest and largest members of oneOf, kept so a
	// page can be pruned against the whole set in one comparison instead of one
	// per member. With a ten thousand host list that difference matters.
	setLo, setHi string
}

// match evaluates the predicate against one value.
func (p predicate) match(v parquet.Value) bool {
	if v.IsNull() {
		// A null is unknown, not a value. Unnegated it fails; negated it
		// passes, which is the IS NULL half of the SQL this mirrors.
		return p.negate
	}
	return p.matches(v) != p.negate
}

// matches is the test itself, before negation and without the null handling.
func (p predicate) matches(v parquet.Value) bool {
	if p.isStatus {
		return v.Int32() == p.status
	}
	s := byteString(v.ByteArray())
	switch {
	case p.equals != "":
		return s == p.equals
	case len(p.prefixes) > 0:
		for _, pre := range p.prefixes {
			if strings.HasPrefix(s, pre) {
				return true
			}
		}
		return false
	case p.contains != "":
		return strings.Contains(s, p.contains)
	case p.oneOf != nil:
		return p.oneOf[s]
	}
	return true
}

// canPrune reports whether page bounds can rule this predicate out. Only
// equality, prefixes and set membership are ordered against min and max; a
// substring match says nothing about where the value sorts.
//
// A negated predicate never prunes. Page statistics do not describe the nulls
// in a page, so a page whose min and max are both "vie" can still hold rows
// with no language at all, and those rows match --not-lang vie. Pruning it
// would drop exactly the rows the flag exists to find.
func (p predicate) canPrune() bool {
	if p.negate {
		return false
	}
	return p.equals != "" || len(p.prefixes) > 0 || p.isStatus || p.oneOf != nil
}

// excludes reports whether a page whose values all fall in [min, max] can be
// skipped entirely. A predicate holding several prefixes only excludes the page
// when every one of them does, since the row needs to match just one.
func (p predicate) excludes(minV, maxV parquet.Value) bool {
	if p.isStatus {
		return p.status < minV.Int32() || p.status > maxV.Int32()
	}
	lo, hi := byteString(minV.ByteArray()), byteString(maxV.ByteArray())
	if p.equals != "" {
		return p.equals < lo || p.equals > hi
	}
	if p.oneOf != nil {
		// Only the span of the set is compared, not every member. That is
		// conservative, it keeps pages that hold none of the hosts but sort
		// between two that it does, and it costs one comparison rather than
		// one per host.
		return p.setHi < lo || p.setLo > hi
	}
	for _, pre := range p.prefixes {
		// Every string with this prefix sorts inside [prefix, prefix+0xff...].
		// If that window overlaps the page's bounds at all then a row in here
		// might start with it, and the page has to be read. Clipping lo to the
		// prefix length is what makes the lower comparison right: "com,ex" is
		// not less than "com,example", but a page starting at "com,example"
		// holds no "com,ex" either.
		if pre <= hi && pre >= clipTo(lo, len(pre)) {
			return false
		}
	}
	return len(p.prefixes) > 0
}

// clipTo cuts s to at most n bytes, so a prefix can be compared against the
// same number of leading bytes of a bound.
func clipTo(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// predicatesFor turns the flag-shaped query into the filters the scan applies.
// The surtkey prefixes that SQL() adds for pruning are added here too, and for
// the same reason: it is the column the files are sorted on, so it is the one
// that actually skips row groups.
func predicatesFor(q ColumnarQuery) []predicate {
	var ps []predicate
	if q.Domain != "" {
		ps = append(ps, predicate{col: "url_host_registered_domain", equals: q.Domain})
		// Both patterns, exactly as the SQL does: the apex host's captures end
		// the host with ')' and every subdomain's continue with ','. Keeping
		// them in one predicate rather than two is what stops the apex rows
		// from being filtered out by the subdomain form.
		ps = appendSurtPredicate(ps, surtPrefixes([]string{q.Domain}, true))
	}
	if q.Host != "" {
		ps = append(ps, predicate{col: "url_host_name", equals: q.Host})
		ps = appendSurtPredicate(ps, surtPrefixes([]string{q.Host}, false))
	}
	if q.TLD != "" {
		ps = append(ps, predicate{col: "url_host_tld", equals: q.TLD})
	}
	if q.MIME != "" {
		ps = append(ps, predicate{col: "content_mime_detected", equals: q.MIME})
	}
	if q.Lang != "" {
		ps = append(ps, predicate{col: "content_languages", contains: q.Lang})
	}
	if q.PathPrefix != "" {
		ps = append(ps, predicate{col: "url_path", prefixes: []string{q.PathPrefix}})
	}
	if q.Status != 0 {
		ps = append(ps, predicate{col: "fetch_status", status: int32(q.Status), isStatus: true})
	}
	// The set forms get the same surtkey prefixes as the single ones. Without
	// them a --hosts-file read every row group in the crawl: the set span the
	// membership test prunes on is a span of url_host_name, and that is not the
	// column the files are sorted on, so a page holding com,a) through com,z)
	// spans nearly the whole alphabet of forward host names and never excludes.
	if len(q.Hosts) > 0 {
		ps = append(ps, setPredicate("url_host_name", q.Hosts))
		ps = appendSurtPredicate(ps, surtPrefixes(q.Hosts, false))
	}
	if len(q.Domains) > 0 {
		ps = append(ps, setPredicate("url_host_registered_domain", q.Domains))
		ps = appendSurtPredicate(ps, surtPrefixes(q.Domains, true))
	}
	if q.NotTLD != "" {
		ps = append(ps, predicate{col: "url_host_tld", equals: q.NotTLD, negate: true})
	}
	if q.NotMIME != "" {
		ps = append(ps, predicate{col: "content_mime_detected", equals: q.NotMIME, negate: true})
	}
	if q.NotLang != "" {
		ps = append(ps, predicate{col: "content_languages", contains: q.NotLang, negate: true})
	}
	if q.NotStatus != 0 {
		ps = append(ps, predicate{col: "fetch_status", status: int32(q.NotStatus), isStatus: true, negate: true})
	}
	return ps
}

// appendSurtPredicate adds the prefix test the SQL spells as a LIKE. Every
// prefix goes in one predicate rather than one each, because a row needs to
// match only one of them and separate predicates would be an AND.
func appendSurtPredicate(ps []predicate, prefixes []string) []predicate {
	if len(prefixes) == 0 {
		return ps
	}
	return append(ps, predicate{col: "url_surtkey", prefixes: prefixes})
}

// setPredicate builds a membership test over vals, recording the span of the
// set so a page can be pruned without walking every member.
func setPredicate(col string, vals []string) predicate {
	p := predicate{col: col, oneOf: make(map[string]bool, len(vals))}
	for i, v := range vals {
		p.oneOf[v] = true
		if i == 0 || v < p.setLo {
			p.setLo = v
		}
		if i == 0 || v > p.setHi {
			p.setHi = v
		}
	}
	return p
}

// nativeResult is what one file's scan contributes.
type nativeResult struct {
	count  int64
	groups map[string]int64
}

// RunColumnarNative executes the scan across every file, concurrently, and
// streams results to emit. Rows arrive in whatever order the files finish,
// which is what duckdb over an unordered file list does too.
func RunColumnarNative(ctx context.Context, h *HTTPClient, s NativeScan, emit func(map[string]any) error) error {
	if len(s.URLs) == 0 {
		return errors.New("no parquet files to scan")
	}
	if s.Workers <= 0 {
		s.Workers = 8
	}

	plan, err := newScanPlan(s)
	if err != nil {
		return err
	}
	// Ranged metadata reads are not what the inter-request delay is priced for,
	// and leaving it on makes this engine four times slower than the duckdb it
	// is meant to replace.
	h = h.WithoutDelay()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		mu      sync.Mutex
		total   int64
		groups  = map[string]int64{}
		emitted int
		emitErr error
		done    bool
	)

	// sink is called with one matching row's projected values, under the lock,
	// so emit sees rows one at a time even though files are read in parallel.
	sink := func(vals []parquet.Value) bool {
		mu.Lock()
		defer mu.Unlock()
		if done {
			return false
		}
		row := make(map[string]any, len(plan.selectCols))
		for i, c := range plan.selectCols {
			row[c] = goValue(vals[i])
		}
		if err := emit(row); err != nil {
			emitErr, done = err, true
			cancel()
			return false
		}
		emitted++
		if s.Limit > 0 && emitted >= s.Limit {
			done = true
			cancel()
			return false
		}
		return true
	}

	jobs := make(chan string)
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once

	for i := 0; i < s.Workers && i < len(s.URLs); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for u := range jobs {
				res, err := scanFile(ctx, h, u, plan, sink)
				if err != nil {
					// A cancelled context is how the limit stops the scan, so it
					// is not an error the user should see.
					if ctx.Err() == nil {
						errOnce.Do(func() { firstErr = fmt.Errorf("scan %s: %w", u, err) })
						cancel()
					}
					return
				}
				mu.Lock()
				total += res.count
				for k, v := range res.groups {
					groups[k] += v
				}
				mu.Unlock()
			}
		}()
	}
	for _, u := range s.URLs {
		select {
		case jobs <- u:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
	}
	close(jobs)
	wg.Wait()

	if emitErr != nil {
		return emitErr
	}
	if firstErr != nil {
		return firstErr
	}

	switch s.Aggregate {
	case NativeCount:
		return emit(map[string]any{"n": total})
	case NativeGroupCount:
		return emitGroups(groups, s.Limit, emit)
	}
	return nil
}

// emitGroups writes the breakdown ordered by count descending, then by value,
// so a tie does not reorder between runs.
func emitGroups(groups map[string]int64, limit int, emit func(map[string]any) error) error {
	type kv struct {
		k string
		n int64
	}
	out := make([]kv, 0, len(groups))
	for k, n := range groups {
		out = append(out, kv{k, n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].n != out[j].n {
			return out[i].n > out[j].n
		}
		return out[i].k < out[j].k
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	for _, e := range out {
		row := map[string]any{"n": e.n}
		if e.k == nullGroup {
			row["value"] = nil
		} else {
			row["value"] = e.k
		}
		if err := emit(row); err != nil {
			return err
		}
	}
	return nil
}

// nullGroup stands in for a NULL group key, which a Go map cannot hold apart
// from the empty string. Common Crawl has rows with an empty content_languages
// and rows with none at all, and duckdb reports those as two groups.
const nullGroup = "\x00null"

// scanPlan is the per-query work shared by every file: which columns to read
// for filtering, which to read for output, and in what order.
type scanPlan struct {
	preds      []predicate
	filterCols []string // distinct predicate columns, cheapest first
	predsBy    map[string][]predicate
	selectCols []string // columns the output needs, in output order
	aggregate  NativeAggregate
	groupBy    string
}

// filterCost orders the filter columns so the one most likely to eliminate rows
// for the fewest bytes runs first. url_host_tld is a handful of bytes per row
// and dictionary encodes to almost nothing; url and url_path are the widest
// columns in the file.
var filterCost = map[string]int{
	"url_host_tld":               0,
	"fetch_status":               1,
	"content_mime_detected":      2,
	"url_host_registered_domain": 3,
	"url_host_name":              4,
	"content_languages":          5,
	"url_surtkey":                6,
	"url_path":                   7,
	"url":                        8,
}

func newScanPlan(s NativeScan) (*scanPlan, error) {
	p := &scanPlan{
		preds:     predicatesFor(s.Query),
		predsBy:   map[string][]predicate{},
		aggregate: s.Aggregate,
		groupBy:   s.GroupBy,
	}
	for _, pr := range p.preds {
		if !nativeColumns[pr.col] {
			return nil, fmt.Errorf("native engine cannot filter on %s", pr.col)
		}
		if _, seen := p.predsBy[pr.col]; !seen {
			p.filterCols = append(p.filterCols, pr.col)
		}
		p.predsBy[pr.col] = append(p.predsBy[pr.col], pr)
	}
	sort.SliceStable(p.filterCols, func(i, j int) bool {
		return filterCost[p.filterCols[i]] < filterCost[p.filterCols[j]]
	})

	switch s.Aggregate {
	case NativeRows:
		p.selectCols = s.Select
		if len(p.selectCols) == 0 {
			return nil, errors.New("native row scan needs columns to select")
		}
	case NativeGroupCount:
		if s.GroupBy == "" {
			return nil, errors.New("native group scan needs a column to group by")
		}
		p.selectCols = []string{s.GroupBy}
	}
	return p, nil
}

// scanFile reads one Parquet part. It walks row groups, prunes the ones whose
// statistics rule them out, builds a match mask from the filter columns, and
// only then reads the output columns for the rows that survived.
func scanFile(ctx context.Context, h *HTTPClient, url string, plan *scanPlan, sink func([]parquet.Value) bool) (nativeResult, error) {
	var res nativeResult
	if plan.aggregate == NativeGroupCount {
		res.groups = map[string]int64{}
	}

	pf, err := openRemoteParquet(ctx, h, url)
	if err != nil {
		return res, err
	}
	schema := pf.Schema()

	// Resolve every column this query touches to its leaf index once, and bail
	// out if the file's schema is not the flat one Common Crawl publishes.
	idx := map[string]int{}
	for _, c := range append(append([]string{}, plan.filterCols...), plan.selectCols...) {
		if _, ok := idx[c]; ok {
			continue
		}
		leaf, ok := schema.Lookup(c)
		if !ok {
			return res, fmt.Errorf("column %s is not in this file", c)
		}
		if leaf.MaxRepetitionLevel > 0 {
			return res, fmt.Errorf("column %s is repeated, which the native engine does not read", c)
		}
		idx[c] = leaf.ColumnIndex
	}

	for _, rg := range pf.RowGroups() {
		if err := ctx.Err(); err != nil {
			return res, nil
		}
		chunks := rg.ColumnChunks()
		if pruneRowGroup(plan, idx, chunks) {
			continue
		}
		n := int(rg.NumRows())
		mask, matched, err := buildMask(plan, idx, chunks, n)
		if err != nil {
			return res, err
		}
		if matched == 0 {
			continue
		}
		switch plan.aggregate {
		case NativeCount:
			res.count += int64(matched)
			continue
		case NativeGroupCount:
			if err := countGroups(chunks[idx[plan.groupBy]], mask, res.groups); err != nil {
				return res, err
			}
			continue
		}
		if err := emitRows(plan, idx, chunks, mask, matched, sink); err != nil {
			return res, err
		}
		if ctx.Err() != nil {
			return res, nil
		}
	}
	return res, nil
}

// pruneRowGroup reports whether the whole row group can be skipped. It reads
// only the column index, which lives in the footer, so a pruned group costs no
// data pages at all.
func pruneRowGroup(plan *scanPlan, idx map[string]int, chunks []parquet.ColumnChunk) bool {
	for _, col := range plan.filterCols {
		for _, pr := range plan.predsBy[col] {
			if !pr.canPrune() {
				continue
			}
			ci, err := chunks[idx[col]].ColumnIndex()
			if err != nil || ci.NumPages() == 0 {
				continue // no statistics, so nothing can be ruled out
			}
			if chunkExcludes(pr, ci) {
				return true
			}
		}
	}
	return false
}

// chunkExcludes reports whether every page in the chunk is ruled out by the
// predicate. Page level bounds are strictly better than one bound for the whole
// chunk, and this is where the ordering of url_surtkey pays off.
func chunkExcludes(pr predicate, ci parquet.ColumnIndex) bool {
	for i := 0; i < ci.NumPages(); i++ {
		if ci.NullPage(i) {
			continue
		}
		if !pr.excludes(ci.MinValue(i), ci.MaxValue(i)) {
			return false
		}
	}
	return true
}

// buildMask reads the filter columns and returns which rows of the group match
// every predicate. Columns are read cheapest first and the read stops as soon
// as no row is left alive, so an expensive column is never touched for a group
// a cheap one already eliminated.
func buildMask(plan *scanPlan, idx map[string]int, chunks []parquet.ColumnChunk, rows int) ([]bool, int, error) {
	mask := make([]bool, rows)
	for i := range mask {
		mask[i] = true
	}
	matched := rows

	for _, col := range plan.filterCols {
		preds := plan.predsBy[col]
		matched = 0
		err := eachValue(chunks[idx[col]], rows, func(row int, v parquet.Value) {
			if !mask[row] {
				return
			}
			for _, pr := range preds {
				if !pr.match(v) {
					mask[row] = false
					return
				}
			}
			matched++
		})
		if err != nil {
			return nil, 0, err
		}
		if matched == 0 {
			return mask, 0, nil
		}
	}
	return mask, matched, nil
}

// countGroups tallies the group column over the masked rows.
func countGroups(chunk parquet.ColumnChunk, mask []bool, groups map[string]int64) error {
	return eachValue(chunk, len(mask), func(row int, v parquet.Value) {
		if !mask[row] {
			return
		}
		if v.IsNull() {
			groups[nullGroup]++
			return
		}
		groups[string(v.ByteArray())]++
	})
}

// emitRows reads the projected columns for the masked rows and hands each row
// to the sink. The columns are read one at a time and the values held until the
// group is done, because a Parquet file stores columns apart and there is no
// way to read a row without doing exactly this.
func emitRows(plan *scanPlan, idx map[string]int, chunks []parquet.ColumnChunk, mask []bool, matched int, sink func([]parquet.Value) bool) error {
	cols := make([][]parquet.Value, len(plan.selectCols))
	for i, c := range plan.selectCols {
		vals := make([]parquet.Value, 0, matched)
		err := eachValue(chunks[idx[c]], len(mask), func(row int, v parquet.Value) {
			if mask[row] {
				vals = append(vals, v.Clone())
			}
		})
		if err != nil {
			return err
		}
		// Every column of a row group holds the same number of rows, so a short
		// read here means the walk lost alignment and the row about to be built
		// would pair one column's value with another column's row. That is a
		// wrong answer rather than a missing one, so it stops the scan.
		if len(vals) != matched {
			return fmt.Errorf("column %s yielded %d of %d masked rows", c, len(vals), matched)
		}
		cols[i] = vals
	}
	row := make([]parquet.Value, len(cols))
	for r := 0; r < matched; r++ {
		for i := range cols {
			row[i] = cols[i][r]
		}
		if !sink(row) {
			return nil
		}
	}
	return nil
}

// eachValue walks one column chunk and calls fn once per row, in row order.
// Common Crawl's columns are flat and optional, so one page value is one row
// and nulls come through as null values, which keeps the row numbering aligned
// across columns without materialising anything.
func eachValue(chunk parquet.ColumnChunk, rows int, fn func(row int, v parquet.Value)) error {
	pages := chunk.Pages()
	defer func() { _ = pages.Close() }()

	buf := make([]parquet.Value, 1024)
	row := 0
	for row < rows {
		page, err := pages.ReadPage()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		vr := page.Values()
		for {
			n, rerr := vr.ReadValues(buf)
			for i := 0; i < n && row < rows; i++ {
				fn(row, buf[i])
				row++
			}
			if rerr != nil {
				if errors.Is(rerr, io.EOF) {
					break
				}
				parquet.Release(page)
				return rerr
			}
		}
		parquet.Release(page)
	}
	return nil
}

// goValue converts a Parquet value into what the JSON row map carries, matching
// what duckdb's JSON output produces for the same column.
func goValue(v parquet.Value) any {
	if v.IsNull() {
		return nil
	}
	switch v.Kind() {
	case parquet.Boolean:
		return v.Boolean()
	case parquet.Int32:
		return int64(v.Int32())
	case parquet.Int64:
		return v.Int64()
	case parquet.Float:
		return float64(v.Float())
	case parquet.Double:
		return v.Double()
	default:
		return string(v.ByteArray())
	}
}

// byteString reads a byte array column's value as a string, so the predicates
// read as string comparisons rather than bytes.Compare calls.
func byteString(b []byte) string { return string(b) }

// parquetBlockSize is the span httpReaderAt fetches per request, and
// parquetCacheBlocks is how many of them one open file keeps. 256 KiB is the
// measured middle: small enough that opening a part and pruning it costs about
// 200 KiB rather than megabytes, large enough that reading a real column chunk
// is not a string of round trips.
const (
	parquetBlockSize   = 256 << 10
	parquetCacheBlocks = 64
)

// openRemoteParquet opens a Parquet file over ranged HTTP with the options a
// remote read actually wants. Every one of these was measured against a real
// Common Crawl part, and together they take the cost of opening one and pruning
// it from 4.9 MiB down to about 200 KiB:
//
//   - the magic bytes at offset 0 are a whole extra block fetched to look at
//     four bytes, on a file we already know is Parquet;
//   - the bloom filter headers are never consulted here;
//   - the eager page index read pulls the index for all thirty odd columns,
//     where a query touches two or three. Skipping it does not turn pruning
//     off, because ColumnIndex() then reads that one column's index on demand,
//     which is exactly the part worth paying for;
//   - the optimistic read speculatively pulls a megabyte and a half of tail,
//     which is the single largest cost and buys nothing once the block cache
//     is doing the same job.
func openRemoteParquet(ctx context.Context, h *HTTPClient, url string) (*parquet.File, error) {
	size, err := h.ContentLength(ctx, url)
	if err != nil {
		return nil, err
	}
	ra := newHTTPReaderAt(ctx, h, url, size, parquetBlockSize, parquetCacheBlocks)
	return parquet.OpenFile(ra, size,
		parquet.SkipMagicBytes(true),
		parquet.SkipBloomFilters(true),
		parquet.SkipPageIndex(true),
		parquet.OptimisticRead(false),
	)
}

// NativeSchema reads one part's footer and returns the column names and their
// types, spelled the way duckdb's DESCRIBE spells them so both engines print
// the same table.
func NativeSchema(ctx context.Context, h *HTTPClient, url string) ([][2]string, error) {
	pf, err := openRemoteParquet(ctx, h, url)
	if err != nil {
		return nil, err
	}
	var out [][2]string
	for _, f := range pf.Schema().Fields() {
		out = append(out, [2]string{f.Name(), duckTypeName(f)})
	}
	return out, nil
}

// duckTypeName maps a Parquet leaf type onto the name duckdb reports for it, so
// `columnar schema` prints the same table whichever engine answered. The
// logical type is what decides most of it: fetch_status is a physical int32
// carrying an INT(16) annotation, and duckdb calls that a SMALLINT.
func duckTypeName(n parquet.Node) string {
	if !n.Leaf() {
		return "STRUCT"
	}
	if lt := n.Type().LogicalType(); lt != nil {
		switch {
		case lt.UTF8 != nil, lt.Enum != nil, lt.Json != nil:
			return "VARCHAR"
		case lt.UUID != nil:
			return "UUID"
		case lt.Date != nil:
			return "DATE"
		case lt.Time != nil:
			return "TIME"
		case lt.Timestamp != nil:
			if lt.Timestamp.IsAdjustedToUTC {
				return "TIMESTAMP WITH TIME ZONE"
			}
			return "TIMESTAMP"
		case lt.Decimal != nil:
			return fmt.Sprintf("DECIMAL(%d,%d)", lt.Decimal.Precision, lt.Decimal.Scale)
		case lt.Integer != nil:
			return duckIntName(lt.Integer.BitWidth, lt.Integer.IsSigned)
		}
	}
	switch n.Type().Kind() {
	case parquet.Boolean:
		return "BOOLEAN"
	case parquet.Int32:
		return "INTEGER"
	case parquet.Int64:
		return "BIGINT"
	case parquet.Float:
		return "FLOAT"
	case parquet.Double:
		return "DOUBLE"
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return "BLOB"
	}
	return strings.ToUpper(n.Type().String())
}

// duckIntName spells an annotated integer width the way duckdb does.
func duckIntName(bits int8, signed bool) string {
	name := map[int8]string{8: "TINYINT", 16: "SMALLINT", 32: "INTEGER", 64: "BIGINT"}[bits]
	if name == "" {
		return "BIGINT"
	}
	if !signed {
		return "U" + name
	}
	return name
}

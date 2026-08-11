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

// Publishing checks that a shard landed, never that what landed is right. An
// upload that was cut off part way through leaves an object the hub reports as
// present at whatever size arrived, and the resume path, which asks only whether
// the path exists, will never look at it again. Nothing downstream notices until
// somebody reads the dataset and gets an error out of a Parquet library.
//
// Verify reads each published shard's footer over ranged requests and asks the
// questions the publish path never asks: does the file parse, is it the schema
// this dataset promises, do the row groups add up to the row count the footer
// claims, and does every column chunk sit inside the bytes the hub is holding.
// A truncated upload fails at least one of those, usually the first. The cost is
// two or three small range requests per shard rather than the shard itself.
//
// --sample goes further and decodes rows out of the body, which is the only way
// to catch a page whose bytes are wrong rather than missing. It costs a page of
// every column instead of a footer, so it is off by default.

// Shard verdicts. A shard is either ok or it is one of the ways it can be wrong,
// ordered roughly by how early the check that found it runs.
const (
	VerifyOK         = "ok"
	VerifyMissing    = "missing"    // the hub does not have it
	VerifyUnreadable = "unreadable" // the footer will not parse, which is what truncation usually looks like
	VerifyTruncated  = "truncated"  // the footer parses but points past the end of the object
	VerifySchema     = "schema"     // it parses and it is not the dataset's schema
	VerifyEmpty      = "empty"      // it parses and holds no rows
	VerifyCorrupt    = "corrupt"    // --sample could not decode the rows it read
	VerifyNoAccess   = "no-access"  // the store would not serve it, which says nothing about the shard
)

// ShardCheck is one shard's verdict.
type ShardCheck struct {
	Path     string `json:"path"`
	Index    int    `json:"index"`
	Status   string `json:"status"`
	Detail   string `json:"detail,omitempty"`
	Rows     int64  `json:"rows"`
	Bytes    int64  `json:"bytes"`
	Repaired bool   `json:"repaired,omitempty"`
}

// OK reports whether the shard passed every check.
func (c ShardCheck) OK() bool { return c.Status == VerifyOK }

// VerifyReport is what one verify run found.
type VerifyReport struct {
	Repo   string `json:"repo"`
	Scope  string `json:"scope"`
	Shards int    `json:"shards"`
	Passed int    `json:"passed"`
	Failed int    `json:"failed"`

	Rows  int64 `json:"rows"`  // rows the shards actually hold
	Bytes int64 `json:"bytes"` // bytes the hub is holding for them

	LedgerShards int   `json:"ledger_shards"`
	LedgerRows   int64 `json:"ledger_rows"`
	LedgerBytes  int64 `json:"ledger_bytes"`

	// BytesRead is what the verify itself transferred, which is the number that
	// decides whether this is a cheap check or a download.
	BytesRead int64 `json:"bytes_read"`

	Checks []ShardCheck `json:"checks"`
	// Notes are the ledger disagreements, which are about the crawl rather than
	// about any one shard.
	Notes []string `json:"notes,omitempty"`
}

// Failures returns the shards that did not pass, in shard order.
func (r *VerifyReport) Failures() []ShardCheck {
	var out []ShardCheck
	for _, c := range r.Checks {
		if !c.OK() {
			out = append(out, c)
		}
	}
	return out
}

// Clean reports whether every shard passed and the ledger agrees with them.
func (r *VerifyReport) Clean() bool { return r.Failed == 0 && len(r.Notes) == 0 }

// ShardStore is where the published shards live. The hub is the only
// implementation that ships, and the interface is what lets the checks run
// against a local server in a test.
type ShardStore interface {
	// Sizes returns the byte size of each path that exists. A path that is not
	// there is absent from the map rather than an error.
	Sizes(ctx context.Context, paths []string) (map[string]int64, error)
	// URL is where a path can be read with ranged GETs.
	URL(path string) string
}

// HFShardStore reads shards from a HuggingFace dataset repo.
type HFShardStore struct {
	HF   *HFClient
	Repo string
}

// Sizes asks the hub's paths-info endpoint for the sizes it is holding.
func (s HFShardStore) Sizes(ctx context.Context, paths []string) (map[string]int64, error) {
	return s.HF.PathsInfo(ctx, s.Repo, paths)
}

// URL is the public download URL for a path on the repo's main branch.
func (s HFShardStore) URL(path string) string { return hfResolveURL(s.Repo, path) }

// VerifyOptions configures a verify run over a list of shard paths.
type VerifyOptions struct {
	Workers int             // shards checked at once, 0 picks a default
	Sample  int             // rows to decode from each shard, 0 reads the footer alone
	Schema  *parquet.Schema // the schema the dataset promises
	Repair  bool            // rebuild and re-upload the shards that fail
	Logf    func(string, ...any)
}

// VerifyShards checks every path in the list and returns what it found. The
// paths are expected to be the complete set for the unit, so a path the store
// does not have is a missing shard rather than something to skip.
func VerifyShards(ctx context.Context, h *HTTPClient, store ShardStore, paths []string, o VerifyOptions) (*VerifyReport, error) {
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
	workers := o.Workers
	if workers <= 0 {
		workers = budgetProcess(0)
	}
	sizes, err := store.Sizes(ctx, paths)
	if err != nil {
		return nil, err
	}

	rep := &VerifyReport{Shards: len(paths), Checks: make([]ShardCheck, len(paths))}
	var (
		mu   sync.Mutex
		read int64
	)
	jobs := make(chan int)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if ctx.Err() != nil {
					return
				}
				path := paths[i]
				c, n := verifyShard(ctx, h, store.URL(path), path, sizes[path], o)
				c.Index = i
				mu.Lock()
				rep.Checks[i] = c
				read += n
				mu.Unlock()
				if !c.OK() {
					o.Logf("%s: %s (%s)", path, c.Status, c.Detail)
				}
			}
		}()
	}
	for i := range paths {
		select {
		case jobs <- i:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return nil, ctx.Err()
		}
	}
	close(jobs)
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	rep.BytesRead = read
	for _, c := range rep.Checks {
		if c.OK() {
			rep.Passed++
		} else {
			rep.Failed++
		}
		rep.Rows += c.Rows
		rep.Bytes += c.Bytes
	}
	return rep, nil
}

// verifyShard runs the checks on one shard and reports the verdict along with
// the bytes it had to transfer to reach it.
func verifyShard(ctx context.Context, h *HTTPClient, url, path string, size int64, o VerifyOptions) (ShardCheck, int64) {
	c := ShardCheck{Path: path, Bytes: size}
	if size <= 0 {
		c.Status = VerifyMissing
		c.Detail = "not on the hub"
		return c, 0
	}

	// Small blocks: a footer read that pulls 8 MiB at a time would cost more
	// than the shards it is checking.
	ra := newHTTPReaderAt(ctx, h, url, size, 256<<10, 4)
	pf, err := parquet.OpenFile(ra, size)
	if err != nil {
		// A shard the store will not serve is not a bad shard. The reads are
		// unauthenticated, because the published datasets are public, so this is
		// what a private repo looks like from here.
		var status *httpStatusError
		if errors.As(err, &status) && (status.Status == 401 || status.Status == 403) {
			c.Status = VerifyNoAccess
			c.Detail = fmt.Sprintf("the store answered HTTP %d, so this says nothing about the shard", status.Status)
			return c, ra.BytesFetched()
		}
		c.Status = VerifyUnreadable
		c.Detail = err.Error()
		return c, ra.BytesFetched()
	}
	c.Rows = pf.NumRows()

	if detail := checkShardStructure(pf, size); detail != "" {
		c.Status = VerifyTruncated
		c.Detail = detail
		return c, ra.BytesFetched()
	}
	if o.Schema != nil {
		if detail := checkShardSchema(pf.Schema(), o.Schema); detail != "" {
			c.Status = VerifySchema
			c.Detail = detail
			return c, ra.BytesFetched()
		}
	}
	if c.Rows == 0 {
		c.Status = VerifyEmpty
		c.Detail = "no rows, which means the projection that wrote it produced nothing"
		return c, ra.BytesFetched()
	}
	if o.Sample > 0 {
		if detail := sampleShardRows(pf, o.Sample); detail != "" {
			c.Status = VerifyCorrupt
			c.Detail = detail
			return c, ra.BytesFetched()
		}
	}
	c.Status = VerifyOK
	return c, ra.BytesFetched()
}

// checkShardStructure asks whether the footer describes the object the hub is
// holding: the row groups have to add up to the file's row count, and every
// column chunk has to sit inside the file. An upload cut short can leave a
// footer that still parses when the tail happened to survive, and this is what
// catches that.
func checkShardStructure(pf *parquet.File, size int64) string {
	md := pf.Metadata()
	var rows int64
	for i := range md.RowGroups {
		rg := &md.RowGroups[i]
		rows += rg.NumRows
		for j := range rg.Columns {
			cm := &rg.Columns[j].MetaData
			start := cm.DataPageOffset
			if cm.DictionaryPageOffset > 0 && cm.DictionaryPageOffset < start {
				start = cm.DictionaryPageOffset
			}
			end := start + cm.TotalCompressedSize
			if start < 0 || end > size {
				return fmt.Sprintf("row group %d column %s ends at %d, past the %d bytes the store has",
					i, strings.Join(cm.PathInSchema, "."), end, size)
			}
		}
	}
	if rows != md.NumRows {
		return fmt.Sprintf("row groups hold %d rows and the footer claims %d", rows, md.NumRows)
	}
	return ""
}

// checkShardSchema compares the shard's leaf columns against the schema the
// dataset promises. It compares the columns rather than the whole schema string
// so a shard written by an older writer with the same columns still passes.
func checkShardSchema(got, want *parquet.Schema) string {
	wantCols := columnTypes(want)
	gotCols := columnTypes(got)
	var missing, extra, wrong []string
	for name, wt := range wantCols {
		gt, ok := gotCols[name]
		switch {
		case !ok:
			missing = append(missing, name)
		case gt != wt:
			wrong = append(wrong, fmt.Sprintf("%s is %s and should be %s", name, gt, wt))
		}
	}
	for name := range gotCols {
		if _, ok := wantCols[name]; !ok {
			extra = append(extra, name)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	sort.Strings(wrong)
	var parts []string
	if len(missing) > 0 {
		parts = append(parts, "missing "+strings.Join(missing, ", "))
	}
	if len(extra) > 0 {
		parts = append(parts, "unexpected "+strings.Join(extra, ", "))
	}
	if len(wrong) > 0 {
		parts = append(parts, strings.Join(wrong, "; "))
	}
	return strings.Join(parts, "; ")
}

// columnTypes maps each leaf column path to its physical type.
func columnTypes(s *parquet.Schema) map[string]string {
	out := map[string]string{}
	for _, path := range s.Columns() {
		name := strings.Join(path, ".")
		leaf, ok := s.Lookup(path...)
		if !ok {
			out[name] = "unknown"
			continue
		}
		out[name] = leaf.Node.Type().String()
	}
	return out
}

// sampleShardRows decodes rows out of the last row group, which is where a
// truncated body loses its data first, and reports what went wrong if the read
// fails. It reads every column, so a bad page in any of them shows up.
func sampleShardRows(pf *parquet.File, n int) string {
	groups := pf.RowGroups()
	if len(groups) == 0 {
		return ""
	}
	rg := groups[len(groups)-1]
	rows := rg.Rows()
	defer func() { _ = rows.Close() }()
	buf := make([]parquet.Row, min(int64(n), rg.NumRows()))
	if len(buf) == 0 {
		return ""
	}
	read := 0
	for read < len(buf) {
		got, err := rows.ReadRows(buf[read:])
		read += got
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Sprintf("reading rows from the last row group: %v", err)
		}
		if got == 0 {
			break
		}
	}
	if read == 0 {
		return "the last row group claims rows and returned none"
	}
	return ""
}

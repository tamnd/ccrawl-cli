package ccrawl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// A recrawl reads a work list that is already written down, which is the whole
// reason this file exists instead of a frontier.
//
// The frontier earns its keep on a discovery crawl: the queue is not known in
// advance, outlinks arrive as the crawl goes, and a crash has to leave something
// resumable behind. A recrawl has none of those properties. The list is
// published, deduplicated and sorted, and materialising it into SQLite only to
// read it back is copying a list in order to walk it. The measured cost of that
// copy is about 135 bytes a URL, so one server's share of a threefold recrawl of
// a monthly crawl is roughly 283 GB of database before a page is fetched, on
// machines with 2.8 GB and 6.6 GB free. It does not fit and it does not nearly
// fit, and the admit rate falls fourfold between two million and five million
// rows on the way to not fitting.
//
// So the work list streams. Parts are read in order, a row at a time, and the
// only state kept is which part and how far into it, which is two numbers.

// WorkItem is one URL to recrawl, with enough of its position to resume.
type WorkItem struct {
	URL  string
	Part int   // which published part it came from
	Row  int64 // its row number within that part, counted before the shard filter
}

// WorkSource names a published dataset to recrawl and how to turn its rows into
// URLs.
type WorkSource struct {
	// Repo is the HuggingFace dataset, for example open-index/ccrawl-domains.
	Repo string
	// Dir is the directory of parts inside it, for example
	// data/cc-main-2026-apr-may-jun.
	Dir string
	// Column is the string column holding the work, either "domain" or "url".
	// A domain is turned into its homepage, since a domain is not a URL.
	Column string
}

// Key identifies a source well enough to refuse a checkpoint written against a
// different one. Pointing a half finished run at another dataset would resume at
// a row offset that means nothing there, so it is worth catching.
func (s WorkSource) Key() string { return s.Repo + "/" + s.Dir + "#" + s.Column }

// Validate reports whether the source is one we can read.
func (s WorkSource) Validate() error {
	if s.Repo == "" {
		return errors.New("no dataset repo, for example open-index/ccrawl-domains")
	}
	if s.Column != "domain" && s.Column != "url" {
		return fmt.Errorf("column %q is neither domain nor url, and those are the two the published datasets carry", s.Column)
	}
	return nil
}

// FileURL is where a file inside the dataset lives.
func (s WorkSource) FileURL(file string) string {
	return "https://huggingface.co/datasets/" + s.Repo + "/resolve/main/" + strings.TrimPrefix(file, "/")
}

// datasetFiles lists every parquet file in a published dataset.
//
// The repo API answers without a token for a public dataset, which is what the
// fleet reads, and it is one request against a list that changes once a month.
// It returns every file in the repo in one response, so both the release
// directories and the parts inside one come out of the same call.
func datasetFiles(ctx context.Context, h *HTTPClient, repo string) ([]string, error) {
	url := "https://huggingface.co/api/datasets/" + repo
	resp, err := h.Get(ctx, url)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var payload struct {
		Siblings []struct {
			Path string `json:"rfilename"`
		} `json:"siblings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("reading the file list of %s: %w", repo, err)
	}
	var files []string
	for _, s := range payload.Siblings {
		if strings.HasSuffix(s.Path, ".parquet") {
			files = append(files, s.Path)
		}
	}
	return files, nil
}

// DatasetDirs lists the directories of parts inside a published dataset,
// newest first.
func DatasetDirs(ctx context.Context, h *HTTPClient, repo string) ([]string, error) {
	files, err := datasetFiles(ctx, h, repo)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var dirs []string
	for _, f := range files {
		i := strings.LastIndex(f, "/")
		if i < 0 {
			continue
		}
		d := f[:i]
		if !seen[d] {
			seen[d] = true
			dirs = append(dirs, d)
		}
	}
	sortDatasetDirs(dirs)
	return dirs, nil
}

// DatasetParts lists the parts of one release, in the order a run reads them.
//
// Parts used to be discovered by asking for part-000.parquet and treating the
// first 404 as the end of the dataset, which read the part number straight off
// a counter and formatted it three digits wide. open-index/ccrawl-domains
// happens to name its parts that way and open-index/ccrawl-urls names them
// part-00000.parquet, so a run pointed at the URL corpus asked for a file that
// is not there, read the 404 as the end of the work list, and reported that it
// had finished after fetching nothing. It exited zero while doing it.
//
// Listing the files says what is actually published instead of guessing at the
// name, it costs the one request the release directory already costs, and it
// still leaves nobody having to keep a part count in step with the next
// publish.
func DatasetParts(ctx context.Context, h *HTTPClient, repo, dir string) ([]string, error) {
	files, err := datasetFiles(ctx, h, repo)
	if err != nil {
		return nil, err
	}
	prefix := strings.Trim(dir, "/")
	if prefix != "" {
		prefix += "/"
	}
	var parts []string
	for _, f := range files {
		rest, ok := strings.CutPrefix(f, prefix)
		if !ok || strings.Contains(rest, "/") {
			continue
		}
		parts = append(parts, f)
	}
	sortParts(parts)
	return parts, nil
}

// sortParts orders parts by the number in the name rather than by the name.
//
// Zero padded names sort the same either way as long as every part in a release
// is padded to the same width, which every published one is. Sorting on the
// number means a release that is not padded, or one that outgrows its padding,
// is still read in the order it was written.
func sortParts(parts []string) {
	sort.Slice(parts, func(i, j int) bool {
		ni, nj := partNumber(parts[i]), partNumber(parts[j])
		if ni != nj {
			return ni < nj
		}
		return parts[i] < parts[j]
	})
}

// partNumber pulls the digits out of a part name, or -1 if there are none, so
// an unnumbered file sorts ahead of the numbered ones rather than among them.
func partNumber(p string) int {
	base := path.Base(p)
	start := strings.IndexFunc(base, func(r rune) bool { return r >= '0' && r <= '9' })
	if start < 0 {
		return -1
	}
	n := 0
	for _, r := range base[start:] {
		if r < '0' || r > '9' {
			break
		}
		n = n*10 + int(r-'0')
	}
	return n
}

// sortDatasetDirs puts the newest release first.
//
// Web graph releases are named cc-main-2026-apr-may-jun, which does not sort by
// string, so they go through the same rank the graph commands use. Crawl IDs
// are named CC-MAIN-2026-25, which does sort by string. A directory that is
// neither falls back to its name, and since the two conventions never appear in
// one repo, the fallback never has to compare across them.
func sortDatasetDirs(dirs []string) {
	sort.Slice(dirs, func(i, j int) bool {
		ri, rj := graphRank(strings.ToLower(path.Base(dirs[i]))), graphRank(strings.ToLower(path.Base(dirs[j])))
		if ri != rj {
			return ri > rj
		}
		return dirs[i] > dirs[j]
	})
}

// domainWork and urlWork project the one column the work list needs out of a
// published row. Reading a single column is the point: parquet-go fetches only
// that column's chunks, so streaming the work list out of a shard costs a
// fraction of the shard.
type domainWork struct {
	Domain string `parquet:"domain"`
}

type urlWork struct {
	URL string `parquet:"url"`
}

// Checkpoint is everything a killed recrawl needs to pick up where it stopped.
//
// It is two numbers and the identity of what they point into, and that is
// deliberate. The state a run keeps has to be bounded and independent of the
// size of the work list, or it becomes the frontier this whole design exists to
// avoid. A billion row work list and a thousand row one have the same
// checkpoint, a few hundred bytes.
type Checkpoint struct {
	Source  string `json:"source"`
	Shard   int    `json:"shard"`
	Shards  int    `json:"shards"`
	Part    int    `json:"part"`
	Row     int64  `json:"row"` // rows consumed from Part, counted before the shard filter
	Done    bool   `json:"done"`
	Fetched int64  `json:"fetched"`
}

// LoadCheckpoint reads a checkpoint, returning a zero one if the file is not
// there yet. A first run and a resumed run then take the same path.
func LoadCheckpoint(path string) (Checkpoint, error) {
	var c Checkpoint
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return c, nil
	}
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return c, fmt.Errorf("the state file %s is not a checkpoint: %w", path, err)
	}
	return c, nil
}

// Save writes the checkpoint so that a kill at any moment leaves either the old
// one or the new one on disk and never half of either.
//
// The write goes to a temporary file in the same directory, is flushed to the
// platter, and is then renamed over the target, which is atomic on every
// filesystem we run on. Writing in place would leave a truncated file if the
// process died between the truncate and the write, and a recrawl that cannot
// read its own checkpoint starts from the beginning of a hundred day run.
func (c Checkpoint) Save(path string) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".ckpt-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }()
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// WorkList streams a published dataset as URLs to recrawl.
//
// It holds one part open at a time and a read buffer, so its memory is the same
// on the first row and the billionth. The parts themselves are listed once from
// the dataset and read in order, so the end of the work list is the end of the
// list of parts rather than the first name that happens to 404.
type WorkList struct {
	src   WorkSource
	shard Shard
	h     *HTTPClient
	// open resolves a part to something parquet can read. It is a field so the
	// tests can hand it local files instead of standing up a dataset.
	open func(ctx context.Context, p int) (io.ReaderAt, int64, error)
	// parts is the published part list, resolved on first use so that
	// constructing a work list stays free of the network.
	parts []string

	part int
	row  int64
	rows *partReader
	done bool
}

// partReader is one part being read, and the buffer it is read through.
type partReader struct {
	pf     *parquet.File
	domain *parquet.GenericReader[domainWork]
	url    *parquet.GenericReader[urlWork]
	dbuf   []domainWork
	ubuf   []urlWork
	n, i   int // rows in the buffer, and how far through it we are
}

func (p *partReader) Close() {
	if p.domain != nil {
		_ = p.domain.Close()
	}
	if p.url != nil {
		_ = p.url.Close()
	}
}

// errPartNotPublished says a part number is past the end of the dataset.
var errPartNotPublished = errors.New("part not published")

// NewWorkList opens a work list at the position the checkpoint names. A zero
// checkpoint starts at the beginning.
func NewWorkList(src WorkSource, shard Shard, h *HTTPClient, at Checkpoint) (*WorkList, error) {
	if err := src.Validate(); err != nil {
		return nil, err
	}
	if err := shard.Validate(); err != nil {
		return nil, err
	}
	if at.Source != "" && at.Source != src.Key() {
		return nil, fmt.Errorf("the checkpoint was written against %s and this run reads %s, so its row offset points at nothing here", at.Source, src.Key())
	}
	if at.Source != "" && (at.Shard != shard.Index || at.Shards != shard.Count) {
		return nil, fmt.Errorf("the checkpoint was written by shard %d of %d and this is shard %d of %d, so resuming it would skip work", at.Shard, at.Shards, shard.Index, shard.Count)
	}
	w := &WorkList{src: src, shard: shard, h: h, part: at.Part, row: at.Row, done: at.Done}
	w.open = w.openRemotePart
	return w, nil
}

// openRemotePart resolves a part to a ranged reader over the published file.
func (w *WorkList) openRemotePart(ctx context.Context, p int) (io.ReaderAt, int64, error) {
	if w.parts == nil {
		parts, err := DatasetParts(ctx, w.h, w.src.Repo, w.src.Dir)
		if err != nil {
			return nil, 0, err
		}
		if len(parts) == 0 {
			return nil, 0, fmt.Errorf("the dataset %s publishes no parts under %s, so there is nothing to recrawl", w.src.Repo, w.src.Dir)
		}
		w.parts = parts
	}
	if p >= len(w.parts) {
		return nil, 0, errPartNotPublished
	}
	url := w.src.FileURL(w.parts[p])
	size, err := w.h.ContentLength(ctx, url)
	if err != nil {
		// A part that is listed but cannot be fetched is a failure, not the end
		// of the work list. Reading it as the end is how a run reports that it
		// finished a hundred day job in half a second.
		var se *httpStatusError
		if errors.As(err, &se) && se.Status == http.StatusNotFound {
			return nil, 0, fmt.Errorf("the part %s is listed in %s but is not there", w.parts[p], w.src.Repo)
		}
		return nil, 0, err
	}
	return newHTTPReaderAt(ctx, w.h, url, size, 8<<20, 4), size, nil
}

// Position is where the next unread row sits, which is what a checkpoint saves.
func (w *WorkList) Position() (part int, row int64, done bool) {
	return w.part, w.row, w.done
}

// Next fills buf with the next items this shard owns and returns how many. It
// returns 0 with a nil error only at the end of the work list.
//
// Rows this shard does not own are counted and dropped as they go by, so the
// row offset means the same thing on every machine in the fleet and a
// checkpoint is comparable across them.
func (w *WorkList) Next(ctx context.Context, buf []WorkItem) (int, error) {
	var n int
	for n < len(buf) {
		if w.done {
			return n, nil
		}
		if w.rows == nil {
			r, err := w.openPart(ctx, w.part)
			if errors.Is(err, errPartNotPublished) {
				w.done = true
				return n, nil
			}
			if err != nil {
				return n, err
			}
			w.rows = r
			// Skipping is a read rather than a seek, because a row offset is not
			// a byte offset and parquet has no cheap way to turn one into the
			// other without an index we do not publish. Reading through is a
			// column scan of at most one part, which costs seconds against a run
			// measured in days.
			if err := w.skipTo(ctx, w.row); err != nil {
				return n, err
			}
			// skipTo lands past the end of a part when the checkpoint named its
			// last row, and moves to the next one rather than stalling. There is
			// nothing open to read from until the loop comes round again.
			if w.rows == nil {
				continue
			}
		}
		raw, err := w.readRow(ctx)
		if errors.Is(err, io.EOF) {
			w.rows.Close()
			w.rows = nil
			w.part++
			w.row = 0
			continue
		}
		if err != nil {
			return n, err
		}
		row := w.row
		w.row++
		u := w.toURL(raw)
		if u == "" || !w.shard.Owns(u) {
			continue
		}
		buf[n] = WorkItem{URL: u, Part: w.part, Row: row}
		n++
	}
	return n, nil
}

// toURL turns a published cell into something fetchable. A URL is already one.
// A domain is not, and the homepage is what a domain level recrawl means.
func (w *WorkList) toURL(cell string) string {
	cell = strings.TrimSpace(cell)
	if cell == "" {
		return ""
	}
	if w.src.Column == "url" {
		return cell
	}
	return "https://" + cell + "/"
}

// openPart opens one part for reading.
func (w *WorkList) openPart(ctx context.Context, p int) (*partReader, error) {
	ra, size, err := w.open(ctx, p)
	if err != nil {
		return nil, err
	}
	pf, err := parquet.OpenFile(ra, size)
	if err != nil {
		return nil, fmt.Errorf("part %d: %w", p, err)
	}
	r := &partReader{pf: pf}
	// The buffer is the only thing that grows with anything, and it is fixed.
	// Four thousand rows of one string column is well under a megabyte and it is
	// the same megabyte for a dataset of any size.
	const buffered = 4096
	if w.src.Column == "url" {
		r.url = parquet.NewGenericReader[urlWork](pf)
		r.ubuf = make([]urlWork, buffered)
	} else {
		r.domain = parquet.NewGenericReader[domainWork](pf)
		r.dbuf = make([]domainWork, buffered)
	}
	return r, nil
}

// readRow returns the next raw cell from the open part, or io.EOF at its end.
func (w *WorkList) readRow(ctx context.Context) (string, error) {
	r := w.rows
	for r.i >= r.n {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		var err error
		if r.url != nil {
			r.n, err = r.url.Read(r.ubuf)
		} else {
			r.n, err = r.domain.Read(r.dbuf)
		}
		r.i = 0
		if r.n == 0 {
			if err == nil {
				err = io.EOF
			}
			return "", err
		}
		// A short read with an error still handed us rows, and dropping them
		// because the error arrived in the same call would lose the tail of
		// every part.
		if err != nil && !errors.Is(err, io.EOF) {
			return "", err
		}
	}
	i := r.i
	r.i++
	if r.url != nil {
		return r.ubuf[i].URL, nil
	}
	return r.dbuf[i].Domain, nil
}

// skipTo walks the open part forward to a row offset.
func (w *WorkList) skipTo(ctx context.Context, row int64) error {
	for i := int64(0); i < row; i++ {
		if _, err := w.readRow(ctx); err != nil {
			if errors.Is(err, io.EOF) {
				// The checkpoint points past the end of the part, which is what a
				// run that finished a part and died before writing the next one
				// leaves behind. Move on rather than stall.
				w.rows.Close()
				w.rows = nil
				w.part++
				w.row = 0
				return nil
			}
			return err
		}
	}
	return nil
}

// Close releases the part currently open.
func (w *WorkList) Close() {
	if w.rows != nil {
		w.rows.Close()
		w.rows = nil
	}
}

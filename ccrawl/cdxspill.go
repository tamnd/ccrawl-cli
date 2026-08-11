package ccrawl

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

// This file holds the pieces that let a CDX query pick one capture per URL, or
// skip URLs it has already emitted, without holding the whole result set.
//
// Everything here leans on one property of the index: a CDX response is sorted
// by urlkey, so every capture of a URL arrives in one contiguous run. A query
// over several crawls is therefore a set of sorted streams rather than one, and
// that is the difference between "buffer everything and reduce at the end" and
// "reduce each stream as it arrives, then merge". The first is a map the size of
// the result set, which on a domain wildcard across every crawl is the most
// likely out of memory anywhere in the read path. The second is a few records
// per stream plus whatever the merge writes to disk.
//
// The urlkey is a function of the URL, so grouping by urlkey never splits a URL
// across two groups. Captures of two different URLs can share a urlkey and can
// interleave inside the group, which is why a group is reduced with a small map
// rather than a single slot.

// DefaultCDXMaxBuffer is how many records a picker or a URL log keeps in memory
// before it starts writing to disk. Five million CDX records is a few hundred
// megabytes, which is a lot to hold and still far less than a wildcard query
// over every crawl would ask for.
const DefaultCDXMaxBuffer = 5_000_000

// CDXPicker keeps at most one record per URL across a series of CDX streams and
// emits the winners in urlkey order.
//
// Records are added one stream at a time, in the order the index returned them,
// with EndStream between streams. The winner of a URL inside one stream is
// decided as the stream is read, so a stream costs one urlkey group of memory
// no matter how many records it carries. Sealed streams are merged at the end,
// which costs one group per stream.
//
// Past MaxBuffer records a sealed stream lives in a temporary file instead of
// on the heap, and Spilled reports it so a caller can say why the query got
// slower.
type CDXPicker struct {
	// better reports whether cand should replace cur as the winner for a URL.
	// It is only ever asked about two records with the same URL. Returning
	// false on a tie is what keeps the earliest stream's record, which is the
	// order the buffered implementation resolved ties in.
	better func(cand, cur CDXRecord) bool

	max  int
	dir  string
	runs []*cdxRun

	// mem is how many records every stream is holding between them, and onDisk
	// says the budget is spent and everything goes to a file now.
	mem    int
	onDisk bool

	cur     *cdxRun
	group   map[string]CDXRecord
	order   []string
	groupID string
	started bool

	spilled bool
}

// NewCDXPicker returns a picker that resolves competing captures of one URL
// with better. maxBuffer is the in memory record budget per stream; zero or
// less means DefaultCDXMaxBuffer.
func NewCDXPicker(better func(cand, cur CDXRecord) bool, maxBuffer int) *CDXPicker {
	if maxBuffer <= 0 {
		maxBuffer = DefaultCDXMaxBuffer
	}
	return &CDXPicker{better: better, max: maxBuffer, group: map[string]CDXRecord{}}
}

// Add offers one record to the picker. Records must arrive in the order the
// index returned them, which is urlkey order.
func (p *CDXPicker) Add(r CDXRecord) error {
	if p.cur == nil {
		p.cur = &cdxRun{}
	}
	if !p.started || r.URLKey != p.groupID {
		if err := p.closeGroup(); err != nil {
			return err
		}
		p.groupID, p.started = r.URLKey, true
	}
	cur, ok := p.group[r.URL]
	if !ok {
		p.group[r.URL] = r
		p.order = append(p.order, r.URL)
		return nil
	}
	if p.better(r, cur) {
		p.group[r.URL] = r
	}
	return nil
}

// EndStream seals the stream in progress. Every stream needs one, including the
// last, and a stream that produced nothing is dropped rather than sealed.
func (p *CDXPicker) EndStream() error {
	if err := p.closeGroup(); err != nil {
		return err
	}
	p.started = false
	if p.cur == nil {
		return nil
	}
	if err := p.cur.seal(); err != nil {
		return err
	}
	if p.cur.len() > 0 {
		p.runs = append(p.runs, p.cur)
	} else {
		_ = p.cur.remove()
	}
	p.cur = nil
	return nil
}

// closeGroup writes the winners of the urlkey group just finished into the
// stream in progress, in the order their URLs first appeared.
func (p *CDXPicker) closeGroup() error {
	if len(p.order) == 0 {
		return nil
	}
	for _, u := range p.order {
		if err := p.write(p.group[u]); err != nil {
			return err
		}
	}
	p.order = p.order[:0]
	clear(p.group)
	return nil
}

// write puts one winner into the stream in progress, on the heap while the
// budget allows and in a file after that.
//
// The budget covers every stream at once rather than each one on its own. A
// query over a hundred crawls keeps a hundred sealed streams, and the same URL
// usually turns up in most of them, so a per stream budget would be a hundred
// times the ceiling it looks like.
func (p *CDXPicker) write(rec CDXRecord) error {
	p.cur.n++
	if !p.onDisk {
		if p.mem < p.max {
			p.mem++
			p.cur.recs = append(p.cur.recs, rec)
			return nil
		}
		if err := p.spillAll(); err != nil {
			return err
		}
	}
	if p.cur.f == nil {
		if err := p.cur.spill(p.dir); err != nil {
			return err
		}
	}
	return writeCDXLine(p.cur.w, rec)
}

// spillAll moves every stream still on the heap into a file and leaves the
// picker writing straight to disk from then on.
func (p *CDXPicker) spillAll() error {
	if p.dir == "" {
		dir, err := os.MkdirTemp("", "ccrawl-cdx-")
		if err != nil {
			return err
		}
		p.dir = dir
	}
	for _, run := range p.runs {
		if run.onDisk() || len(run.recs) == 0 {
			continue
		}
		if err := run.spill(p.dir); err != nil {
			return err
		}
		if err := run.seal(); err != nil {
			return err
		}
	}
	if p.cur != nil && !p.cur.onDisk() {
		if err := p.cur.spill(p.dir); err != nil {
			return err
		}
	}
	p.mem, p.onDisk, p.spilled = 0, true, true
	return nil
}

// Spilled reports whether any stream had to be written to disk.
func (p *CDXPicker) Spilled() bool { return p.spilled }

// Each merges the sealed streams and calls fn once per URL, in urlkey order.
// It is valid to call it once.
func (p *CDXPicker) Each(fn func(CDXRecord) error) error {
	readers := make([]*cdxRunReader, 0, len(p.runs))
	for _, run := range p.runs {
		rd, err := run.reader()
		if err != nil {
			return err
		}
		defer rd.close()
		readers = append(readers, rd)
	}

	group := map[string]CDXRecord{}
	var order []string
	for {
		key, ok := smallestKey(readers)
		if !ok {
			return nil
		}
		// Drain the group from every stream that has it. Reader order is stream
		// order, so a tie inside the group is settled by whichever stream came
		// first, exactly as a single pass over the concatenated streams would.
		for _, rd := range readers {
			for {
				r, ok, err := rd.peek()
				if err != nil {
					return err
				}
				if !ok || r.URLKey != key {
					break
				}
				rd.advance()
				cur, seen := group[r.URL]
				if !seen {
					group[r.URL] = r
					order = append(order, r.URL)
					continue
				}
				if p.better(r, cur) {
					group[r.URL] = r
				}
			}
		}
		for _, u := range order {
			if err := fn(group[u]); err != nil {
				return err
			}
		}
		order = order[:0]
		clear(group)
	}
}

// Close removes anything the picker wrote to disk.
func (p *CDXPicker) Close() error {
	var first error
	if p.cur != nil {
		if err := p.cur.remove(); err != nil && first == nil {
			first = err
		}
		p.cur = nil
	}
	for _, run := range p.runs {
		if err := run.remove(); err != nil && first == nil {
			first = err
		}
	}
	p.runs = nil
	if p.dir != "" {
		if err := os.RemoveAll(p.dir); err != nil && first == nil {
			first = err
		}
		p.dir = ""
	}
	return first
}

// smallestKey returns the lowest urlkey any reader is sitting on.
func smallestKey(readers []*cdxRunReader) (string, bool) {
	key, found := "", false
	for _, rd := range readers {
		r, ok, err := rd.peek()
		if err != nil || !ok {
			continue
		}
		if !found || r.URLKey < key {
			key, found = r.URLKey, true
		}
	}
	return key, found
}

// cdxRun is one sorted run of records: a slice while it is small enough, a file
// of JSON Lines once it is not.
type cdxRun struct {
	recs []CDXRecord
	n    int

	path string
	f    *os.File
	w    *bufio.Writer
}

func (r *cdxRun) len() int     { return r.n }
func (r *cdxRun) onDisk() bool { return r.path != "" }

// spill moves the records held so far into a file and switches the run over to
// writing. The run keeps its place in the merge either way.
func (r *cdxRun) spill(dir string) error {
	f, err := os.CreateTemp(dir, "run-*.jsonl")
	if err != nil {
		return err
	}
	r.f, r.path, r.w = f, f.Name(), bufio.NewWriterSize(f, 1<<20)
	for _, rec := range r.recs {
		if err := writeCDXLine(r.w, rec); err != nil {
			return err
		}
	}
	r.recs = nil
	return nil
}

func (r *cdxRun) seal() error {
	if r.f == nil {
		return nil
	}
	if err := r.w.Flush(); err != nil {
		return err
	}
	err := r.f.Close()
	r.f, r.w = nil, nil
	return err
}

func (r *cdxRun) remove() error {
	if r.f != nil {
		_ = r.f.Close()
		r.f, r.w = nil, nil
	}
	if r.path == "" {
		return nil
	}
	err := os.Remove(r.path)
	r.path = ""
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (r *cdxRun) reader() (*cdxRunReader, error) {
	if r.path == "" {
		return &cdxRunReader{recs: r.recs}, nil
	}
	f, err := os.Open(r.path)
	if err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	return &cdxRunReader{f: f, sc: sc}, nil
}

// cdxRunReader reads a sealed run one record at a time, from memory or from
// disk, with a single record of lookahead so the merge can compare heads.
type cdxRunReader struct {
	recs []CDXRecord
	i    int

	f  *os.File
	sc *bufio.Scanner

	head   CDXRecord
	loaded bool
	done   bool
}

func (rd *cdxRunReader) peek() (CDXRecord, bool, error) {
	if rd.loaded {
		return rd.head, true, nil
	}
	if rd.done {
		return CDXRecord{}, false, nil
	}
	if rd.sc == nil {
		if rd.i >= len(rd.recs) {
			rd.done = true
			return CDXRecord{}, false, nil
		}
		rd.head, rd.loaded = rd.recs[rd.i], true
		rd.i++
		return rd.head, true, nil
	}
	for rd.sc.Scan() {
		line := strings.TrimSpace(rd.sc.Text())
		if line == "" {
			continue
		}
		var rec CDXRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return CDXRecord{}, false, fmt.Errorf("read spilled cdx run: %w", err)
		}
		rd.head, rd.loaded = rec, true
		return rd.head, true, nil
	}
	if err := rd.sc.Err(); err != nil {
		return CDXRecord{}, false, fmt.Errorf("read spilled cdx run: %w", err)
	}
	rd.done = true
	return CDXRecord{}, false, nil
}

func (rd *cdxRunReader) advance() { rd.loaded = false }

func (rd *cdxRunReader) close() {
	if rd.f != nil {
		_ = rd.f.Close()
		rd.f = nil
	}
}

func writeCDXLine(w *bufio.Writer, rec CDXRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	return w.WriteByte('\n')
}

// CDXURLLog answers "did an earlier crawl already emit this URL" for a query
// that streams its output instead of collecting it.
//
// The set of URLs emitted from one crawl arrives in urlkey order and is written
// down in that order, so the next crawl, which also arrives in urlkey order, can
// be checked against it with a cursor that only ever moves forward. That makes
// the check exact at any result size, for the price of one open reader per
// crawl already done, rather than a map holding every URL of the query.
//
// The logs stay on the heap until they hold maxBuffer entries between them and
// move to temporary files after that.
type CDXURLLog struct {
	max int
	dir string

	past []*urlLogCursor
	cur  *urlLog

	// mem is how many entries every log is holding between them, and onDisk
	// says the budget is spent and everything goes to a file now.
	mem    int
	onDisk bool

	group   map[string]bool
	groupID string
	started bool

	spilled bool
}

// NewCDXURLLog returns an empty log. maxBuffer is the in memory entry budget
// across every crawl; zero or less means DefaultCDXMaxBuffer.
func NewCDXURLLog(maxBuffer int) *CDXURLLog {
	if maxBuffer <= 0 {
		maxBuffer = DefaultCDXMaxBuffer
	}
	return &CDXURLLog{max: maxBuffer, group: map[string]bool{}}
}

// Seen reports whether this URL was already emitted, either earlier in the
// crawl being read or by a crawl already finished. Records must arrive in
// urlkey order.
func (l *CDXURLLog) Seen(r CDXRecord) (bool, error) {
	if !l.started || r.URLKey != l.groupID {
		l.groupID, l.started = r.URLKey, true
		clear(l.group)
		for _, c := range l.past {
			if err := c.seek(r.URLKey); err != nil {
				return false, err
			}
		}
	}
	if l.group[r.URL] {
		return true, nil
	}
	for _, c := range l.past {
		if c.has(r.URL) {
			return true, nil
		}
	}
	return false, nil
}

// Emitted records that this URL went out, so a later crawl skips it.
func (l *CDXURLLog) Emitted(r CDXRecord) error {
	l.group[r.URL] = true
	if l.cur == nil {
		l.cur = &urlLog{}
	}
	if !l.onDisk {
		if l.mem < l.max {
			l.mem++
			l.cur.keys, l.cur.urls = append(l.cur.keys, r.URLKey), append(l.cur.urls, r.URL)
			return nil
		}
		if err := l.spillAll(); err != nil {
			return err
		}
	}
	if l.cur.f == nil {
		if err := l.cur.spill(l.dir); err != nil {
			return err
		}
	}
	return writeURLLine(l.cur.w, r.URLKey, r.URL)
}

// spillAll moves every log still on the heap into a file, including the ones
// already being read, and leaves the log writing straight to disk from then on.
//
// The budget covers every crawl at once. A query over a hundred crawls keeps a
// hundred logs, so a budget applied to each of them on its own would be a
// hundred times the ceiling the flag appears to set.
func (l *CDXURLLog) spillAll() error {
	if l.dir == "" {
		dir, err := os.MkdirTemp("", "ccrawl-cdx-")
		if err != nil {
			return err
		}
		l.dir = dir
	}
	for _, c := range l.past {
		if err := c.spill(l.dir); err != nil {
			return err
		}
	}
	if l.cur != nil && l.cur.f == nil {
		if err := l.cur.spill(l.dir); err != nil {
			return err
		}
	}
	l.mem, l.onDisk, l.spilled = 0, true, true
	return nil
}

// EndCrawl seals the crawl in progress so the next one can be checked against
// it. Skip it after the final crawl and nothing is written.
//
// A cursor only moves forward, so the crawl that just finished left every
// earlier one parked at the end of its log. They all go back to the start here,
// which makes each crawl one sequential pass over each earlier log. That is
// more passes than a map would need and it is still nowhere near the cost of
// the HTTP requests that produced the records in the first place.
func (l *CDXURLLog) EndCrawl() error {
	l.started = false
	clear(l.group)
	for _, c := range l.past {
		if err := c.rewind(); err != nil {
			return err
		}
	}
	if l.cur == nil {
		return nil
	}
	c, err := l.cur.cursor()
	if err != nil {
		return err
	}
	l.spilled = l.spilled || l.cur.path != ""
	l.past = append(l.past, c)
	l.cur = nil
	return nil
}

// Spilled reports whether any crawl's URLs had to be written to disk.
func (l *CDXURLLog) Spilled() bool { return l.spilled }

// Close releases the readers and removes anything written to disk.
func (l *CDXURLLog) Close() error {
	for _, c := range l.past {
		c.close()
	}
	l.past = nil
	if l.cur != nil {
		_ = l.cur.close()
		l.cur = nil
	}
	if l.dir == "" {
		return nil
	}
	err := os.RemoveAll(l.dir)
	l.dir = ""
	return err
}

// urlLog is the (urlkey, url) pairs one crawl emitted, in urlkey order.
type urlLog struct {
	keys []string
	urls []string

	path string
	f    *os.File
	w    *bufio.Writer
}

// spill moves the pairs held so far into a file and switches the log over to
// writing.
func (l *urlLog) spill(dir string) error {
	f, err := os.CreateTemp(dir, "urls-*.tsv")
	if err != nil {
		return err
	}
	l.f, l.path, l.w = f, f.Name(), bufio.NewWriterSize(f, 1<<20)
	for i := range l.keys {
		if err := writeURLLine(l.w, l.keys[i], l.urls[i]); err != nil {
			return err
		}
	}
	l.keys, l.urls = nil, nil
	return nil
}

func (l *urlLog) cursor() (*urlLogCursor, error) {
	if l.f == nil {
		return &urlLogCursor{keys: l.keys, urls: l.urls, group: map[string]bool{}}, nil
	}
	if err := l.w.Flush(); err != nil {
		return nil, err
	}
	if err := l.f.Close(); err != nil {
		return nil, err
	}
	l.f, l.w = nil, nil
	f, err := os.Open(l.path)
	if err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	return &urlLogCursor{f: f, sc: sc, group: map[string]bool{}}, nil
}

func (l *urlLog) close() error {
	if l.f != nil {
		_ = l.f.Close()
		l.f, l.w = nil, nil
	}
	if l.path == "" {
		return nil
	}
	err := os.Remove(l.path)
	l.path = ""
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// urlLogCursor walks one crawl's log forward only. seek moves it to a urlkey
// and loads that group's URLs, which is the only part of the log resident at
// any moment.
type urlLogCursor struct {
	keys []string
	urls []string
	i    int

	f  *os.File
	sc *bufio.Scanner

	headKey, headURL string
	loaded, done     bool

	group   map[string]bool
	groupID string
	atGroup bool
}

func (c *urlLogCursor) seek(key string) error {
	c.atGroup, c.groupID = false, key
	clear(c.group)
	for {
		k, u, ok, err := c.peek()
		if err != nil || !ok {
			return err
		}
		if k < key {
			c.loaded = false
			continue
		}
		if k > key {
			return nil
		}
		c.atGroup = true
		c.group[u] = true
		c.loaded = false
	}
}

func (c *urlLogCursor) has(url string) bool { return c.atGroup && c.group[url] }

// spill writes a cursor that is still on the heap out to a file, so a budget
// overrun reclaims the logs being read as well as the one being written. The
// cursor is usually part way through its log, so the reader it ends up with is
// parked at the entry the slices were parked at rather than at the start.
func (c *urlLogCursor) spill(dir string) error {
	if c.sc != nil {
		return nil
	}
	if len(c.keys) == 0 {
		c.keys, c.urls = nil, nil
		return nil
	}
	f, err := os.CreateTemp(dir, "urls-*.tsv")
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	for i := range c.keys {
		if err := writeURLLine(w, c.keys[i], c.urls[i]); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	pos := c.i
	if c.loaded {
		pos--
	}
	c.keys, c.urls, c.i = nil, nil, 0
	c.f = f
	c.sc = bufio.NewScanner(f)
	c.sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for n := 0; n < pos; n++ {
		if !c.sc.Scan() {
			break
		}
	}
	if !c.loaded {
		return nil
	}
	c.loaded = false
	_, _, ok, err := c.peek()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("spilled url log lost its place at entry %d", pos)
	}
	return nil
}

// rewind puts the cursor back at the first entry so the next crawl can be
// checked against the whole log again.
func (c *urlLogCursor) rewind() error {
	c.i, c.loaded, c.done, c.atGroup = 0, false, false, false
	c.groupID = ""
	clear(c.group)
	if c.f == nil {
		return nil
	}
	if _, err := c.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	c.sc = bufio.NewScanner(c.f)
	c.sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	return nil
}

func (c *urlLogCursor) peek() (string, string, bool, error) {
	if c.loaded {
		return c.headKey, c.headURL, true, nil
	}
	if c.done {
		return "", "", false, nil
	}
	if c.sc == nil {
		if c.i >= len(c.keys) {
			c.done = true
			return "", "", false, nil
		}
		c.headKey, c.headURL, c.loaded = c.keys[c.i], c.urls[c.i], true
		c.i++
		return c.headKey, c.headURL, true, nil
	}
	for c.sc.Scan() {
		line := c.sc.Text()
		tab := strings.IndexByte(line, '\t')
		if tab < 0 {
			continue
		}
		c.headKey, c.headURL, c.loaded = line[:tab], line[tab+1:], true
		return c.headKey, c.headURL, true, nil
	}
	if err := c.sc.Err(); err != nil {
		return "", "", false, fmt.Errorf("read spilled url log: %w", err)
	}
	c.done = true
	return "", "", false, nil
}

func (c *urlLogCursor) close() {
	if c.f != nil {
		_ = c.f.Close()
		c.f = nil
	}
}

func writeURLLine(w *bufio.Writer, key, url string) error {
	if _, err := w.WriteString(key); err != nil {
		return err
	}
	if err := w.WriteByte('\t'); err != nil {
		return err
	}
	if _, err := w.WriteString(url); err != nil {
		return err
	}
	return w.WriteByte('\n')
}

// CDXDigestSet is the set behind --dedup: the payload digests already emitted.
//
// Digests are not sorted in a CDX response and nothing about the index says
// where a digest will turn up again, so the cursor trick the URL log uses does
// not apply and this is a plain set with a ceiling. Past the ceiling it forgets
// its coldest entries rather than growing, on the same two generation scheme
// the crawl frontier uses, so the error is a duplicate that gets through rather
// than a unique record that gets dropped. That direction is not negotiable: a
// filter that quietly removes pages you asked for is worse than one that leaves
// a few extra in.
type CDXDigestSet struct {
	seen    *seenCache
	evicted bool
	limit   int
}

// NewCDXDigestSet returns a set holding up to about maxBuffer digests; zero or
// less means DefaultCDXMaxBuffer.
func NewCDXDigestSet(maxBuffer int) *CDXDigestSet {
	if maxBuffer <= 0 {
		maxBuffer = DefaultCDXMaxBuffer
	}
	return &CDXDigestSet{seen: newSeenCache(maxBuffer), limit: maxBuffer}
}

// Add records a digest and reports whether it was already there.
func (s *CDXDigestSet) Add(digest string) bool {
	known := s.seen.add(seenKey(digest))
	if !known && s.seen.len() > s.limit {
		s.evicted = true
	}
	return known
}

// Evicted reports whether the set has passed its ceiling, which is the point
// past which a duplicate can slip through.
func (s *CDXDigestSet) Evicted() bool { return s.evicted }

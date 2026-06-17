package ccrawl

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
)

// ── tokenization ──────────────────────────────────────────────────────────────

// englishStopwords is a minimal set of high-frequency English stopwords.
var englishStopwords = map[string]bool{
	"a": true, "an": true, "the": true, "and": true, "or": true, "but": true,
	"in": true, "on": true, "at": true, "to": true, "for": true, "of": true,
	"with": true, "by": true, "from": true, "is": true, "are": true,
	"was": true, "were": true, "be": true, "been": true, "being": true,
	"have": true, "has": true, "had": true, "do": true, "does": true,
	"did": true, "will": true, "would": true, "could": true, "should": true,
	"it": true, "its": true, "this": true, "that": true, "these": true,
	"those": true, "i": true, "we": true, "you": true, "he": true, "she": true,
	"they": true, "me": true, "us": true, "him": true, "her": true, "them": true,
	"not": true, "no": true, "nor": true, "so": true, "yet": true,
}

// Tokenize splits text into lowercase, stopword-filtered, min-2-char tokens.
func Tokenize(text string) []string {
	var tokens []string
	var buf strings.Builder
	flush := func() {
		if buf.Len() >= 2 {
			tok := buf.String()
			if !englishStopwords[tok] && len(tok) <= 50 {
				tokens = append(tokens, tok)
			}
		}
		buf.Reset()
	}
	for _, r := range strings.ToLower(text) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			buf.WriteRune(r)
		} else {
			flush()
		}
	}
	flush()
	return tokens
}

// ── BM25 scoring ─────────────────────────────────────────────────────────────

// BM25Params holds the BM25 hyper-parameters.
type BM25Params struct {
	K1 float64 // term frequency saturation (default 1.2)
	B  float64 // length normalization (default 0.75)
}

// DefaultBM25Params are the standard Okapi BM25 defaults.
var DefaultBM25Params = BM25Params{K1: 1.2, B: 0.75}

// BM25IDF computes the IDF component for a term given N total docs and df doc
// frequency.
func BM25IDF(N, df int) float64 {
	if df == 0 {
		return 0
	}
	return math.Log((float64(N)-float64(df)+0.5)/(float64(df)+0.5) + 1)
}

// BM25TF computes the BM25 TF component.
func BM25TF(tf int, dl, avgDL int, p BM25Params) float64 {
	tfF := float64(tf)
	ratio := float64(dl) / float64(max(1, avgDL))
	return (tfF * (p.K1 + 1)) / (tfF + p.K1*(1-p.B+p.B*ratio))
}

// ── inverted index ────────────────────────────────────────────────────────────

// PostingEntry is one (doc_id, term_freq) pair in a posting list.
type PostingEntry struct {
	DocID uint64
	TF    uint32
}

// InvertedIndexBuilder accumulates (term → postings) in memory and writes the
// index to disk when Flush() is called.
type InvertedIndexBuilder struct {
	dir      string
	postings map[string][]PostingEntry // term → posting list
	docFreq  map[string]int            // df per term
	N        int                       // total docs indexed
	totalLen int                       // sum of doc lengths (for avgdl)
}

// NewInvertedIndexBuilder creates a builder that writes to dir.
func NewInvertedIndexBuilder(dir string) (*InvertedIndexBuilder, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &InvertedIndexBuilder{
		dir:      dir,
		postings: make(map[string][]PostingEntry),
		docFreq:  make(map[string]int),
	}, nil
}

// Add indexes a single document.
func (b *InvertedIndexBuilder) Add(docID uint64, tokens []string) {
	b.N++
	b.totalLen += len(tokens)

	// count term frequencies in this doc
	tf := make(map[string]int)
	for _, tok := range tokens {
		tf[tok]++
	}
	for term, freq := range tf {
		b.postings[term] = append(b.postings[term], PostingEntry{DocID: docID, TF: uint32(freq)})
		b.docFreq[term]++
	}
}

// Flush writes the inverted index to disk in shard_NNN/ directories.
func (b *InvertedIndexBuilder) Flush() error {
	// sort terms
	terms := make([]string, 0, len(b.postings))
	for t := range b.postings {
		terms = append(terms, t)
	}
	sort.Strings(terms)

	// write terms.dat (term\toffset\n) and postings.dat (VByte delta-encoded)
	termsPath := filepath.Join(b.dir, "terms.dat")
	postPath := filepath.Join(b.dir, "postings.dat")
	statsPath := filepath.Join(b.dir, "stats.dat")

	tf, err := os.Create(termsPath)
	if err != nil {
		return err
	}
	defer tf.Close()

	pf, err := os.Create(postPath)
	if err != nil {
		return err
	}
	defer pf.Close()

	tw := bufio.NewWriter(tf)
	pw := bufio.NewWriter(pf)
	var postOffset int64

	avgDL := 1
	if b.N > 0 {
		avgDL = b.totalLen / b.N
	}

	for _, term := range terms {
		posts := b.postings[term]
		// sort by doc_id for delta encoding
		sort.Slice(posts, func(i, j int) bool { return posts[i].DocID < posts[j].DocID })

		df := b.docFreq[term]
		idf := BM25IDF(b.N, df)

		// write terms entry: "term\toffset\tdf\tidf\n"
		line := fmt.Sprintf("%s\t%d\t%d\t%.6f\n", term, postOffset, df, idf)
		if _, err := tw.WriteString(line); err != nil {
			return err
		}

		// write posting list: [count:varint] [delta:varint tf:varint] ...
		startOff := postOffset
		_ = startOff

		writeVarInt(pw, uint64(len(posts)))
		var prev uint64
		for _, p := range posts {
			delta := p.DocID - prev
			prev = p.DocID
			writeVarInt(pw, delta)
			writeVarInt(pw, uint64(p.TF))
		}
		postOffset += int64(varIntSize(uint64(len(posts))))
		prev = 0
		for _, p := range posts {
			delta := p.DocID - prev
			prev = p.DocID
			postOffset += int64(varIntSize(delta) + varIntSize(uint64(p.TF)))
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if err := pw.Flush(); err != nil {
		return err
	}

	// write stats: N avgdl
	sf, err := os.Create(statsPath)
	if err != nil {
		return err
	}
	defer sf.Close()
	_, err = fmt.Fprintf(sf, "%d\t%d\n", b.N, avgDL)
	return err
}

// ── posting list reader ───────────────────────────────────────────────────────

// IndexReader reads a flushed inverted index from disk.
type IndexReader struct {
	dir     string
	terms   map[string]termEntry // populated by loadTerms()
	N       int
	AvgDL   int
	postF   *os.File
}

type termEntry struct {
	offset int64
	df     int
	idf    float64
}

// OpenIndex opens a flushed inverted index directory.
func OpenIndex(dir string) (*IndexReader, error) {
	r := &IndexReader{dir: dir, terms: make(map[string]termEntry)}
	if err := r.loadStats(); err != nil {
		return nil, fmt.Errorf("open index stats: %w", err)
	}
	if err := r.loadTerms(); err != nil {
		return nil, fmt.Errorf("open index terms: %w", err)
	}
	pf, err := os.Open(filepath.Join(dir, "postings.dat"))
	if err != nil {
		return nil, err
	}
	r.postF = pf
	return r, nil
}

// Close releases the index file handles.
func (r *IndexReader) Close() error {
	if r.postF != nil {
		return r.postF.Close()
	}
	return nil
}

func (r *IndexReader) loadStats() error {
	f, err := os.Open(filepath.Join(r.dir, "stats.dat"))
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fscan(f, &r.N, &r.AvgDL)
	return err
}

func (r *IndexReader) loadTerms() error {
	f, err := os.Open(filepath.Join(r.dir, "terms.dat"))
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		parts := strings.Split(line, "\t")
		if len(parts) != 4 {
			continue
		}
		var offset int64
		var df int
		var idf float64
		_, _ = fmt.Sscan(parts[1], &offset)
		_, _ = fmt.Sscan(parts[2], &df)
		_, _ = fmt.Sscan(parts[3], &idf)
		r.terms[parts[0]] = termEntry{offset: offset, df: df, idf: idf}
	}
	return sc.Err()
}

// Lookup returns the posting list for a term.
func (r *IndexReader) Lookup(term string) ([]PostingEntry, float64, bool) {
	te, ok := r.terms[term]
	if !ok {
		return nil, 0, false
	}
	if _, err := r.postF.Seek(te.offset, io.SeekStart); err != nil {
		return nil, 0, false
	}
	br := bufio.NewReader(r.postF)
	count, err := readVarInt(br)
	if err != nil {
		return nil, 0, false
	}
	posts := make([]PostingEntry, count)
	var prev uint64
	for i := range posts {
		delta, e1 := readVarInt(br)
		tf, e2 := readVarInt(br)
		if e1 != nil || e2 != nil {
			break
		}
		prev += delta
		posts[i] = PostingEntry{DocID: prev, TF: uint32(tf)}
	}
	return posts, te.idf, true
}

// ScoredDoc is a document with its BM25 score.
type ScoredDoc struct {
	DocID uint64
	Score float64
}

// Search returns the top-k documents for query tokens using BM25.
func (r *IndexReader) Search(tokens []string, k int) []ScoredDoc {
	scores := make(map[uint64]float64)
	p := DefaultBM25Params

	for _, tok := range tokens {
		posts, idf, ok := r.Lookup(tok)
		if !ok {
			continue
		}
		for _, pe := range posts {
			tf := BM25TF(int(pe.TF), r.AvgDL, r.AvgDL, p)
			scores[pe.DocID] += idf * tf
		}
	}

	docs := make([]ScoredDoc, 0, len(scores))
	for id, sc := range scores {
		docs = append(docs, ScoredDoc{DocID: id, Score: sc})
	}
	sort.Slice(docs, func(i, j int) bool { return docs[i].Score > docs[j].Score })
	if k > 0 && len(docs) > k {
		docs = docs[:k]
	}
	return docs
}

// ── VByte encoding ────────────────────────────────────────────────────────────

func writeVarInt(w io.ByteWriter, v uint64) {
	for v >= 0x80 {
		_ = w.WriteByte(byte(v&0x7f) | 0x80)
		v >>= 7
	}
	_ = w.WriteByte(byte(v))
}

func varIntSize(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

func readVarInt(r io.ByteReader) (uint64, error) {
	var v uint64
	var shift uint
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		v |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return v, nil
		}
		shift += 7
	}
}

// ── forward index ─────────────────────────────────────────────────────────────

// ForwardDoc is one row in the forward index.
type ForwardDoc struct {
	DocID       uint64 `json:"doc_id" table:"doc_id"`
	URL         string `json:"url" table:"url"`
	CanonURL    string `json:"canon_url" table:"canon_url"`
	Host        string `json:"host" table:"host"`
	Title       string `json:"title" table:"title"`
	Description string `json:"description" table:"description"`
	Language    string `json:"language" table:"language"`
	WordCount   int    `json:"word_count" table:"word_count"`
	LinkScore   float32 `json:"link_score" table:"link_score"`
	Snippet     string `json:"snippet" table:"snippet"`
}

// ForwardIndexWriter appends ForwardDoc rows to a JSONL file.
type ForwardIndexWriter struct {
	f *os.File
	w *bufio.Writer
}

// NewForwardIndexWriter opens (or creates) a JSONL forward index file.
func NewForwardIndexWriter(path string) (*ForwardIndexWriter, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &ForwardIndexWriter{f: f, w: bufio.NewWriter(f)}, nil
}

// Write appends one ForwardDoc to the index.
func (fw *ForwardIndexWriter) Write(d ForwardDoc) error {
	line := fmt.Sprintf(`{"doc_id":%d,"url":%q,"canon_url":%q,"host":%q,"title":%q,"language":%q,"word_count":%d,"snippet":%q}`,
		d.DocID, d.URL, d.CanonURL, d.Host, d.Title, d.Language, d.WordCount, d.Snippet)
	_, err := fmt.Fprintln(fw.w, line)
	return err
}

// Close flushes and closes the writer.
func (fw *ForwardIndexWriter) Close() error {
	if err := fw.w.Flush(); err != nil {
		return err
	}
	return fw.f.Close()
}

// ── link-graph score blending ─────────────────────────────────────────────────

// LinkBoost blends a BM25 score with a host's harmonic centrality value.
// alpha controls the weight of the link signal (spec recommends 0.3).
func LinkBoost(bm25Score, harmonicVal, alpha float64) float64 {
	return bm25Score * (1 + alpha*math.Log1p(harmonicVal))
}

// HarmonicTier returns a 1–10 tier for a host's harmonic rank position
// (1 = most important, 10 = long tail). Used for multiplicative ranking boosts.
func HarmonicTier(harmonicPos int64, totalHosts int64) int {
	if totalHosts == 0 {
		return 10
	}
	pct := float64(harmonicPos) / float64(totalHosts)
	switch {
	case pct <= 0.001:
		return 1
	case pct <= 0.005:
		return 2
	case pct <= 0.01:
		return 3
	case pct <= 0.05:
		return 4
	case pct <= 0.10:
		return 5
	case pct <= 0.20:
		return 6
	case pct <= 0.40:
		return 7
	case pct <= 0.60:
		return 8
	case pct <= 0.80:
		return 9
	default:
		return 10
	}
}

// TierBoost returns the multiplicative ranking boost for a harmonic tier.
// Tier 1 = 2.0×, tier 10 = 1.0×.
func TierBoost(tier int) float64 {
	if tier < 1 {
		tier = 1
	}
	if tier > 10 {
		tier = 10
	}
	return 1.0 + float64(10-tier)*0.1
}

// ── protobuf-free VByte helpers (exported for testing) ───────────────────────

// EncodeVarInt encodes v as a VByte-encoded byte slice.
func EncodeVarInt(v uint64) []byte {
	var buf [10]byte
	n := 0
	for v >= 0x80 {
		buf[n] = byte(v&0x7f) | 0x80
		v >>= 7
		n++
	}
	buf[n] = byte(v)
	return buf[:n+1]
}

// DecodeVarInt decodes a VByte-encoded integer from b, returning the value and
// the number of bytes consumed.
func DecodeVarInt(b []byte) (uint64, int) {
	var v uint64
	var shift uint
	for i, byt := range b {
		v |= uint64(byt&0x7f) << shift
		if byt&0x80 == 0 {
			return v, i + 1
		}
		shift += 7
		if shift >= 64 {
			return 0, i + 1
		}
	}
	return 0, len(b)
}

// to use binary.Read (not needed currently but keeps import live)
var _ = binary.LittleEndian

package ccrawl

import (
	"os"
	"testing"
)

func TestTokenize(t *testing.T) {
	tokens := Tokenize("The Go programming language is great for systems programming!")
	// "the", "is", "for" are stopwords
	for _, stop := range []string{"the", "is", "for"} {
		for _, tok := range tokens {
			if tok == stop {
				t.Errorf("stopword %q should be removed", stop)
			}
		}
	}
	found := map[string]bool{}
	for _, tok := range tokens {
		found[tok] = true
	}
	for _, want := range []string{"go", "programming", "language", "great", "systems"} {
		if !found[want] {
			t.Errorf("expected token %q", want)
		}
	}
}

func TestTokenizeMinLength(t *testing.T) {
	tokens := Tokenize("I a to in")
	// single-char and stopwords removed
	for _, tok := range tokens {
		if len(tok) < 2 {
			t.Errorf("token too short: %q", tok)
		}
	}
}

func TestBM25IDF(t *testing.T) {
	// IDF(N=1000, df=10) should be positive
	idf := BM25IDF(1000, 10)
	if idf <= 0 {
		t.Errorf("IDF should be positive, got %f", idf)
	}
	// higher df → lower IDF
	idf2 := BM25IDF(1000, 100)
	if idf2 >= idf {
		t.Errorf("higher df should give lower IDF")
	}
	// df=0 → 0
	if BM25IDF(1000, 0) != 0 {
		t.Error("IDF(df=0) should be 0")
	}
}

func TestInvertedIndexRoundtrip(t *testing.T) {
	dir := t.TempDir()
	b, err := NewInvertedIndexBuilder(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Index 3 documents
	doc1 := uint64(1001)
	doc2 := uint64(1002)
	doc3 := uint64(1003)
	b.Add(doc1, Tokenize("go programming language systems"))
	b.Add(doc2, Tokenize("go web server http programming"))
	b.Add(doc3, Tokenize("python machine learning data science"))

	if err := b.Flush(); err != nil {
		t.Fatal("Flush:", err)
	}

	// Open and search
	r, err := OpenIndex(dir)
	if err != nil {
		t.Fatal("OpenIndex:", err)
	}
	defer func() { _ = r.Close() }()

	if r.N != 3 {
		t.Errorf("N = %d, want 3", r.N)
	}

	// "go" appears in docs 1 and 2
	posts, idf, ok := r.Lookup("go")
	if !ok {
		t.Fatal("'go' not found in index")
	}
	if idf <= 0 {
		t.Errorf("idf for 'go' should be > 0, got %f", idf)
	}
	if len(posts) != 2 {
		t.Errorf("'go' posting list len = %d, want 2", len(posts))
	}

	// search "go programming" → should rank doc1 and doc2 over doc3
	results := r.Search(Tokenize("go programming"), 10)
	if len(results) < 2 {
		t.Fatalf("search returned %d results, want >= 2", len(results))
	}
	// doc3 should not appear (no "go" or "programming")
	for _, res := range results {
		if res.DocID == doc3 {
			t.Error("doc3 should not match 'go programming'")
		}
	}
	// top result should be doc1 or doc2
	if results[0].DocID != doc1 && results[0].DocID != doc2 {
		t.Errorf("top result DocID = %d, expected doc1 or doc2", results[0].DocID)
	}
}

func TestVarIntRoundtrip(t *testing.T) {
	cases := []uint64{0, 1, 127, 128, 255, 256, 16383, 16384, 1<<32 - 1, 1<<63 - 1}
	for _, v := range cases {
		enc := EncodeVarInt(v)
		dec, n := DecodeVarInt(enc)
		if dec != v {
			t.Errorf("varint(%d) round-trip = %d", v, dec)
		}
		if n != len(enc) {
			t.Errorf("varint(%d) consumed %d bytes, encoded len %d", v, n, len(enc))
		}
	}
}

func TestLinkBoost(t *testing.T) {
	base := 1.0
	boosted := LinkBoost(base, 1e7, 0.3)
	if boosted <= base {
		t.Errorf("LinkBoost should increase score: base=%f boosted=%f", base, boosted)
	}
	// zero harmonic val → no boost (log1p(0)=0)
	same := LinkBoost(base, 0, 0.3)
	if same != base {
		t.Errorf("LinkBoost with harmonicVal=0 should return base score")
	}
}

func TestHarmonicTierAndBoost(t *testing.T) {
	total := int64(262_000_000)
	// top 0.1% → tier 1
	tier1 := HarmonicTier(1_000, total)
	if tier1 != 1 {
		t.Errorf("top host should be tier 1, got %d", tier1)
	}
	// rank 200M / 262M = ~76% → tier 9
	tier9 := HarmonicTier(200_000_000, total)
	if tier9 < 8 {
		t.Errorf("low-rank host should be tier >=8, got %d", tier9)
	}
	// boost decreases with tier
	b1 := TierBoost(1)
	b5 := TierBoost(5)
	b10 := TierBoost(10)
	if b1 <= b5 || b5 <= b10 {
		t.Errorf("tier boosts should decrease: t1=%f t5=%f t10=%f", b1, b5, b10)
	}
	if b10 != 1.0 {
		t.Errorf("tier 10 boost should be 1.0, got %f", b10)
	}
}

func TestForwardIndexWriter(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/forward.jsonl"
	fw, err := NewForwardIndexWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	doc := ForwardDoc{
		DocID: 12345, URL: "https://example.com/page",
		Host: "example.com", Title: "Test Page",
		Language: "en", WordCount: 100,
		Snippet: "First 500 characters of content...",
	}
	if err := fw.Write(doc); err != nil {
		t.Fatal(err)
	}
	if err := fw.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Error("forward index file should not be empty")
	}
	if !contains(string(data), "example.com") {
		t.Error("forward index should contain host name")
	}
}

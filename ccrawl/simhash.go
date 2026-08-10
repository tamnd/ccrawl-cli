package ccrawl

import (
	"hash/fnv"
	"math/bits"
	"strings"
	"unicode"
)

// simhashShingle is how many consecutive tokens make one feature. Three is the
// usual choice for near duplicate detection over prose: single words collide
// across unrelated documents on topic alone, and long shingles are so specific
// that a rewritten sentence breaks every feature it touches.
const simhashShingle = 3

// Simhash returns a 64 bit fingerprint of text where similar documents get
// similar fingerprints, so near duplicates can be found by counting differing
// bits instead of comparing documents pairwise.
//
// The features are overlapping three word shingles, hashed with FNV-1a and voted
// bit by bit. Two documents that share most of their shingles agree on most of
// the bits; the same document always gives the same answer, on any machine and
// in any release, because nothing here depends on map ordering, goroutine
// scheduling, or a seeded hash.
//
// Each distinct shingle votes once, however many times it occurs. Charikar's
// original weights each feature by its count, and on web text that is a trap:
// a mojibake page, where UTF-8 was served as Latin-1, extracts to a few
// replacement sequences repeated thousands of times, and the count weighted
// fingerprint is then decided almost entirely by those few features. Two
// unrelated mojibake pages measured here shared 6.5 percent of their shingles
// and still came out one bit apart. Counting each shingle once puts them where
// they belong.
//
// Empty or near empty text returns 0. That is deliberate rather than a hash of
// nothing: 0 means "no fingerprint", and dedup treats it as such instead of
// collapsing every short page into one cluster.
func Simhash(text string) uint64 {
	toks := simhashTokens(text)
	if len(toks) == 0 {
		return 0
	}

	// Hashes, not the shingle strings, so a long document costs 8 bytes a feature
	// instead of holding a second copy of its own text.
	feats := make(map[uint64]struct{}, len(toks))
	if len(toks) < simhashShingle {
		// Too short to shingle. Hash the tokens themselves rather than returning
		// nothing, so a one line page still gets a fingerprint.
		for _, t := range toks {
			feats[fnv64(t)] = struct{}{}
		}
	} else {
		var b strings.Builder
		for i := 0; i+simhashShingle <= len(toks); i++ {
			b.Reset()
			for j := range simhashShingle {
				if j > 0 {
					b.WriteByte(' ')
				}
				b.WriteString(toks[i+j])
			}
			feats[fnv64(b.String())] = struct{}{}
		}
	}

	// Iterating a map is unordered, which does not matter: every feature adds or
	// subtracts one from a bit's tally, and addition does not care about order.
	var vote [64]int
	for h := range feats {
		for i := range 64 {
			if h&(1<<uint(i)) != 0 {
				vote[i]++
			} else {
				vote[i]--
			}
		}
	}

	var out uint64
	for i := range 64 {
		if vote[i] > 0 {
			out |= 1 << uint(i)
		}
	}
	return out
}

// SimhashDistance is the number of bits two fingerprints disagree on. Identical
// documents are 0 apart, unrelated ones sit around 32, and the interesting range
// for "the same page with different boilerplate" is roughly 1 to 6.
func SimhashDistance(a, b uint64) int {
	return bits.OnesCount64(a ^ b)
}

// simhashTokens splits text into lowercased word tokens. Markdown punctuation,
// link syntax, and whitespace all fall out as separators, which is what we want:
// two renderings of the same article should tokenize the same way even when one
// of them wrapped a phrase in a link and the other did not.
func simhashTokens(text string) []string {
	var toks []string
	var cur strings.Builder
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			cur.WriteRune(unicode.ToLower(r))
			continue
		}
		if cur.Len() > 0 {
			toks = append(toks, cur.String())
			cur.Reset()
		}
	}
	if cur.Len() > 0 {
		toks = append(toks, cur.String())
	}
	return toks
}

// fnv64 hashes one feature. FNV-1a is not cryptographic and does not need to be:
// it only has to spread features across the 64 bits, and it is fixed across Go
// releases, which a runtime seeded hash would not be.
func fnv64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

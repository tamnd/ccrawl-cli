package ccrawl

import (
	"strings"
	"unicode"

	"github.com/RadhiFadlillah/whatlanggo"
)

// MinLangText is how much text a detection needs before the answer means
// anything. Trigram identification on a nav bar and a cookie banner is a coin
// flip, and a coin flip that reports 0.9 confidence is worse than no answer, so
// anything shorter is reported as unknown rather than guessed at.
const MinLangText = 40

// DefaultMinLangConfidence is the confidence a document has to clear to be kept
// by a --lang filter. It matches whatlanggo's own reliability threshold.
const DefaultMinLangConfidence = 0.8

// DetectLanguage identifies the language of a document and returns an ISO 639-3
// code with the detector's confidence in it.
//
// This is a document level check on the extracted text, which is the point: the
// columnar index carries CLD2 labels computed by Common Crawl over the raw HTML,
// and those labels are unreliable for low resource languages and absent
// entirely for a good share of rows. Running the identifier over the Markdown we
// actually extracted asks the question about the text we are actually keeping.
//
// The code is "" with confidence 0 when there is too little text to be worth
// identifying. Callers should treat that as unknown, not as a failed match.
func DetectLanguage(text string) (string, float64) {
	text = langSample(text)
	if len([]rune(text)) < MinLangText {
		return "", 0
	}
	info := whatlanggo.Detect(text)
	code := whatlanggo.LangToString(info.Lang)
	if code == "" {
		return "", 0
	}
	return code, info.Confidence
}

// LangMatches reports whether a document's detected language satisfies a filter.
// An empty want matches everything, which is what makes the filter opt in.
//
// A document with too little text to identify is dropped by a language filter
// rather than kept. A filtered export is asking for documents known to be in one
// language, and "we could not tell" is not that.
func LangMatches(want, got string, conf, min float64) bool {
	if want == "" {
		return true
	}
	if got == "" {
		return false
	}
	return got == want && conf >= min
}

// langSample trims the text handed to the identifier. Trigram profiling reaches
// a stable answer within a few thousand characters and then spends the rest of
// the document confirming it, so a long article is pure cost.
//
// The URLs have to go. A link heavy page, which on a news site means the
// homepage and every section index, carries more characters of slug than of
// prose, and a Vietnamese slug is stripped of its diacritics by definition. Fed
// to the identifier raw, one of those pages reads as some Latin language nobody
// asked about. The link text stays: it is the headline, and it is exactly the
// prose we want to judge.
func langSample(text string) string {
	var b strings.Builder
	b.Grow(min(len(text), langSampleBytes))
	rs := []rune(text)
	for i := 0; i < len(rs) && b.Len() < langSampleBytes; i++ {
		r := rs[i]
		switch {
		case r == '(' && i > 0 && rs[i-1] == ']':
			// A link target, right after its text. Skip to the close paren.
			for i < len(rs) && rs[i] != ')' {
				i++
			}
			b.WriteByte(' ')
		case r == '<':
			// An autolink or a stray tag, neither of which is prose.
			for i < len(rs) && rs[i] != '>' {
				i++
			}
			b.WriteByte(' ')
		case isURLStart(rs, i):
			for i < len(rs) && !unicode.IsSpace(rs[i]) && rs[i] != ')' {
				i++
			}
			i--
			b.WriteByte(' ')
		case r == '#' || r == '*' || r == '_' || r == '|' || r == '`' || r == '[' || r == ']':
			b.WriteByte(' ')
		case unicode.IsSpace(r):
			b.WriteByte(' ')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// isURLStart reports whether a bare URL begins at rs[i]. Only at a word
// boundary, so it does not fire in the middle of an ordinary word.
func isURLStart(rs []rune, i int) bool {
	if i > 0 && !unicode.IsSpace(rs[i-1]) {
		return false
	}
	for _, p := range [...]string{"http://", "https://", "www.", "javascript:"} {
		if len(rs)-i >= len(p) && string(rs[i:i+len(p)]) == p {
			return true
		}
	}
	return false
}

// langSampleBytes is how much text the identifier gets. Measured on Vietnamese
// news pages, the answer stops moving well before this.
const langSampleBytes = 4096

// LangSample exposes the text DetectLanguage judged, truncated to n runes, so
// "content lang" can show what the answer was based on. A wrong answer is
// usually a wrong input.
func LangSample(text string, n int) string {
	s := strings.Join(strings.Fields(langSample(text)), " ")
	rs := []rune(s)
	if len(rs) > n {
		return string(rs[:n]) + "..."
	}
	return s
}

package ccrawl

import (
	"bytes"
	"hash/fnv"
	"math"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/net/html"
)

// TextResult holds the output of HTML-to-text extraction.
type TextResult struct {
	Title       string
	Description string   // <meta name="description">
	CanonURL    string   // canonical URL from meta or link[rel=canonical]
	Body        string   // extracted clean body text
	WordCount   int
	Language    string  // BCP-47 code inferred from lang attribute
}

// QualityResult holds per-document quality signals computed from a TextResult.
type QualityResult struct {
	WordCount          int
	TitleLength        int
	HasMainContent     bool
	SpamScore          float64 // 0–1
	IsParked           bool
	IsShortContent     bool // word_count < 50
}

// ExtractContent parses HTML bytes and returns a TextResult with clean text,
// title, description, canonical URL, and inferred language.
func ExtractContent(htmlBytes []byte) TextResult {
	doc, err := html.Parse(bytes.NewReader(htmlBytes))
	if err != nil {
		return TextResult{}
	}

	var tr TextResult
	var bodyBuf strings.Builder
	extractNode(doc, &tr, &bodyBuf, 0)
	tr.Body = collapseWS(bodyBuf.String())
	tr.WordCount = countWords(tr.Body)
	return tr
}

// HTMLCanonicalURL returns the best canonical URL from HTML headers (in priority
// order): link[rel=canonical], og:url. Returns "" if none found.
func HTMLCanonicalURL(htmlBytes []byte) string {
	doc, err := html.Parse(bytes.NewReader(htmlBytes))
	if err != nil {
		return ""
	}
	return findCanonicalURL(doc)
}

func findCanonicalURL(n *html.Node) string {
	if n.Type == html.ElementNode {
		switch n.Data {
		case "link":
			rel := attrVal(n, "rel")
			if strings.EqualFold(rel, "canonical") {
				if href := attrVal(n, "href"); href != "" {
					return href
				}
			}
		case "meta":
			prop := attrVal(n, "property")
			name := attrVal(n, "name")
			if strings.EqualFold(prop, "og:url") || strings.EqualFold(name, "og:url") {
				if content := attrVal(n, "content"); content != "" {
					return content
				}
			}
		}
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if u := findCanonicalURL(c); u != "" {
			return u
		}
	}
	return ""
}

// QualitySignals computes per-document quality signals from a TextResult.
func QualitySignals(tr TextResult) QualityResult {
	q := QualityResult{
		WordCount:      tr.WordCount,
		TitleLength:    utf8.RuneCountInString(tr.Title),
		HasMainContent: tr.WordCount >= 50,
		IsShortContent: tr.WordCount < 50,
	}
	q.SpamScore = spamScore(tr.Title + " " + tr.Body)
	q.IsParked = isParkedDomain(tr.Title, tr.Body, tr.WordCount)
	return q
}

// StripTrackingParams removes known tracking query parameters from a raw URL
// query string. Returns the cleaned query string (without leading "?").
func StripTrackingParams(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	tracking := map[string]bool{
		"utm_source": true, "utm_medium": true, "utm_campaign": true,
		"utm_term": true, "utm_content": true, "fbclid": true,
		"gclid": true, "msclkid": true, "ref": true, "source": true, "_ga": true,
	}
	var keep []string
	for pair := range strings.SplitSeq(rawQuery, "&") {
		key, _, _ := strings.Cut(pair, "=")
		if !tracking[key] {
			keep = append(keep, pair)
		}
	}
	return strings.Join(keep, "&")
}

// DocumentID returns a stable 64-bit document ID for a canonical URL.
func DocumentID(canonicalURL string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(canonicalURL))
	return h.Sum64()
}

// ── internal helpers ──────────────────────────────────────────────────────────

// skipTags are HTML elements whose subtrees are discarded during extraction.
var skipTags = map[string]bool{
	"script": true, "style": true, "noscript": true,
	"nav": true, "header": true, "footer": true, "aside": true,
}

// blockTags cause a space break between their content and surrounding text.
var blockTags = map[string]bool{
	"p": true, "div": true, "section": true, "article": true,
	"h1": true, "h2": true, "h3": true, "h4": true, "h5": true, "h6": true,
	"li": true, "td": true, "th": true, "br": true, "hr": true,
}

func extractNode(n *html.Node, tr *TextResult, body *strings.Builder, depth int) {
	if n.Type == html.ElementNode {
		tag := strings.ToLower(n.Data)
		if skipTags[tag] {
			return
		}

		// extract metadata from head elements
		switch tag {
		case "title":
			if tr.Title == "" {
				tr.Title = strings.TrimSpace(nodeText(n))
			}
			return
		case "meta":
			name := strings.ToLower(attrVal(n, "name"))
			prop := strings.ToLower(attrVal(n, "property"))
			content := attrVal(n, "content")
			if name == "description" && tr.Description == "" {
				tr.Description = content
			}
			if (prop == "og:url" || name == "og:url") && tr.CanonURL == "" {
				tr.CanonURL = content
			}
			if (name == "language" || name == "lang") && tr.Language == "" {
				tr.Language = content
			}
			return
		case "link":
			if strings.EqualFold(attrVal(n, "rel"), "canonical") && tr.CanonURL == "" {
				tr.CanonURL = attrVal(n, "href")
			}
			return
		case "html":
			if lang := attrVal(n, "lang"); lang != "" && tr.Language == "" {
				tr.Language = lang
			}
		}

		if blockTags[tag] {
			body.WriteByte(' ')
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			extractNode(c, tr, body, depth+1)
		}
		if blockTags[tag] {
			body.WriteByte(' ')
		}
		return
	}

	if n.Type == html.TextNode {
		body.WriteString(n.Data)
	}

	for c := n.FirstChild; c != nil; c = c.NextSibling {
		extractNode(c, tr, body, depth+1)
	}
}

func nodeText(n *html.Node) string {
	var b strings.Builder
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.TextNode {
			b.WriteString(c.Data)
		}
	}
	return b.String()
}

func attrVal(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if strings.EqualFold(a.Key, key) {
			return a.Val
		}
	}
	return ""
}

func countWords(s string) int {
	count := 0
	inWord := false
	for _, r := range s {
		if unicode.IsSpace(r) {
			inWord = false
		} else if !inWord {
			inWord = true
			count++
		}
	}
	return count
}

// spamScore returns a 0–1 estimate of how spammy the text looks based on
// density of common spam trigger phrases.
func spamScore(text string) float64 {
	lower := strings.ToLower(text)
	spamPhrases := []string{
		"buy now", "click here", "limited offer", "make money", "work from home",
		"free gift", "risk free", "no obligation", "act now", "cheap",
		"casino", "viagra", "cryptocurrency", "earn $", "guaranteed",
	}
	hits := 0
	words := countWords(lower)
	if words == 0 {
		return 0
	}
	for _, phrase := range spamPhrases {
		if strings.Contains(lower, phrase) {
			hits++
		}
	}
	// Each phrase match adds ~0.1 to score, capped at 1.0
	return math.Min(1.0, float64(hits)*0.1)
}

var reParked = regexp.MustCompile(`(?i)(domain.*for sale|buy this domain|parked domain|coming soon|under construction)`)

func isParkedDomain(title, body string, wordCount int) bool {
	if wordCount > 150 {
		return false
	}
	combined := title + " " + body
	return reParked.MatchString(combined)
}

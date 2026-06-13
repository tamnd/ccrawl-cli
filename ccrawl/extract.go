package ccrawl

import (
	"bytes"
	"net/url"
	"strings"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

// ExtractTitle returns the <title> text of an HTML document.
func ExtractTitle(body []byte) string {
	z := html.NewTokenizer(bytes.NewReader(body))
	inTitle := false
	for {
		switch z.Next() {
		case html.ErrorToken:
			return ""
		case html.StartTagToken:
			if t := z.Token(); t.DataAtom == atom.Title {
				inTitle = true
			}
		case html.TextToken:
			if inTitle {
				return collapseWS(string(z.Text()))
			}
		case html.EndTagToken:
			inTitle = false
		}
	}
}

// ExtractText returns readable plain text from an HTML document, dropping the
// contents of script and style elements and collapsing whitespace.
func ExtractText(body []byte) string {
	z := html.NewTokenizer(bytes.NewReader(body))
	var b strings.Builder
	skip := 0
	for {
		switch z.Next() {
		case html.ErrorToken:
			return collapseWS(b.String())
		case html.StartTagToken, html.SelfClosingTagToken:
			t := z.Token()
			switch t.DataAtom {
			case atom.Script, atom.Style, atom.Noscript, atom.Head:
				skip++
			case atom.P, atom.Br, atom.Div, atom.Li, atom.Tr, atom.H1, atom.H2,
				atom.H3, atom.H4, atom.H5, atom.H6, atom.Section, atom.Article:
				b.WriteByte('\n')
			}
		case html.EndTagToken:
			switch z.Token().DataAtom {
			case atom.Script, atom.Style, atom.Noscript, atom.Head:
				if skip > 0 {
					skip--
				}
			}
		case html.TextToken:
			if skip == 0 {
				b.Write(z.Text())
				b.WriteByte(' ')
			}
		}
	}
}

// ExtractLinks returns the outbound hyperlinks of an HTML document, resolved
// against base when possible.
func ExtractLinks(base string, body []byte) []WATLink {
	var baseURL *url.URL
	if base != "" {
		baseURL, _ = url.Parse(base)
	}
	z := html.NewTokenizer(bytes.NewReader(body))
	var links []WATLink
	var cur *WATLink
	var text strings.Builder
	for {
		switch z.Next() {
		case html.ErrorToken:
			return links
		case html.StartTagToken, html.SelfClosingTagToken:
			t := z.Token()
			if t.DataAtom == atom.A {
				if cur != nil {
					cur.Text = strings.TrimSpace(text.String())
					links = append(links, *cur)
				}
				href, title := "", ""
				for _, a := range t.Attr {
					switch a.Key {
					case "href":
						href = a.Val
					case "title":
						title = a.Val
					}
				}
				if href == "" || strings.HasPrefix(href, "#") || strings.HasPrefix(href, "javascript:") {
					cur = nil
					continue
				}
				resolved := href
				if baseURL != nil {
					if u, err := baseURL.Parse(href); err == nil {
						resolved = u.String()
					}
				}
				cur = &WATLink{Path: "A@/href", URL: resolved, Title: title}
				text.Reset()
			}
		case html.TextToken:
			if cur != nil {
				text.Write(z.Text())
			}
		case html.EndTagToken:
			if z.Token().DataAtom == atom.A && cur != nil {
				cur.Text = strings.TrimSpace(text.String())
				links = append(links, *cur)
				cur = nil
			}
		}
	}
}

// ExtractMarkdown converts an HTML document to a compact Markdown approximation.
// It is intentionally light: headings, paragraphs, list items, links, and
// emphasis, which covers the bulk of crawled article content.
func ExtractMarkdown(body []byte) (string, error) {
	doc, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	var b strings.Builder
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode {
			switch n.DataAtom {
			case atom.Script, atom.Style, atom.Head, atom.Noscript:
				return
			case atom.H1:
				b.WriteString("\n# ")
			case atom.H2:
				b.WriteString("\n## ")
			case atom.H3:
				b.WriteString("\n### ")
			case atom.H4, atom.H5, atom.H6:
				b.WriteString("\n#### ")
			case atom.Li:
				b.WriteString("\n- ")
			case atom.P, atom.Div, atom.Br:
				b.WriteString("\n")
			case atom.A:
				href := attr(n, "href")
				if href != "" {
					b.WriteString("[")
					childText(n, &b)
					b.WriteString("](" + href + ")")
					return
				}
			case atom.Strong, atom.B:
				b.WriteString("**")
				childText(n, &b)
				b.WriteString("**")
				return
			case atom.Em, atom.I:
				b.WriteString("*")
				childText(n, &b)
				b.WriteString("*")
				return
			}
		}
		if n.Type == html.TextNode {
			b.WriteString(n.Data)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	return strings.TrimSpace(collapseBlankLines(b.String())), nil
}

func childText(n *html.Node, b *strings.Builder) {
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.TextNode {
			b.WriteString(c.Data)
		} else {
			childText(c, b)
		}
	}
}

func attr(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}

func collapseWS(s string) string {
	var b strings.Builder
	lastSpace, lastNL := true, true
	for _, r := range s {
		switch r {
		case '\n':
			if !lastNL {
				b.WriteByte('\n')
			}
			lastNL, lastSpace = true, true
		case ' ', '\t', '\r':
			if !lastSpace {
				b.WriteByte(' ')
			}
			lastSpace = true
		default:
			b.WriteRune(r)
			lastSpace, lastNL = false, false
		}
	}
	return strings.TrimSpace(b.String())
}

func collapseBlankLines(s string) string {
	for strings.Contains(s, "\n\n\n") {
		s = strings.ReplaceAll(s, "\n\n\n", "\n\n")
	}
	return s
}

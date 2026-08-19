package ccrawl

import (
	"strings"
	"testing"
	"time"
)

// newsRecord builds a stored CC-NEWS response the way the archives hold one:
// a WARC response record whose block is the whole HTTP message.
func newsRecord(uri, headers, body string) WARCRecord {
	block := headers + "\r\n\r\n" + body
	return WARCRecord{
		Header: WARCHeader{
			Type:          "response",
			TargetURI:     uri,
			Date:          time.Date(2026, 7, 1, 2, 25, 1, 0, time.UTC),
			HTTPStatus:    200,
			HTTPMIME:      "text/html",
			PayloadDigest: "sha1:ABCDEF0123456789",
			RecordID:      "<urn:uuid:0d2f4e6a-1111-2222-3333-444455556666>",
			ContentLength: int64(len(block)),
			WARCOffset:    71779,
			WARCLength:    20806,
		},
		Block: []byte(block),
	}
}

func TestNewsIndexRowFields(t *testing.T) {
	rec := newsRecord(
		"https://www.kommersant.ru/doc/12345",
		"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=UTF-8\r\nServer: nginx",
		`<!doctype html><html lang="ru"><head><title>Заголовок</title></head><body>`+
			`<p>Правительство России в понедельник объявило о новых мерах поддержки экономики страны.</p>`+
			`<p>Решение было принято после совещания с представителями крупнейших компаний и банков.</p></body></html>`)

	row, ok := NewsIndexRow("crawl-data/CC-NEWS/2026/07/CC-NEWS-20260701022501-08467.warc.gz", rec)
	if !ok {
		t.Fatal("NewsIndexRow refused a response record")
	}

	if row.URL != "https://www.kommersant.ru/doc/12345" {
		t.Errorf("url = %q", row.URL)
	}
	if row.URLHostName != "www.kommersant.ru" {
		t.Errorf("host = %q", row.URLHostName)
	}
	if row.URLHostRegisteredDomain != "kommersant.ru" {
		t.Errorf("registered domain = %q", row.URLHostRegisteredDomain)
	}
	if row.URLHostTLD != "ru" {
		t.Errorf("tld = %q", row.URLHostTLD)
	}
	if row.URLProtocol != "https" {
		t.Errorf("protocol = %q", row.URLProtocol)
	}
	if row.FetchStatus != 200 {
		t.Errorf("status = %d", row.FetchStatus)
	}
	// The digest is the payload digest with the algorithm prefix trimmed, which
	// is exactly the shape cc-index uses for content_digest.
	if row.ContentDigest != "ABCDEF0123456789" {
		t.Errorf("digest = %q", row.ContentDigest)
	}
	if row.ContentMIMEType != "text/html" {
		t.Errorf("mime type = %q", row.ContentMIMEType)
	}
	if row.ContentCharset != "UTF-8" {
		t.Errorf("charset = %q", row.ContentCharset)
	}
	if !strings.Contains(row.ContentMIMEDetected, "html") {
		t.Errorf("detected mime = %q", row.ContentMIMEDetected)
	}
	if row.ContentLanguages != "rus" {
		t.Errorf("detected language = %q, want rus", row.ContentLanguages)
	}
	if row.ContentLanguageDeclared != "ru" {
		t.Errorf("declared language = %q", row.ContentLanguageDeclared)
	}
	if row.ContentLanguageConfidence <= 0 {
		t.Errorf("confidence = %v, want above zero", row.ContentLanguageConfidence)
	}
	if row.WARCFilename != "crawl-data/CC-NEWS/2026/07/CC-NEWS-20260701022501-08467.warc.gz" {
		t.Errorf("filename = %q", row.WARCFilename)
	}
	if row.WARCRecordOffset != 71779 || row.WARCRecordLength != 20806 {
		t.Errorf("span = %d+%d, want 71779+20806", row.WARCRecordOffset, row.WARCRecordLength)
	}
	if row.WARCRecordID != "urn:uuid:0d2f4e6a-1111-2222-3333-444455556666" {
		t.Errorf("record id = %q", row.WARCRecordID)
	}
	if row.URLSurtKey == "" {
		t.Error("surt key is empty")
	}
	if row.FetchTime.Format("20060102150405") != "20260701022501" {
		t.Errorf("fetch time = %v", row.FetchTime)
	}
}

func TestNewsIndexRowSkipsNonResponses(t *testing.T) {
	rec := newsRecord("https://a.example/", "HTTP/1.1 200 OK", "hi")
	rec.Header.Type = "warcinfo"
	if _, ok := NewsIndexRow("f.warc.gz", rec); ok {
		t.Error("indexed a warcinfo record")
	}

	rec = newsRecord("", "HTTP/1.1 200 OK", "hi")
	if _, ok := NewsIndexRow("f.warc.gz", rec); ok {
		t.Error("indexed a record with no target URI")
	}
}

func TestNewsIndexRowRedirect(t *testing.T) {
	rec := newsRecord("https://a.example/old",
		"HTTP/1.1 301 Moved Permanently\r\nLocation: https://a.example/new\r\nContent-Type: text/html", "")
	rec.Header.HTTPStatus = 301

	row, ok := NewsIndexRow("f.warc.gz", rec)
	if !ok {
		t.Fatal("NewsIndexRow refused a redirect")
	}
	if row.FetchRedirect != "https://a.example/new" {
		t.Errorf("redirect = %q", row.FetchRedirect)
	}
	if row.FetchStatus != 301 {
		t.Errorf("status = %d", row.FetchStatus)
	}
}

// TestNewsIndexRowNonHTMLHasNoLanguage guards the rule that a language is only
// guessed for a document there is text to read. A trigram profile of a PDF or an
// image is noise wearing a confidence score.
func TestNewsIndexRowNonHTMLHasNoLanguage(t *testing.T) {
	rec := newsRecord("https://a.example/paper.pdf",
		"HTTP/1.1 200 OK\r\nContent-Type: application/pdf",
		"%PDF-1.4\n1 0 obj\n<< /Type /Catalog >>\nendobj\n")

	row, ok := NewsIndexRow("f.warc.gz", rec)
	if !ok {
		t.Fatal("NewsIndexRow refused a PDF response")
	}
	if row.ContentLanguages != "" {
		t.Errorf("language = %q, want empty for a PDF", row.ContentLanguages)
	}
	if row.ContentMIMEDetected != "application/pdf" {
		t.Errorf("detected mime = %q", row.ContentMIMEDetected)
	}
}

func TestSplitContentType(t *testing.T) {
	cases := []struct{ in, mime, charset string }{
		{"text/html; charset=UTF-8", "text/html", "UTF-8"},
		{"text/html;charset=windows-1251", "text/html", "WINDOWS-1251"},
		{"TEXT/HTML", "text/html", ""},
		{"text/html; charset=", "text/html", ""},
		{"", "", ""},
	}
	for _, c := range cases {
		m, cs := splitContentType(c.in)
		if m != c.mime || cs != c.charset {
			t.Errorf("splitContentType(%q) = %q,%q want %q,%q", c.in, m, cs, c.mime, c.charset)
		}
	}
}

func TestParseResponseHeaders(t *testing.T) {
	block := []byte("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nX-Thing: a\r\nX-Thing: b\r\n\r\nbody")
	h := parseResponseHeaders(block)
	if h.Get("Content-Type") != "text/html" {
		t.Errorf("content type = %q", h.Get("Content-Type"))
	}
	if got := h.Values("X-Thing"); len(got) != 2 {
		t.Errorf("X-Thing = %v, want two values", got)
	}
	// The status line is not a header and must not be parsed as one.
	if h.Get("HTTP/1.1 200 OK") != "" {
		t.Error("status line was parsed as a header")
	}
}

func TestTLDAndScheme(t *testing.T) {
	if got := tldOf("www.example.co.uk"); got != "uk" {
		t.Errorf("tldOf = %q", got)
	}
	if got := tldOf("localhost"); got != "" {
		t.Errorf("tldOf(localhost) = %q", got)
	}
	if got := schemeOf("HTTPS://a.example/"); got != "https" {
		t.Errorf("schemeOf = %q", got)
	}
	if got := schemeOf("a.example/"); got != "" {
		t.Errorf("schemeOf bare = %q", got)
	}
}

func TestNewsColumnsMatchSchema(t *testing.T) {
	// NewsColumns is what the dataset card and the docs describe. If a field is
	// added to the row and not to the list, the card stops describing the data.
	if len(NewsColumns) != 22 {
		t.Fatalf("NewsColumns has %d entries, want 22", len(NewsColumns))
	}
	seen := map[string]bool{}
	for _, c := range NewsColumns {
		if seen[c] {
			t.Errorf("duplicate column %q", c)
		}
		seen[c] = true
	}
	for _, want := range []string{"url", "url_host_name", "fetch_time", "fetch_status",
		"content_digest", "warc_filename", "warc_record_offset", "warc_record_length"} {
		if !seen[want] {
			t.Errorf("NewsColumns is missing %q", want)
		}
	}
}

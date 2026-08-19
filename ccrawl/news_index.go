package ccrawl

import (
	"mime"
	"net/http"
	"strings"

	"golang.org/x/net/publicsuffix"
)

// NewsIndexRow builds one index row from a CC-NEWS response record. It returns
// false for a record that does not belong in the index: anything that is not a
// response, and any response with no target URL to key it on.
//
// The record's WARCOffset and WARCLength have to be filled in already, which
// means it has to have come from IterateFrom rather than Iterate. A row without
// them still describes an article but cannot be fetched, and the whole point of
// the index is that it can be.
func NewsIndexRow(filename string, rec WARCRecord) (NewsRow, bool) {
	h := rec.Header
	if h.Type != "response" || h.TargetURI == "" {
		return NewsRow{}, false
	}

	host := HostOf(h.TargetURI)
	row := NewsRow{
		URLSurtKey:       SURT(h.TargetURI),
		URL:              h.TargetURI,
		URLHostName:      host,
		URLHostTLD:       tldOf(host),
		URLProtocol:      schemeOf(h.TargetURI),
		FetchTime:        h.Date,
		FetchStatus:      int32(h.HTTPStatus),
		ContentDigest:    strings.TrimPrefix(h.PayloadDigest, "sha1:"),
		ContentTruncated: h.Truncated,
		WARCFilename:     filename,
		WARCRecordOffset: h.WARCOffset,
		WARCRecordLength: h.WARCLength,
		WARCRecordID:     strings.Trim(h.RecordID, "<>"),
	}
	if host != "" {
		// publicsuffix answers for a host it does not know by returning the last
		// two labels, which is the right guess often enough to be worth keeping.
		if d, err := publicsuffix.EffectiveTLDPlusOne(host); err == nil {
			row.URLHostRegisteredDomain = d
		}
	}

	// The stored block is the whole HTTP message. Everything below reads the
	// headers the origin sent, which is why these columns are reported rather
	// than detected.
	headers := parseResponseHeaders(rec.Block)
	row.ContentMIMEType, row.ContentCharset = splitContentType(headers.Get("Content-Type"))
	if row.ContentMIMEType == "" {
		row.ContentMIMEType = h.HTTPMIME
	}
	if isRedirect(h.HTTPStatus) {
		row.FetchRedirect = headers.Get("Location")
	}

	body := HTTPBody(rec.Block)
	row.ContentLength = int64(len(body))
	row.ContentMIMEDetected = detectMIME(body)
	row.ContentLanguages, row.ContentLanguageConfidence, row.ContentLanguageDeclared =
		detectNewsLanguage(body, row.ContentMIMEDetected, headers.Get("Content-Type"))
	return row, true
}

// detectNewsLanguage identifies the language of a response body and also reports
// what the page claimed for itself. CC-NEWS ships no language label of any kind,
// so this is the only place either can come from.
//
// The text handed to the identifier is the extracted document text, not the raw
// HTML: a trigram profile of markup, script and CSS class names is a profile of
// the boilerplate rather than of the article. Anything that is not HTML is left
// alone, since there is no document to extract from a PDF or an image and a
// guess made on its bytes would be noise wearing a confidence score.
//
// The declared code is the <html lang> attribute, returned as the page spelled
// it. It is worth keeping next to the detected one precisely because the two
// disagree: a publisher's own label is high signal on a news site and useless on
// a template that ships lang="en" on every edition in every language.
//
// contentType is the response's own Content-Type header, passed through so a
// page in a legacy encoding is read in that encoding. Skipping it is not a
// cosmetic loss: a Windows-1251 Russian article decoded as UTF-8 identifies as
// Estonian, which is a wrong answer rather than a missing one.
func detectNewsLanguage(body []byte, mimeDetected, contentType string) (detected string, conf float64, declared string) {
	if !strings.Contains(mimeDetected, "html") {
		return "", 0, ""
	}
	if utf8 := transcodeAs(body, contentType); utf8 != nil {
		body = utf8
	}
	tr := ExtractContent(body)
	code, c := DetectLanguage(tr.Body)
	if code == "" {
		return "", 0, tr.Language
	}
	return code, c, tr.Language
}

// detectMIME sniffs the response body. CC-NEWS has no equivalent of cc-index's
// content_mime_detected, and the header the origin sent is the one thing on a
// response nobody checks: a server that labels every page text/html is common
// enough that the label alone cannot be trusted to find the HTML.
//
// http.DetectContentType implements the WHATWG sniffing algorithm and reads at
// most the first 512 bytes. It returns a charset parameter on text types that
// the caller does not want here, so it is trimmed back to the media type.
func detectMIME(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	ct := http.DetectContentType(body)
	if i := strings.Index(ct, ";"); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	return ct
}

// splitContentType separates a Content-Type header into its media type and
// charset. A header too broken to parse still usually has a usable media type in
// front of the first semicolon, so it falls back to that rather than to nothing.
func splitContentType(v string) (mimeType, charset string) {
	v = strings.TrimSpace(v)
	if v == "" {
		return "", ""
	}
	mt, params, err := mime.ParseMediaType(v)
	if err != nil {
		if i := strings.Index(v, ";"); i >= 0 {
			return strings.ToLower(strings.TrimSpace(v[:i])), ""
		}
		return strings.ToLower(v), ""
	}
	return mt, strings.ToUpper(params["charset"])
}

// parseResponseHeaders reads the header block of a stored HTTP response. It
// returns empty headers rather than an error for a block it cannot parse,
// because a response whose headers are malformed is still a capture worth
// indexing by its URL and its bytes.
func parseResponseHeaders(block []byte) http.Header {
	raw := HTTPHeaders(block)
	h := http.Header{}
	lines := strings.Split(string(raw), "\n")
	for _, line := range lines[min(1, len(lines)):] { // drop the status line
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		h.Add(strings.TrimSpace(name), strings.TrimSpace(value))
	}
	return h
}

// isRedirect reports whether a status code carries a Location worth recording.
func isRedirect(status int) bool {
	switch status {
	case 301, 302, 303, 307, 308:
		return true
	}
	return false
}

// tldOf returns the last label of a host.
func tldOf(host string) string {
	if i := strings.LastIndex(host, "."); i >= 0 {
		return host[i+1:]
	}
	return ""
}

// schemeOf returns the URL scheme, lower-cased, or "" when there is none.
func schemeOf(raw string) string {
	if i := strings.Index(raw, "://"); i > 0 {
		return strings.ToLower(raw[:i])
	}
	return ""
}

package ccrawl

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// CDXQuery describes a query against the CDX URL index.
type CDXQuery struct {
	URL    string // URL or pattern
	Match  string // exact|prefix|host|domain (empty -> inferred from URL)
	From   string // 14-digit (or loose) lower time bound
	To     string // 14-digit (or loose) upper time bound
	Status string // HTTP status filter (e.g. "200")
	MIME   string // mime-detected filter
	Lang   string // languages filter (ISO-639-3)
	Filter []string
	// URLContains and URLNotContains are substring filters on the capture URL.
	// They go to the server as regex filters, so the pages come back with the
	// unwanted rows already gone instead of arriving here to be dropped.
	URLContains    string
	URLNotContains string
	// NoPushFilters keeps URLContains and URLNotContains off the wire. The
	// caller still has to apply them, and the only reason to set it is a server
	// whose filtering disagrees with ours.
	NoPushFilters bool
	Limit         int

	// OnPageError is called when a page of the result could not be read, after
	// the retries have been spent on it. Returning nil drops that page and the
	// stream carries on with the next one; returning an error ends the stream
	// with it. A nil handler ends the stream, which is what a caller that has
	// not thought about partial results should get.
	//
	// Page is -1 when it was the page count itself that could not be read, and
	// so the whole crawl is being dropped rather than one page of it.
	OnPageError func(crawlID string, page int, err error) error
}

// CDXRequestURL renders the request one crawl's index server answers for this
// query, without the page parameter. It is what --explain prints: paste it into
// curl and the rows that come back are the rows the command reads.
func CDXRequestURL(crawlID string, q CDXQuery) string {
	return cdxAPIURL(crawlID) + "?" + q.cdxValues(-1).Encode()
}

// cdxValues builds the query string for one page of a CDX request.
func (q CDXQuery) cdxValues(page int) url.Values {
	target, match := q.URL, q.Match
	if match == "" {
		target, match = InferMatchType(q.URL)
	}
	v := url.Values{
		"url":       {target},
		"matchType": {match},
		"output":    {"json"},
	}
	if page >= 0 {
		v.Set("page", strconv.Itoa(page))
	}
	if q.From != "" {
		v.Set("from", looseTimestamp(q.From, false))
	}
	if q.To != "" {
		v.Set("to", looseTimestamp(q.To, true))
	}
	for _, f := range q.serverFilters() {
		v.Add("filter", f)
	}
	return v
}

// serverFilters merges the convenience filters and any raw --filter into the
// CDX server's filter syntax (field:regex, optionally prefixed with ! or =).
func (q CDXQuery) serverFilters() []string {
	var f []string
	if q.Status != "" {
		f = append(f, "=status:"+q.Status)
	}
	if q.MIME != "" {
		f = append(f, "mime-detected:"+regexEscape(q.MIME))
	}
	if q.Lang != "" {
		f = append(f, "languages:"+q.Lang)
	}
	if !q.NoPushFilters {
		if q.URLContains != "" {
			f = append(f, "url:"+containsRegex(q.URLContains))
		}
		if q.URLNotContains != "" {
			f = append(f, "!url:"+containsRegex(q.URLNotContains))
		}
	}
	f = append(f, q.Filter...)
	return f
}

// containsRegex turns a substring into a filter regex that matches it anywhere
// in the field. The leading and trailing .* are not decoration: the index server
// anchors a filter regex at the start of the value, so "budget" on its own only
// matches a URL that begins with the word, which no URL does.
func containsRegex(sub string) string { return ".*" + regexEscape(sub) + ".*" }

// CDXNumPages returns the number of result pages for a query.
func CDXNumPages(ctx context.Context, h *HTTPClient, crawlID string, q CDXQuery) (int, error) {
	v := q.cdxValues(-1)
	v.Set("showNumPages", "true")
	data, err := cdxPageBody(ctx, h, cdxAPIURL(crawlID)+"?"+v.Encode())
	if err != nil {
		return 0, err
	}
	h.cdxBytes.Add(int64(len(data)))
	var m struct {
		Pages int `json:"pages"`
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, fmt.Errorf("parse numPages: %w (body: %s)", err, truncate(string(data), 200))
	}
	return m.Pages, nil
}

// CDXSearch runs a query and collects matching records (bounded by q.Limit).
func CDXSearch(ctx context.Context, h *HTTPClient, crawlID string, q CDXQuery) ([]CDXRecord, error) {
	var recs []CDXRecord
	err := CDXStream(ctx, h, crawlID, q, func(r CDXRecord) error {
		recs = append(recs, r)
		return nil
	})
	return recs, err
}

// CDXStream runs a query and calls fn for each matching record, paginating
// through the server's pages and stopping at q.Limit.
func CDXStream(ctx context.Context, h *HTTPClient, crawlID string, q CDXQuery, fn func(CDXRecord) error) error {
	pages, err := CDXNumPages(ctx, h, crawlID, q)
	if err != nil {
		// Without a page count there is no crawl to read, so this drops all of it
		// rather than one page. A run over six crawls should still return the five
		// that answered.
		if q.OnPageError == nil {
			return err
		}
		return q.OnPageError(crawlID, -1, err)
	}
	if pages == 0 {
		pages = 1
	}
	count := 0
	for page := 0; page < pages; page++ {
		stop := false
		// The callback's error is carried out here rather than returned through
		// cdxPage, so it reaches the caller exactly as it was raised. Callers stop
		// a stream by returning a sentinel and comparing it on the way back, and
		// half of them, kit's row limit among them, compare it by identity. Adding
		// "CDX page 0: " to it turns "stop here" into a failed search.
		var cbErr error
		err := cdxPage(ctx, h, crawlID, q, page, func(r CDXRecord) error {
			r.CrawlID = crawlID
			if err := fn(r); err != nil {
				cbErr = err
				return errStop
			}
			count++
			if q.Limit > 0 && count >= q.Limit {
				stop = true
				return errStop
			}
			return nil
		})
		if cbErr != nil {
			return cbErr
		}
		if err != nil && err != errStop {
			pageErr := fmt.Errorf("read CDX page %d: %w", page, err)
			if q.OnPageError == nil {
				return pageErr
			}
			if herr := q.OnPageError(crawlID, page, pageErr); herr != nil {
				return herr
			}
			continue
		}
		if stop {
			break
		}
	}
	return nil
}

var errStop = fmt.Errorf("stop")

// cdxTruncated is a page whose body stopped early. The request itself succeeded,
// so the retry loop in the client never saw it: by the time the connection drops
// the status line and the headers are long gone and the reader is halfway
// through the records.
type cdxTruncated struct {
	URL  string
	Read int
	Err  error
}

func (e *cdxTruncated) Error() string {
	return fmt.Sprintf("body stopped after %d bytes: %v", e.Read, e.Err)
}

func (e *cdxTruncated) Unwrap() error { return e.Err }

// cdxPageBody reads one whole page into memory, retrying a body that stops
// early. The page is read to the end before any of its records go out, because
// a page that failed half way would otherwise emit its first half twice once
// the retry succeeded.
//
// The index truncates responses often enough on a busy day that a wide query
// which does not retry this returns a different number of records every time
// it runs. Only the truncation is retried here: a request that failed outright
// has already had every attempt the client gives it.
func cdxPageBody(ctx context.Context, h *HTTPClient, url string) ([]byte, error) {
	var last error
	for attempt := 0; attempt <= h.retries; attempt++ {
		if attempt > 0 {
			if err := h.sleepBackoff(ctx, attempt, 0, false); err != nil {
				return nil, err
			}
		}
		data, err := cdxFetch(ctx, h, url)
		if err == nil {
			return data, nil
		}
		var trunc *cdxTruncated
		if !errors.As(err, &trunc) {
			return nil, err
		}
		last = err
	}
	return nil, fmt.Errorf("all %d attempts came back short: %w", h.retries+1, last)
}

// cdxFetch performs one attempt at a page.
func cdxFetch(ctx context.Context, h *HTTPClient, url string) ([]byte, error) {
	resp, err := h.Get(ctx, url)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return nil, &httpStatusError{URL: url, Status: resp.StatusCode}
	}
	// Counted here rather than around the record scanner, because this is the
	// one place an index response is read and it is the place a page that
	// arrived and was thrown away still gets counted. An attempt that came back
	// short and is about to be retried moved those bytes too, and the question
	// the total answers is how much of the index the query had to move.
	data, err := io.ReadAll(h.countCDX(resp.Body))
	if err != nil {
		return nil, &cdxTruncated{URL: url, Read: len(data), Err: err}
	}
	return data, nil
}

func cdxPage(ctx context.Context, h *HTTPClient, crawlID string, q CDXQuery, page int, fn func(CDXRecord) error) error {
	body, err := cdxPageBody(ctx, h, cdxAPIURL(crawlID)+"?"+q.cdxValues(page).Encode())
	if err != nil {
		return err
	}

	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var raw map[string]string
		if json.Unmarshal([]byte(line), &raw) != nil {
			continue
		}
		rec := CDXRecord{
			URLKey:       raw["urlkey"],
			Timestamp:    raw["timestamp"],
			URL:          raw["url"],
			MIME:         raw["mime"],
			MIMEDetected: raw["mime-detected"],
			Status:       raw["status"],
			Digest:       raw["digest"],
			Length:       raw["length"],
			Offset:       raw["offset"],
			Filename:     raw["filename"],
			Charset:      raw["charset"],
			Languages:    raw["languages"],
			Truncated:    raw["truncated"],
			Redirect:     raw["redirect"],
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
	return sc.Err()
}

// looseTimestamp normalizes a loose date ("2024", "2024-06", "2024-06-15") into
// the 14-digit form. When upper is true, missing components are filled with the
// maximum value so the bound is inclusive.
func looseTimestamp(s string, upper bool) string {
	digits := strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, s)
	if len(digits) >= 14 {
		return digits[:14]
	}
	pad := "00000000000000"
	if upper {
		// year, month, day, hour, min, sec maxima.
		pad = "99991231235959"
	}
	if len(digits) == 0 {
		return ""
	}
	return digits + pad[len(digits):]
}

func regexEscape(s string) string {
	// CDX filters are regex; escape characters that would otherwise be special.
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '.', '+', '*', '?', '(', ')', '[', ']', '{', '}', '^', '$', '|', '\\':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

package ccrawl

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
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
	Limit  int
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
	f = append(f, q.Filter...)
	return f
}

// CDXNumPages returns the number of result pages for a query.
func CDXNumPages(ctx context.Context, h *HTTPClient, crawlID string, q CDXQuery) (int, error) {
	v := q.cdxValues(-1)
	v.Set("showNumPages", "true")
	data, err := h.FetchBytes(ctx, cdxAPIURL(crawlID)+"?"+v.Encode())
	if err != nil {
		return 0, err
	}
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
		return err
	}
	if pages == 0 {
		pages = 1
	}
	count := 0
	for page := 0; page < pages; page++ {
		stop := false
		err := cdxPage(ctx, h, crawlID, q, page, func(r CDXRecord) error {
			r.CrawlID = crawlID
			if err := fn(r); err != nil {
				return err
			}
			count++
			if q.Limit > 0 && count >= q.Limit {
				stop = true
				return errStop
			}
			return nil
		})
		if err != nil && err != errStop {
			return fmt.Errorf("CDX page %d: %w", page, err)
		}
		if stop {
			break
		}
	}
	return nil
}

var errStop = fmt.Errorf("stop")

func cdxPage(ctx context.Context, h *HTTPClient, crawlID string, q CDXQuery, page int, fn func(CDXRecord) error) error {
	resp, err := h.Get(ctx, cdxAPIURL(crawlID)+"?"+q.cdxValues(page).Encode())
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	sc := bufio.NewScanner(resp.Body)
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

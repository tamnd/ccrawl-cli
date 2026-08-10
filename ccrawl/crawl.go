package ccrawl

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/andybalholm/brotli"
)

// The frontier itself lives in frontier.go, on disk. It used to be a heap and
// a map right here, which is fine until the seed set is bigger than memory.
// robots.txt lives in robots.go for the same reason: RFC 9309 is more than the
// dozen lines of prefix matching that used to sit in this file.

// ContentSHA1 returns the hex SHA-1 of raw content bytes (matches CC's digest
// field in CDX records).
func ContentSHA1(content []byte) string {
	h := sha1.New()
	_, _ = h.Write(content)
	return hex.EncodeToString(h.Sum(nil))
}

// ── crawl result ──────────────────────────────────────────────────────────────

// CrawlResult is the output of fetching a single URL.
type CrawlResult struct {
	URL         string
	FinalURL    string // after redirects
	Status      int
	ContentType string
	Body        []byte
	Digest      string // SHA-1 of body
	FetchedAt   time.Time
	// Links extracted from HTML (relative links resolved to FinalURL)
	Links []string
}

// CrawlConfig holds configuration for the crawler.
type CrawlConfig struct {
	UserAgent   string
	MaxRedirect int
	Timeout     time.Duration
}

// DefaultCrawlConfig returns sensible defaults for the crawler.
var DefaultCrawlConfig = CrawlConfig{
	UserAgent:   "CCrawl/2.0 (+https://ccrawl.tamnd.com/bot)",
	MaxRedirect: 5,
	Timeout:     120 * time.Second,
}

// sharedTransport is a package-level transport so all CrawlURL calls share a
// connection pool (keep-alives reused across requests to the same host).
var sharedTransport = &http.Transport{
	MaxIdleConns:        200,
	MaxIdleConnsPerHost: 10,
	IdleConnTimeout:     90 * time.Second,
}

// CrawlURL fetches a single URL and returns a CrawlResult. It does not consult
// the robots.txt cache; the caller must do that before calling CrawlURL.
func CrawlURL(ctx context.Context, rawURL string, cfg CrawlConfig) (*CrawlResult, error) {
	client := &http.Client{
		Transport: sharedTransport,
		Timeout:   cfg.Timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= cfg.MaxRedirect {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", cfg.UserAgent)
	req.Header.Set("Accept-Encoding", "gzip, br")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := readBody(resp)
	if err != nil {
		return nil, err
	}

	res := &CrawlResult{
		URL:         rawURL,
		FinalURL:    resp.Request.URL.String(),
		Status:      resp.StatusCode,
		ContentType: resp.Header.Get("Content-Type"),
		Body:        body,
		Digest:      ContentSHA1(body),
		FetchedAt:   time.Now(),
	}

	// extract links from HTML
	ct := strings.ToLower(res.ContentType)
	if strings.Contains(ct, "html") {
		res.Links = ExtractOutLinks(body, res.FinalURL)
	}

	return res, nil
}

// ExtractOutLinks extracts absolute URLs from HTML anchor hrefs, resolving
// relative URLs against baseURL.
func ExtractOutLinks(htmlBytes []byte, baseURL string) []string {
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil
	}
	links := ExtractLinks(baseURL, htmlBytes)
	var out []string
	for _, l := range links {
		ref, err := url.Parse(l.URL)
		if err != nil {
			continue
		}
		abs := base.ResolveReference(ref)
		if abs.Scheme == "http" || abs.Scheme == "https" {
			out = append(out, abs.String())
		}
	}
	return out
}

func readBody(resp *http.Response) ([]byte, error) {
	var r io.Reader = resp.Body
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		defer func() { _ = gz.Close() }()
		r = gz
	case "br":
		r = brotli.NewReader(resp.Body)
	}
	var buf bytes.Buffer
	const maxBody = 10 << 20 // 10 MB max body
	_, err := io.Copy(&buf, io.LimitReader(r, maxBody))
	return buf.Bytes(), err
}

// NormalizeURL applies light URL normalization: lowercase scheme+host, remove
// default port, strip fragment, strip known tracking parameters.
func NormalizeURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	u.Fragment = "" // drop fragment
	if q := StripTrackingParams(u.RawQuery); q != u.RawQuery {
		u.RawQuery = q
	}
	// remove default ports
	host := u.Hostname()
	port := u.Port()
	if (u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443") {
		u.Host = host
	}
	return u.String()
}

// ── WARC record writer ────────────────────────────────────────────────────────

// NewWARCRecord holds the fields to write a fresh WARC response record.
type NewWARCRecord struct {
	TargetURI string
	Date      string
	RecordID  string
	Block     []byte // raw HTTP response bytes
}

// WriteWARCResponse writes a WARC/1.0 response record to w.
func WriteWARCResponse(w io.Writer, rec NewWARCRecord) error {
	const crlf = "\r\n"
	contentLen := len(rec.Block)
	hdr := fmt.Sprintf(
		"WARC/1.0\r\nWARC-Type: response\r\nWARC-Target-URI: %s\r\nWARC-Date: %s\r\nWARC-Record-ID: <%s>\r\nContent-Type: application/http; msgtype=response\r\nContent-Length: %d\r\n\r\n",
		rec.TargetURI, rec.Date, rec.RecordID, contentLen)
	if _, err := io.WriteString(w, hdr); err != nil {
		return err
	}
	if _, err := w.Write(rec.Block); err != nil {
		return err
	}
	// WARC record ends with two CRLFs
	_, err := io.WriteString(w, crlf+crlf)
	return err
}

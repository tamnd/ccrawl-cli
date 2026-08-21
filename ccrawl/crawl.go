package ccrawl

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"sort"
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

	// Everything a WARC record needs and a plain fetch does not.
	//
	// The HTTP messages are reconstructed rather than captured off the wire,
	// because net/http hands back a decoded body and a parsed header and never
	// the original bytes. What matters for an archive is that the record is
	// self consistent: the headers stored here describe the body stored here,
	// so a reader that trusts the Content-Length gets the payload the digest
	// was taken over.
	RequestHeader  []byte // request line and headers, ending in a blank line
	ResponseHeader []byte // status line and headers, ending in a blank line
	RemoteAddr     string // IP of the server that answered, for WARC-IP-Address
	Truncated      bool   // the body cap cut the response short

	// The response validators, lifted out of the header so a capture can be read
	// back as a seed for the next pass. A recrawl that has these can ask the
	// server whether anything moved instead of downloading the page to find out.
	ETag         string
	LastModified string

	// Timing, measured from the moment the request goes out. TTFB is the wait
	// for the first response byte and Duration is the whole fetch including the
	// body, so the difference between them is the transfer.
	TTFB     time.Duration
	Duration time.Duration
}

// CrawlConfig holds configuration for the crawler.
type CrawlConfig struct {
	UserAgent   string
	MaxRedirect int
	Timeout     time.Duration
	// MaxBody caps the stored body. A response longer than this is kept up to
	// the cap and flagged truncated rather than dropped, which is what the WARC
	// spec's WARC-Truncated is for. Zero means DefaultMaxBody.
	MaxBody int64
	// OnRequestWritten, when set, is called with the instant the request bytes
	// went out, once per hop of a redirect chain. It is how a caller holding a
	// host to one request per delay measures from the wire rather than from its
	// own dispatch, which are a connection setup apart.
	OnRequestWritten func(time.Time)
	// Transport, when set, is the connection layer to fetch through instead of
	// the package one. A caller that also fetches robots.txt wants both requests
	// on the same transport, or the page opens a second connection to a host it
	// finished talking to a second ago. See webpool.go.
	Transport http.RoundTripper
}

// DefaultMaxBody is the default cap on a stored response body.
const DefaultMaxBody int64 = 10 << 20

// DefaultCrawlConfig returns sensible defaults for the crawler.
var DefaultCrawlConfig = CrawlConfig{
	UserAgent:   "CCrawl/2.0 (+https://ccrawl.tamnd.com/bot)",
	MaxRedirect: 5,
	Timeout:     120 * time.Second,
	MaxBody:     DefaultMaxBody,
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
	tr := cfg.Transport
	if tr == nil {
		tr = sharedTransport
	}
	client := &http.Client{
		Transport: tr,
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

	// The address is only observable while the connection is being set up, and
	// it is the one thing in a WARC record that cannot be reconstructed after
	// the fact. On a redirect chain the last connection is the one the record
	// describes, which is what a plain assignment leaves behind.
	var remoteAddr string
	// Timing is taken from the wire rather than from the call, because a request
	// that waited on a connection or on a politeness clock did not take that long
	// to serve and a dataset that says it did is telling a story about us rather
	// than about the site. sent is stamped on every hop, so a redirect chain
	// reports the last hop, which is the one the row describes.
	var sent, firstByte time.Time
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Conn != nil {
				remoteAddr = info.Conn.RemoteAddr().String()
			}
		},
		WroteRequest: func(httptrace.WroteRequestInfo) {
			sent = time.Now()
			if cfg.OnRequestWritten != nil {
				cfg.OnRequestWritten(sent)
			}
		},
		GotFirstResponseByte: func() { firstByte = time.Now() },
	}))

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, decoded, truncated, err := readBody(resp, cfg.MaxBody)
	if err != nil {
		return nil, err
	}

	res := &CrawlResult{
		URL:            rawURL,
		FinalURL:       resp.Request.URL.String(),
		Status:         resp.StatusCode,
		ContentType:    resp.Header.Get("Content-Type"),
		Body:           body,
		Digest:         ContentSHA1(body),
		FetchedAt:      time.Now(),
		RequestHeader:  requestHeaderBlock(resp.Request),
		ResponseHeader: responseHeaderBlock(resp, len(body), decoded),
		RemoteAddr:     hostOnly(remoteAddr),
		Truncated:      truncated,
		ETag:           resp.Header.Get("ETag"),
		LastModified:   resp.Header.Get("Last-Modified"),
	}
	if !sent.IsZero() {
		res.Duration = res.FetchedAt.Sub(sent)
		if !firstByte.IsZero() {
			res.TTFB = firstByte.Sub(sent)
		}
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

// readBody reads and decodes a response body up to maxBody bytes. It reports
// whether it decoded a Content-Encoding and whether the cap cut the body short,
// both of which change what the stored headers have to say.
func readBody(resp *http.Response, maxBody int64) (body []byte, decoded, truncated bool, err error) {
	if maxBody <= 0 {
		maxBody = DefaultMaxBody
	}
	var r io.Reader = resp.Body
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		gz, gzErr := gzip.NewReader(resp.Body)
		if gzErr != nil {
			return nil, false, false, gzErr
		}
		defer func() { _ = gz.Close() }()
		r, decoded = gz, true
	case "br":
		r, decoded = brotli.NewReader(resp.Body), true
	}
	// One byte past the cap, so that hitting it exactly is distinguishable from
	// a body that happened to be that long. Guessing wrong here means either a
	// WARC-Truncated on a complete record or, worse, none on a clipped one.
	var buf bytes.Buffer
	n, err := io.Copy(&buf, io.LimitReader(r, maxBody+1))
	if n > maxBody {
		buf.Truncate(int(maxBody))
		truncated = true
	}
	return buf.Bytes(), decoded, truncated, err
}

// requestHeaderBlock rebuilds the HTTP request as it went out, for the WARC
// request record.
func requestHeaderBlock(req *http.Request) []byte {
	if req == nil {
		return nil
	}
	target := req.URL.RequestURI()
	proto := req.Proto
	if proto == "" {
		proto = "HTTP/1.1"
	}
	var b bytes.Buffer
	fmt.Fprintf(&b, "%s %s %s\r\n", req.Method, target, proto)
	// Host is a field on the request rather than a header, and it is the one
	// header a request cannot be missing.
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	fmt.Fprintf(&b, "Host: %s\r\n", host)
	writeSortedHeaders(&b, req.Header, nil)
	b.WriteString("\r\n")
	return b.Bytes()
}

// responseHeaderBlock rebuilds the HTTP response headers so that they describe
// the body actually stored with them.
//
// Two of them cannot be copied through. Go dechunks as it reads, so a stored
// body is never chunked whatever the server said, and when we decode a
// Content-Encoding the stored body is no longer in that encoding. Both would
// send a reader looking for bytes that are not there. Content-Length is always
// rewritten for the same reason: the wire length describes the wire, and after
// dechunking, decoding or a truncation it is not the length of anything here.
func responseHeaderBlock(resp *http.Response, bodyLen int, decoded bool) []byte {
	skip := map[string]bool{"Content-Length": true, "Transfer-Encoding": true}
	if decoded {
		skip["Content-Encoding"] = true
	}
	proto := resp.Proto
	if proto == "" {
		proto = "HTTP/1.1"
	}
	status := resp.Status
	if status == "" {
		status = fmt.Sprintf("%d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
	}
	var b bytes.Buffer
	fmt.Fprintf(&b, "%s %s\r\n", proto, status)
	writeSortedHeaders(&b, resp.Header, skip)
	fmt.Fprintf(&b, "Content-Length: %d\r\n\r\n", bodyLen)
	return b.Bytes()
}

// writeSortedHeaders writes headers in a stable order, because a Go map is not
// one and a record that differs run to run is a record nobody can diff.
func writeSortedHeaders(b *bytes.Buffer, h http.Header, skip map[string]bool) {
	keys := make([]string, 0, len(h))
	for k := range h {
		if !skip[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		for _, v := range h[k] {
			fmt.Fprintf(b, "%s: %s\r\n", k, v)
		}
	}
}

// hostOnly strips the port from a host:port address, leaving the bare IP that
// WARC-IP-Address wants.
func hostOnly(addr string) string {
	if addr == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
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

// Package fakecc stands a Common Crawl up on localhost: the crawl list, the CDX
// index server, the path manifests, and the WARC files those manifests point at.
//
// It exists because most of what can go wrong in this program is wiring. A flag
// that is parsed and never reaches the query, a record whose offset is read as
// zero, a paging loop that stops one page early: none of those are visible to a
// unit test of the function that holds the bug, and all of them are obvious the
// moment a command runs end to end. The pieces here are consistent with each
// other, so a capture the CDX server reports really does live at that byte range
// in the WARC file the manifest lists, and a command that fetches it gets the
// page back.
//
// Start swaps ccrawl.Endpoints for the duration of the test and restores them
// through t.Cleanup, so a test that forgets to tear down cannot leak a local URL
// into the next one.
package fakecc

import (
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// PageSize is how many CDX records the server puts on one page. It is small on
// purpose: a fixture with three captures and a page size of two exercises the
// paging loop, which a realistic page size would never do.
const PageSize = 2

// Crawls are the crawl IDs the fake publishes, newest first. Two years are
// present so the year, integer, comma list and "all" forms of -c each resolve to
// something different and a test can tell them apart.
var Crawls = []string{
	"CC-MAIN-2026-30",
	"CC-MAIN-2026-05",
	"CC-MAIN-2025-33",
}

// WARCPath is the file every capture in the fixture lives in, in the relative
// form Common Crawl publishes and ccrawl stores in a location record.
const WARCPath = "crawl-data/CC-MAIN-2026-30/segments/1700000000000.0/warc/CC-MAIN-20260101000000-20260101010000-00000.warc.gz"

// Capture is one page in the fixture: what the CDX server says about it, and the
// HTML the WARC file holds for it.
type Capture struct {
	URL       string
	Timestamp string
	Status    string
	MIME      string
	Languages string
	Title     string
	Body      string

	// Filled in by build, once the WARC file exists and the record's place in it
	// is known.
	offset, length int64
	digest         string
}

// Captures is the default fixture. Two hosts under one registered domain and one
// unrelated host, so a host match and a domain match give different answers, and
// one non-200 so a status filter has something to remove.
var Captures = []Capture{
	{
		URL: "https://example.com/", Timestamp: "20260101000000",
		Status: "200", MIME: "text/html", Languages: "eng",
		Title: "Example Domain",
		Body:  "<html><head><title>Example Domain</title></head><body><h1>Example Domain</h1><p>This domain is for use in illustrative examples.</p><a href=\"/about\">About</a></body></html>",
	},
	{
		URL: "https://example.com/about", Timestamp: "20260101001500",
		Status: "200", MIME: "text/html", Languages: "eng",
		Title: "About",
		Body:  "<html><head><title>About</title></head><body><p>About this example.</p></body></html>",
	},
	{
		URL: "https://www.example.com/gone", Timestamp: "20260101003000",
		Status: "404", MIME: "text/html", Languages: "eng",
		Title: "Not Found",
		Body:  "<html><head><title>Not Found</title></head><body><p>Gone.</p></body></html>",
	},
	{
		URL: "https://other.test/index.html", Timestamp: "20260101004500",
		Status: "200", MIME: "text/html", Languages: "fra",
		Title: "Autre",
		Body:  "<html><head><title>Autre</title></head><body><p>Bonjour.</p></body></html>",
	},
}

// Server is a running fake Common Crawl.
type Server struct {
	URL  string
	WARC []byte // the whole WARC file, as the data host serves it

	captures []Capture

	mu     sync.Mutex
	hits   map[string]int
	failed map[string]bool
}

// Start brings a fake Common Crawl up, points ccrawl at it, and tears both down
// when the test ends.
func Start(t *testing.T) *Server {
	t.Helper()
	s := &Server{hits: map[string]int{}, failed: map[string]bool{}}
	s.build()

	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	s.URL = srv.URL

	old := ccrawl.Endpoints
	ccrawl.Endpoints = ccrawl.Hosts{
		CollInfo: srv.URL + "/collinfo.json",
		Data:     srv.URL + "/",
		CDX:      srv.URL + "/",
	}
	t.Cleanup(func() { ccrawl.Endpoints = old })
	return s
}

// Hits reports how many requests the server answered for a path, so a test can
// prove a cache was used, or that a stream stopped before fetching a page it did
// not need.
func (s *Server) Hits(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[path]
}

// Capture returns the fixture capture for a URL, with the byte range it occupies
// in the WARC file filled in.
func (s *Server) Capture(url string) (Capture, bool) {
	for _, c := range s.captures {
		if c.URL == url {
			return c, true
		}
	}
	return Capture{}, false
}

// Location renders a capture as the location JSONL that fetch and export read on
// stdin.
func (s *Server) Location(url string) string {
	c, ok := s.Capture(url)
	if !ok {
		return ""
	}
	b, _ := json.Marshal(map[string]any{
		"filename": WARCPath, "offset": c.offset, "length": c.length, "url": c.URL,
	})
	return string(b)
}

// build assembles the WARC file and records where each capture landed in it. The
// offsets have to come from the real file rather than be made up, because they
// are what a ranged fetch asks for, and a fixture whose offsets are wrong tests
// nothing except that the fetch failed.
func (s *Server) build() {
	s.captures = append([]Capture(nil), Captures...)
	var buf bytes.Buffer
	for i := range s.captures {
		c := &s.captures[i]
		block := httpResponseBlock(*c)
		c.digest = sha1Base32(block)
		member := gzipMember(warcResponseRecord(*c, block))
		c.offset = int64(buf.Len())
		c.length = int64(len(member))
		buf.Write(member)
	}
	s.WARC = buf.Bytes()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	s.mu.Lock()
	s.hits[path]++
	s.mu.Unlock()

	switch {
	case path == "collinfo.json":
		s.serveCollInfo(w)
	case strings.HasSuffix(path, "-index"):
		s.serveCDX(w, r, strings.TrimSuffix(path, "-index"))
	case strings.HasSuffix(path, ".paths.gz"):
		s.servePaths(w, path)
	case path == WARCPath:
		http.ServeContent(w, r, "f.warc.gz", time.Time{}, bytes.NewReader(s.WARC))
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) serveCollInfo(w http.ResponseWriter) {
	list := make([]ccrawl.Crawl, len(Crawls))
	for i, id := range Crawls {
		list[i] = ccrawl.Crawl{
			ID:     id,
			Name:   strings.Replace(id, "CC-MAIN-", "Common Crawl ", 1),
			CDXAPI: s.URL + "/" + id + "-index",
		}
	}
	writeJSON(w, list)
}

// serveCDX answers the URL index. It applies the match type, the time bounds and
// the filters the way the real server does, because a client side filter that
// silently stands in for a server side one is a bug this fixture is meant to
// catch rather than hide.
func (s *Server) serveCDX(w http.ResponseWriter, r *http.Request, crawlID string) {
	q := r.URL.Query()

	// One page of every query 403s the first time it is asked for, which is what
	// Common Crawl's fronting does under load. The client retries it, so a test
	// that gets the full result set has proved the retry works, and one that does
	// not has found a path where a transient 403 loses records.
	if q.Get("page") == "1" {
		s.mu.Lock()
		first := !s.failed[crawlID]
		s.failed[crawlID] = true
		s.mu.Unlock()
		if first {
			w.WriteHeader(http.StatusForbidden)
			return
		}
	}

	recs := s.match(q)
	if q.Get("showNumPages") == "true" {
		writeJSON(w, map[string]int{"pages": (len(recs) + PageSize - 1) / PageSize})
		return
	}
	if len(recs) == 0 {
		// The real server answers a query it has nothing for with a 404 and a
		// plain text body, which is why a 404 has to read as an empty page.
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprintln(w, "No Captures found for: "+q.Get("url"))
		return
	}
	page, _ := strconv.Atoi(q.Get("page"))
	start := page * PageSize
	if start >= len(recs) {
		return
	}
	for _, rec := range recs[start:min(start+PageSize, len(recs))] {
		writeJSONLine(w, rec)
	}
}

// match applies the query to the fixture and returns the CDX rows for it.
func (s *Server) match(q map[string][]string) []map[string]string {
	get := func(k string) string {
		if v := q[k]; len(v) > 0 {
			return v[0]
		}
		return ""
	}
	var out []map[string]string
	for _, c := range s.captures {
		if !matchURL(c.URL, get("url"), get("matchType")) {
			continue
		}
		if from := get("from"); from != "" && c.Timestamp < from {
			continue
		}
		if to := get("to"); to != "" && c.Timestamp > to {
			continue
		}
		if !matchFilters(c, q["filter"]) {
			continue
		}
		out = append(out, map[string]string{
			"urlkey":        surtish(c.URL),
			"timestamp":     c.Timestamp,
			"url":           c.URL,
			"mime":          c.MIME,
			"mime-detected": c.MIME,
			"status":        c.Status,
			"digest":        c.digest,
			"languages":     c.Languages,
			"filename":      WARCPath,
			"offset":        strconv.FormatInt(c.offset, 10),
			"length":        strconv.FormatInt(c.length, 10),
		})
	}
	return out
}

// servePaths answers a crawl's path manifest. Every published kind resolves to
// the one WARC file the fixture holds, which is enough for the commands that
// list, count or download paths, and it keeps the fixture to a single archive.
// A kind Common Crawl does not publish is a 404 here as it is there, since the
// program has no list of kinds to validate against and finds out from the
// response.
func (s *Server) servePaths(w http.ResponseWriter, path string) {
	rest, ok := strings.CutPrefix(path, "crawl-data/")
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	id, kind, found := strings.Cut(rest, "/")
	if !found || !slices.Contains(Crawls, id) {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	kind = strings.TrimSuffix(kind, ".paths.gz")
	if !slices.Contains(ccrawl.PathKinds, kind) {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	body := WARCPath + "\n"
	if kind == "cc-index-table" {
		body = ccrawl.ColumnarPrefix + "crawl=" + id + "/subset=warc/part-00000.c000.gz.parquet\n"
	}
	w.Header().Set("Content-Type", "application/gzip")
	zw := gzip.NewWriter(w)
	_, _ = zw.Write([]byte(body))
	_ = zw.Close()
}

// matchURL applies a CDX match type. The real server works on SURT keys; this
// works on the URL, which agrees with it for every shape the fixture holds.
func matchURL(url, target, match string) bool {
	if target == "" {
		return true
	}
	host := hostOf(url)
	switch match {
	case "exact":
		return trimScheme(url) == trimScheme(target)
	case "host":
		return host == trimScheme(target)
	case "domain":
		reg := trimScheme(target)
		return host == reg || strings.HasSuffix(host, "."+reg)
	default: // prefix
		return strings.HasPrefix(trimScheme(url), strings.TrimSuffix(trimScheme(target), "*"))
	}
}

// matchFilters applies the CDX filter syntax the client sends: field:regex, with
// = for an exact match and a leading ! to negate. Only the fields ccrawl builds
// filters for are understood, and an unknown one keeps the row rather than
// silently dropping every result.
func matchFilters(c Capture, filters []string) bool {
	for _, f := range filters {
		neg := strings.HasPrefix(f, "!")
		f = strings.TrimPrefix(f, "!")
		f = strings.TrimPrefix(f, "=")
		field, want, ok := strings.Cut(f, ":")
		if !ok {
			continue
		}
		var have string
		switch field {
		case "status":
			have = c.Status
		case "mime", "mime-detected":
			have = c.MIME
		case "languages":
			have = c.Languages
		case "url":
			have = c.URL
		default:
			continue
		}
		// The client escapes regex metacharacters in the values it builds, so
		// unescaping them back is enough to compare as plain text.
		want = strings.ReplaceAll(want, `\`, "")
		if strings.Contains(have, want) == neg {
			return false
		}
	}
	return true
}

// warcResponseRecord builds one uncompressed WARC/1.0 response record.
func warcResponseRecord(c Capture, block []byte) []byte {
	var rec bytes.Buffer
	rec.WriteString("WARC/1.0\r\n")
	rec.WriteString("WARC-Type: response\r\n")
	fmt.Fprintf(&rec, "WARC-Target-URI: %s\r\n", c.URL)
	fmt.Fprintf(&rec, "WARC-Date: %s\r\n", warcDate(c.Timestamp))
	fmt.Fprintf(&rec, "WARC-Record-ID: <urn:uuid:%s>\r\n", uuidFor(c.URL))
	fmt.Fprintf(&rec, "WARC-Payload-Digest: sha1:%s\r\n", sha1Base32([]byte(c.Body)))
	rec.WriteString("Content-Type: application/http; msgtype=response\r\n")
	fmt.Fprintf(&rec, "Content-Length: %d\r\n\r\n", len(block))
	rec.Write(block)
	rec.WriteString("\r\n\r\n")
	return rec.Bytes()
}

// httpResponseBlock is the stored HTTP response: status line, headers, body.
func httpResponseBlock(c Capture) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "HTTP/1.1 %s %s\r\n", c.Status, statusText(c.Status))
	fmt.Fprintf(&b, "Content-Type: %s; charset=UTF-8\r\n", c.MIME)
	fmt.Fprintf(&b, "Content-Length: %d\r\n", len(c.Body))
	b.WriteString("\r\n")
	b.WriteString(c.Body)
	return b.Bytes()
}

func gzipMember(b []byte) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(b)
	_ = zw.Close()
	return buf.Bytes()
}

func sha1Base32(b []byte) string {
	sum := sha1.Sum(b)
	return base32.StdEncoding.EncodeToString(sum[:])
}

// uuidFor is a stable record ID per URL, so two runs over the same fixture
// produce byte identical WARC files and a golden comparison stays honest.
func uuidFor(url string) string {
	h := sha1Base32([]byte(url))
	h = strings.ToLower(strings.TrimRight(h, "="))
	return fmt.Sprintf("%s-%s-4%s-8%s-%s", h[0:8], h[8:12], h[12:15], h[15:18], h[18:30])
}

func warcDate(ts string) string {
	t, err := time.Parse("20060102150405", ts)
	if err != nil {
		return "2026-01-01T00:00:00Z"
	}
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

func statusText(code string) string {
	n, _ := strconv.Atoi(code)
	if s := http.StatusText(n); s != "" {
		return s
	}
	return "OK"
}

// surtish renders the URL key the way the CDX server does, near enough for a
// fixture: the host reversed, then the path.
func surtish(url string) string {
	rest := trimScheme(url)
	host, path, _ := strings.Cut(rest, "/")
	labels := strings.Split(host, ".")
	for i, j := 0, len(labels)-1; i < j; i, j = i+1, j-1 {
		labels[i], labels[j] = labels[j], labels[i]
	}
	return strings.Join(labels, ",") + ")/" + path
}

func hostOf(url string) string {
	host, _, _ := strings.Cut(trimScheme(url), "/")
	return host
}

func trimScheme(s string) string {
	for _, p := range []string{"https://", "http://"} {
		if rest, ok := strings.CutPrefix(s, p); ok {
			return rest
		}
	}
	return s
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONLine(w http.ResponseWriter, v any) {
	b, _ := json.Marshal(v)
	_, _ = w.Write(append(b, '\n'))
}

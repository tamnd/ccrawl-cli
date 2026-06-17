package ccrawl

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
)

// ServeConfig holds configuration for the HTTP API server.
type ServeConfig struct {
	Addr    string // e.g. ":8080"
	DBPath  string // path to DuckDB/SQLite host database (optional)
	IndexDir string // path to the inverted index directory (optional)
}

// HostStore is the interface for host metadata lookups used by the API.
type HostStore interface {
	GetHost(ctx context.Context, host string) (*HostRecord, error)
	TopHosts(ctx context.Context, n int, tld string) ([]HostRecord, error)
}

// SearchStore is the interface for full-text search used by the API.
type SearchStore interface {
	Search(ctx context.Context, query string, k int) ([]SearchResult, error)
}

// SearchResult is one search hit returned by the API.
type SearchResult struct {
	DocID    uint64  `json:"doc_id"`
	URL      string  `json:"url"`
	Host     string  `json:"host"`
	Title    string  `json:"title"`
	Snippet  string  `json:"snippet"`
	Score    float64 `json:"score"`
	Language string  `json:"language,omitempty"`
}

// APIServer is the ccrawl v2 HTTP API server.
type APIServer struct {
	cfg    ServeConfig
	hosts  HostStore
	search SearchStore
	srv    *http.Server
}

// NewAPIServer creates an API server with the given stores. Either store may be
// nil; endpoints that require the missing store return 503.
func NewAPIServer(cfg ServeConfig, hosts HostStore, search SearchStore) *APIServer {
	s := &APIServer{cfg: cfg, hosts: hosts, search: search}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v2/host/{host}", s.handleHostGet)
	mux.HandleFunc("GET /v2/hosts", s.handleHostList)
	mux.HandleFunc("GET /v2/search", s.handleSearch)
	mux.HandleFunc("GET /v2/health", s.handleHealth)
	s.srv = &http.Server{Addr: cfg.Addr, Handler: mux}
	return s
}

// Addr returns the address the server is listening on. Valid after ListenAndServe
// returns its listener address (useful in tests with ":0").
func (s *APIServer) Addr() string { return s.cfg.Addr }

// ListenAndServe starts the HTTP server. It blocks until the context is done or
// an unrecoverable error occurs.
func (s *APIServer) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	s.cfg.Addr = ln.Addr().String() // update in case port was 0
	s.srv.Addr = s.cfg.Addr
	errCh := make(chan error, 1)
	go func() { errCh <- s.srv.Serve(ln) }()
	select {
	case <-ctx.Done():
		_ = s.srv.Shutdown(context.Background())
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (s *APIServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]string{"status": "ok"})
}

func (s *APIServer) handleHostGet(w http.ResponseWriter, r *http.Request) {
	if s.hosts == nil {
		writeJSON(w, 503, apiError("host store not configured"))
		return
	}
	hostName := r.PathValue("host")
	if hostName == "" {
		writeJSON(w, 400, apiError("missing host name"))
		return
	}
	rec, err := s.hosts.GetHost(r.Context(), hostName)
	if err != nil {
		writeJSON(w, 404, apiError(err.Error()))
		return
	}
	writeJSON(w, 200, rec)
}

func (s *APIServer) handleHostList(w http.ResponseWriter, r *http.Request) {
	if s.hosts == nil {
		writeJSON(w, 503, apiError("host store not configured"))
		return
	}
	q := r.URL.Query()
	n := 100
	if nStr := q.Get("n"); nStr != "" {
		if v, err := strconv.Atoi(nStr); err == nil && v > 0 {
			n = min(v, 10000)
		}
	}
	tld := q.Get("tld")
	hosts, err := s.hosts.TopHosts(r.Context(), n, tld)
	if err != nil {
		writeJSON(w, 500, apiError(err.Error()))
		return
	}
	writeJSON(w, 200, hosts)
}

func (s *APIServer) handleSearch(w http.ResponseWriter, r *http.Request) {
	if s.search == nil {
		writeJSON(w, 503, apiError("search index not configured"))
		return
	}
	q := r.URL.Query().Get("q")
	if q == "" {
		writeJSON(w, 400, apiError("missing query parameter 'q'"))
		return
	}
	k := 10
	if kStr := r.URL.Query().Get("k"); kStr != "" {
		if v, err := strconv.Atoi(kStr); err == nil && v > 0 {
			k = min(v, 100)
		}
	}
	results, err := s.search.Search(r.Context(), q, k)
	if err != nil {
		writeJSON(w, 500, apiError(err.Error()))
		return
	}
	writeJSON(w, 200, map[string]any{"query": q, "results": results})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func apiError(msg string) map[string]string { return map[string]string{"error": msg} }

// ── in-memory HostStore for CLI use ─────────────────────────────────────────

// MemHostStore is a simple in-memory HostStore backed by a slice of HostRecord.
type MemHostStore struct {
	byHost map[string]*HostRecord
	byRank []HostRecord // sorted by HarmonicPos
}

// NewMemHostStore builds a MemHostStore from a slice of HostRecord.
func NewMemHostStore(recs []HostRecord) *MemHostStore {
	s := &MemHostStore{byHost: make(map[string]*HostRecord), byRank: recs}
	for i := range recs {
		s.byHost[recs[i].Host] = &recs[i]
	}
	return s
}

// GetHost returns the HostRecord for a given hostname.
func (s *MemHostStore) GetHost(_ context.Context, host string) (*HostRecord, error) {
	r, ok := s.byHost[host]
	if !ok {
		return nil, fmt.Errorf("host not found: %s", host)
	}
	return r, nil
}

// TopHosts returns up to n hosts filtered optionally by TLD.
func (s *MemHostStore) TopHosts(_ context.Context, n int, tld string) ([]HostRecord, error) {
	var out []HostRecord
	for _, r := range s.byRank {
		if tld != "" && !strings.HasSuffix(r.Host, "."+tld) && hostTLD(r.Host) != tld {
			continue
		}
		out = append(out, r)
		if len(out) >= n {
			break
		}
	}
	return out, nil
}

// ── IndexSearchStore wraps an IndexReader as a SearchStore ───────────────────

// IndexSearchStore wraps an IndexReader to implement the SearchStore interface.
// The forward index is looked up from a map of docID → ForwardDoc.
type IndexSearchStore struct {
	idx     *IndexReader
	forward map[uint64]ForwardDoc
}

// NewIndexSearchStore wraps an IndexReader.
func NewIndexSearchStore(idx *IndexReader, forward map[uint64]ForwardDoc) *IndexSearchStore {
	return &IndexSearchStore{idx: idx, forward: forward}
}

// Search tokenizes the query and returns top-k search results with snippets.
func (s *IndexSearchStore) Search(_ context.Context, query string, k int) ([]SearchResult, error) {
	tokens := Tokenize(query)
	hits := s.idx.Search(tokens, k)
	results := make([]SearchResult, 0, len(hits))
	for _, h := range hits {
		r := SearchResult{DocID: h.DocID, Score: h.Score}
		if fd, ok := s.forward[h.DocID]; ok {
			r.URL = fd.URL
			r.Host = fd.Host
			r.Title = fd.Title
			r.Snippet = fd.Snippet
			r.Language = fd.Language
		}
		results = append(results, r)
	}
	return results, nil
}

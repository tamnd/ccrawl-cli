package ccrawl

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// WebGraph describes one Common Crawl web-graph release.
type WebGraph struct {
	ID        string // e.g. cc-main-2026-mar-apr-may
	BaseURL   string // https://data.commoncrawl.org/projects/hyperlinkgraph/{ID}/
	HostNodes int64
	HostArcs  int64
	Published string
}

// HostRankURL is the single gzipped rank table for hosts.
func (g WebGraph) HostRankURL() string { return g.BaseURL + "host/" + g.ID + "-host-ranks.txt.gz" }

// HostVerticesManifestURL is the manifest listing vertex part files.
func (g WebGraph) HostVerticesManifestURL() string {
	return g.BaseURL + "host/" + g.ID + "-host-vertices.paths.gz"
}

// HostEdgesManifestURL is the manifest listing edge part files.
func (g WebGraph) HostEdgesManifestURL() string {
	return g.BaseURL + "host/" + g.ID + "-host-edges.paths.gz"
}

// graphBaseURL is the root of all hyperlinkgraph releases.
const graphBaseURL = "https://data.commoncrawl.org/projects/hyperlinkgraph/"

// WebGraphBaseURL returns the release directory URL for a web-graph id, so a
// caller that names a release explicitly can build a WebGraph without a resolve.
func WebGraphBaseURL(id string) string { return graphBaseURL + id + "/" }

// reGraphID matches a web-graph release directory name from the full HF URL
// (e.g. href="https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2026-mar-apr-may/...")
// or from a relative path (e.g. href="cc-main-.../").
var reGraphID = regexp.MustCompile(`hyperlinkgraph/(cc-main-[^/"]+)[/"]`)

// LatestWebGraph fetches the CC web-graphs index page and returns the most recent
// host-level graph release. Results are cached for 24 hours.
func LatestWebGraph(ctx context.Context, h *HTTPClient, cache *Cache) (WebGraph, error) {
	const cacheKey = "webgraph:latest"
	if cache != nil {
		if data, ok := cache.Get(cacheKey, 24*time.Hour); ok {
			parts := strings.SplitN(string(data), "\n", 2)
			if len(parts) == 2 {
				return WebGraph{
					ID:      parts[0],
					BaseURL: graphBaseURL + parts[0] + "/",
				}, nil
			}
		}
	}

	data, err := h.FetchBytes(ctx, "https://commoncrawl.org/web-graphs")
	if err != nil {
		return WebGraph{}, fmt.Errorf("fetch web-graphs page: %w", err)
	}

	// Extract release URLs from the page; they appear ordered newest-first.
	ms := reGraphID.FindAllSubmatch(data, -1)
	if len(ms) == 0 {
		// Fallback: parse the index page from data.commoncrawl.org
		data2, err2 := h.FetchBytes(ctx, graphBaseURL)
		if err2 == nil {
			ms = reGraphID.FindAllSubmatch(data2, -1)
		}
	}
	if len(ms) == 0 {
		return WebGraph{}, fmt.Errorf("no web-graph releases found")
	}
	id := string(ms[0][1])
	g := WebGraph{ID: id, BaseURL: graphBaseURL + id + "/"}
	if cache != nil {
		cache.Put(cacheKey, []byte(id+"\n"))
	}
	return g, nil
}

// VertexRecord is one row from the host vertices files: a numeric graph ID and
// the host name in reversed form (com.example.www).
type VertexRecord struct {
	ID      int64  `json:"id" kit:"id" table:"id"`
	HostRev string `json:"host_rev" table:"host_rev"`
	Host    string `json:"host" table:"host"`
}

// VertexStream downloads and streams all vertex part files listed in the manifest
// at manifestURL, calling fn for each record. Parts are fetched sequentially;
// use multiple goroutines externally for parallelism.
func VertexStream(ctx context.Context, h *HTTPClient, manifestURL string, fn func(VertexRecord) error) error {
	parts, err := streamManifest(ctx, h, manifestURL)
	if err != nil {
		return err
	}
	for _, part := range parts {
		url := resolvePartURL(part)
		if err := vertexStreamPart(ctx, h, url, fn); err != nil {
			return err
		}
	}
	return nil
}

func vertexStreamPart(ctx context.Context, h *HTTPClient, url string, fn func(VertexRecord) error) error {
	resp, err := h.GetDownload(ctx, url)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return fmt.Errorf("vertices HTTP %d (%s)", resp.StatusCode, url)
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return err
	}
	defer func() { _ = gz.Close() }()

	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 1<<20), 4<<20)
	for sc.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := sc.Text()
		if line == "" {
			continue
		}
		idStr, rev, ok := strings.Cut(line, "\t")
		if !ok {
			continue
		}
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			continue
		}
		r := VertexRecord{ID: id, HostRev: rev, Host: reverseHost(rev)}
		if err := fn(r); err != nil {
			return err
		}
	}
	return sc.Err()
}

// resolvePartURL turns a manifest path into a fetchable URL. If the path is
// already an absolute HTTP/S URL (used in tests) it is returned as-is;
// otherwise DataBaseURL is prepended.
func resolvePartURL(part string) string {
	if strings.HasPrefix(part, "http://") || strings.HasPrefix(part, "https://") {
		return part
	}
	return DataBaseURL + part
}

// EdgeDegrees holds the in-degree and out-degree for one node.
type EdgeDegrees struct {
	NodeID    int64 `json:"node_id"`
	InDegree  int32 `json:"in_degree"`
	OutDegree int32 `json:"out_degree"`
}

// ComputeEdgeDegrees streams all edge part files and computes in-degree and
// out-degree for every node. It requires knowing nodeCount (the total number of
// vertices) to allocate the degree arrays. Degrees are returned as two arrays
// indexed by node ID; the caller should join with vertex IDs.
func ComputeEdgeDegrees(ctx context.Context, h *HTTPClient, manifestURL string, nodeCount int64) (inDeg, outDeg []int32, err error) {
	inDeg = make([]int32, nodeCount)
	outDeg = make([]int32, nodeCount)

	parts, err := streamManifest(ctx, h, manifestURL)
	if err != nil {
		return nil, nil, err
	}
	for _, part := range parts {
		url := resolvePartURL(part)
		if err := edgeStreamPart(ctx, h, url, nodeCount, inDeg, outDeg); err != nil {
			return nil, nil, err
		}
	}
	return inDeg, outDeg, nil
}

func edgeStreamPart(ctx context.Context, h *HTTPClient, url string, nodeCount int64, inDeg, outDeg []int32) error {
	resp, err := h.GetDownload(ctx, url)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return fmt.Errorf("edges HTTP %d (%s)", resp.StatusCode, url)
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return err
	}
	defer func() { _ = gz.Close() }()

	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 1<<20), 4<<20)
	for sc.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := sc.Text()
		if line == "" {
			continue
		}
		srcStr, dstStr, ok := strings.Cut(line, "\t")
		if !ok {
			continue
		}
		src, e1 := strconv.ParseInt(srcStr, 10, 64)
		dst, e2 := strconv.ParseInt(dstStr, 10, 64)
		if e1 != nil || e2 != nil {
			continue
		}
		if src >= 0 && src < nodeCount {
			outDeg[src]++
		}
		if dst >= 0 && dst < nodeCount {
			inDeg[dst]++
		}
	}
	return sc.Err()
}

// streamManifest downloads a .paths.gz manifest and returns the listed paths.
func streamManifest(ctx context.Context, h *HTTPClient, url string) ([]string, error) {
	resp, err := h.Get(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("fetch manifest %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("manifest HTTP %d (%s)", resp.StatusCode, url)
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return nil, err
	}
	defer func() { _ = gz.Close() }()
	var parts []string
	sc := bufio.NewScanner(gz)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line != "" {
			parts = append(parts, line)
		}
	}
	return parts, sc.Err()
}

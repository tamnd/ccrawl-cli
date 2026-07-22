package ccrawl

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// gzBytes returns gz-compressed content of lines joined by "\n".
func gzLines(lines []string) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	for _, l := range lines {
		_, _ = w.Write([]byte(l + "\n"))
	}
	_ = w.Close()
	return buf.Bytes()
}

func TestVertexStream(t *testing.T) {
	body := gzLines([]string{
		"0\tcom.example.www",
		"1\tcom.github",
		"2\torg.golang",
	})
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/manifest.paths.gz" {
			// manifest lists one absolute part URL so VertexStream can fetch it
			manifest := gzLines([]string{srv.URL + "/vertices/part-00000.txt.gz"})
			_, _ = w.Write(manifest)
			return
		}
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	h := NewHTTPClient(DefaultConfig())
	var got []VertexRecord
	err := VertexStream(context.Background(), h, srv.URL+"/manifest.paths.gz", func(v VertexRecord) error {
		got = append(got, v)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 vertices, got %d", len(got))
	}
	if got[0].ID != 0 || got[0].Host != "www.example.com" {
		t.Errorf("row 0: %+v", got[0])
	}
	if got[1].ID != 1 || got[1].Host != "github.com" {
		t.Errorf("row 1: %+v", got[1])
	}
	if got[2].ID != 2 || got[2].Host != "golang.org" {
		t.Errorf("row 2: %+v", got[2])
	}
}

func TestComputeEdgeDegrees(t *testing.T) {
	// 3 nodes: 0→1, 0→2, 1→2
	body := gzLines([]string{
		"0\t1",
		"0\t2",
		"1\t2",
	})
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/edges.paths.gz" {
			manifest := gzLines([]string{srv.URL + "/edges/part-00000.txt.gz"})
			_, _ = w.Write(manifest)
			return
		}
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	h := NewHTTPClient(DefaultConfig())
	inDeg, outDeg, err := ComputeEdgeDegrees(context.Background(), h, srv.URL+"/edges.paths.gz", 3)
	if err != nil {
		t.Fatal(err)
	}

	// in-degree: node 0 = 0, node 1 = 1 (from 0), node 2 = 2 (from 0 and 1)
	wantIn := []int32{0, 1, 2}
	// out-degree: node 0 = 2, node 1 = 1, node 2 = 0
	wantOut := []int32{2, 1, 0}
	for i := range wantIn {
		if inDeg[i] != wantIn[i] {
			t.Errorf("inDeg[%d] = %d, want %d", i, inDeg[i], wantIn[i])
		}
		if outDeg[i] != wantOut[i] {
			t.Errorf("outDeg[%d] = %d, want %d", i, outDeg[i], wantOut[i])
		}
	}
}

func TestParseWebGraphIDs(t *testing.T) {
	// A page that lists releases out of order and repeats one, with a stray
	// non-release href mixed in. parseWebGraphIDs should dedupe and sort newest
	// first regardless of page order.
	page := []byte(`
		<a href="https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2025-sep-oct-nov/index.html">older</a>
		<a href="https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2026-apr-may-jun/">newest</a>
		<a href="https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2026-jan-feb-mar/">mid</a>
		<a href="https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2026-apr-may-jun/domain/">newest dup</a>
		<a href="https://example.com/not-a-graph/">skip</a>
	`)
	got := parseWebGraphIDs(page)
	want := []string{
		"cc-main-2026-apr-may-jun",
		"cc-main-2026-jan-feb-mar",
		"cc-main-2025-sep-oct-nov",
	}
	if len(got) != len(want) {
		t.Fatalf("got %d ids %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("id[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestGraphRankOrdering(t *testing.T) {
	if graphRank("cc-main-2026-apr-may-jun") <= graphRank("cc-main-2026-jan-feb-mar") {
		t.Error("later quarter of the same year should rank higher")
	}
	if graphRank("cc-main-2026-jan-feb-mar") <= graphRank("cc-main-2025-oct-nov-dec") {
		t.Error("a new year should rank higher than the prior year")
	}
	if graphRank("garbage") != 0 {
		t.Error("an unparseable id should rank 0")
	}
}

func TestSelectDomainGraph(t *testing.T) {
	ids := []string{
		"cc-main-2026-apr-may-jun", // newest, but its domain table is not out yet
		"cc-main-2026-jan-feb-mar", // this one has the table
		"cc-main-2025-oct-nov-dec",
	}
	// The newest release 404s (size 0), the next one has a real table. Selection
	// should skip the empty newest and return the nearest release that exists.
	got, err := selectDomainGraph(ids, func(g WebGraph) (int64, error) {
		switch g.ID {
		case "cc-main-2026-apr-may-jun":
			return 0, nil
		case "cc-main-2026-jan-feb-mar":
			return 4096, nil
		default:
			return 8192, nil
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "cc-main-2026-jan-feb-mar" {
		t.Errorf("selected %q, want cc-main-2026-jan-feb-mar", got.ID)
	}
	if got.BaseURL != graphBaseURL+got.ID+"/" {
		t.Errorf("BaseURL = %q, not derived from id", got.BaseURL)
	}

	// When no release has a table, the last probe error is surfaced.
	_, err = selectDomainGraph(ids, func(WebGraph) (int64, error) {
		return 0, context.DeadlineExceeded
	})
	if err == nil {
		t.Error("want an error when no release has a domain-ranks table")
	}
}

func TestVertexReversal(t *testing.T) {
	cases := []struct{ rev, want string }{
		{"com.example.www", "www.example.com"},
		{"org.golang", "golang.org"},
		{"com.github", "github.com"},
	}
	for _, c := range cases {
		got := reverseHost(c.rev)
		if got != c.want {
			t.Errorf("reverseHost(%q) = %q, want %q", c.rev, got, c.want)
		}
	}
}

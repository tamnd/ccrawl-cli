package ccrawl

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// A trimmed sample of a real domain-ranks table: a header row whose every
// column name starts with '#', then rows with the trailing n_hosts column. The
// key (host_rev) is the fifth column, not the last.
const sampleRankTable = "" +
	"#harmonicc_pos\t#harmonicc_val\t#pr_pos\t#pr_val\t#host_rev\t#n_hosts\n" +
	"1\t3.1897818E7\t2\t0.0147521903\tcom.googleapis\t3477\n" +
	"2\t3.1860334E7\t3\t0.0098232944\tcom.facebook\t3670\n" +
	"3\t3.1388032E7\t1\t0.0157558065\tcom.google\t15136\n" +
	"54\t2.1460100E7\t134\t0.0001935125\tgov.nih\t900\n"

func rankTableServer(t *testing.T) (*httptest.Server, *HTTPClient) {
	t.Helper()
	var gzbuf bytes.Buffer
	zw := gzip.NewWriter(&gzbuf)
	if _, err := zw.Write([]byte(sampleRankTable)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	body := gzbuf.Bytes()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv, NewHTTPClient(DefaultConfig())
}

func TestRankTopParsesHostRevColumn(t *testing.T) {
	srv, h := rankTableServer(t)
	got, err := RankTop(context.Background(), h, srv.URL, "", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 rows, got %d (header row must be skipped)", len(got))
	}
	// Key is host_rev (col 4) reversed back to a normal domain, not n_hosts.
	if got[0].Key != "googleapis.com" {
		t.Errorf("Key = %q, want googleapis.com", got[0].Key)
	}
	if got[0].HarmonicPos != 1 || got[0].PageRankPos != 2 {
		t.Errorf("positions = (%d,%d), want (1,2)", got[0].HarmonicPos, got[0].PageRankPos)
	}
	if got[2].Key != "google.com" {
		t.Errorf("third Key = %q, want google.com", got[2].Key)
	}
}

func TestRankTopTLDFilter(t *testing.T) {
	srv, h := rankTableServer(t)
	got, err := RankTop(context.Background(), h, srv.URL, "gov", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != "nih.gov" {
		t.Fatalf("tld=gov filter = %+v, want one row nih.gov", got)
	}
}

func TestRankLookup(t *testing.T) {
	srv, h := rankTableServer(t)
	got, err := RankLookup(context.Background(), h, srv.URL, "google.com")
	if err != nil {
		t.Fatal(err)
	}
	if got.Key != "google.com" || got.HarmonicPos != 3 || got.PageRankPos != 1 {
		t.Errorf("lookup google.com = %+v", got)
	}
	if _, err := RankLookup(context.Background(), h, srv.URL, "nonesuch.example"); err == nil {
		t.Error("want not-found error for a domain absent from the table")
	}
}

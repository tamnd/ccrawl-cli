package ccrawl

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The API server had no tests. These cover the answers a caller acts on: what
// health says about a server that cannot serve, and that a missing store is a
// 503 rather than an empty result, which reads as "nothing found".

// serverFor builds an APIServer and returns a live test server for it.
func serverFor(t *testing.T, hosts HostStore, search SearchStore) *httptest.Server {
	t.Helper()
	api := NewAPIServer(ServeConfig{}, hosts, search)
	srv := httptest.NewServer(api.srv.Handler)
	t.Cleanup(srv.Close)
	return srv
}

func getJSON(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	res, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	var body map[string]any
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("GET %s returned %d and a body that is not JSON: %v", url, res.StatusCode, err)
	}
	return res.StatusCode, body
}

// A server with no stores answers health 200, because it is running, and has to
// say that it can serve neither hosts nor search. "status: ok" on its own would
// be true and useless.
func TestHealthReportsWhichStoresAreLoaded(t *testing.T) {
	srv := serverFor(t, nil, nil)
	code, body := getJSON(t, srv.URL+"/v2/health")
	if code != 200 {
		t.Fatalf("health returned %d", code)
	}
	if body["hosts"] != false || body["search"] != false {
		t.Fatalf("an empty server claims stores: %v", body)
	}

	full := serverFor(t, NewMemHostStore(nil), &IndexSearchStore{})
	code, body = getJSON(t, full.URL+"/v2/health")
	if code != 200 || body["hosts"] != true || body["search"] != true {
		t.Fatalf("a loaded server reported %d %v", code, body)
	}
}

func TestMissingStoresAnswer503(t *testing.T) {
	srv := serverFor(t, nil, nil)
	for _, path := range []string{"/v2/host/example.com", "/v2/hosts", "/v2/search?q=go"} {
		code, body := getJSON(t, srv.URL+path)
		if code != 503 {
			t.Errorf("GET %s returned %d, want 503", path, code)
		}
		if body["error"] == nil {
			t.Errorf("GET %s returned no error message: %v", path, body)
		}
	}
}

func TestHostEndpoints(t *testing.T) {
	store := NewMemHostStore([]HostRecord{
		{Host: "example.com", HarmonicPos: 1},
		{Host: "example.gov", HarmonicPos: 2},
		{Host: "other.com", HarmonicPos: 3},
	})
	srv := serverFor(t, store, nil)

	code, body := getJSON(t, srv.URL+"/v2/host/example.com")
	if code != 200 || body["host"] != "example.com" {
		t.Fatalf("host lookup returned %d %v", code, body)
	}

	if code, _ := getJSON(t, srv.URL+"/v2/host/nowhere.invalid"); code != 404 {
		t.Fatalf("a host that is not in the store returned %d, want 404", code)
	}

	res, err := http.Get(srv.URL + "/v2/hosts?tld=gov")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	var hosts []HostRecord
	if err := json.NewDecoder(res.Body).Decode(&hosts); err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 1 || hosts[0].Host != "example.gov" {
		t.Fatalf("the tld filter returned %v", hosts)
	}
}

func TestSearchEndpointNeedsAQuery(t *testing.T) {
	srv := serverFor(t, nil, &IndexSearchStore{})
	if code, _ := getJSON(t, srv.URL+"/v2/search"); code != 400 {
		t.Fatalf("a search with no q returned %d, want 400", code)
	}
}

// The search store is the same reader the CLI uses, so a round trip through the
// HTTP layer is worth one test: index two documents, ask for one.
func TestSearchEndpointRanksAndCapsResults(t *testing.T) {
	dir := t.TempDir()
	b, err := NewInvertedIndexBuilder(dir)
	if err != nil {
		t.Fatal(err)
	}
	forward := map[uint64]ForwardDoc{}
	for _, d := range []struct {
		url, text string
	}{
		{"https://a.test/", "widget widget widget review"},
		{"https://b.test/", "sprocket review"},
	} {
		id := DocumentID(d.url)
		b.Add(id, Tokenize(d.text))
		forward[id] = ForwardDoc{DocID: id, URL: d.url, Host: hostOf(d.url)}
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	idx, err := OpenIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = idx.Close() }()

	srv := serverFor(t, nil, NewIndexSearchStore(idx, forward))
	code, body := getJSON(t, srv.URL+"/v2/search?q=widget+review&k=1")
	if code != 200 {
		t.Fatalf("search returned %d: %v", code, body)
	}
	results, _ := body["results"].([]any)
	if len(results) != 1 {
		t.Fatalf("k=1 returned %d results: %v", len(results), body)
	}
	first, _ := results[0].(map[string]any)
	if first["url"] != "https://a.test/" {
		t.Fatalf("the document holding both terms did not rank first: %v", first)
	}
}

// hostOf is the test's own tiny URL host reader, so the test does not depend on
// the CLI package.
func hostOf(rawURL string) string {
	for _, p := range []string{"https://", "http://"} {
		if len(rawURL) > len(p) && rawURL[:len(p)] == p {
			rest := rawURL[len(p):]
			for i := range rest {
				if rest[i] == '/' {
					return rest[:i]
				}
			}
			return rest
		}
	}
	return rawURL
}

package ccrawl

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// The publisher is the part of a hundred day crawl that is hardest to test by
// reasoning, because everything that goes wrong with it goes wrong between two
// processes or between three machines. So these tests run a real hub that keeps
// state: the commit body is parsed, the files land in it, and the next call sees
// what the last one wrote. A test that stubbed the commit endpoint and asserted
// on the request would pass whether or not the ledger merge worked.

// recrawlHub is a small stateful HuggingFace hub: files by repo path, the
// endpoints the publisher touches, and nothing else.
type recrawlHub struct {
	t   *testing.T
	srv *httptest.Server

	mu      sync.Mutex
	files   map[string][]byte
	commits int
	// hold, when set, blocks inside the commit handler, which is how a test
	// makes two servers commit at the same instant on purpose.
	hold func(paths []string)
}

func newRecrawlHub(t *testing.T) *recrawlHub {
	t.Helper()
	h := &recrawlHub{t: t, files: map[string][]byte{}}
	h.srv = httptest.NewServer(http.HandlerFunc(h.route))
	t.Cleanup(h.srv.Close)

	prevEndpoint, prevBackoff := hfEndpoint, hfRetryBase
	hfEndpoint, hfRetryBase = h.srv.URL, time.Millisecond
	t.Cleanup(func() { hfEndpoint, hfRetryBase = prevEndpoint, prevBackoff })
	return h
}

func (h *recrawlHub) client() *HFClient {
	return &HFClient{token: "test-token", http: &http.Client{Timeout: 30 * time.Second}}
}

func (h *recrawlHub) route(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	switch {
	case strings.HasSuffix(p, "/api/repos/create"):
		hubJSON(w, map[string]string{"url": "ok"})
	case strings.Contains(p, "/preupload/"):
		h.preupload(w, r)
	case strings.Contains(p, "/paths-info/"):
		h.pathsInfo(w, r)
	case strings.Contains(p, "/commit/"):
		h.commit(w, r)
	case strings.HasPrefix(p, "/datasets/") && strings.Contains(p, "/resolve/main/"):
		h.resolve(w, p)
	case strings.HasPrefix(p, "/api/datasets/"):
		h.siblings(w)
	default:
		h.t.Errorf("hub: unexpected request %s %s", r.Method, p)
		w.WriteHeader(404)
	}
}

// preupload answers "regular" for everything, so the client inlines the bytes in
// the commit body and the hub ends up holding the real file contents. That is
// what makes the ledger merge testable at all.
func (h *recrawlHub) preupload(w http.ResponseWriter, r *http.Request) {
	var req hfPreuploadReq
	_ = json.NewDecoder(r.Body).Decode(&req)
	out := hfPreuploadResp{CommitOID: "deadbeef"}
	for _, f := range req.Files {
		out.Files = append(out.Files, struct {
			Path         string `json:"path"`
			UploadMode   string `json:"uploadMode"`
			ShouldIgnore bool   `json:"shouldIgnore"`
		}{Path: f.Path, UploadMode: "regular"})
	}
	hubJSON(w, out)
}

func (h *recrawlHub) pathsInfo(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Paths []string `json:"paths"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	h.mu.Lock()
	var out []pathInfoEntry
	for _, p := range req.Paths {
		if b, ok := h.files[p]; ok {
			out = append(out, pathInfoEntry{Path: p, Size: int64(len(b))})
		}
	}
	h.mu.Unlock()
	hubJSON(w, out)
}

func (h *recrawlHub) commit(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	staged := map[string][]byte{}
	var paths []string
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		var m struct {
			Key   string          `json:"key"`
			Value json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal([]byte(line), &m); err != nil || m.Key != "file" {
			continue
		}
		var v struct {
			Path    string `json:"path"`
			Content string `json:"content"`
		}
		_ = json.Unmarshal(m.Value, &v)
		raw, err := base64.StdEncoding.DecodeString(v.Content)
		if err != nil {
			h.t.Errorf("hub: %s is not base64: %v", v.Path, err)
		}
		staged[v.Path] = raw
		paths = append(paths, v.Path)
	}
	sort.Strings(paths)
	if h.hold != nil {
		h.hold(paths)
	}
	h.mu.Lock()
	for p, b := range staged {
		h.files[p] = b
	}
	h.commits++
	h.mu.Unlock()
	hubJSON(w, hfCommitResp{CommitURL: "https://hub.example/commit/abc", CommitOID: "abc"})
}

func (h *recrawlHub) resolve(w http.ResponseWriter, p string) {
	_, rest, _ := strings.Cut(p, "/resolve/main/")
	h.mu.Lock()
	b, ok := h.files[rest]
	h.mu.Unlock()
	if !ok {
		w.WriteHeader(404)
		return
	}
	_, _ = w.Write(b)
}

func (h *recrawlHub) siblings(w http.ResponseWriter) {
	h.mu.Lock()
	names := make([]string, 0, len(h.files))
	for p := range h.files {
		names = append(names, p)
	}
	h.mu.Unlock()
	sort.Strings(names)
	type sib struct {
		Filename string `json:"rfilename"`
	}
	sibs := make([]sib, len(names))
	for i, n := range names {
		sibs[i] = sib{Filename: n}
	}
	hubJSON(w, map[string]any{"siblings": sibs})
}

func (h *recrawlHub) paths() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.files))
	for p := range h.files {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func (h *recrawlHub) ledger(t *testing.T, repoPath string) []RecrawlStat {
	t.Helper()
	h.mu.Lock()
	b, ok := h.files[repoPath]
	h.mu.Unlock()
	if !ok {
		return nil
	}
	local := filepath.Join(t.TempDir(), "led.csv")
	if err := os.WriteFile(local, b, 0o644); err != nil {
		t.Fatal(err)
	}
	rows, err := ReadRecrawlStats(local)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// writeShards writes n sealed capture shards of rows rows each into dir, which
// is what recrawl run leaves behind for the publisher to pick up.
func writeShards(t *testing.T, dir, prefix string, n, rows int) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := range n {
		w, err := NewCaptureWriter(dir, fmt.Sprintf("%s-%d", prefix, i), 1<<40)
		if err != nil {
			t.Fatal(err)
		}
		for j := range rows {
			if err := w.Write(Capture{
				URL:    fmt.Sprintf("https://%s-%d.example/%d", prefix, i, j),
				Host:   fmt.Sprintf("%s-%d.example", prefix, i),
				Status: 200,
				Body:   []byte(strings.Repeat("x", 64)),
			}); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func testPublishCfg(dir, server string, shard, shards int) RecrawlPublishConfig {
	return RecrawlPublishConfig{
		Dir:         dir,
		Repo:        "open-index/ccrawl-recrawl-domains",
		Kind:        "domains",
		Server:      server,
		Shard:       shard,
		Shards:      shards,
		CommitEvery: 2,
		DoCommit:    true,
		Logf:        func(string, ...any) {},
	}
}

func TestPublishRecrawlCommitsShardsAndFreesDisk(t *testing.T) {
	hub := newRecrawlHub(t)
	dir := t.TempDir()
	writeShards(t, dir, "a", 3, 5)

	stat, err := PublishRecrawl(context.Background(), hub.client(), testPublishCfg(dir, "server1", 0, 3))
	if err != nil {
		t.Fatal(err)
	}
	if stat.Files != 3 || stat.Rows != 15 {
		t.Fatalf("published %d files %d rows, want 3 and 15", stat.Files, stat.Rows)
	}

	var shards int
	for _, p := range hub.paths() {
		if strings.HasPrefix(p, "data/") {
			shards++
			if !strings.HasPrefix(p, "data/server1-shard0of3-") {
				t.Errorf("shard %s does not name its server and slice", p)
			}
		}
	}
	if shards != 3 {
		t.Fatalf("hub holds %d shards, want 3", shards)
	}
	for _, want := range []string{"README.md", RecrawlLedgerPath("server1", 0, 3)} {
		if !slices.Contains(hub.paths(), want) {
			t.Errorf("the hub has no %s, so the dataset is not readable", want)
		}
	}

	// The point of committing as shards close is that a hundred day run does not
	// need a hundred days of disk.
	left, err := closedShards(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Errorf("%d shards left on disk after commit, want none", len(left))
	}

	rows := hub.ledger(t, RecrawlLedgerPath("server1", 0, 3))
	if len(rows) != 1 || rows[0].Files != 3 || rows[0].Rows != 15 {
		t.Fatalf("ledger = %+v, want one row of 3 files and 15 rows", rows)
	}
}

func TestPublishRecrawlSkipsAShardTheHubAlreadyHas(t *testing.T) {
	hub := newRecrawlHub(t)
	dir := t.TempDir()
	writeShards(t, dir, "a", 2, 4)
	cfg := testPublishCfg(dir, "server1", 0, 1)
	cfg.Keep = true

	if _, err := PublishRecrawl(context.Background(), hub.client(), cfg); err != nil {
		t.Fatal(err)
	}
	first := hub.paths()

	// Keep left the files behind, so a second run sees exactly what a run killed
	// between the commit landing and the local delete would see.
	cfg.Keep = false
	stat, err := PublishRecrawl(context.Background(), hub.client(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(hub.paths()); got != len(first) {
		t.Fatalf("the hub holds %d paths after the replay, want the same %d", got, len(first))
	}
	if stat.Files != 2 || stat.Rows != 8 {
		t.Fatalf("replayed run reports %d files %d rows, want 2 and 8 unchanged", stat.Files, stat.Rows)
	}
	if left, _ := closedShards(dir); len(left) != 0 {
		t.Errorf("%d duplicate shards left on disk, want them dropped", len(left))
	}
}

func TestPublishRecrawlResumesFromTheHubNotFromDisk(t *testing.T) {
	hub := newRecrawlHub(t)
	first := t.TempDir()
	writeShards(t, first, "a", 2, 6)
	cfg := testPublishCfg(first, "server1", 0, 1)
	if _, err := PublishRecrawl(context.Background(), hub.client(), cfg); err != nil {
		t.Fatal(err)
	}

	// A new machine, or the same one after its staging disk was wiped. Nothing
	// local says anything happened before.
	second := t.TempDir()
	writeShards(t, second, "b", 1, 6)
	cfg.Dir = second
	stat, err := PublishRecrawl(context.Background(), hub.client(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if stat.Files != 3 || stat.Rows != 18 {
		t.Fatalf("resumed run reports %d files %d rows, want 3 and 18 carried on from the hub", stat.Files, stat.Rows)
	}
	if rows := hub.ledger(t, RecrawlLedgerPath("server1", 0, 1)); len(rows) != 1 || rows[0].Files != 3 {
		t.Fatalf("ledger = %+v, want one row of 3 files", rows)
	}
}

func TestPublishRecrawlKeepsEveryServersLedgerUnderConcurrentCommits(t *testing.T) {
	hub := newRecrawlHub(t)
	const servers = 3

	// Every commit waits until all three are inside the handler, so the three
	// publishers really are writing at the same instant rather than being
	// serialized by luck. A shared stats.csv loses two thirds of its rows here.
	var mu sync.Mutex
	gate := make(chan struct{})
	arrived := 0
	hub.hold = func([]string) {
		mu.Lock()
		arrived++
		if arrived == servers {
			close(gate)
		}
		mu.Unlock()
		<-gate
	}

	dirs := make([]string, servers)
	for i := range dirs {
		dirs[i] = t.TempDir()
		writeShards(t, dirs[i], fmt.Sprintf("s%d", i), 2, 4)
	}

	var wg sync.WaitGroup
	errs := make([]error, servers)
	for i := range servers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cfg := testPublishCfg(dirs[i], fmt.Sprintf("server%d", i+1), i, servers)
			cfg.CommitEvery = 2
			_, errs[i] = PublishRecrawl(context.Background(), hub.client(), cfg)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("server%d: %v", i+1, err)
		}
	}

	var all []RecrawlStat
	for i := range servers {
		p := RecrawlLedgerPath(fmt.Sprintf("server%d", i+1), i, servers)
		rows := hub.ledger(t, p)
		if len(rows) != 1 {
			t.Fatalf("%s holds %d rows, want exactly the one its server wrote", p, len(rows))
		}
		if rows[0].Files != 2 || rows[0].Rows != 8 {
			t.Errorf("%s = %+v, want 2 files and 8 rows", p, rows[0])
		}
		all = append(all, rows...)
	}
	total := TotalRecrawlStats(MergeRecrawlStats(all))
	if total.Servers != servers || total.Files != servers*2 || total.Rows != int64(servers*8) {
		t.Fatalf("fleet totals = %+v, want %d servers, %d files, %d rows", total, servers, servers*2, servers*8)
	}
	var shards int
	for _, p := range hub.paths() {
		if strings.HasPrefix(p, "data/") {
			shards++
		}
	}
	if shards != servers*2 {
		t.Fatalf("the hub holds %d shards, want %d, so a concurrent commit dropped one", shards, servers*2)
	}
}

func TestPublishRecrawlReportsWorkListPosition(t *testing.T) {
	hub := newRecrawlHub(t)
	dir := t.TempDir()
	writeShards(t, dir, "a", 1, 3)
	state := filepath.Join(t.TempDir(), "recrawl.json")
	if err := (Checkpoint{Part: 4, Row: 9000, Fetched: 9000}).Save(state); err != nil {
		t.Fatal(err)
	}
	cfg := testPublishCfg(dir, "server1", 0, 1)
	cfg.StatePath = state

	stat, err := PublishRecrawl(context.Background(), hub.client(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if stat.Part != 4 || stat.Row != 9000 || stat.Done {
		t.Fatalf("stat position = part %d row %d done %v, want part 4 row 9000 unfinished", stat.Part, stat.Row, stat.Done)
	}
	card := hub.card(t)
	if !strings.Contains(card, "part 4 row 9,000") {
		t.Errorf("the card does not state the position in the work list:\n%s", card)
	}
}

func (h *recrawlHub) card(t *testing.T) string {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	b, ok := h.files["README.md"]
	if !ok {
		t.Fatal("the hub has no README.md")
	}
	return string(b)
}

func TestPublishRecrawlWatchStopsWhenTheCrawlIsDone(t *testing.T) {
	hub := newRecrawlHub(t)
	dir := t.TempDir()
	writeShards(t, dir, "a", 1, 2)
	state := filepath.Join(t.TempDir(), "recrawl.json")
	if err := (Checkpoint{Part: 2, Row: 10, Done: true}).Save(state); err != nil {
		t.Fatal(err)
	}
	cfg := testPublishCfg(dir, "server1", 0, 1)
	cfg.StatePath = state
	cfg.Poll = time.Millisecond

	done := make(chan error, 1)
	go func() {
		_, err := PublishRecrawl(context.Background(), hub.client(), cfg)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the watcher never stopped, so a finished crawl leaves a publisher polling forever")
	}
}

func TestPublishRecrawlRejectsAConfigThatWouldProduceAnUnreadableRepo(t *testing.T) {
	base := testPublishCfg(t.TempDir(), "server1", 0, 3)
	cases := []struct {
		name string
		edit func(*RecrawlPublishConfig)
	}{
		{"no directory", func(c *RecrawlPublishConfig) { c.Dir = "" }},
		{"no server", func(c *RecrawlPublishConfig) { c.Server = " " }},
		{"repo is not org/name", func(c *RecrawlPublishConfig) { c.Repo = "ccrawl-recrawl" }},
		{"kind is neither", func(c *RecrawlPublishConfig) { c.Kind = "news" }},
		{"shard outside the fleet", func(c *RecrawlPublishConfig) { c.Shard = 3 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.edit(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("the config was accepted")
			}
		})
	}
}

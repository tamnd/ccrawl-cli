package ccrawl

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeHub is a stand-in for the HuggingFace Hub that speaks enough of the commit
// protocol to drive the whole conversation: preupload, LFS batch, part uploads,
// verify, and the NDJSON commit. It records what it received so a test can
// assert on the bytes that actually arrived.
type fakeHub struct {
	t   *testing.T
	srv *httptest.Server

	mu sync.Mutex
	// stored maps an LFS oid to the bytes the client uploaded, reassembled
	// from however many parts it chose to send.
	stored map[string][]byte
	parts  map[string]map[int][]byte
	// have is the set of oids the hub already holds, which get no upload
	// action at all.
	have map[string]bool
	// commit is the parsed NDJSON body of the final commit.
	commit []map[string]json.RawMessage
	// chunkSize, when non-zero, makes the hub hand out multipart uploads.
	chunkSize int64
	// regular is the set of repo paths the hub wants inline rather than in LFS.
	regular map[string]bool
	// ignore is the set of repo paths the hub claims gitignore matched.
	ignore map[string]bool
	// verified counts verify calls, which must happen once per uploaded object.
	verified int
	// failParts maps a part number to how many times it should fail before
	// succeeding, so the retry path gets exercised.
	failParts map[int]int
}

func newFakeHub(t *testing.T) *fakeHub {
	h := &fakeHub{
		t:         t,
		stored:    map[string][]byte{},
		parts:     map[string]map[int][]byte{},
		have:      map[string]bool{},
		regular:   map[string]bool{},
		ignore:    map[string]bool{},
		failParts: map[int]int{},
	}
	h.srv = httptest.NewServer(http.HandlerFunc(h.route))
	t.Cleanup(h.srv.Close)

	prev := hfEndpoint
	hfEndpoint = h.srv.URL
	t.Cleanup(func() { hfEndpoint = prev })
	return h
}

func (h *fakeHub) route(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.Contains(r.URL.Path, "/preupload/"):
		h.handlePreupload(w, r)
	case strings.HasSuffix(r.URL.Path, "/info/lfs/objects/batch"):
		h.handleBatch(w, r)
	case strings.HasPrefix(r.URL.Path, "/upload/"):
		h.handleUpload(w, r)
	case strings.HasPrefix(r.URL.Path, "/verify/"):
		h.handleVerify(w, r)
	case strings.Contains(r.URL.Path, "/commit/"):
		h.handleCommit(w, r)
	default:
		h.t.Errorf("fake hub: unexpected request %s %s", r.Method, r.URL.Path)
		w.WriteHeader(404)
	}
}

func (h *fakeHub) handlePreupload(w http.ResponseWriter, r *http.Request) {
	if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
		h.t.Errorf("preupload auth = %q", got)
	}
	var req hfPreuploadReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.t.Fatalf("preupload decode: %v", err)
	}
	out := hfPreuploadResp{CommitOID: "deadbeef"}
	for _, f := range req.Files {
		if _, err := base64.StdEncoding.DecodeString(f.Sample); err != nil {
			h.t.Errorf("preupload sample for %s is not base64: %v", f.Path, err)
		}
		mode := "lfs"
		if h.regular[f.Path] {
			mode = "regular"
		}
		out.Files = append(out.Files, struct {
			Path         string `json:"path"`
			UploadMode   string `json:"uploadMode"`
			ShouldIgnore bool   `json:"shouldIgnore"`
		}{Path: f.Path, UploadMode: mode, ShouldIgnore: h.ignore[f.Path]})
	}
	hubJSON(w, out)
}

func (h *fakeHub) handleBatch(w http.ResponseWriter, r *http.Request) {
	if got := r.Header.Get("Content-Type"); got != "application/vnd.git-lfs+json" {
		h.t.Errorf("lfs batch content-type = %q", got)
	}
	var req struct {
		Operation string `json:"operation"`
		Objects   []struct {
			OID  string `json:"oid"`
			Size int64  `json:"size"`
		} `json:"objects"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.t.Fatalf("lfs batch decode: %v", err)
	}

	var out lfsBatchResp
	for _, o := range req.Objects {
		obj := lfsObject{OID: o.OID, Size: o.Size}
		if !h.have[o.OID] {
			obj.Actions = &struct {
				Upload *lfsAction `json:"upload"`
				Verify *lfsAction `json:"verify"`
			}{
				Upload: &lfsAction{Href: h.srv.URL + "/upload/" + o.OID, Header: map[string]string{}},
				Verify: &lfsAction{Href: h.srv.URL + "/verify/" + o.OID},
			}
			if h.chunkSize > 0 && o.Size > h.chunkSize {
				n := int((o.Size + h.chunkSize - 1) / h.chunkSize)
				obj.Actions.Upload.Header["chunk_size"] = fmt.Sprint(h.chunkSize)
				for i := 1; i <= n; i++ {
					obj.Actions.Upload.Header[fmt.Sprint(i)] = fmt.Sprintf("%s/upload/%s?part=%d", h.srv.URL, o.OID, i)
				}
			}
		}
		out.Objects = append(out.Objects, obj)
	}
	hubJSON(w, out)
}

// handleUpload takes both the part PUTs and the multipart completion POST, since
// the hub hands out the same base href for both.
func (h *fakeHub) handleUpload(w http.ResponseWriter, r *http.Request) {
	oid := strings.TrimPrefix(r.URL.Path, "/upload/")
	body, _ := io.ReadAll(r.Body)

	if r.Method == "POST" {
		// Completion: parts must be listed in order with the ETags we handed out.
		var req struct {
			OID   string `json:"oid"`
			Parts []struct {
				PartNumber int    `json:"partNumber"`
				ETag       string `json:"etag"`
			} `json:"parts"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			h.t.Fatalf("completion decode: %v", err)
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		var whole []byte
		for i, p := range req.Parts {
			if p.PartNumber != i+1 {
				h.t.Errorf("completion parts out of order: index %d has partNumber %d", i, p.PartNumber)
			}
			want := fmt.Sprintf(`"etag-%s-%d"`, oid[:8], p.PartNumber)
			if p.ETag != want {
				h.t.Errorf("part %d etag = %q, want %q", p.PartNumber, p.ETag, want)
			}
			whole = append(whole, h.parts[oid][p.PartNumber]...)
		}
		h.stored[oid] = whole
		hubJSON(w, map[string]any{"ok": true})
		return
	}

	partNum := 0
	if q := r.URL.Query().Get("part"); q != "" {
		n, err := strconv.Atoi(q)
		if err != nil {
			h.t.Fatalf("bad part number %q: %v", q, err)
		}
		partNum = n
	}

	h.mu.Lock()
	if left := h.failParts[partNum]; left > 0 {
		h.failParts[partNum] = left - 1
		h.mu.Unlock()
		w.WriteHeader(503)
		return
	}
	if partNum == 0 {
		h.stored[oid] = body
	} else {
		if h.parts[oid] == nil {
			h.parts[oid] = map[int][]byte{}
		}
		h.parts[oid][partNum] = body
	}
	h.mu.Unlock()

	if partNum > 0 {
		w.Header().Set("ETag", fmt.Sprintf(`"etag-%s-%d"`, oid[:8], partNum))
	}
	w.WriteHeader(200)
}

func (h *fakeHub) handleVerify(w http.ResponseWriter, r *http.Request) {
	if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
		h.t.Errorf("verify auth = %q", got)
	}
	h.mu.Lock()
	h.verified++
	h.mu.Unlock()
	w.WriteHeader(200)
}

func (h *fakeHub) handleCommit(w http.ResponseWriter, r *http.Request) {
	if got := r.Header.Get("Content-Type"); got != "application/x-ndjson" {
		h.t.Errorf("commit content-type = %q", got)
	}
	body, _ := io.ReadAll(r.Body)
	for line := range strings.SplitSeq(strings.TrimSpace(string(body)), "\n") {
		var m map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			h.t.Fatalf("commit line %q: %v", line, err)
		}
		h.commit = append(h.commit, m)
	}
	hubJSON(w, hfCommitResp{CommitURL: "https://hub.example/commit/abc", CommitOID: "abc"})
}

func hubJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// key returns the value of the NDJSON line with the given key, or nil.
func (h *fakeHub) line(key string) map[string]any {
	for _, m := range h.commit {
		var k string
		_ = json.Unmarshal(m["key"], &k)
		if k != key {
			continue
		}
		var v map[string]any
		_ = json.Unmarshal(m["value"], &v)
		return v
	}
	return nil
}

func (h *fakeHub) lines(key string) []map[string]any {
	var out []map[string]any
	for _, m := range h.commit {
		var k string
		_ = json.Unmarshal(m["key"], &k)
		if k != key {
			continue
		}
		var v map[string]any
		_ = json.Unmarshal(m["value"], &v)
		out = append(out, v)
	}
	return out
}

func testClient() *HFClient {
	return &HFClient{token: "test-token", http: &http.Client{Timeout: 30 * time.Second}}
}

// writeTestFile writes n deterministic bytes and returns the path and sha256.
func writeTestFile(t *testing.T, dir, name string, n int) (string, string) {
	t.Helper()
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = byte(i%251) ^ name[0]
	}
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(buf)
	return p, hex.EncodeToString(sum[:])
}

func TestCreateCommitGoSinglePart(t *testing.T) {
	hub := newFakeHub(t)
	dir := t.TempDir()
	pathA, sumA := writeTestFile(t, dir, "a.parquet", 4096)
	pathB, sumB := writeTestFile(t, dir, "b.parquet", 1024)

	url, err := testClient().createCommitGo(t.Context(), "org/ds", "add two shards", []HFOperation{
		{LocalPath: pathA, PathInRepo: "data/a.parquet"},
		{LocalPath: pathB, PathInRepo: "data/b.parquet"},
	})
	if err != nil {
		t.Fatalf("createCommitGo: %v", err)
	}
	if url != "https://hub.example/commit/abc" {
		t.Errorf("commit url = %q", url)
	}

	// Both objects reached storage with their bytes intact.
	for name, sum := range map[string]string{"a": sumA, "b": sumB} {
		got, ok := hub.stored[sum]
		if !ok {
			t.Fatalf("%s: oid %s never uploaded", name, sum)
		}
		if h := sha256.Sum256(got); hex.EncodeToString(h[:]) != sum {
			t.Errorf("%s: uploaded bytes hash to %x, want %s", name, h, sum)
		}
	}
	if hub.verified != 2 {
		t.Errorf("verify calls = %d, want 2", hub.verified)
	}

	// The commit body names them by hash, not by content.
	if got := hub.line("header")["summary"]; got != "add two shards" {
		t.Errorf("summary = %v", got)
	}
	lfs := hub.lines("lfsFile")
	if len(lfs) != 2 {
		t.Fatalf("lfsFile lines = %d, want 2", len(lfs))
	}
	for _, l := range lfs {
		if l["algo"] != "sha256" {
			t.Errorf("algo = %v", l["algo"])
		}
		if l["oid"] != sumA && l["oid"] != sumB {
			t.Errorf("unexpected oid %v", l["oid"])
		}
	}
	if n := len(hub.lines("file")); n != 0 {
		t.Errorf("inline file lines = %d, want 0", n)
	}
}

func TestCreateCommitGoMultipart(t *testing.T) {
	hub := newFakeHub(t)
	hub.chunkSize = 1000
	// One part fails twice before succeeding, so the retry path has to work for
	// the reassembled object to hash correctly.
	hub.failParts[3] = 2

	dir := t.TempDir()
	path, sum := writeTestFile(t, dir, "big.parquet", 4500) // 5 parts, last one short

	if _, err := testClient().createCommitGo(t.Context(), "org/ds", "add big shard", []HFOperation{
		{LocalPath: path, PathInRepo: "data/big.parquet"},
	}); err != nil {
		t.Fatalf("createCommitGo: %v", err)
	}

	got, ok := hub.stored[sum]
	if !ok {
		t.Fatalf("oid %s never completed", sum)
	}
	if len(got) != 4500 {
		t.Fatalf("reassembled %d bytes, want 4500", len(got))
	}
	if h := sha256.Sum256(got); hex.EncodeToString(h[:]) != sum {
		t.Errorf("reassembled bytes hash to %x, want %s", h, sum)
	}
	if n := len(hub.parts[sum]); n != 5 {
		t.Errorf("parts received = %d, want 5", n)
	}
}

// A file the hub already holds gets no upload action, and must still appear in
// the commit. This is the dedup path that makes a re-run of a finished shard
// nearly free.
func TestCreateCommitGoSkipsExistingObject(t *testing.T) {
	hub := newFakeHub(t)
	dir := t.TempDir()
	path, sum := writeTestFile(t, dir, "known.parquet", 2048)
	hub.have[sum] = true

	if _, err := testClient().createCommitGo(t.Context(), "org/ds", "recommit", []HFOperation{
		{LocalPath: path, PathInRepo: "data/known.parquet"},
	}); err != nil {
		t.Fatalf("createCommitGo: %v", err)
	}
	if len(hub.stored) != 0 {
		t.Errorf("uploaded %d objects, want 0", len(hub.stored))
	}
	if hub.verified != 0 {
		t.Errorf("verify calls = %d, want 0", hub.verified)
	}
	lfs := hub.lines("lfsFile")
	if len(lfs) != 1 || lfs[0]["oid"] != sum {
		t.Errorf("lfsFile lines = %v, want one naming %s", lfs, sum)
	}
}

// Small files come back as "regular" and their bytes ride inside the commit
// body, never touching the LFS endpoints.
func TestCreateCommitGoRegularFileInline(t *testing.T) {
	hub := newFakeHub(t)
	dir := t.TempDir()
	readme := filepath.Join(dir, "README.md")
	if err := os.WriteFile(readme, []byte("# dataset\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	hub.regular["README.md"] = true

	if _, err := testClient().createCommitGo(t.Context(), "org/ds", "add readme", []HFOperation{
		{LocalPath: readme, PathInRepo: "README.md"},
	}); err != nil {
		t.Fatalf("createCommitGo: %v", err)
	}
	if len(hub.stored) != 0 {
		t.Errorf("regular file went through LFS")
	}
	files := hub.lines("file")
	if len(files) != 1 {
		t.Fatalf("file lines = %d, want 1", len(files))
	}
	if files[0]["encoding"] != "base64" {
		t.Errorf("encoding = %v", files[0]["encoding"])
	}
	content, err := base64.StdEncoding.DecodeString(files[0]["content"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "# dataset\n" {
		t.Errorf("inline content = %q", content)
	}
}

// A file gitignore matched is dropped from the commit rather than uploaded.
func TestCreateCommitGoHonorsShouldIgnore(t *testing.T) {
	hub := newFakeHub(t)
	dir := t.TempDir()
	pathA, _ := writeTestFile(t, dir, "a.parquet", 512)
	pathB, sumB := writeTestFile(t, dir, "b.parquet", 512)
	hub.ignore["data/a.parquet"] = true

	if _, err := testClient().createCommitGo(t.Context(), "org/ds", "one ignored", []HFOperation{
		{LocalPath: pathA, PathInRepo: "data/a.parquet"},
		{LocalPath: pathB, PathInRepo: "data/b.parquet"},
	}); err != nil {
		t.Fatalf("createCommitGo: %v", err)
	}
	lfs := hub.lines("lfsFile")
	if len(lfs) != 1 || lfs[0]["oid"] != sumB {
		t.Errorf("lfsFile lines = %v, want only %s", lfs, sumB)
	}
}

// A local file that vanished between being written and being committed is
// skipped with a warning, matching what the ledger expects.
func TestCreateCommitGoSkipsMissingFile(t *testing.T) {
	hub := newFakeHub(t)
	dir := t.TempDir()
	path, sum := writeTestFile(t, dir, "here.parquet", 512)

	if _, err := testClient().createCommitGo(t.Context(), "org/ds", "one missing", []HFOperation{
		{LocalPath: filepath.Join(dir, "gone.parquet"), PathInRepo: "data/gone.parquet"},
		{LocalPath: path, PathInRepo: "data/here.parquet"},
	}); err != nil {
		t.Fatalf("createCommitGo: %v", err)
	}
	lfs := hub.lines("lfsFile")
	if len(lfs) != 1 || lfs[0]["oid"] != sum {
		t.Errorf("lfsFile lines = %v, want only %s", lfs, sum)
	}
}

func TestCreateCommitGoNoToken(t *testing.T) {
	c := &HFClient{http: &http.Client{}}
	_, err := c.createCommitGo(context.Background(), "org/ds", "x", []HFOperation{{LocalPath: "/tmp/x", PathInRepo: "x"}})
	if !errors.Is(err, ErrHFAuth) {
		t.Errorf("err = %v, want ErrHFAuth", err)
	}
}

// The typed errors are what a retry loop branches on, so each status has to map
// to the right sentinel.
func TestClassifyHF(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   error
	}{
		{401, "invalid token", ErrHFAuth},
		{403, "you do not have write access", ErrHFAuth},
		{403, "storage quota exceeded for this repo", ErrHFQuota},
		{403, "Private repository storage limit reached, please upgrade your plan", ErrHFQuota},
		{507, "no space", ErrHFQuota},
		{409, "branch moved", ErrHFConflict},
		{412, "precondition failed", ErrHFConflict},
		{429, "slow down", ErrHFRateLimited},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%d", tc.status), func(t *testing.T) {
			resp := &http.Response{
				StatusCode: tc.status,
				Header:     http.Header{"Retry-After": []string{"30"}},
				Body:       io.NopCloser(strings.NewReader(tc.body)),
			}
			err := classifyHF("test", resp)
			if !errors.Is(err, tc.want) {
				t.Fatalf("classifyHF(%d, %q) = %v, want %v", tc.status, tc.body, err, tc.want)
			}
			if tc.status == 429 {
				var rl *RateLimitError
				if !errors.As(err, &rl) {
					t.Fatal("429 must still be a *RateLimitError for existing callers")
				}
				if rl.RetryAfter != 30*time.Second {
					t.Errorf("RetryAfter = %s, want 30s", rl.RetryAfter)
				}
			}
		})
	}
}

// A permission failure must not be retried; burning four attempts on a bad token
// only delays the error.
func TestCreateCommitGoAuthFailsFast(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(403)
		_, _ = w.Write([]byte("you do not have write access"))
	}))
	defer srv.Close()
	prev := hfEndpoint
	hfEndpoint = srv.URL
	defer func() { hfEndpoint = prev }()

	dir := t.TempDir()
	path, _ := writeTestFile(t, dir, "a.parquet", 128)
	_, err := testClient().createCommitGo(t.Context(), "org/ds", "x", []HFOperation{
		{LocalPath: path, PathInRepo: "data/a.parquet"},
	})
	if !errors.Is(err, ErrHFAuth) {
		t.Fatalf("err = %v, want ErrHFAuth", err)
	}
	if calls != 1 {
		t.Errorf("preupload attempts = %d, want 1", calls)
	}
}

func TestSortedPartURLs(t *testing.T) {
	header := map[string]string{
		"chunk_size": "1000",
		"10":         "u10",
		"2":          "u2",
		"1":          "u1",
		"x-amz-acl":  "private",
	}
	got := sortedPartURLs(header)
	want := []string{"u1", "u2", "u10"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestSplitCommitMessage(t *testing.T) {
	summary, desc := splitCommitMessage("add shard 42\n\n90000 documents")
	if summary != "add shard 42" {
		t.Errorf("summary = %q", summary)
	}
	if desc != "90000 documents" {
		t.Errorf("description = %q", desc)
	}
	summary, desc = splitCommitMessage("just a summary")
	if summary != "just a summary" || desc != "" {
		t.Errorf("got %q / %q", summary, desc)
	}
}

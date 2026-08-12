package ccrawl

// Pure-Go implementation of a HuggingFace Hub commit. This is the path that
// replaces `uv run hf_commit.py`, so publishing a dataset needs nothing on the
// box but the ccrawl binary.
//
// The hub's commit protocol has three steps and they have to happen in order:
//
//  1. preupload: tell the API the path, size, and first 512 bytes of every file.
//     It answers with an upload mode per file. "regular" means the bytes ride
//     along inside the commit body; "lfs" means they go to object storage first.
//     It can also say shouldIgnore, which means gitignore matched and the file is
//     not part of the commit at all.
//  2. LFS: ask the batch endpoint where to put each large object. The answer is
//     either a single PUT url or a set of numbered part urls plus a chunk size,
//     which is S3 multipart. Parts are PUT in any order but must be completed in
//     order with their ETags. An object already in storage comes back with no
//     actions at all, which is the dedup case and costs nothing.
//  3. commit: POST an NDJSON body, one line per operation, with the LFS files
//     referenced by their sha256 rather than their bytes.
//
// The whole thing is one HTTP conversation, so every failure is an HTTP status
// we can classify instead of a subprocess exit code.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

// hfEndpoint is the hub base URL. It is a variable so the tests can point the
// whole conversation at an httptest server.
var hfEndpoint = "https://huggingface.co"

// hfRetryBase scales the wait between hub retries: the nth retry waits n squared
// times this. Five seconds is the right first wait against the real hub, which a
// test cannot afford to sit through, so it is a variable rather than a constant.
var hfRetryBase = 5 * time.Second

// Typed errors for the failure modes a caller needs to tell apart. A retry loop
// should back off on ErrHFRateLimited, give up immediately on ErrHFAuth and
// ErrHFQuota, and re-read the repo state on ErrHFConflict.
var (
	// ErrHFRateLimited means the hub asked us to slow down (HTTP 429).
	ErrHFRateLimited = errors.New("huggingface: rate limited")
	// ErrHFAuth means the token is missing, wrong, or not allowed to write here.
	ErrHFAuth = errors.New("huggingface: authentication or permission failure")
	// ErrHFQuota means the account or repo is out of storage.
	ErrHFQuota = errors.New("huggingface: storage quota exceeded")
	// ErrHFConflict means the branch moved under us and the commit did not apply.
	ErrHFConflict = errors.New("huggingface: commit conflict")
)

// hfHTTPError carries the status and the response body snippet alongside one of
// the sentinel errors above, so a log line says what the hub actually said.
type hfHTTPError struct {
	Op     string
	Status int
	Body   string
	kind   error // one of the sentinels, or nil for an unclassified failure
}

func (e *hfHTTPError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("%s: HTTP %d", e.Op, e.Status)
	}
	return fmt.Sprintf("%s: HTTP %d: %s", e.Op, e.Status, e.Body)
}

func (e *hfHTTPError) Unwrap() error { return e.kind }

// retryable reports whether the request is worth sending again. Rate limits and
// 5xx are the hub or its object store being busy; everything else is a decision.
func (e *hfHTTPError) retryable() bool {
	return e.Status == 429 || e.Status >= 500
}

// classifyHF maps an HTTP response to a typed error. The body is read here
// because the hub puts its reason in the body, not the status.
func classifyHF(op string, resp *http.Response) error {
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	body := strings.TrimSpace(string(snippet))
	e := &hfHTTPError{Op: op, Status: resp.StatusCode, Body: body}

	switch resp.StatusCode {
	case 429:
		after, _ := parseRetryAfter(resp.Header.Get("Retry-After"))
		return &RateLimitError{RetryAfter: after, Msg: body}
	case 401:
		e.kind = ErrHFAuth
	case 403:
		// The hub returns 403 for both "your token cannot do this" and "you are
		// out of space", and only the body tells them apart. It words the second
		// one as either a quota or a storage limit depending on the endpoint.
		lower := strings.ToLower(body)
		if strings.Contains(lower, "quota") || strings.Contains(lower, "storage limit") {
			e.kind = ErrHFQuota
		} else {
			e.kind = ErrHFAuth
		}
	case 409, 412:
		e.kind = ErrHFConflict
	case 507:
		e.kind = ErrHFQuota
	}
	return e
}

// ── file metadata ─────────────────────────────────────────────────────────────

// hfFile is one local file measured for upload: its sha256 (the LFS object ID),
// its size, and the first 512 bytes the preupload endpoint wants as a sample.
type hfFile struct {
	op     HFOperation
	size   int64
	sha256 string
	sample []byte
	mode   string // "lfs" or "regular", filled in by preupload
	ignore bool
}

// measureFile hashes a file in one pass and keeps the head for the sample. The
// hash is the LFS object ID, so this pass is not optional even for a file the
// hub already has.
func measureFile(op HFOperation) (*hfFile, error) {
	f, err := os.Open(op.LocalPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	h := sha256.New()
	sample := make([]byte, 0, 512)
	buf := make([]byte, 1<<20)
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
			if len(sample) < 512 {
				sample = append(sample, buf[:min(n, 512-len(sample))]...)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return nil, rerr
		}
	}
	return &hfFile{
		op:     op,
		size:   info.Size(),
		sha256: hex.EncodeToString(h.Sum(nil)),
		sample: sample,
	}, nil
}

// ── HTTP plumbing ─────────────────────────────────────────────────────────────

// hfRetries is how many times a transient hub or object-store failure is retried
// inside one commit. The caller retries the whole commit on top of this, so this
// only has to survive a blip, not an outage.
const hfRetries = 4

// doJSON sends a request with the hub auth header and decodes a JSON response
// into out. Transient failures are retried with backoff; anything else comes
// back as a typed error on the first try.
func (c *HFClient) doJSON(ctx context.Context, op, method, url string, body []byte, headers map[string]string, out any) error {
	var lastErr error
	for attempt := range hfRetries {
		if attempt > 0 {
			if err := hfBackoff(ctx, attempt, lastErr); err != nil {
				return err
			}
		}
		var rdr io.Reader
		if body != nil {
			rdr = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, url, rdr)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("User-Agent", hfUserAgent)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("%s: %w", op, err)
			continue
		}
		if resp.StatusCode >= 400 {
			herr := classifyHF(op, resp)
			_ = resp.Body.Close()
			// A rate limit is not an hfHTTPError, and it is always worth
			// waiting out, so only a non-retryable hfHTTPError stops the loop.
			var he *hfHTTPError
			if errors.As(herr, &he) && !he.retryable() {
				return herr
			}
			lastErr = herr
			continue
		}
		if out == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			return nil
		}
		derr := json.NewDecoder(resp.Body).Decode(out)
		_ = resp.Body.Close()
		if derr != nil {
			return fmt.Errorf("%s: decode response: %w", op, derr)
		}
		return nil
	}
	return fmt.Errorf("%s after %d attempts: %w", op, hfRetries, lastErr)
}

// hfBackoff waits before a retry, honouring a Retry-After the hub sent.
func hfBackoff(ctx context.Context, attempt int, lastErr error) error {
	wait := time.Duration(1<<attempt) * time.Second
	var rl *RateLimitError
	if errors.As(lastErr, &rl) && rl.RetryAfter > wait {
		wait = rl.RetryAfter
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(wait):
		return nil
	}
}

const hfUserAgent = "ccrawl-cli (+https://github.com/tamnd/ccrawl-cli)"

// lfsJSONHeaders is the content type the LFS batch and completion endpoints
// require. Sending plain application/json gets a 406.
var lfsJSONHeaders = map[string]string{
	"Accept":       "application/vnd.git-lfs+json",
	"Content-Type": "application/vnd.git-lfs+json",
}

// ── step 1: preupload ─────────────────────────────────────────────────────────

type hfPreuploadReq struct {
	Files []hfPreuploadReqFile `json:"files"`
}

type hfPreuploadReqFile struct {
	Path   string `json:"path"`
	Sample string `json:"sample"`
	Size   int64  `json:"size"`
}

type hfPreuploadResp struct {
	Files []struct {
		Path         string `json:"path"`
		UploadMode   string `json:"uploadMode"`
		ShouldIgnore bool   `json:"shouldIgnore"`
	} `json:"files"`
	CommitOID string `json:"commitOid"`
}

// preupload asks the hub how each file should be sent. It is also the call that
// applies the repo's .gitattributes, so the answer is authoritative: a file the
// hub calls "regular" must not go through LFS and vice versa.
func (c *HFClient) preupload(ctx context.Context, repoID, revision string, files []*hfFile) error {
	// The endpoint takes a bounded batch, and a commit of a few hundred shards
	// would otherwise send one enormous body.
	const batch = 100
	byPath := make(map[string]*hfFile, len(files))
	for _, f := range files {
		byPath[f.op.PathInRepo] = f
	}
	url := fmt.Sprintf("%s/api/datasets/%s/preupload/%s", hfEndpoint, repoID, revision)

	for start := 0; start < len(files); start += batch {
		end := min(start+batch, len(files))
		req := hfPreuploadReq{Files: make([]hfPreuploadReqFile, 0, end-start)}
		for _, f := range files[start:end] {
			req.Files = append(req.Files, hfPreuploadReqFile{
				Path:   f.op.PathInRepo,
				Sample: base64.StdEncoding.EncodeToString(f.sample),
				Size:   f.size,
			})
		}
		body, err := json.Marshal(req)
		if err != nil {
			return err
		}
		var resp hfPreuploadResp
		if err := c.doJSON(ctx, "preupload", "POST", url, body,
			map[string]string{"Content-Type": "application/json"}, &resp); err != nil {
			return err
		}
		for _, got := range resp.Files {
			f, ok := byPath[got.Path]
			if !ok {
				continue
			}
			f.mode = got.UploadMode
			f.ignore = got.ShouldIgnore
		}
	}

	// A file the hub did not answer for would silently go missing from the
	// commit, so refuse rather than publish a hole in the dataset.
	for _, f := range files {
		if f.mode == "" && !f.ignore {
			return fmt.Errorf("preupload: hub returned no upload mode for %s", f.op.PathInRepo)
		}
	}
	return nil
}

// ── step 2: LFS ───────────────────────────────────────────────────────────────

type lfsAction struct {
	Href   string            `json:"href"`
	Header map[string]string `json:"header"`
}

type lfsObject struct {
	OID     string `json:"oid"`
	Size    int64  `json:"size"`
	Actions *struct {
		Upload *lfsAction `json:"upload"`
		Verify *lfsAction `json:"verify"`
	} `json:"actions"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type lfsBatchResp struct {
	Objects []lfsObject `json:"objects"`
}

// lfsBatch asks object storage for upload instructions. An object that is
// already stored comes back with no actions, which is how the hub deduplicates:
// re-committing an unchanged shard transfers nothing.
func (c *HFClient) lfsBatch(ctx context.Context, repoID string, files []*hfFile) (map[string]lfsObject, error) {
	url := fmt.Sprintf("%s/datasets/%s.git/info/lfs/objects/batch", hfEndpoint, repoID)
	out := make(map[string]lfsObject, len(files))

	const batch = 100
	for start := 0; start < len(files); start += batch {
		end := min(start+batch, len(files))
		objs := make([]map[string]any, 0, end-start)
		for _, f := range files[start:end] {
			objs = append(objs, map[string]any{"oid": f.sha256, "size": f.size})
		}
		body, err := json.Marshal(map[string]any{
			"operation": "upload",
			"transfers": []string{"basic", "multipart"},
			"objects":   objs,
			"hash_algo": "sha_256",
		})
		if err != nil {
			return nil, err
		}
		var resp lfsBatchResp
		if err := c.doJSON(ctx, "lfs batch", "POST", url, body, lfsJSONHeaders, &resp); err != nil {
			return nil, err
		}
		for _, o := range resp.Objects {
			if o.Error != nil {
				return nil, fmt.Errorf("lfs batch: object %s: %s (code %d)", o.OID, o.Error.Message, o.Error.Code)
			}
			out[o.OID] = o
		}
	}
	return out, nil
}

// uploadLFS puts one file's bytes into object storage. It picks single or
// multipart from the batch response and verifies afterwards when the server asks
// for it.
func (c *HFClient) uploadLFS(ctx context.Context, sem chan struct{}, f *hfFile, obj lfsObject) error {
	if obj.Actions == nil || obj.Actions.Upload == nil {
		return nil // already stored, nothing to transfer
	}
	up := obj.Actions.Upload

	if chunkStr, ok := up.Header["chunk_size"]; ok && chunkStr != "" {
		chunk, err := strconv.ParseInt(chunkStr, 10, 64)
		if err != nil || chunk <= 0 {
			return fmt.Errorf("lfs upload %s: bad chunk_size %q", f.op.PathInRepo, chunkStr)
		}
		if err := c.uploadMultipart(ctx, sem, f, up, chunk); err != nil {
			return err
		}
	} else if err := c.uploadSinglePart(ctx, sem, f, up); err != nil {
		return err
	}

	if v := obj.Actions.Verify; v != nil {
		body, _ := json.Marshal(map[string]any{"oid": f.sha256, "size": f.size})
		if err := c.doJSON(ctx, "lfs verify", "POST", v.Href, body,
			map[string]string{"Content-Type": "application/json"}, nil); err != nil {
			return err
		}
	}
	return nil
}

// uploadSinglePart PUTs the whole file to one url. The presigned url carries its
// own credentials, so the hub token must not be attached.
func (c *HFClient) uploadSinglePart(ctx context.Context, sem chan struct{}, f *hfFile, up *lfsAction) error {
	_, err := c.putBytes(ctx, sem, "lfs upload "+f.op.PathInRepo, up.Href, f.op.LocalPath, 0, f.size)
	return err
}

// uploadMultipart PUTs the file in chunks and then tells the hub the ETag of
// each part. Parts upload concurrently, but the completion body has to list them
// in order, which is why the ETags land in a slice indexed by part number.
func (c *HFClient) uploadMultipart(ctx context.Context, sem chan struct{}, f *hfFile, up *lfsAction, chunk int64) error {
	urls := sortedPartURLs(up.Header)
	want := int((f.size + chunk - 1) / chunk)
	if len(urls) != want {
		return fmt.Errorf("lfs upload %s: hub returned %d part urls, expected %d for %d bytes at %d per part",
			f.op.PathInRepo, len(urls), want, f.size, chunk)
	}

	etags := make([]string, len(urls))
	g, gctx := errgroup.WithContext(ctx)
	for i, u := range urls {
		g.Go(func() error {
			offset := int64(i) * chunk
			length := min(chunk, f.size-offset)
			etag, err := c.putBytes(gctx, sem, fmt.Sprintf("lfs part %d of %s", i+1, f.op.PathInRepo),
				u, f.op.LocalPath, offset, length)
			if err != nil {
				return err
			}
			if etag == "" {
				return fmt.Errorf("lfs part %d of %s: no ETag in response", i+1, f.op.PathInRepo)
			}
			etags[i] = etag
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	parts := make([]map[string]any, len(etags))
	for i, tag := range etags {
		parts[i] = map[string]any{"partNumber": i + 1, "etag": tag}
	}
	body, err := json.Marshal(map[string]any{"oid": f.sha256, "parts": parts})
	if err != nil {
		return err
	}
	return c.doJSON(ctx, "lfs complete "+f.op.PathInRepo, "POST", up.Href, body, lfsJSONHeaders, nil)
}

// hfUploadConcurrency bounds the PUTs in flight across the whole commit, not per
// file. Nesting a per-file limit inside a per-commit one multiplies out: eight
// files of eight parts each would open sixty four connections and spend the
// bandwidth on TLS handshakes instead of bytes. One shared budget also means the
// number does not change when the commit batch size does.
const hfUploadConcurrency = 16

// sortedPartURLs pulls the numbered part urls out of the upload header and
// returns them in part order. The header also holds chunk_size and any transfer
// headers, which are not part urls.
func sortedPartURLs(header map[string]string) []string {
	type part struct {
		n   int
		url string
	}
	var parts []part
	for k, v := range header {
		n, err := strconv.Atoi(k)
		if err != nil || n <= 0 {
			continue
		}
		parts = append(parts, part{n: n, url: v})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].n < parts[j].n })
	urls := make([]string, len(parts))
	for i, p := range parts {
		urls[i] = p.url
	}
	return urls
}

// putBytes PUTs a byte range of a local file and returns the response ETag. It
// opens the file per call so a retry can re-read from the start of the range
// without sharing a seek offset with another part. The semaphore is held for the
// whole transfer including retries, so a stalled part does not let a new one
// start alongside it.
func (c *HFClient) putBytes(ctx context.Context, sem chan struct{}, op, url, localPath string, offset, length int64) (string, error) {
	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
	case <-ctx.Done():
		return "", ctx.Err()
	}

	var lastErr error
	for attempt := range hfRetries {
		if attempt > 0 {
			if err := hfBackoff(ctx, attempt, lastErr); err != nil {
				return "", err
			}
		}
		f, err := os.Open(localPath)
		if err != nil {
			return "", err
		}
		req, err := http.NewRequestWithContext(ctx, "PUT", url, io.NewSectionReader(f, offset, length))
		if err != nil {
			_ = f.Close()
			return "", err
		}
		req.ContentLength = length
		req.Header.Set("User-Agent", hfUserAgent)
		resp, err := c.http.Do(req)
		_ = f.Close()
		if err != nil {
			lastErr = fmt.Errorf("%s: %w", op, err)
			continue
		}
		if resp.StatusCode >= 400 {
			herr := classifyHF(op, resp)
			_ = resp.Body.Close()
			var he *hfHTTPError
			if errors.As(herr, &he) && !he.retryable() {
				return "", herr
			}
			lastErr = herr
			continue
		}
		etag := resp.Header.Get("ETag")
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return etag, nil
	}
	return "", fmt.Errorf("%s after %d attempts: %w", op, hfRetries, lastErr)
}

// ── step 3: commit ────────────────────────────────────────────────────────────

// hfMaxInlineBytes caps what a "regular" file may be. The hub sends anything
// past a few megabytes to LFS on its own, so this is a floor under a bad answer
// rather than a policy of ours.
const hfMaxInlineBytes = 16 << 20

type hfCommitResp struct {
	CommitURL string `json:"commitUrl"`
	CommitOID string `json:"commitOid"`
}

// commitNDJSON builds the commit body: a header line, then one line per file.
// LFS files are referenced by hash, regular files carry their bytes inline as
// base64.
func commitNDJSON(message string, files []*hfFile) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)

	summary, description := splitCommitMessage(message)
	if err := enc.Encode(map[string]any{
		"key":   "header",
		"value": map[string]any{"summary": summary, "description": description},
	}); err != nil {
		return nil, err
	}

	for _, f := range files {
		if f.ignore {
			continue
		}
		var line map[string]any
		if f.mode == "lfs" {
			line = map[string]any{
				"key": "lfsFile",
				"value": map[string]any{
					"path": f.op.PathInRepo,
					"algo": "sha256",
					"oid":  f.sha256,
					"size": f.size,
				},
			}
		} else {
			// A regular file goes into the commit body as base64, so a large one
			// would mean holding it in memory twice and posting a body a third
			// bigger than the file. The hub routes anything sizable to LFS, so
			// this only fires if it told us something surprising.
			if f.size > hfMaxInlineBytes {
				return nil, fmt.Errorf("commit: hub asked to inline %s (%d bytes), which is over the %d byte limit",
					f.op.PathInRepo, f.size, hfMaxInlineBytes)
			}
			content, err := os.ReadFile(f.op.LocalPath)
			if err != nil {
				return nil, err
			}
			line = map[string]any{
				"key": "file",
				"value": map[string]any{
					"path":     f.op.PathInRepo,
					"content":  base64.StdEncoding.EncodeToString(content),
					"encoding": "base64",
				},
			}
		}
		if err := enc.Encode(line); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// splitCommitMessage takes the first line as the commit summary and the rest as
// the description, which is what a git client does and what the hub UI expects.
func splitCommitMessage(message string) (summary, description string) {
	summary, description, _ = strings.Cut(message, "\n")
	return strings.TrimSpace(summary), strings.TrimSpace(description)
}

// ── the whole conversation ────────────────────────────────────────────────────

// createCommitGo runs the three-step commit entirely in Go and returns the
// commit URL.
func (c *HFClient) createCommitGo(ctx context.Context, repoID, message string, ops []HFOperation) (string, error) {
	if !c.Valid() {
		return "", fmt.Errorf("HF commit: %w: no token, set HF_TOKEN", ErrHFAuth)
	}

	// Same ceiling the Python path had. The publish stall clock is what turns a
	// wedged commit into a supervised restart, and it can only do that if the
	// commit gives up rather than blocking on a dead socket forever.
	ctx, cancel := context.WithTimeout(ctx, 45*time.Minute)
	defer cancel()

	// Measure every file first: one read gives the sha256 that LFS keys on, the
	// size, and the sample preupload wants. A file that vanished between being
	// written and being committed is skipped with a warning rather than aborting,
	// which is what the Python helper did and what the ledger expects.
	files := make([]*hfFile, 0, len(ops))
	var missing int
	for _, op := range ops {
		f, err := measureFile(op)
		if err != nil {
			if os.IsNotExist(err) {
				fmt.Fprintf(os.Stderr, "  HF commit: file not found, skipping: %s\n", op.LocalPath)
				missing++
				continue
			}
			return "", fmt.Errorf("HF commit: read %s: %w", op.LocalPath, err)
		}
		files = append(files, f)
	}
	if missing > 0 {
		fmt.Fprintf(os.Stderr, "  HF commit: %d file(s) missing locally\n", missing)
	}
	if len(files) == 0 {
		return "", errors.New("HF commit: no files to commit")
	}

	if err := c.preupload(ctx, repoID, "main", files); err != nil {
		return "", err
	}

	// Only the LFS files need object storage. Regular files ride inside the
	// commit body, and ignored files are not in the commit at all.
	var lfsFiles []*hfFile
	for _, f := range files {
		if f.mode == "lfs" && !f.ignore {
			lfsFiles = append(lfsFiles, f)
		}
	}

	if len(lfsFiles) > 0 {
		objects, err := c.lfsBatch(ctx, repoID, lfsFiles)
		if err != nil {
			return "", err
		}

		// Objects the hub already holds come back with no upload action. Saying
		// how many were skipped explains why a resumed run finishes in seconds.
		var toUpload []*hfFile
		var uploadBytes int64
		for _, f := range lfsFiles {
			obj, ok := objects[f.sha256]
			if !ok {
				return "", fmt.Errorf("lfs batch: no instructions for %s (oid %s)", f.op.PathInRepo, f.sha256)
			}
			if obj.Actions != nil && obj.Actions.Upload != nil {
				toUpload = append(toUpload, f)
				uploadBytes += f.size
			}
		}
		fmt.Fprintf(os.Stderr, "  HF commit: %d file(s), %s to upload, %d already on the hub\n",
			len(files), humanBytes(uploadBytes), len(lfsFiles)-len(toUpload))

		start := time.Now()
		sem := make(chan struct{}, hfUploadConcurrency)
		g, gctx := errgroup.WithContext(ctx)
		for _, f := range toUpload {
			g.Go(func() error { return c.uploadLFS(gctx, sem, f, objects[f.sha256]) })
		}
		if err := g.Wait(); err != nil {
			return "", err
		}
		if uploadBytes > 0 {
			elapsed := time.Since(start)
			fmt.Fprintf(os.Stderr, "  HF commit: uploaded %s in %s (%.1f MB/s)\n",
				humanBytes(uploadBytes), elapsed.Round(time.Second),
				float64(uploadBytes)/(1<<20)/elapsed.Seconds())
		}
	}

	body, err := commitNDJSON(message, files)
	if err != nil {
		return "", err
	}
	url := fmt.Sprintf("%s/api/datasets/%s/commit/main", hfEndpoint, repoID)
	var resp hfCommitResp
	if err := c.doJSON(ctx, "commit", "POST", url, body,
		map[string]string{"Content-Type": "application/x-ndjson"}, &resp); err != nil {
		return "", err
	}
	return resp.CommitURL, nil
}

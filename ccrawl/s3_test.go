package ccrawl

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestS3Endpoint(t *testing.T) {
	cases := []struct {
		uri, region, want string
		ok                bool
	}{
		{"s3://commoncrawl/crawl-data/CC-MAIN-2026-30/x.warc.gz", "us-east-1",
			"https://commoncrawl.s3.us-east-1.amazonaws.com/crawl-data/CC-MAIN-2026-30/x.warc.gz", true},
		{"s3://commoncrawl/a/b.parquet", "eu-west-1",
			"https://commoncrawl.s3.eu-west-1.amazonaws.com/a/b.parquet", true},
		// An empty region falls back rather than building a broken hostname.
		{"s3://commoncrawl/a", "", "https://commoncrawl.s3.us-east-1.amazonaws.com/a", true},
		{"https://data.commoncrawl.org/a", "us-east-1", "", false},
		{"s3://commoncrawl", "us-east-1", "", false},
		{"s3://commoncrawl/", "us-east-1", "", false},
		{"", "us-east-1", "", false},
	}
	for _, c := range cases {
		got, ok := s3Endpoint(c.uri, c.region)
		if ok != c.ok || got != c.want {
			t.Errorf("s3Endpoint(%q, %q) = %q, %v; want %q, %v", c.uri, c.region, got, ok, c.want, c.ok)
		}
	}
}

// The expected signature was produced by botocore signing the identical request
// with the same frozen clock, so this pins our SigV4 against an implementation
// nobody here wrote. Regenerate it with the oracle in the PR that added this if
// the canonical request ever has to change:
//
//	SigV4Auth(Credentials(key, secret), "s3", "us-east-1").add_auth(request)
func TestSignS3MatchesBotocore(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet,
		"https://commoncrawl.s3.us-east-1.amazonaws.com/crawl-data/CC-MAIN-2026-30/x.warc.gz", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", "bytes=0-99")
	// Signed last in the client too, and deliberately not part of the signature.
	req.Header.Set("User-Agent", "ccrawl/test")

	creds := awsCreds{AccessKey: "AKIAIOSFODNN7EXAMPLE", Secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}
	when := time.Date(2026, 8, 6, 10, 11, 12, 0, time.UTC)
	signS3(req, creds, "us-east-1", when)

	const want = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260806/us-east-1/s3/aws4_request, " +
		"SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, " +
		"Signature=1aa085dbeb64371a73a8007c279a85b4fb3e8799b4f2a795aac55e93f96e3774"
	if got := req.Header.Get("Authorization"); got != want {
		t.Errorf("Authorization:\n got %s\nwant %s", got, want)
	}
	if got := req.Header.Get("X-Amz-Date"); got != "20260806T101112Z" {
		t.Errorf("X-Amz-Date = %q", got)
	}
	if got := req.Header.Get("X-Amz-Content-Sha256"); got != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Errorf("payload hash for an empty body = %q", got)
	}
}

// A session token has to be sent and signed, otherwise temporary credentials
// fail in a way that looks like a bad secret.
func TestSignS3SignsSessionToken(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "https://commoncrawl.s3.us-east-1.amazonaws.com/a", nil)
	signS3(req, awsCreds{AccessKey: "k", Secret: "s", Session: "tok"}, "us-east-1", time.Now())
	if req.Header.Get("X-Amz-Security-Token") != "tok" {
		t.Error("session token not sent")
	}
	if !strings.Contains(req.Header.Get("Authorization"), "x-amz-security-token") {
		t.Errorf("session token not signed: %s", req.Header.Get("Authorization"))
	}
}

func TestCredsFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials")
	body := `
# a comment
[default]
aws_access_key_id = DEFAULTKEY
aws_secret_access_key = defaultsecret

[profile scratch]
aws_access_key_id = SCRATCHKEY
aws_secret_access_key = scratchsecret
aws_session_token = scratchtoken
region = eu-west-1
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := credsFromFile(path, "default"); got.AccessKey != "DEFAULTKEY" || got.Secret != "defaultsecret" || got.Session != "" {
		t.Errorf("default profile = %+v", got)
	}
	if got := credsFromFile(path, "scratch"); got.AccessKey != "SCRATCHKEY" || got.Session != "scratchtoken" {
		t.Errorf("scratch profile = %+v", got)
	}
	if got := credsFromFile(path, "nope"); got.valid() {
		t.Errorf("unknown profile returned %+v", got)
	}
	if got := credsFromFile(filepath.Join(dir, "missing"), "default"); got.valid() {
		t.Errorf("missing file returned %+v", got)
	}
}

func TestResolveAWSCredsPrefersEnv(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "ENVKEY")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "envsecret")
	t.Setenv("AWS_SESSION_TOKEN", "envtoken")
	got := resolveAWSCreds()
	if got.AccessKey != "ENVKEY" || got.Secret != "envsecret" || got.Session != "envtoken" {
		t.Errorf("resolveAWSCreds() = %+v", got)
	}
}

func TestDataURLFollowsSource(t *testing.T) {
	cfg := DefaultConfig()
	if got := NewHTTPClient(cfg).DataURL("a/b.warc.gz"); got != DataBaseURL+"a/b.warc.gz" {
		t.Errorf("https DataURL = %q", got)
	}
	cfg.Source = SourceS3
	if got := NewHTTPClient(cfg).DataURL("/a/b.warc.gz"); got != S3BaseURL+"a/b.warc.gz" {
		t.Errorf("s3 DataURL = %q", got)
	}
}

// Without credentials the S3 path has to fail on the first attempt with
// something that says what to do, not burn six retries on a 403.
func TestS3WithoutCredentialsFailsFast(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "none"))

	cfg := DefaultConfig()
	cfg.Source = SourceS3
	cfg.Delay = 0
	h := NewHTTPClient(cfg)
	if h.Source() != SourceS3 {
		t.Fatalf("Source() = %q, want s3", h.Source())
	}

	start := time.Now()
	_, err := h.Get(t.Context(), h.DataURL("crawl-data/CC-MAIN-2026-30/x.warc.gz"))
	if !errors.Is(err, ErrNoAWSCredentials) {
		t.Fatalf("err = %v, want ErrNoAWSCredentials", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("took %s, it should not have retried", elapsed)
	}
}

func TestCallerRegionPrefersEnv(t *testing.T) {
	t.Setenv("AWS_REGION", "eu-west-2")
	if got := callerRegion(t.Context()); got != "eu-west-2" {
		t.Errorf("callerRegion() = %q, want eu-west-2", got)
	}
}

// IMDS answers only on EC2. Everywhere else the probe has to give up quickly
// rather than stalling the first read.
func TestIMDSRegionGivesUpOffEC2(t *testing.T) {
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")

	blackhole := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(400 * time.Millisecond)
	}))
	defer blackhole.Close()

	old, oldTimeout := imdsBase, imdsTimeout
	imdsBase, imdsTimeout = blackhole.URL, 100*time.Millisecond
	defer func() { imdsBase, imdsTimeout = old, oldTimeout }()

	start := time.Now()
	if got := callerRegion(context.Background()); got != "" {
		t.Errorf("callerRegion() = %q, want empty when IMDS does not answer", got)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("the IMDS probe took %s, it has to give up fast", elapsed)
	}
}

func TestIMDSRegionOnEC2(t *testing.T) {
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/latest/api/token":
			if r.Header.Get("X-aws-ec2-metadata-token-ttl-seconds") == "" {
				t.Error("token request without a TTL header")
			}
			_, _ = w.Write([]byte("tok"))
		case r.URL.Path == "/latest/meta-data/placement/region":
			if r.Header.Get("X-aws-ec2-metadata-token") != "tok" {
				t.Errorf("region request carried token %q", r.Header.Get("X-aws-ec2-metadata-token"))
			}
			_, _ = w.Write([]byte("us-east-1"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	old := imdsBase
	imdsBase = srv.URL
	defer func() { imdsBase = old }()

	if got := callerRegion(context.Background()); got != "us-east-1" {
		t.Errorf("callerRegion() = %q, want us-east-1", got)
	}
}

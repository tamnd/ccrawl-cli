package ccrawl

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// S3 access to the public commoncrawl bucket.
//
// There is no AWS SDK here on purpose. An object read is a GET against the REST
// endpoint with an optional Range header plus a SigV4 Authorization header, and
// signing one canonical GET is about eighty lines. Translating the s3:// URI
// into that request inside the existing HTTP client means the S3 path shares the
// throttle, the retry loop, the backoff and the user agent with the HTTPS path
// rather than growing a second copy of all of it.
//
// The reason to bother: reads of s3://commoncrawl from inside us-east-1 do not
// pay egress and the CloudFront mirror does, on a dataset where one crawl is
// measured in terabytes. A recovery pass pinned to us-east-1 is the case this
// exists for.
//
// The bucket used to allow anonymous reads. It does not any more, it answers
// AccessDenied without a signature, so credentials are required even though
// nothing is charged for the objects themselves.

// s3Bucket and s3BucketRegion identify the Common Crawl bucket. The region is
// fixed rather than read from AWS_REGION because it names where the bucket is,
// not where the caller is: signing for the wrong region gets a redirect.
const (
	s3Bucket       = "commoncrawl"
	s3BucketRegion = "us-east-1"
)

// ErrNoAWSCredentials is returned when --source s3 is used with nothing to sign
// with. It is not retryable, so the client fails on it immediately instead of
// burning the retry budget.
var ErrNoAWSCredentials = errors.New("no AWS credentials for --source s3: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or drop --source s3 to read the free HTTPS mirror")

// awsCreds is one set of credentials. Session is empty for long-lived keys.
type awsCreds struct {
	AccessKey string
	Secret    string
	Session   string
}

func (c awsCreds) valid() bool { return c.AccessKey != "" && c.Secret != "" }

// s3Endpoint turns an s3://bucket/key URI into the REST URL for it, reporting
// false for anything that is not an s3 URI. Virtual-hosted style is used because
// path style is deprecated for new buckets, and the regional hostname avoids a
// redirect on the first request.
func s3Endpoint(uri, region string) (string, bool) {
	rest, ok := strings.CutPrefix(uri, "s3://")
	if !ok {
		return "", false
	}
	bucket, key, ok := strings.Cut(rest, "/")
	if !ok || bucket == "" || key == "" {
		return "", false
	}
	if region == "" {
		region = s3BucketRegion
	}
	return fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", bucket, region, key), true
}

// resolveAWSCreds finds credentials the way the AWS tools do, minus the parts
// that need a network round trip: the environment first, then the shared
// credentials file for the selected profile. Instance roles are deliberately not
// consulted, because a bulk run wants a key it can be handed rather than one
// that expires halfway through.
func resolveAWSCreds() awsCreds {
	c := awsCreds{
		AccessKey: strings.TrimSpace(os.Getenv("AWS_ACCESS_KEY_ID")),
		Secret:    strings.TrimSpace(os.Getenv("AWS_SECRET_ACCESS_KEY")),
		Session:   strings.TrimSpace(os.Getenv("AWS_SESSION_TOKEN")),
	}
	if c.valid() {
		return c
	}
	return credsFromFile(sharedCredentialsPath(), awsProfile())
}

func awsProfile() string {
	if p := strings.TrimSpace(os.Getenv("AWS_PROFILE")); p != "" {
		return p
	}
	return "default"
}

func sharedCredentialsPath() string {
	if p := strings.TrimSpace(os.Getenv("AWS_SHARED_CREDENTIALS_FILE")); p != "" {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".aws", "credentials")
}

// credsFromFile reads one profile out of an ini-style credentials file. It is a
// deliberately small parser: keys we do not recognise are skipped rather than
// rejected, because the file is shared with tools that write more than this.
func credsFromFile(path, profile string) awsCreds {
	var c awsCreds
	if path == "" {
		return c
	}
	f, err := os.Open(path)
	if err != nil {
		return c
	}
	defer func() { _ = f.Close() }()

	inProfile := false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			name := strings.TrimSpace(line[1 : len(line)-1])
			// A config file spells the same profile "profile foo".
			name = strings.TrimPrefix(name, "profile ")
			inProfile = name == profile
			continue
		}
		if !inProfile {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key, val = strings.TrimSpace(key), strings.TrimSpace(val)
		switch key {
		case "aws_access_key_id":
			c.AccessKey = val
		case "aws_secret_access_key":
			c.Secret = val
		case "aws_session_token":
			c.Session = val
		}
	}
	return c
}

// signS3 adds the SigV4 headers to a GET. Only what a read needs is implemented:
// no body, so the payload hash is the hash of the empty string, and no query
// signing beyond an already-encoded query string.
//
// The canonical request, string to sign and signing key are built exactly as the
// AWS documentation describes them, which is what makes this testable against
// the published vectors rather than only against the service.
func signS3(req *http.Request, c awsCreds, region string, now time.Time) {
	now = now.UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	payloadHash := hex.EncodeToString(sha256.New().Sum(nil))
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if c.Session != "" {
		req.Header.Set("X-Amz-Security-Token", c.Session)
	}
	if req.Host != "" {
		req.Header.Set("Host", req.Host)
	} else {
		req.Header.Set("Host", req.URL.Host)
	}

	signed, canonicalHeaders := canonicalHeaders(req)
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURIPath(req.URL.EscapedPath()),
		req.URL.RawQuery,
		canonicalHeaders,
		signed,
		payloadHash,
	}, "\n")

	scope := strings.Join([]string{dateStamp, region, "s3", "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		sha256Hex(canonicalRequest),
	}, "\n")

	key := hmacSHA256([]byte("AWS4"+c.Secret), dateStamp)
	key = hmacSHA256(key, region)
	key = hmacSHA256(key, "s3")
	key = hmacSHA256(key, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(key, stringToSign))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.AccessKey, scope, signed, signature))
}

// unsignedHeaders are left out of the signature. User-Agent is the one that
// matters: a proxy is allowed to rewrite it, and a rewritten signed header is an
// unfixable SignatureDoesNotMatch. The AWS SDKs skip the same set.
var unsignedHeaders = map[string]bool{
	"authorization":     true,
	"user-agent":        true,
	"expect":            true,
	"x-amzn-trace-id":   true,
	"content-length":    true,
	"accept-encoding":   true,
	"transfer-encoding": true,
}

// canonicalHeaders returns the signed header list and the canonical header block.
// Everything on the request is signed except the headers above, which is what
// the SDKs do and what keeps the signature stable across a proxy.
func canonicalHeaders(req *http.Request) (signed, canonical string) {
	names := make([]string, 0, len(req.Header)+1)
	values := make(map[string]string, len(req.Header)+1)
	for name, vals := range req.Header {
		lower := strings.ToLower(name)
		if unsignedHeaders[lower] {
			continue
		}
		trimmed := make([]string, len(vals))
		for i, v := range vals {
			trimmed[i] = strings.Join(strings.Fields(v), " ")
		}
		names = append(names, lower)
		values[lower] = strings.Join(trimmed, ",")
	}
	sort.Strings(names)
	var b strings.Builder
	for _, n := range names {
		b.WriteString(n)
		b.WriteString(":")
		b.WriteString(values[n])
		b.WriteString("\n")
	}
	return strings.Join(names, ";"), b.String()
}

// canonicalURIPath keeps the path as S3 wants it: already percent-encoded, with
// an empty path spelled "/".
func canonicalURIPath(p string) string {
	if p == "" {
		return "/"
	}
	return p
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

// s3Error is the error document S3 returns with a 4xx. Only the code and the
// message are useful to a caller.
type s3Error struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// s3AuthError turns a 4xx from the bucket into a permanent error when the body
// says the problem is the credentials. Without this the client treats 403 as the
// transient throttle CloudFront uses it for and burns every retry on a request
// that will never succeed.
func s3AuthError(uri string, resp *http.Response) error {
	if resp.StatusCode != http.StatusForbidden && resp.StatusCode != http.StatusBadRequest {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var e s3Error
	_ = xml.Unmarshal(body, &e)
	switch e.Code {
	case "InvalidAccessKeyId", "SignatureDoesNotMatch", "AccessDenied", "ExpiredToken", "InvalidToken", "RequestTimeTooSkewed":
		return fmt.Errorf("get %s: S3 %s: %s", uri, e.Code, e.Message)
	}
	return nil
}

// imdsTimeout bounds the instance metadata probe. On EC2 it answers in
// single-digit milliseconds; anywhere else the address is unroutable, so the
// probe has to give up fast enough that nobody notices it happened.
var imdsTimeout = 300 * time.Millisecond

// imdsBase is the instance metadata endpoint, a var so tests can point it at a
// local server instead of the link-local address.
var imdsBase = "http://169.254.169.254"

// callerRegion reports the region this process is running in, or "" if it cannot
// tell. It prefers AWS_REGION, since that is what a container or a Lambda sets,
// and only then asks IMDSv2.
func callerRegion(ctx context.Context) string {
	for _, env := range []string{"AWS_REGION", "AWS_DEFAULT_REGION"} {
		if v := strings.TrimSpace(os.Getenv(env)); v != "" {
			return v
		}
	}
	return imdsRegion(ctx)
}

// imdsRegion asks IMDSv2 for the placement region. IMDSv1 is not attempted: a v1
// request on a v2-only instance hangs rather than failing, and a token round trip
// is cheap when it works.
func imdsRegion(ctx context.Context) string {
	ctx, cancel := context.WithTimeout(ctx, imdsTimeout)
	defer cancel()
	client := &http.Client{Timeout: imdsTimeout}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, imdsBase+"/latest/api/token", nil)
	if err != nil {
		return ""
	}
	req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "60")
	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	token, err := readAllLimited(resp, 128)
	if err != nil || token == "" {
		return ""
	}

	req, err = http.NewRequestWithContext(ctx, http.MethodGet, imdsBase+"/latest/meta-data/placement/region", nil)
	if err != nil {
		return ""
	}
	req.Header.Set("X-aws-ec2-metadata-token", token)
	resp, err = client.Do(req)
	if err != nil {
		return ""
	}
	region, err := readAllLimited(resp, 64)
	if err != nil {
		return ""
	}
	return region
}

func readAllLimited(resp *http.Response, limit int64) (string, error) {
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("the server answered HTTP %d", resp.StatusCode)
	}
	buf := make([]byte, limit)
	n, _ := resp.Body.Read(buf)
	return strings.TrimSpace(string(buf[:n])), nil
}

// warnEgressOnce keeps the region warning to one line per run no matter how many
// files a bulk command touches.
var warnEgressOnce sync.Once

// warnIfS3Egress prints one warning when a run reads the bucket from outside the
// region it lives in, which is the expensive mistake: in region those reads are
// free and cross region they are billed per byte. It says nothing when it cannot
// work out where it is running, since a guess either way is worse than silence.
func warnIfS3Egress(ctx context.Context) {
	warnEgressOnce.Do(func() {
		region := callerRegion(ctx)
		if region == "" || region == s3BucketRegion {
			return
		}
		fmt.Fprintf(os.Stderr,
			"warning: --source s3 reads s3://%s (%s) from %s, which bills cross-region egress. Drop --source s3 to use the free HTTPS mirror.\n",
			s3Bucket, s3BucketRegion, region)
	})
}

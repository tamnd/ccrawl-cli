package ccrawl

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// TestErrFingerprintGroupsTheSameFailure is the whole point of the fingerprint:
// the same kind of failure on a million hosts has to count as one line, or the
// report is just the error log again.
func TestErrFingerprintGroupsTheSameFailure(t *testing.T) {
	same := []string{
		`Get "https://a.example/": remote error: tls: handshake failure`,
		`Get "https://b.example/page?x=1": remote error: tls: handshake failure`,
		`GET "http://c.example:8080/": remote error: tls: handshake failure`,
	}
	first := errFingerprint(same[0])
	if first == "" {
		t.Fatal("a real error fingerprinted to nothing")
	}
	for _, m := range same[1:] {
		if got := errFingerprint(m); got != first {
			t.Fatalf("%q fingerprinted as %q, want %q", m, got, first)
		}
	}
	if strings.Contains(first, "example") {
		t.Fatalf("the fingerprint %q still names the host, so every host is its own line", first)
	}

	// Addresses and ports differ per request and must not split a bucket.
	a := errFingerprint(`Get "https://a.example/": dial tcp 1.2.3.4:443: i/o error`)
	b := errFingerprint(`Get "https://b.example/": dial tcp 5.6.7.8:80: i/o error`)
	if a != b {
		t.Fatalf("two dials fingerprinted apart: %q and %q", a, b)
	}

	// Different failures must stay apart, or the report says one thing about two.
	if errFingerprint("unexpected EOF") == errFingerprint("http2: server sent GOAWAY") {
		t.Fatal("two different failures share a fingerprint")
	}
}

// TestErrFingerprintBoundsWhatItPrints keeps a hostile error string from
// becoming a hostile log line. The message comes off the wire in the end.
func TestErrFingerprintBoundsWhatItPrints(t *testing.T) {
	long := errFingerprint("remote error: " + strings.Repeat("blah ", 200))
	if len(long) > 130 {
		t.Fatalf("a 1000 character error printed as %d characters", len(long))
	}
	if !strings.HasSuffix(long, "...") {
		t.Fatalf("a truncated fingerprint does not say it was truncated: %q", long)
	}
	if errFingerprint("   ") != "" {
		t.Fatal("an empty error made a fingerprint")
	}
}

// TestErrSamplesKeepsTheHead checks the two properties the report needs: the
// most common shapes come out first, and a run that meets an unbounded variety
// of errors does not grow a map for the length of the crawl.
func TestErrSamplesKeepsTheHead(t *testing.T) {
	var s errSamples
	for range 50 {
		s.add(errors.New("remote error: tls: handshake failure"))
	}
	for range 20 {
		s.add(errors.New("unexpected EOF"))
	}
	for range 5 {
		s.add(errors.New("http2: server sent GOAWAY and closed the connection"))
	}
	top := s.top(2)
	if len(top) != 2 {
		t.Fatalf("asked for the top 2 and got %d", len(top))
	}
	if !strings.Contains(top[0].Message, "handshake") || top[0].Count != 50 {
		t.Fatalf("the most common shape came back as %+v", top[0])
	}
	if !strings.Contains(top[1].Message, "unexpected eof") || top[1].Count != 20 {
		t.Fatalf("the second shape came back as %+v", top[1])
	}

	for i := range 1000 {
		s.add(fmt.Errorf("a failure of kind %s", strings.Repeat("x", i%97+1)))
	}
	if got := len(s.top(1 << 20)); got > maxErrSamples {
		t.Fatalf("the sampler kept %d distinct shapes, and the cap is %d", got, maxErrSamples)
	}
	// The head survives the flood, which is the property that matters: a run
	// that meets a thousand one-off errors still reports the one that happened
	// fifty times.
	if again := s.top(1); again[0].Count != 50 {
		t.Fatalf("after the flood the top shape is %+v", again[0])
	}
}

// TestErrSamplesIsConcurrencySafe runs it the way a crawl does, since every
// worker classifies its own failures.
func TestErrSamplesIsConcurrencySafe(t *testing.T) {
	var s errSamples
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for range 100 {
				s.add(fmt.Errorf("dial tcp 10.0.0.%d:443: i/o error", w))
			}
		}(w)
	}
	wg.Wait()
	top := s.top(3)
	if len(top) != 1 {
		t.Fatalf("eight workers writing one shape produced %d shapes: %+v", len(top), top)
	}
	if top[0].Count != 800 {
		t.Fatalf("counted %d of 800", top[0].Count)
	}
}

// TestErrOtherLineSaysHowMuchItCovered is about not overclaiming. A top three
// that explains a tenth of the bucket reads exactly like one that explains all
// of it unless the line says otherwise.
func TestErrOtherLineSaysHowMuchItCovered(t *testing.T) {
	top := []ErrSample{{Message: "unexpected eof", Count: 10}, {Message: "http2: goaway", Count: 5}}
	line := ErrOtherLine(15, top)
	if strings.Contains(line, " of ") {
		t.Fatalf("the top shapes covered the whole bucket and the line hedges: %q", line)
	}
	if !strings.Contains(line, "unexpected eof 10") {
		t.Fatalf("the line does not name the shapes: %q", line)
	}
	line = ErrOtherLine(200, top)
	if !strings.Contains(line, "15 of 200") {
		t.Fatalf("the line claims the whole bucket: %q", line)
	}
	if ErrOtherLine(0, nil) != "" || ErrOtherLine(10, nil) != "" {
		t.Fatal("a run with nothing to report printed a line about it")
	}
}

// TestClassifyRecordsWhatItCouldNotName wires the two halves together: an error
// the classifier does not recognise has to reach the sampler, and one it does
// must not, or the report describes the bucket next door.
func TestClassifyRecordsWhatItCouldNotName(t *testing.T) {
	var c crawlCounters
	classifyCrawlErr(errors.New(`Get "https://a.example/": unexpected EOF`), &c)
	classifyCrawlErr(errors.New(`Get "https://b.example/": unexpected EOF`), &c)
	classifyCrawlErr(errors.New(`Get "https://c.example/": dial tcp: lookup c.example: no such host`), &c)

	if got := c.errOther.Load(); got != 2 {
		t.Fatalf("the other bucket holds %d, want 2", got)
	}
	top := c.otherSamples.top(3)
	if len(top) != 1 || top[0].Count != 2 {
		t.Fatalf("the sampler holds %+v", top)
	}
	if strings.Contains(top[0].Message, "no such host") {
		t.Fatalf("a classified failure was sampled as unclassified: %q", top[0].Message)
	}
}

// TestClassifyCountsTheHandshakeApart is the bucket the sampler paid for. TLS
// was 676 of the 1575 unclassified failures in a 20 000 row run against the live
// domain list, the largest single thing in there, so it has a class now.
//
// It is worth keeping apart from refused. A refusal is a host that will not talk
// to anybody; a handshake failure is a host that is talking and whose
// certificate expired four years ago, and only one of those is a candidate for
// falling back to http.
func TestClassifyCountsTheHandshakeApart(t *testing.T) {
	tls := []string{
		`Get "https://a.example/": remote error: tls: handshake failure`,
		`Get "https://b.example/": remote error: tls: internal error`,
		`Get "https://c.example/": tls: failed to verify certificate: x509: certificate has expired or is not yet valid: current time 2026-08-22 is after 2022-01-01`,
		`Get "https://d.example/": x509: certificate signed by unknown authority`,
	}
	var c crawlCounters
	for _, m := range tls {
		classifyCrawlErr(errors.New(m), &c)
	}
	if got := c.errTLS.Load(); got != int64(len(tls)) {
		t.Fatalf("the tls bucket holds %d of %d", got, len(tls))
	}
	if got := c.errOther.Load(); got != 0 {
		t.Fatalf("%d handshake failures still landed in other", got)
	}
	if got := len(c.otherSamples.top(3)); got != 0 {
		t.Fatalf("a classified failure was sampled as unclassified: %+v", c.otherSamples.top(3))
	}

	// A reset is still a refusal, not a handshake. The order the classifier
	// tests in is what keeps that true, so it is worth a case.
	var d crawlCounters
	classifyCrawlErr(errors.New(`Get "https://e.example/": read tcp 1.2.3.4:443: connection reset by peer`), &d)
	if d.errRefused.Load() != 1 || d.errTLS.Load() != 0 {
		t.Fatalf("a reset on port 443 was counted refused %d tls %d", d.errRefused.Load(), d.errTLS.Load())
	}
}

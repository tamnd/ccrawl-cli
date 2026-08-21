package ccrawl

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

// Saying what is in the bucket marked other.
//
// A run reports its failures in five buckets and four of them are actionable:
// dns is a resolver or a dead domain, timeout is patience, refused is a host
// saying no, skipped is our own back pressure. Other is the bucket for
// everything the classifier does not recognise, and on the live domain list it
// is 1700 to 2000 of the roughly 7500 failures in a 20 000 row run. That is a
// tenth of the work list going into a box with no label on it, and a tenth is
// too much to leave unexplained when the question is why the rate is what it is.
//
// The fix is not to guess at more substring matches. It is to make the run say
// what it saw, so the next classification is written against the messages that
// actually turn up rather than against the ones somebody expected. So the
// unclassified errors are fingerprinted and counted, and the run prints the
// handful that account for most of the bucket.

// errSamples counts unclassified failures by fingerprint.
//
// It is bounded, because the fingerprints come from error strings and an error
// string is ultimately attacker controlled: a corpus that is a third dead hosts
// is also a corpus with some hosts that will happily return whatever they like
// in a TLS alert. Past the cap new fingerprints are dropped rather than stored,
// which loses the tail and never the head, and the head is what the report is
// about.
type errSamples struct {
	mu sync.Mutex
	n  map[string]int64
}

// maxErrSamples is how many distinct fingerprints are kept. It is a diagnostic,
// not a census, and a bucket whose top few entries do not explain it is a bucket
// that needs looking at by hand anyway.
const maxErrSamples = 64

// add records one unclassified failure.
func (s *errSamples) add(err error) {
	if err == nil {
		return
	}
	f := errFingerprint(err.Error())
	if f == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.n == nil {
		s.n = make(map[string]int64, maxErrSamples)
	}
	if _, seen := s.n[f]; !seen && len(s.n) >= maxErrSamples {
		return
	}
	s.n[f]++
}

// ErrSample is one shape of failure and how often a run hit it.
type ErrSample struct {
	Message string
	Count   int64
}

// top returns the most common fingerprints, largest first, at most n of them.
// Ties break on the message so a run that is given the same failures twice
// reports them in the same order, which is what makes two logs comparable.
func (s *errSamples) top(n int) []ErrSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ErrSample, 0, len(s.n))
	for m, c := range s.n {
		out = append(out, ErrSample{Message: m, Count: c})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Message < out[j].Message
	})
	if len(out) > n {
		out = out[:n]
	}
	return out
}

// errFingerprint reduces one error string to the shape of the failure, so a
// million failures on a million hosts count as one line rather than a million.
//
// What has to go is everything that names the particular request: the URL, the
// address it dialled, the port, and any number at all, since a number in one of
// these is a port, a byte count or a status and never the reason. What is left
// is the sentence Go or the TLS stack produced, which is the thing worth
// counting.
func errFingerprint(msg string) string {
	msg = strings.ToLower(strings.TrimSpace(msg))
	if msg == "" {
		return ""
	}
	// Drop the Get "https://host/path": prefix that net/http puts on everything,
	// including its quotes, since the URL is per request and the rest is not.
	if i := strings.Index(msg, `": `); i >= 0 {
		if q := strings.IndexByte(msg, '"'); q >= 0 && q < i {
			msg = msg[i+3:]
		}
	}
	fields := strings.Fields(msg)
	out := fields[:0]
	for _, f := range fields {
		switch {
		case strings.ContainsFunc(f, unicode.IsDigit):
			// An address, a port, a byte count or a status code. All of them are
			// this request rather than this kind of failure.
			out = append(out, "N")
		case strings.HasPrefix(f, "http://"), strings.HasPrefix(f, "https://"):
			out = append(out, "URL")
		default:
			out = append(out, f)
		}
	}
	f := strings.Join(out, " ")
	// A cap, because these are printed and an error string has no length anybody
	// promised. It cuts on a space so a truncated fingerprint still reads as
	// words.
	const maxLen = 120
	if len(f) > maxLen {
		f = f[:maxLen]
		if i := strings.LastIndexByte(f, ' '); i > 0 {
			f = f[:i]
		}
		f += " ..."
	}
	return f
}

// ErrOtherLine is the one line a run prints about its unclassified failures, or
// empty when there were none. It names the shapes rather than the hosts, and it
// says how much of the bucket it accounted for, because a top three that covers
// a tenth of the bucket is a different message from one that covers all of it.
func ErrOtherLine(total int64, top []ErrSample) string {
	if total <= 0 || len(top) == 0 {
		return ""
	}
	var covered int64
	parts := make([]string, 0, len(top))
	for _, s := range top {
		covered += s.Count
		parts = append(parts, s.Message+" "+strconv.FormatInt(s.Count, 10))
	}
	line := "the unclassified failures are mostly: " + strings.Join(parts, ", ")
	if covered < total {
		line += ", which is " + strconv.FormatInt(covered, 10) + " of " + strconv.FormatInt(total, 10)
	}
	return line
}

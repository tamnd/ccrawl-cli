package ccrawl

import (
	"encoding/json"
	"strings"
	"testing"
)

// A host record carries two kinds of number: the rank signals, which every
// command that emits one has looked up, and the topology and CDX counts, which
// most of them never measure. They used to be plain int64s, so a host nobody
// counted the links of and a host with no inbound links both came out as
// in_degree 0, and a reader had no way to tell a measurement from a blank.
func TestUnmeasuredNumbersAreAbsentRatherThanZero(t *testing.T) {
	rec := HostFromRank(Rank{Key: "example.com", HarmonicPos: 7, PageRankPos: 9})
	got := marshal(t, rec)

	for _, key := range []string{"in_degree", "out_degree", "url_count", "status_2xx", "status_3xx", "status_4xx", "status_5xx", "total_bytes"} {
		if strings.Contains(got, `"`+key+`"`) {
			t.Errorf("%s is in a record nothing measured it for:\n%s", key, got)
		}
	}
	// The rank signals are the part that was looked up, so they stay, including
	// the ones that are legitimately zero.
	for _, key := range []string{`"harmonic_pos":7`, `"pagerank_pos":9`, `"harmonic_val":0`} {
		if !strings.Contains(got, key) {
			t.Errorf("%s is missing from a record that has it:\n%s", key, got)
		}
	}
}

// The other half of the same claim: a count somebody took and found to be zero
// is a fact about the host, and dropping it would trade one wrong answer for
// another.
func TestAMeasuredZeroSurvives(t *testing.T) {
	rec := HostFromRank(Rank{Key: "example.com"})
	rec.InDegree = Counted(0)
	rec.URLCount = Counted(0)
	rec.TotalBytes = Counted(4)

	got := marshal(t, rec)
	for _, want := range []string{`"in_degree":0`, `"url_count":0`, `"total_bytes":4`} {
		if !strings.Contains(got, want) {
			t.Errorf("%s is missing from a record that was counted:\n%s", want, got)
		}
	}
	if strings.Contains(got, `"status_2xx"`) {
		t.Errorf("a count nobody took came back anyway:\n%s", got)
	}
}

func marshal(t *testing.T, rec HostRecord) string {
	t.Helper()
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

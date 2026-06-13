package ccrawl

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"strconv"
	"strings"
)

// reverseHost turns "www.example.com" into "com.example.www", the form the rank
// tables key on.
func reverseHost(host string) string {
	labels := strings.Split(strings.ToLower(host), ".")
	for i, j := 0, len(labels)-1; i < j; i, j = i+1, j-1 {
		labels[i], labels[j] = labels[j], labels[i]
	}
	return strings.Join(labels, ".")
}

// hostRevField is the column holding the reversed host/domain key (host_rev).
// The rank tables have the columns:
//
//	harmonicc_pos harmonicc_val pr_pos pr_val host_rev [n_hosts]
//
// host_rev sits at this fixed index whether or not the trailing n_hosts column
// is present, so it is addressed by position rather than as the last field.
const hostRevField = 4

// rankComment reports whether a line is the header or a comment. The table's
// header row starts every column name with '#'.
func rankComment(line string) bool { return strings.HasPrefix(line, "#") }

// RankLookup streams a gzipped rank table from url and returns the entry whose
// reversed key matches the given host or domain, or a not-found error.
func RankLookup(ctx context.Context, h *HTTPClient, url, hostOrDomain string) (Rank, error) {
	want := reverseHost(hostOrDomain)
	resp, err := h.GetDownload(ctx, url)
	if err != nil {
		return Rank{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return Rank{}, fmt.Errorf("rank table HTTP %d (%s)", resp.StatusCode, url)
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return Rank{}, err
	}
	defer gz.Close()

	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	for sc.Scan() {
		line := sc.Text()
		if rankComment(line) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) <= hostRevField {
			continue
		}
		if fields[hostRevField] != want {
			continue
		}
		return parseRank(fields), nil
	}
	if err := sc.Err(); err != nil {
		return Rank{}, err
	}
	return Rank{}, fmt.Errorf("%s not found in rank table", hostOrDomain)
}

// RankTop streams a rank table and returns the first n rows (the table is sorted
// by harmonic centrality, most central first).
func RankTop(ctx context.Context, h *HTTPClient, url, tld string, n int) ([]Rank, error) {
	resp, err := h.GetDownload(ctx, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("rank table HTTP %d (%s)", resp.StatusCode, url)
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return nil, err
	}
	defer gz.Close()

	suffix := ""
	if tld != "" {
		suffix = reverseHost(tld) // tld reversed is the leading label
	}
	var out []Rank
	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	for sc.Scan() && len(out) < n {
		line := sc.Text()
		if rankComment(line) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) <= hostRevField {
			continue
		}
		if suffix != "" && !strings.HasPrefix(fields[hostRevField], suffix+".") {
			continue
		}
		out = append(out, parseRank(fields))
	}
	return out, sc.Err()
}

func parseRank(fields []string) Rank {
	key := reverseHost(fields[hostRevField])
	hp, _ := strconv.ParseInt(fields[0], 10, 64)
	hv, _ := strconv.ParseFloat(fields[1], 64)
	pp, _ := strconv.ParseInt(fields[2], 10, 64)
	pv, _ := strconv.ParseFloat(fields[3], 64)
	return Rank{Key: key, HarmonicPos: hp, HarmonicVal: hv, PageRankPos: pp, PageRankVal: pv}
}

package ccrawl

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// exposition is the metrics text for m, as a scraper would see it.
func exposition(t *testing.T, m *Metrics) string {
	t.Helper()
	var b strings.Builder
	n, err := m.WriteTo(&b)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if int(n) != b.Len() {
		t.Fatalf("WriteTo reported %d bytes, wrote %d", n, b.Len())
	}
	return b.String()
}

// hasLine reports whether the exposition contains exactly this line.
func hasLine(out, want string) bool {
	for _, line := range strings.Split(out, "\n") {
		if line == want {
			return true
		}
	}
	return false
}

func TestMetricsExposition(t *testing.T) {
	m := NewMetrics()

	// Nothing has happened yet, so nothing is exposed. A series that only exists
	// because it was declared would report a zero that is not true.
	if out := exposition(t, m); out != "" {
		t.Fatalf("empty registry rendered %q", out)
	}

	m.Add(MetricShards, 1, "pipeline", "markdown export", "status", StatusOK)
	m.Add(MetricShards, 2, "pipeline", "markdown export", "status", StatusOK)
	m.Add(MetricShards, 1, "pipeline", "markdown export", "status", StatusFailed)
	m.Set(MetricInflight, 3, "pipeline", "markdown export")
	m.Set(MetricRSS, 123456)

	out := exposition(t, m)
	for _, want := range []string{
		"# HELP ccrawl_shards_total Shards finished, by pipeline and outcome.",
		"# TYPE ccrawl_shards_total counter",
		`ccrawl_shards_total{pipeline="markdown export",status="failed"} 1`,
		`ccrawl_shards_total{pipeline="markdown export",status="ok"} 3`,
		"# TYPE ccrawl_inflight gauge",
		`ccrawl_inflight{pipeline="markdown export"} 3`,
		"ccrawl_rss_bytes 123456",
	} {
		if !hasLine(out, want) {
			t.Fatalf("exposition missing %q\ngot:\n%s", want, out)
		}
	}

	// An unlabelled series carries no braces at all.
	if strings.Contains(out, "ccrawl_rss_bytes{}") {
		t.Fatalf("unlabelled series rendered empty braces:\n%s", out)
	}
}

func TestMetricsHistogram(t *testing.T) {
	m := NewMetrics()
	for _, s := range []float64{0.5, 10, 45, 5000} {
		m.Observe(MetricPhaseSeconds, s, "pipeline", "refetch", "phase", "fetch")
	}
	out := exposition(t, m)

	for _, want := range []string{
		"# TYPE ccrawl_phase_duration_seconds histogram",
		`ccrawl_phase_duration_seconds_bucket{phase="fetch",pipeline="refetch",le="1"} 1`,
		`ccrawl_phase_duration_seconds_bucket{phase="fetch",pipeline="refetch",le="15"} 2`,
		`ccrawl_phase_duration_seconds_bucket{phase="fetch",pipeline="refetch",le="60"} 3`,
		`ccrawl_phase_duration_seconds_bucket{phase="fetch",pipeline="refetch",le="3600"} 3`,
		`ccrawl_phase_duration_seconds_bucket{phase="fetch",pipeline="refetch",le="+Inf"} 4`,
		`ccrawl_phase_duration_seconds_sum{phase="fetch",pipeline="refetch"} 5055.5`,
		`ccrawl_phase_duration_seconds_count{phase="fetch",pipeline="refetch"} 4`,
	} {
		if !hasLine(out, want) {
			t.Fatalf("histogram missing %q\ngot:\n%s", want, out)
		}
	}
}

func TestMetricsLabelEscaping(t *testing.T) {
	m := NewMetrics()
	m.Add(MetricItems, 1, "pipeline", "download", "status", "a\"b\\c\nd")
	out := exposition(t, m)
	want := `ccrawl_items_total{pipeline="download",status="a\"b\\c\nd"} 1`
	if !hasLine(out, want) {
		t.Fatalf("escaping wrong, want %q\ngot:\n%s", want, out)
	}
}

func TestMetricsNilAndUnknownName(t *testing.T) {
	var m *Metrics
	m.Add(MetricShards, 1, "pipeline", "x")
	m.Set(MetricDone, 1)
	m.Observe(MetricPhaseSeconds, 1)
	if got := m.Value(MetricShards, "pipeline", "x"); got != 0 {
		t.Fatalf("nil registry Value = %v, want 0", got)
	}
	if n, err := m.WriteTo(io.Discard); n != 0 || err != nil {
		t.Fatalf("nil registry WriteTo = %d, %v", n, err)
	}

	// A typo in a metric name is dropped, not panicked on: it must never take a
	// long run down.
	live := NewMetrics()
	live.Add("ccrawl_not_a_metric", 1, "pipeline", "x")
	if out := exposition(t, live); out != "" {
		t.Fatalf("unknown metric was recorded:\n%s", out)
	}
	if got := live.Value("ccrawl_not_a_metric"); got != 0 {
		t.Fatalf("unknown metric Value = %v, want 0", got)
	}
}

func TestServeMetrics(t *testing.T) {
	m := NewMetrics()
	m.Set(MetricDone, 7, "pipeline", "download")

	srv, err := ServeMetrics("127.0.0.1:0", m)
	if err != nil {
		t.Fatalf("ServeMetrics: %v", err)
	}
	defer func() { _ = srv.Close() }()

	// The listener is bound before ServeMetrics returns, so a second server on
	// the same address has to fail here rather than in a goroutine later.
	addr := srv.Addr
	if dup, derr := ServeMetrics(addr, m); derr == nil {
		_ = dup.Close()
		t.Fatal("ServeMetrics on a busy port did not fail")
	}

	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("Content-Type = %q, want text/plain", ct)
	}
	if !hasLine(string(body), `ccrawl_done{pipeline="download"} 7`) {
		t.Fatalf("scrape missing the gauge:\n%s", body)
	}
}

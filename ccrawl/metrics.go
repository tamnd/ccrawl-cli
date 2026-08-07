package ccrawl

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The metric names a run publishes. They are constants because the reporter and
// the registry have to agree on the spelling, and a test walks this list to
// check that every name the reporter writes is one the registry knows.
const (
	MetricShards       = "ccrawl_shards_total"
	MetricItems        = "ccrawl_items_total"
	MetricRows         = "ccrawl_rows_total"
	MetricBytes        = "ccrawl_bytes_total"
	MetricPhaseSeconds = "ccrawl_phase_duration_seconds"
	MetricInflight     = "ccrawl_inflight"
	MetricDone         = "ccrawl_done"
	MetricTotal        = "ccrawl_total"
	MetricRate         = "ccrawl_rate_per_hour"
	MetricElapsed      = "ccrawl_run_elapsed_seconds"
	MetricFreeDisk     = "ccrawl_free_disk_bytes"
	MetricRSS          = "ccrawl_rss_bytes"
)

// phaseBuckets covers a shard phase from a second to an hour. A markdown shard
// converts in a couple of minutes and a refetch shard can take most of an hour,
// so the tail matters more than fine resolution at the short end.
var phaseBuckets = []float64{1, 5, 15, 30, 60, 120, 300, 600, 1200, 1800, 3600}

// Metrics is a small Prometheus registry: counters, gauges and histograms
// rendered in the text exposition format.
//
// client_golang would do this too, and it would do more of it. It is a large
// dependency for twelve metrics in a binary whose selling point is that it has
// no dependencies at all, and the exposition format is a dozen lines of
// printing, so we write those instead.
//
// A nil *Metrics is a working no-op.
type Metrics struct {
	mu    sync.Mutex
	defs  map[string]*metricDef
	order []string
}

type metricDef struct {
	name    string
	help    string
	typ     string // counter, gauge, histogram
	buckets []float64
	series  map[string]*metricSeries
	order   []string
}

type metricSeries struct {
	value  float64  // counters and gauges
	counts []uint64 // histogram: one per bucket, plus the +Inf slot
	sum    float64  // histogram
	count  uint64   // histogram
}

// NewMetrics returns the registry a run publishes, with every metric declared so
// the exposition carries the right HELP and TYPE from the first scrape, before
// anything has happened.
func NewMetrics() *Metrics {
	m := &Metrics{defs: map[string]*metricDef{}}
	m.register(MetricShards, "Shards finished, by pipeline and outcome.", "counter", nil)
	m.register(MetricItems, "Files or records finished, by pipeline and outcome.", "counter", nil)
	m.register(MetricRows, "Dataset rows written.", "counter", nil)
	m.register(MetricBytes, "Bytes moved, by pipeline and kind.", "counter", nil)
	m.register(MetricPhaseSeconds, "Wall-clock seconds per shard phase.", "histogram", phaseBuckets)
	m.register(MetricInflight, "Units of work in flight right now.", "gauge", nil)
	m.register(MetricDone, "Units of work finished so far.", "gauge", nil)
	m.register(MetricTotal, "Units of work this run was asked to do.", "gauge", nil)
	m.register(MetricRate, "Units of work finished per hour, averaged over the run.", "gauge", nil)
	m.register(MetricElapsed, "Seconds since the run started.", "gauge", nil)
	m.register(MetricFreeDisk, "Free bytes on the filesystem the run writes to.", "gauge", nil)
	m.register(MetricRSS, "Resident set size of the ccrawl process, in bytes.", "gauge", nil)
	return m
}

func (m *Metrics) register(name, help, typ string, buckets []float64) {
	m.defs[name] = &metricDef{
		name:    name,
		help:    help,
		typ:     typ,
		buckets: buckets,
		series:  map[string]*metricSeries{},
	}
	m.order = append(m.order, name)
}

// Add increments a counter. Labels are alternating names and values.
func (m *Metrics) Add(name string, v float64, labels ...string) {
	m.with(name, labels, func(s *metricSeries) { s.value += v })
}

// Set writes a gauge.
func (m *Metrics) Set(name string, v float64, labels ...string) {
	m.with(name, labels, func(s *metricSeries) { s.value = v })
}

// Observe records one histogram sample.
func (m *Metrics) Observe(name string, v float64, labels ...string) {
	m.with(name, labels, func(s *metricSeries) {
		d := m.defs[name]
		for i, b := range d.buckets {
			if v <= b {
				s.counts[i]++
			}
		}
		s.counts[len(d.buckets)]++ // +Inf
		s.sum += v
		s.count++
	})
}

// Value reads one series back, for tests and for the summary a run prints.
func (m *Metrics) Value(name string, labels ...string) float64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	d := m.defs[name]
	if d == nil {
		return 0
	}
	s := d.series[renderLabels(labels)]
	if s == nil {
		return 0
	}
	if d.typ == "histogram" {
		return s.sum
	}
	return s.value
}

// with locates (or creates) one series and applies f under the lock. An unknown
// metric name is dropped: every name is a constant in this file, so a miss is a
// typo, and a typo should not take a 72 hour run down.
func (m *Metrics) with(name string, labels []string, f func(*metricSeries)) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	d := m.defs[name]
	if d == nil {
		return
	}
	key := renderLabels(labels)
	s := d.series[key]
	if s == nil {
		s = &metricSeries{}
		if d.typ == "histogram" {
			s.counts = make([]uint64, len(d.buckets)+1)
		}
		d.series[key] = s
		d.order = append(d.order, key)
		sort.Strings(d.order)
	}
	f(s)
}

// renderLabels turns alternating names and values into the exposition label set,
// sorted by name so the same labels always render the same way. An odd trailing
// name is dropped rather than printed with a missing value.
func renderLabels(labels []string) string {
	if len(labels) < 2 {
		return ""
	}
	pairs := make([]string, 0, len(labels)/2)
	for i := 0; i+1 < len(labels); i += 2 {
		pairs = append(pairs, labels[i]+`="`+escapeLabel(labels[i+1])+`"`)
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

// escapeLabel escapes the three characters the exposition format reserves inside
// a label value.
func escapeLabel(v string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(v)
}

// WriteTo renders the registry in the Prometheus text exposition format.
func (m *Metrics) WriteTo(w io.Writer) (int64, error) {
	if m == nil {
		return 0, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	var b strings.Builder
	for _, name := range m.order {
		d := m.defs[name]
		if len(d.series) == 0 {
			continue
		}
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", d.name, d.help, d.name, d.typ)
		for _, key := range d.order {
			s := d.series[key]
			if d.typ != "histogram" {
				fmt.Fprintf(&b, "%s%s %s\n", d.name, braces(key), formatFloat(s.value))
				continue
			}
			for i, bucket := range d.buckets {
				fmt.Fprintf(&b, "%s_bucket%s %d\n", d.name,
					braces(withLabel(key, "le", formatFloat(bucket))), s.counts[i])
			}
			fmt.Fprintf(&b, "%s_bucket%s %d\n", d.name,
				braces(withLabel(key, "le", "+Inf")), s.counts[len(d.buckets)])
			fmt.Fprintf(&b, "%s_sum%s %s\n", d.name, braces(key), formatFloat(s.sum))
			fmt.Fprintf(&b, "%s_count%s %d\n", d.name, braces(key), s.count)
		}
	}
	n, err := io.WriteString(w, b.String())
	return int64(n), err
}

func braces(labels string) string {
	if labels == "" {
		return ""
	}
	return "{" + labels + "}"
}

// withLabel appends one label to a rendered set. le goes last by convention, and
// Prometheus does not care about label order on the wire.
func withLabel(labels, name, value string) string {
	pair := name + `="` + escapeLabel(value) + `"`
	if labels == "" {
		return pair
	}
	return labels + "," + pair
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// Handler serves the registry at whatever path it is mounted on.
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = m.WriteTo(w)
	})
}

// ServeMetrics starts an HTTP server exposing m at /metrics on addr. It binds
// before returning, so a busy port or a bad address fails the command at
// startup instead of silently in a goroutine an hour later. The caller closes
// the returned server when the run ends.
//
// The returned server's Addr is the address that was actually bound, which is
// what port 0 resolved to when the caller asked for one.
func ServeMetrics(addr string, m *Metrics) (*http.Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("--metrics-addr %s: %w", addr, err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ccrawl metrics are at /metrics\n")
	})
	srv := &http.Server{Addr: ln.Addr().String(), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	return srv, nil
}

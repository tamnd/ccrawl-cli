package ccrawl

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ProgressMode selects how a long running command narrates itself on stderr.
type ProgressMode string

const (
	// ProgressText writes one human readable line per update.
	ProgressText ProgressMode = "text"
	// ProgressJSON writes the same events the journal records, one JSON object
	// per line, so whatever collects the logs does not need a regex.
	ProgressJSON ProgressMode = "json"
	// ProgressNone says nothing. The journal is still written.
	ProgressNone ProgressMode = "none"
)

// ParseProgressMode resolves the --progress flag. An empty value picks text on a
// terminal and json anywhere else, which is what a log file or a supervisor
// wants without having to be told.
func ParseProgressMode(s string, tty bool) (ProgressMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "auto":
		if tty {
			return ProgressText, nil
		}
		return ProgressJSON, nil
	case "text":
		return ProgressText, nil
	case "json":
		return ProgressJSON, nil
	case "none", "off":
		return ProgressNone, nil
	default:
		return "", fmt.Errorf("invalid --progress %q, want text, json, or none", s)
	}
}

// Event kinds written to the journal.
const (
	EventStart = "start" // the run began, with what it was asked to do
	EventShard = "shard" // one shard finished, committed, skipped, or failed
	EventItem  = "item"  // one file or record finished, for the streaming commands
	EventPhase = "phase" // a multi-phase command entered a new phase
	EventTick  = "tick"  // periodic snapshot of rates and resources
	EventEnd   = "end"   // the run finished, with the final totals
)

// Outcome values for the status field.
const (
	StatusOK      = "ok"
	StatusFailed  = "failed"
	StatusSkipped = "skipped"
)

// RunEvent is one line of run.jsonl.
//
// Four fields are always there: ts, event, run and pipeline. The rest depend on
// the event. A shard or item event carries one unit of work's outcome, a tick
// carries the run's live rates and resource use, and start and end bracket the
// whole thing. Everything a long run needs to answer afterwards is a jq filter
// over this file:
//
//	which shards failed
//	  jq -r 'select(.event=="shard" and .status=="failed") | "\(.shard) \(.error)"' run.jsonl
//	when the rate dropped
//	  jq -r 'select(.event=="tick") | [.ts, .rate_per_hour, .inflight] | @tsv' run.jsonl
type RunEvent struct {
	TS       string `json:"ts"`
	Event    string `json:"event"`
	Run      string `json:"run"`
	Pipeline string `json:"pipeline"`

	Crawl string `json:"crawl,omitempty"`
	Phase string `json:"phase,omitempty"`
	Name  string `json:"name,omitempty"` // the file or URL an item event is about

	// Shard is a pointer because shard 0 is a real shard and omitempty would
	// drop it from the line.
	Shard  *int   `json:"shard,omitempty"`
	Status string `json:"status,omitempty"`
	Error  string `json:"error,omitempty"`

	Rows         int64 `json:"rows,omitempty"`
	Bytes        int64 `json:"bytes,omitempty"`
	WARCBytes    int64 `json:"warc_bytes,omitempty"`
	HTMLBytes    int64 `json:"html_bytes,omitempty"`
	MDBytes      int64 `json:"md_bytes,omitempty"`
	ParquetBytes int64 `json:"parquet_bytes,omitempty"`

	ExtractS float64 `json:"extract_s,omitempty"`
	FetchS   float64 `json:"fetch_s,omitempty"`
	ConvertS float64 `json:"convert_s,omitempty"`
	ExportS  float64 `json:"export_s,omitempty"`
	PublishS float64 `json:"publish_s,omitempty"`

	Done      int   `json:"done,omitempty"`
	Total     int   `json:"total,omitempty"`
	Committed int   `json:"committed,omitempty"`
	Skipped   int   `json:"skipped,omitempty"`
	Failed    int   `json:"failed,omitempty"` // units of work that failed, never URLs
	Inflight  int64 `json:"inflight,omitempty"`
	// FetchFailed counts URLs a refetch shard could not fetch. It is deliberately
	// not folded into Failed: a shard with a thousand dead hosts still succeeded.
	FetchFailed int64 `json:"fetch_failed,omitempty"`

	Rate     float64 `json:"rate_per_hour,omitempty"`
	ETAS     float64 `json:"eta_s,omitempty"`
	ElapsedS float64 `json:"elapsed_s,omitempty"`
	FreeDisk int64   `json:"free_disk_bytes,omitempty"`
	RSS      int64   `json:"rss_bytes,omitempty"`
}

// Journal appends run events to a JSON Lines file. It is safe for concurrent
// use, and every event reaches the kernel before Write returns, so a run that is
// killed still leaves a journal that reads to the last thing that happened.
//
// A nil *Journal is a working no-op, so callers never have to check.
type Journal struct {
	mu   sync.Mutex
	f    *os.File
	enc  *json.Encoder
	path string
}

// OpenJournal opens the journal at path, creating the directory if it is not
// there and appending if the file already exists. Resuming a run appends to the
// same file; the run id on every event keeps the attempts apart.
func OpenJournal(path string) (*Journal, error) {
	if path == "" {
		return nil, nil
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Journal{f: f, enc: json.NewEncoder(f), path: path}, nil
}

// Write appends one event.
func (j *Journal) Write(ev RunEvent) error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.enc.Encode(ev)
}

// Path is where the journal is being written, or empty when there is none.
func (j *Journal) Path() string {
	if j == nil {
		return ""
	}
	return j.path
}

// Close closes the underlying file.
func (j *Journal) Close() error {
	if j == nil || j.f == nil {
		return nil
	}
	return j.f.Close()
}

// RunReporter fans one event out to the three places a long run reports to: the
// journal on disk, the progress stream on stderr, and the metrics a scraper
// reads. Commands report once and every sink gets the right shape.
//
// A nil *RunReporter is a working no-op.
type RunReporter struct {
	pipeline string
	mode     ProgressMode
	journal  *Journal
	metrics  *Metrics
	runID    string

	mu  sync.Mutex
	out io.Writer
}

// NewRunReporter builds a reporter for one command run. The journal and the
// metrics registry may both be nil, which turns those sinks off.
func NewRunReporter(pipeline string, mode ProgressMode, j *Journal, m *Metrics) *RunReporter {
	return &RunReporter{
		pipeline: pipeline,
		mode:     mode,
		journal:  j,
		metrics:  m,
		runID:    newRunID(time.Now()),
		out:      os.Stderr,
	}
}

// newRunID names one attempt at a run. Wall-clock plus pid is enough to tell two
// attempts apart in a journal they both appended to, and it sorts.
func newRunID(t time.Time) string {
	return t.UTC().Format("20060102T150405Z") + "-" + strconv.Itoa(os.Getpid())
}

// orDefault returns r, or a plain text reporter on stderr when the caller did
// not configure one. Library callers that never set a reporter keep the stderr
// narration the pipelines have always had.
func (r *RunReporter) orDefault(pipeline string) *RunReporter {
	if r != nil {
		return r
	}
	return NewRunReporter(pipeline, ProgressText, nil, nil)
}

// SetOutput redirects the progress stream, for tests.
func (r *RunReporter) SetOutput(w io.Writer) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.out = w
}

// RunID identifies this attempt, and is stamped on every event it writes.
func (r *RunReporter) RunID() string {
	if r == nil {
		return ""
	}
	return r.runID
}

// JournalPath is where events are being recorded, or empty when nowhere.
func (r *RunReporter) JournalPath() string {
	if r == nil {
		return ""
	}
	return r.journal.Path()
}

// Mode is the resolved progress mode.
func (r *RunReporter) Mode() ProgressMode {
	if r == nil {
		return ProgressNone
	}
	return r.mode
}

// Metrics is the registry this reporter updates, or nil when metrics are off.
func (r *RunReporter) Metrics() *Metrics {
	if r == nil {
		return nil
	}
	return r.metrics
}

// Event stamps an event and hands it to every sink.
func (r *RunReporter) Event(ev RunEvent) {
	if r == nil {
		return
	}
	ev.TS = time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	ev.Run = r.runID
	if ev.Pipeline == "" {
		ev.Pipeline = r.pipeline
	}
	_ = r.journal.Write(ev)
	r.record(ev)
	if r.mode == ProgressJSON {
		r.mu.Lock()
		_ = json.NewEncoder(r.out).Encode(ev)
		r.mu.Unlock()
	}
}

// Textf writes a human readable progress line, and only in text mode. The
// machine modes get the same information from the matching Event, so a caller
// reports each thing once and both modes come out right.
func (r *RunReporter) Textf(format string, a ...any) {
	if r == nil || r.mode != ProgressText {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, _ = fmt.Fprintf(r.out, format, a...)
}

// record folds an event into the metrics registry.
func (r *RunReporter) record(ev RunEvent) {
	m := r.metrics
	if m == nil {
		return
	}
	p := ev.Pipeline
	switch ev.Event {
	case EventShard, EventItem:
		status := ev.Status
		if status == "" {
			status = StatusOK
		}
		unit := MetricShards
		if ev.Event == EventItem {
			unit = MetricItems
		}
		m.Add(unit, 1, "pipeline", p, "status", status)
		if ev.Rows > 0 {
			m.Add(MetricRows, float64(ev.Rows), "pipeline", p)
		}
		for _, b := range []struct {
			kind string
			n    int64
		}{
			{"fetched", ev.Bytes},
			{"warc", ev.WARCBytes},
			{"html", ev.HTMLBytes},
			{"markdown", ev.MDBytes},
			{"parquet", ev.ParquetBytes},
		} {
			if b.n > 0 {
				m.Add(MetricBytes, float64(b.n), "pipeline", p, "kind", b.kind)
			}
		}
		for _, d := range []struct {
			phase string
			s     float64
		}{
			{"extract", ev.ExtractS},
			{"fetch", ev.FetchS},
			{"convert", ev.ConvertS},
			{"export", ev.ExportS},
			{"publish", ev.PublishS},
		} {
			if d.s > 0 {
				m.Observe(MetricPhaseSeconds, d.s, "pipeline", p, "phase", d.phase)
			}
		}
	case EventTick, EventEnd:
		m.Set(MetricInflight, float64(ev.Inflight), "pipeline", p)
		m.Set(MetricRate, ev.Rate, "pipeline", p)
		m.Set(MetricElapsed, ev.ElapsedS, "pipeline", p)
		m.Set(MetricDone, float64(ev.Done), "pipeline", p)
		if ev.Total > 0 {
			m.Set(MetricTotal, float64(ev.Total), "pipeline", p)
		}
		if ev.FreeDisk > 0 {
			m.Set(MetricFreeDisk, float64(ev.FreeDisk), "pipeline", p)
		}
		if ev.RSS > 0 {
			m.Set(MetricRSS, float64(ev.RSS))
		}
	}
}

// Close releases the journal file.
func (r *RunReporter) Close() error {
	if r == nil {
		return nil
	}
	return r.journal.Close()
}

// DefaultTickInterval is how often a run writes a tick event when the caller
// does not pick something else. Half a minute is often enough to see a stall on
// a 72 hour run and rare enough that the journal stays small.
const DefaultTickInterval = 30 * time.Second

// StreamProgress reports a counting command (download, export, host enrich,
// crawl seed) on the same schedule and in the same event shape as the shard
// pipelines, so one jq query works across all of them. Every method is safe to
// call from several goroutines.
type StreamProgress struct {
	r        *RunReporter
	unit     string // what one item is, for the text line: "files", "records"
	start    time.Time
	interval time.Duration

	// total is atomic because a command that has to read its input before it
	// knows the size of the job (export reading locations on stdin) fills it in
	// after the ticker is already running.
	total  atomic.Int64
	done   atomic.Int64
	failed atomic.Int64
	bytes  atomic.Int64
	rows   atomic.Int64
	phase  atomic.Pointer[string]

	stopOnce sync.Once
	stop     chan struct{}
	ticked   chan struct{}
}

// StartStreamProgress writes the start event and begins ticking. total is 0 when
// the size of the job is not known up front, which turns the ETA off. The caller
// must call Stop, which writes the end event.
func StartStreamProgress(r *RunReporter, unit string, total int, interval time.Duration) *StreamProgress {
	if interval <= 0 {
		interval = DefaultTickInterval
	}
	p := &StreamProgress{
		r:        r.orDefault("stream"),
		unit:     unit,
		start:    time.Now(),
		interval: interval,
		stop:     make(chan struct{}),
		ticked:   make(chan struct{}),
	}
	p.total.Store(int64(total))
	p.r.Event(RunEvent{Event: EventStart, Total: total})
	go p.loop()
	return p
}

// SetTotal fills in the size of the job for a command that only learns it after
// the run has started, which turns the ETA on from the next tick.
func (p *StreamProgress) SetTotal(total int) {
	if p == nil {
		return
	}
	p.total.Store(int64(total))
}

// Add records progress: n items finished, carrying rows and bytes between them.
// It does not write an event, so a command that handles millions of records can
// call it per record and still get a journal of a sane size.
func (p *StreamProgress) Add(n, rows, bytes int64) {
	if p == nil {
		return
	}
	p.done.Add(n)
	p.rows.Add(rows)
	p.bytes.Add(bytes)
}

// Item records one named unit of work, with an event of its own. Use it where
// the count is bounded (files in a download) and skip it where it is not.
func (p *StreamProgress) Item(name string, bytes int64, err error) {
	if p == nil {
		return
	}
	ev := RunEvent{Event: EventItem, Name: name, Bytes: bytes, Status: StatusOK}
	if err != nil {
		ev.Status = StatusFailed
		ev.Error = err.Error()
		p.failed.Add(1)
	} else {
		p.done.Add(1)
		p.bytes.Add(bytes)
	}
	ev.Done = int(p.done.Load())
	ev.Total = int(p.total.Load())
	p.r.Event(ev)
}

// Fail counts one failure without an item event.
func (p *StreamProgress) Fail() {
	if p == nil {
		return
	}
	p.failed.Add(1)
}

// Phase names the stage the command is in, for the commands that have more than
// one. It writes a phase event and shows up on every tick after it.
func (p *StreamProgress) Phase(name string) {
	if p == nil {
		return
	}
	p.phase.Store(&name)
	p.r.Event(RunEvent{Event: EventPhase, Phase: name, Done: int(p.done.Load())})
	p.r.Textf("%s\n", name)
}

// Stop ends the ticker and writes the end event. It is safe to call twice.
func (p *StreamProgress) Stop() {
	if p == nil {
		return
	}
	p.stopOnce.Do(func() {
		close(p.stop)
		<-p.ticked
		p.emit(EventEnd)
	})
}

func (p *StreamProgress) loop() {
	defer close(p.ticked)
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
			p.emit(EventTick)
		}
	}
}

// emit builds one tick or end event from the live counters.
func (p *StreamProgress) emit(kind string) {
	done := p.done.Load()
	total := int(p.total.Load())
	elapsed := time.Since(p.start)
	ev := RunEvent{
		Event:    kind,
		Done:     int(done),
		Total:    total,
		Failed:   int(p.failed.Load()),
		Rows:     p.rows.Load(),
		Bytes:    p.bytes.Load(),
		ElapsedS: elapsed.Seconds(),
		RSS:      currentRSSBytes(),
	}
	if ph := p.phase.Load(); ph != nil {
		ev.Phase = *ph
	}
	if elapsed > 0 {
		ev.Rate = float64(done) / elapsed.Hours()
	}
	if total > 0 && ev.Rate > 0 && int(done) < total {
		ev.ETAS = float64(total-int(done)) / ev.Rate * 3600
	}
	p.r.Event(ev)

	of := ""
	if total > 0 {
		of = fmt.Sprintf("/%d", total)
	}
	eta := ""
	if ev.ETAS > 0 {
		eta = " | ETA " + fmtETA(time.Duration(ev.ETAS*float64(time.Second)))
	}
	p.r.Textf("%d%s %s | %s | %.0f %s/hour%s\n",
		done, of, p.unit, fmtBytes(ev.Bytes), ev.Rate, p.unit, eta)
}

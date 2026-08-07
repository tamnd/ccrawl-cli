package ccrawl

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestParseProgressMode(t *testing.T) {
	cases := []struct {
		in   string
		tty  bool
		want ProgressMode
		bad  bool
	}{
		{in: "", tty: true, want: ProgressText},
		{in: "", tty: false, want: ProgressJSON},
		{in: "auto", tty: true, want: ProgressText},
		{in: "AUTO", tty: false, want: ProgressJSON},
		{in: "text", tty: false, want: ProgressText},
		{in: " json ", tty: true, want: ProgressJSON},
		{in: "none", tty: true, want: ProgressNone},
		{in: "off", tty: true, want: ProgressNone},
		{in: "verbose", bad: true},
	}
	for _, c := range cases {
		got, err := ParseProgressMode(c.in, c.tty)
		if c.bad {
			if err == nil {
				t.Fatalf("ParseProgressMode(%q) = %q, want an error", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("ParseProgressMode(%q, %v): %v", c.in, c.tty, err)
		}
		if got != c.want {
			t.Fatalf("ParseProgressMode(%q, %v) = %q, want %q", c.in, c.tty, got, c.want)
		}
	}
}

// readJournal parses every line of a journal file back into events.
func readJournal(t *testing.T, path string) []RunEvent {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	defer func() { _ = f.Close() }()
	var out []RunEvent
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var ev RunEvent
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			t.Fatalf("journal line %q: %v", sc.Text(), err)
		}
		out = append(out, ev)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan journal: %v", err)
	}
	return out
}

func TestJournalWritesOneLinePerEvent(t *testing.T) {
	// A nested directory, so the mkdir path is covered too.
	path := filepath.Join(t.TempDir(), "runs", "run.jsonl")
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	rep := NewRunReporter("markdown export", ProgressNone, j, nil)

	shard := 0
	rep.Event(RunEvent{Event: EventStart, Total: 2})
	rep.Event(RunEvent{Event: EventShard, Shard: &shard, Status: StatusOK, Rows: 1200})
	shard2 := 7
	rep.Event(RunEvent{Event: EventShard, Shard: &shard2, Status: StatusFailed, Error: "connection reset"})
	rep.Event(RunEvent{Event: EventEnd, Done: 1, Failed: 1})
	if err := rep.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	evs := readJournal(t, path)
	if len(evs) != 4 {
		t.Fatalf("journal has %d events, want 4", len(evs))
	}
	for _, ev := range evs {
		if ev.TS == "" || ev.Run == "" {
			t.Fatalf("event %q missing ts or run: %+v", ev.Event, ev)
		}
		if ev.Pipeline != "markdown export" {
			t.Fatalf("event %q pipeline = %q, want the reporter's", ev.Event, ev.Pipeline)
		}
		if ev.Run != rep.RunID() {
			t.Fatalf("event %q run = %q, want %q", ev.Event, ev.Run, rep.RunID())
		}
	}

	// This is the query the runbook tells an operator to reach for.
	if evs[2].Event != EventShard || evs[2].Status != StatusFailed || evs[2].Error != "connection reset" {
		t.Fatalf("failed shard event wrong: %+v", evs[2])
	}
	if evs[2].Shard == nil || *evs[2].Shard != 7 {
		t.Fatalf("failed shard index wrong: %+v", evs[2].Shard)
	}
	// Shard 0 is a real shard, so it has to survive the round trip.
	if evs[1].Shard == nil || *evs[1].Shard != 0 {
		t.Fatalf("shard 0 dropped from the line: %+v", evs[1])
	}
}

func TestJournalAppendsAcrossRuns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.jsonl")
	var ids []string
	for i := 0; i < 2; i++ {
		j, err := OpenJournal(path)
		if err != nil {
			t.Fatalf("OpenJournal: %v", err)
		}
		rep := NewRunReporter("refetch", ProgressNone, j, nil)
		rep.Event(RunEvent{Event: EventStart})
		ids = append(ids, rep.RunID())
		if err := rep.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	evs := readJournal(t, path)
	if len(evs) != 2 {
		t.Fatalf("resumed journal has %d events, want 2", len(evs))
	}
	if evs[0].Run != ids[0] || evs[1].Run != ids[1] {
		t.Fatalf("run ids not carried through: %q %q", evs[0].Run, evs[1].Run)
	}
}

func TestNilJournalAndReporterAreNoOps(t *testing.T) {
	var j *Journal
	if err := j.Write(RunEvent{Event: EventTick}); err != nil {
		t.Fatalf("nil journal Write: %v", err)
	}
	if p := j.Path(); p != "" {
		t.Fatalf("nil journal Path = %q, want empty", p)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("nil journal Close: %v", err)
	}

	opened, err := OpenJournal("")
	if err != nil || opened != nil {
		t.Fatalf("OpenJournal(\"\") = %v, %v, want nil, nil", opened, err)
	}

	var r *RunReporter
	r.Event(RunEvent{Event: EventEnd})
	r.Textf("nothing %d\n", 1)
	if r.Mode() != ProgressNone || r.RunID() != "" || r.JournalPath() != "" || r.Metrics() != nil {
		t.Fatal("nil reporter did not behave as a no-op")
	}
	if got := r.orDefault("stream"); got == nil || got.Mode() != ProgressText {
		t.Fatal("orDefault did not build a text reporter")
	}
}

func TestReporterTextModeWritesOnlyText(t *testing.T) {
	var buf bytes.Buffer
	rep := NewRunReporter("download", ProgressText, nil, nil)
	rep.SetOutput(&buf)
	rep.Event(RunEvent{Event: EventItem, Name: "a.warc.gz"})
	rep.Textf("[1/2] ok a.warc.gz\n")
	if got := buf.String(); got != "[1/2] ok a.warc.gz\n" {
		t.Fatalf("text mode wrote %q", got)
	}

	buf.Reset()
	rep = NewRunReporter("download", ProgressJSON, nil, nil)
	rep.SetOutput(&buf)
	rep.Textf("this line is for humans\n")
	rep.Event(RunEvent{Event: EventItem, Name: "a.warc.gz"})
	out := buf.String()
	if strings.Contains(out, "for humans") {
		t.Fatalf("json mode leaked a text line: %q", out)
	}
	var ev RunEvent
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &ev); err != nil {
		t.Fatalf("json mode line %q: %v", out, err)
	}
	if ev.Name != "a.warc.gz" || ev.Pipeline != "download" {
		t.Fatalf("json mode event wrong: %+v", ev)
	}

	buf.Reset()
	rep = NewRunReporter("download", ProgressNone, nil, nil)
	rep.SetOutput(&buf)
	rep.Textf("quiet\n")
	rep.Event(RunEvent{Event: EventItem})
	if buf.Len() != 0 {
		t.Fatalf("none mode wrote %q", buf.String())
	}
}

func TestStreamProgressCountsAndBrackets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.jsonl")
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	rep := NewRunReporter("download", ProgressNone, j, NewMetrics())

	// A tick interval short enough that the run cannot finish without one.
	sp := StartStreamProgress(rep, "files", 3, 10*time.Millisecond)
	sp.Item("a.warc.gz", 100, nil)
	sp.Item("b.warc.gz", 0, os.ErrNotExist)
	sp.Add(1, 42, 7)
	time.Sleep(40 * time.Millisecond)
	sp.Stop()
	sp.Stop() // idempotent, and must not write a second end event
	if err := rep.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	evs := readJournal(t, path)
	var start, end, ticks int
	for _, ev := range evs {
		switch ev.Event {
		case EventStart:
			start++
			if ev.Total != 3 {
				t.Fatalf("start total = %d, want 3", ev.Total)
			}
		case EventEnd:
			end++
			if ev.Done != 2 || ev.Failed != 1 {
				t.Fatalf("end done=%d failed=%d, want 2 and 1", ev.Done, ev.Failed)
			}
			if ev.Rows != 42 || ev.Bytes != 107 {
				t.Fatalf("end rows=%d bytes=%d, want 42 and 107", ev.Rows, ev.Bytes)
			}
			if ev.ElapsedS <= 0 {
				t.Fatalf("end elapsed_s = %v, want positive", ev.ElapsedS)
			}
		case EventTick:
			ticks++
		}
	}
	if start != 1 || end != 1 {
		t.Fatalf("start=%d end=%d, want one of each", start, end)
	}
	if ticks == 0 {
		t.Fatal("no tick events, the ticker never ran")
	}

	m := rep.Metrics()
	if got := m.Value(MetricItems, "pipeline", "download", "status", StatusOK); got != 1 {
		t.Fatalf("ok items = %v, want 1", got)
	}
	if got := m.Value(MetricItems, "pipeline", "download", "status", StatusFailed); got != 1 {
		t.Fatalf("failed items = %v, want 1", got)
	}
}

func TestStreamProgressSetTotalTurnsOnETA(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.jsonl")
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	rep := NewRunReporter("export", ProgressNone, j, nil)

	sp := StartStreamProgress(rep, "records", 0, time.Hour)
	sp.SetTotal(100)
	sp.Add(1, 0, 0)
	sp.Stop()
	if err := rep.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	evs := readJournal(t, path)
	end := evs[len(evs)-1]
	if end.Event != EventEnd {
		t.Fatalf("last event is %q, want end", end.Event)
	}
	if end.Total != 100 {
		t.Fatalf("end total = %d, want the total set after the start", end.Total)
	}
	if end.ETAS <= 0 {
		t.Fatalf("end eta_s = %v, want positive once the total is known", end.ETAS)
	}
}

func TestStreamProgressPhaseEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.jsonl")
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	rep := NewRunReporter("host enrich", ProgressNone, j, nil)

	sp := StartStreamProgress(rep, "hosts", 0, time.Hour)
	sp.Phase("vertices")
	sp.Add(2, 2, 0)
	sp.Phase("rank join")
	sp.Stop()
	if err := rep.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	evs := readJournal(t, path)
	var phases []string
	for _, ev := range evs {
		if ev.Event == EventPhase {
			phases = append(phases, ev.Phase)
		}
	}
	if len(phases) != 2 || phases[0] != "vertices" || phases[1] != "rank join" {
		t.Fatalf("phase events = %v, want vertices then rank join", phases)
	}
	// The phase sticks, so a tick or end event says where a stalled run is.
	if end := evs[len(evs)-1]; end.Phase != "rank join" {
		t.Fatalf("end phase = %q, want the last phase entered", end.Phase)
	}
}

func TestCurrentRSSBytes(t *testing.T) {
	got := currentRSSBytes()
	switch runtime.GOOS {
	case "linux", "darwin":
		if got <= 0 {
			t.Fatalf("currentRSSBytes on %s = %d, want positive", runtime.GOOS, got)
		}
		// A Go test binary is megabytes, not gigabytes. This catches a unit
		// mix-up, which is the failure this reader is actually prone to.
		if got < 1<<20 || got > 8<<30 {
			t.Fatalf("currentRSSBytes = %d bytes, outside a believable range", got)
		}
	default:
		if got != 0 {
			t.Fatalf("currentRSSBytes on %s = %d, want 0", runtime.GOOS, got)
		}
	}
}

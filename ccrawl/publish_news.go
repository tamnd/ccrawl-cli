package ccrawl

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/ccrawl-cli/pkg/warc"
)

// NewsPublishOptions configures a ccrawl-news publish run.
type NewsPublishOptions struct {
	Repo        string   // target dataset repo, org/name
	Months      []string // months to build, "YYYY/MM", newest first
	StageDir    string   // local staging root
	CommitEvery int      // shards per commit
	Workers     int      // stream-and-index workers
	Files       int      // cap on source files per month, 0 for the whole month
	Private     bool     // create the repo private
	Keep        bool     // keep local shards after commit
	DoCommit    bool     // false is a dry run: stage and print, never touch the hub
	MinFreeGB   int      // free-disk floor gating new downloads
	MaxStall    time.Duration
	Logf        func(string, ...any)
}

// newsAttempts is how many times a worker re-opens a source WARC before giving
// up on it for this run. Each retry resumes at the last complete record rather
// than restarting the file, so the attempts are cheap after the first.
const newsAttempts = 4

// newsFileJob is one source WARC to index into an output shard.
type newsFileJob struct {
	index     int
	sourceURL string
	warcPath  string // the path as it appears in warc.paths, which is what a row records
	repoPath  string // data/YYYY/MM/CC-NEWS-<stamp>-<seq>.parquet
	tmpPath   string
	outPath   string
}

// newsShardStat is what one source WARC contributed, beyond the row and byte
// counts the committer already tracks.
type newsShardStat struct {
	SourceBytes int64
	Rows2xx     int64
	RowsHTML    int64
	Langs       map[string]int64
}

// PublishNews runs the ccrawl-news pipeline: for each month it streams every
// CC-NEWS WARC, turns each stored response into an index row, and commits one
// Parquet shard per source file to the hub.
//
// It is the odd one out among the publish pipelines. The URL and domain datasets
// republish something Common Crawl already computed, so their cost is bandwidth
// on an index that already exists. CC-NEWS has no index of any kind, so the only
// way to learn where a record sits is to read the archive that holds it: a month
// is around 350 files of roughly a gigabyte each, and all of it has to go past
// the CPU once. That is the whole point of publishing the result. It is a few
// hundred gigabytes of reading that nobody else then has to repeat.
//
// Like the others it resumes from remote truth (shards already on the hub are
// skipped) and keeps local disk flat by deleting each shard once it commits. The
// source WARCs are never written to disk at all; they are indexed as they stream.
func PublishNews(ctx context.Context, h *HTTPClient, hf *HFClient, o NewsPublishOptions) error {
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
	if o.CommitEvery <= 0 {
		o.CommitEvery = 8
	}
	if o.Workers <= 0 {
		o.Workers = budgetProcess(0)
	}
	if err := os.MkdirAll(o.StageDir, 0o755); err != nil {
		return err
	}
	sweepTemps(o.StageDir)

	statsPath := filepath.Join(o.StageDir, "stats.csv")
	langsPath := filepath.Join(o.StageDir, "languages.csv")
	progressPath := filepath.Join(o.StageDir, "publish-progress.json")

	if o.DoCommit {
		if !hf.Valid() {
			return errors.New("no HF token: set HF_TOKEN to publish")
		}
		if err := hf.CreateDatasetRepo(ctx, o.Repo, o.Private); err != nil {
			return err
		}
		// Seed the local ledgers from the hub so rollups stay correct across
		// machines. A missing file just means a fresh dataset.
		for _, seed := range []struct{ remote, local string }{
			{"stats.csv", statsPath},
			{"languages.csv", langsPath},
		} {
			if _, err := os.Stat(seed.local); os.IsNotExist(err) {
				if _, err := hf.DownloadRepoFile(ctx, o.Repo, seed.remote, seed.local); err != nil {
					o.Logf("warning: could not seed %s from hub: %v", seed.remote, err)
				}
			}
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	clock := newStallClock(o.MaxStall, cancel)
	go clock.watch(runCtx)

	var totalNew, totalRemaining int
	for _, month := range o.Months {
		if err := runCtx.Err(); err != nil {
			break
		}
		newly, remaining, err := publishNewsMonth(runCtx, h, hf, o, month, statsPath, langsPath, progressPath, clock)
		if err != nil {
			if clock.stalled() {
				return ErrCommitStall
			}
			return fmt.Errorf("month %s: %w", month, err)
		}
		totalNew += newly
		totalRemaining += remaining
	}
	if clock.stalled() {
		return ErrCommitStall
	}

	if o.DoCommit && totalRemaining > 0 {
		if totalNew > 0 {
			o.Logf("incomplete: %d shard(s) still missing, %d committed this run; exiting for a supervised retry", totalRemaining, totalNew)
		} else {
			o.Logf("incomplete: %d shard(s) still missing and none committed this run; not retrying. Re-run `ccrawl news publish` to fill the gap.", totalRemaining)
		}
	}
	return incompleteAction(o.DoCommit, totalNew, totalRemaining)
}

// publishNewsMonth publishes one month and reports how many shards it newly
// committed and how many are still missing from the hub.
func publishNewsMonth(ctx context.Context, h *HTTPClient, hf *HFClient, o NewsPublishOptions, month, statsPath, langsPath, progressPath string, clock *stallClock) (newly, remaining int, err error) {
	year, mon, err := parseNewsMonth(month)
	if err != nil {
		return 0, 0, err
	}
	files, err := ListNewsFiles(ctx, h, year, mon)
	if err != nil {
		return 0, 0, err
	}
	if len(files) == 0 {
		return 0, 0, fmt.Errorf("no CC-NEWS files published for %s", month)
	}
	// monthTotal is how many files the month publishes, which is what the ledger
	// and the card mean by coverage. --files caps the work, not the month: a run
	// that indexes 2 of 353 files has indexed 2 of 353, and a ledger that called
	// that complete would tell a search it had the whole month.
	monthTotal := len(files)
	if o.Files > 0 && len(files) > o.Files {
		files = files[:o.Files]
	}
	total := len(files)
	label := newsMonthLabel(year, mon)

	monthDir := filepath.Join(o.StageDir, "data", fmt.Sprintf("%04d", year), fmt.Sprintf("%02d", mon))
	if err := os.MkdirAll(monthDir, 0o755); err != nil {
		return 0, 0, err
	}
	jobs := make([]newsFileJob, 0, total)
	repoPaths := make([]string, 0, total)
	for i, f := range files {
		// The shard is named for the WARC it indexes, so a file in the tree says
		// which archive it came from without being opened, and a resume matches on
		// the source rather than on a position in a manifest that grows all month.
		base := strings.TrimSuffix(path.Base(f.Path), ".warc.gz")
		repoPath := fmt.Sprintf("data/%04d/%02d/%s.parquet", year, mon, base)
		repoPaths = append(repoPaths, repoPath)
		jobs = append(jobs, newsFileJob{
			index:     i,
			sourceURL: h.DataURL(f.Path),
			warcPath:  f.Path,
			repoPath:  repoPath,
			tmpPath:   filepath.Join(monthDir, base+".parquet.tmp"),
			outPath:   filepath.Join(monthDir, base+".parquet"),
		})
	}

	done := map[string]bool{}
	if o.DoCommit {
		done, err = hf.PathsExist(ctx, o.Repo, repoPaths)
		if err != nil {
			return 0, 0, err
		}
	}
	work := make([]newsFileJob, 0, total)
	for _, j := range jobs {
		if !done[j.repoPath] {
			work = append(work, j)
		}
	}
	if total < monthTotal {
		o.Logf("month %s: %d of %d WARC files selected, %d already published, %d to do", label, total, monthTotal, total-len(work), len(work))
	} else {
		o.Logf("month %s: %d WARC files, %d already published, %d to do", label, total, total-len(work), len(work))
	}

	base := findNewsStat(statsPath, label)
	doneCount := max(len(done), base.Files)
	c := &committer{
		hf:           hf,
		repo:         o.Repo,
		scope:        label,
		kind:         "news",
		width:        5,
		commitEvery:  o.CommitEvery,
		keep:         o.Keep,
		doCommit:     o.DoCommit,
		progressKey:  label,
		progressPath: progressPath,
		clock:        clock,
		logf:         o.Logf,
		committed:    doneCount,
		rows:         base.Rows,
		bytes:        base.ParquetBytes,
	}

	// Per-shard rollups the committer knows nothing about, folded in when their
	// shards commit. The map is written by workers and read by the committer
	// goroutine, so it is guarded.
	tally := &newsTally{stats: map[string]newsShardStat{}, month: label, base: base}
	if err := tally.seed(langsPath); err != nil {
		return 0, 0, err
	}
	c.sidecar = func(shards int, rows, bytes int64, batch []shard) ([]HFOperation, error) {
		stat := tally.fold(shards, rows, bytes, batch)
		_, ops, err := refreshNewsCard(o, stat, tally.langs(), monthTotal, statsPath, langsPath)
		return ops, err
	}

	skipped := 0
	if len(work) > 0 {
		s, err := runNewsWorkers(ctx, h, o, work, c, tally)
		if err != nil {
			return 0, 0, err
		}
		skipped = s
		if err := c.flush(ctx); err != nil {
			return 0, 0, err
		}
	}

	// Each committed batch already refreshed the card through the sidecar. Only
	// commit a standalone refresh when nothing landed this run, so a resume where
	// every shard already existed still leaves the card describing the month.
	if c.flushes == 0 {
		if err := finalizeNewsMonth(ctx, hf, o, tally, monthTotal, c, statsPath, langsPath); err != nil {
			return 0, 0, err
		}
	}

	if o.DoCommit {
		newly = len(work) - skipped
		remaining = skipped
	}
	return newly, remaining, nil
}

// newsTally accumulates the month rollups that live outside the committer: the
// source bytes read, the status and content splits, and the per-language counts.
type newsTally struct {
	month string
	base  NewsMonthStat

	mu     sync.Mutex
	stats  map[string]newsShardStat // by repo path, pending commit
	langs0 map[string]int64         // committed so far, seeded from the ledger
	src    int64
	r2xx   int64
	rHTML  int64
}

// seed loads the month's already-committed rollups out of the ledgers, so a
// resumed run adds to them instead of restarting the counts at zero.
func (t *newsTally) seed(langsPath string) error {
	rows, err := ReadNewsLangs(langsPath)
	if err != nil {
		return err
	}
	t.langs0 = map[string]int64{}
	for _, r := range rows {
		if r.Month == t.month {
			t.langs0[r.Lang] = r.Rows
		}
	}
	t.src = t.base.SourceBytes
	t.r2xx = t.base.Rows2xx
	t.rHTML = t.base.RowsHTML
	return nil
}

// record files what one finished shard contributed, before it is committed.
func (t *newsTally) record(repoPath string, s newsShardStat) {
	t.mu.Lock()
	t.stats[repoPath] = s
	t.mu.Unlock()
}

// fold moves a committing batch's contributions into the month totals and
// returns the ledger row for the state the commit will leave behind.
func (t *newsTally) fold(shards int, rows, bytes int64, batch []shard) NewsMonthStat {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, s := range batch {
		st, ok := t.stats[s.RepoPath]
		if !ok {
			continue
		}
		delete(t.stats, s.RepoPath)
		t.src += st.SourceBytes
		t.r2xx += st.Rows2xx
		t.rHTML += st.RowsHTML
		for lang, n := range st.Langs {
			t.langs0[lang] += n
		}
	}
	return NewsMonthStat{
		Month:        t.month,
		Files:        shards,
		Rows:         rows,
		ParquetBytes: bytes,
		SourceBytes:  t.src,
		Rows2xx:      t.r2xx,
		RowsHTML:     t.rHTML,
	}
}

// langs returns a copy of the committed language counts.
func (t *newsTally) langs() map[string]int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]int64, len(t.langs0))
	for k, v := range t.langs0 {
		out[k] = v
	}
	return out
}

// runNewsWorkers streams the work list concurrently and feeds finished shards to
// the single committer running on this goroutine. It returns the number of files
// that were skipped because indexing them failed.
func runNewsWorkers(ctx context.Context, h *HTTPClient, o NewsPublishOptions, work []newsFileJob, c *committer, tally *newsTally) (int, error) {
	jobs := make(chan newsFileJob)
	shards := make(chan shard)

	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	var skipped atomic.Int64
	fail := func(err error) {
		once.Do(func() { firstErr = err })
	}

	for i := 0; i < o.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if ctx.Err() != nil {
					return
				}
				if err := waitForDiskFloor(ctx, o.StageDir, o.MinFreeGB, c.clock); err != nil {
					fail(err)
					return
				}
				rows, bytes, st, err := indexNewsFile(ctx, h, j)
				if err != nil {
					// One bad file is not fatal: its target still does not exist
					// on the hub, so the next run retries it. Count it so the
					// caller knows the month is not yet whole.
					o.Logf("skip %s: %v", j.repoPath, err)
					_ = os.Remove(j.tmpPath)
					skipped.Add(1)
					continue
				}
				tally.record(j.repoPath, st)
				select {
				case shards <- shard{Index: j.index, RepoPath: j.repoPath, Local: j.outPath, Rows: rows, Bytes: bytes}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, j := range work {
			select {
			case jobs <- j:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(shards)
	}()

	for s := range shards {
		if err := c.add(ctx, s); err != nil {
			fail(err)
			// Drain remaining shards so producers do not block on send.
			go func() {
				for range shards {
				}
			}()
			break
		}
	}
	return int(skipped.Load()), firstErr
}

// indexNewsFile streams one source WARC and writes its index shard. It returns
// the row count, the output size, and the rollups the card reports.
//
// The WARC is never stored: it is decompressed, indexed, and dropped as it
// arrives, so a run holds one output shard per worker on disk and nothing else.
// A stream that dies partway is resumed from the last complete record rather
// than restarted, which matters at a gigabyte a file.
func indexNewsFile(ctx context.Context, h *HTTPClient, j newsFileJob) (int64, int64, newsShardStat, error) {
	w, err := NewParquetWriter[NewsRow](j.tmpPath)
	if err != nil {
		return 0, 0, newsShardStat{}, err
	}
	st := newsShardStat{Langs: map[string]int64{}}

	var (
		next     int64 // where the next unread record starts
		total    int64 // the file's size, once a response has reported it
		lastErr  error
		complete bool
	)
	for attempt := 0; attempt < newsAttempts && !complete; attempt++ {
		if err := ctx.Err(); err != nil {
			lastErr = err
			break
		}
		var read int64
		read, total, lastErr = streamNewsRange(ctx, h, j, next, total, w, &st)
		next += read
		switch {
		case lastErr != nil:
			continue
		case total <= 0:
			// The server did not say how long the file is, so a clean end of
			// stream is the only end there is to trust.
			complete = true
		case next >= total:
			complete = true
		default:
			// The stream ended tidily on a record boundary but short of the end
			// of the file, which reads exactly like a complete WARC and is not
			// one. Ask for the rest.
			lastErr = fmt.Errorf("stream ended at %d of %d bytes", next, total)
		}
	}
	if !complete {
		_ = w.Close()
		_ = os.Remove(j.tmpPath)
		if lastErr == nil {
			lastErr = errors.New("incomplete stream")
		}
		return 0, 0, newsShardStat{}, lastErr
	}

	st.SourceBytes = next
	if err := w.Close(); err != nil {
		_ = os.Remove(j.tmpPath)
		return 0, 0, newsShardStat{}, err
	}
	if err := os.Rename(j.tmpPath, j.outPath); err != nil {
		return 0, 0, newsShardStat{}, err
	}
	fi, err := os.Stat(j.outPath)
	if err != nil {
		return 0, 0, newsShardStat{}, err
	}
	return w.Rows(), fi.Size(), st, nil
}

// streamNewsRange reads one WARC from a byte offset to wherever the stream ends,
// writing a row per stored response. It returns how many bytes it consumed and
// the file's total size, so the caller can tell a finished file from a dropped
// connection.
func streamNewsRange(ctx context.Context, h *HTTPClient, j newsFileJob, from, total int64, w *ParquetWriter[NewsRow], st *newsShardStat) (int64, int64, error) {
	resp, err := h.GetDownloadFrom(ctx, j.sourceURL, from)
	if err != nil {
		return 0, total, err
	}
	defer func() { _ = resp.Body.Close() }()
	if total <= 0 && resp.ContentLength > 0 {
		total = from + resp.ContentLength
	}

	var (
		consumed int64 // bytes whose rows the writer already holds
		pending  int64 // bytes whose rows are still in the buffer
		p2xx     int64
		phtml    int64
	)
	buf := make([]NewsRow, 0, 512)
	plangs := map[string]int64{}

	// flush hands the buffered rows to the writer and only then moves the resume
	// point and the tallies to match. Both have to move with the rows: counting a
	// record when it parses rather than when it is written is how a published
	// month came out with more 2xx rows than rows, and advancing the resume point
	// there is what lost them, because a retry started after records the shard
	// never got.
	flush := func() error {
		if len(buf) > 0 {
			if err := w.WriteRows(buf); err != nil {
				return err
			}
			buf = buf[:0]
		}
		st.Rows2xx += p2xx
		st.RowsHTML += phtml
		for k, v := range plangs {
			st.Langs[k] += v
		}
		p2xx, phtml = 0, 0
		clear(plangs)
		consumed = pending
		return nil
	}

	err = warc.IterateFrom(resp.Body, from, func(rec warc.Record) error {
		// Every record moves the pending mark, not only the ones that index, so a
		// retry never re-reads a request record it has already passed.
		pending = rec.Header.WARCOffset + rec.Header.WARCLength - from
		row, ok := NewsIndexRow(j.warcPath, rec)
		if !ok {
			return nil
		}
		if row.FetchStatus >= 200 && row.FetchStatus < 300 {
			p2xx++
		}
		if strings.Contains(row.ContentMIMEDetected, "html") {
			phtml++
		}
		if row.ContentLanguages != "" {
			plangs[row.ContentLanguages]++
		}
		buf = append(buf, row)
		if len(buf) == cap(buf) {
			return flush()
		}
		return nil
	})
	if err != nil {
		return consumed, total, err
	}
	return consumed, total, flush()
}

// refreshNewsCard rewrites the month's ledger rows and the dataset card for the
// given progress and returns the ops to commit them.
func refreshNewsCard(o NewsPublishOptions, stat NewsMonthStat, langs map[string]int64, total int, statsPath, langsPath string) (NewsMonthStat, []HFOperation, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	base := findNewsStat(statsPath, stat.Month)
	stat.TotalFiles = total
	stat.Complete = stat.Files >= total && total > 0
	stat.FirstCommitted = base.FirstCommitted
	if stat.FirstCommitted == "" {
		stat.FirstCommitted = now
	}
	stat.LastCommitted = now

	ledger, err := ReadNewsStats(statsPath)
	if err != nil {
		return stat, nil, err
	}
	ledger = UpsertNewsStat(ledger, stat)
	if err := WriteNewsStats(statsPath, ledger); err != nil {
		return stat, nil, err
	}

	langLedger, err := ReadNewsLangs(langsPath)
	if err != nil {
		return stat, nil, err
	}
	langLedger = MergeNewsLangs(langLedger, stat.Month, langs)
	if err := WriteNewsLangs(langsPath, langLedger); err != nil {
		return stat, nil, err
	}

	readmePath := filepath.Join(o.StageDir, "README.md")
	if err := os.WriteFile(readmePath, []byte(GenerateNewsREADME(o.Repo, ledger, langLedger)), 0o644); err != nil {
		return stat, nil, err
	}
	return stat, []HFOperation{
		{LocalPath: statsPath, PathInRepo: "stats.csv"},
		{LocalPath: langsPath, PathInRepo: "languages.csv"},
		{LocalPath: readmePath, PathInRepo: "README.md"},
	}, nil
}

// finalizeNewsMonth refreshes the month's ledger rows and card and commits them.
// The per-batch sidecar handles this while shards land, so it only runs when a
// run committed nothing and the card still needs to reflect the month.
func finalizeNewsMonth(ctx context.Context, hf *HFClient, o NewsPublishOptions, tally *newsTally, total int, c *committer, statsPath, langsPath string) error {
	stat, ops, err := refreshNewsCard(o, tally.fold(c.committed, c.rows, c.bytes, nil), tally.langs(), total, statsPath, langsPath)
	if err != nil {
		return err
	}

	o.Logf("month %s: %d/%d files, %s rows, %s", stat.Month, stat.Files, stat.TotalFiles, humanCountShort(stat.Rows), humanBytes(stat.ParquetBytes))

	if !o.DoCommit {
		o.Logf("[dry-run] would update ledger and card for %s", stat.Month)
		return nil
	}
	if _, err := hf.CommitWithRetry(ctx, o.Repo, finalizeNewsMessage(stat), ops, 5); err != nil {
		return err
	}
	c.clock.mark()
	return nil
}

// findNewsStat returns the ledger row for a month, or a zero row when absent.
func findNewsStat(statsPath, month string) NewsMonthStat {
	rows, err := ReadNewsStats(statsPath)
	if err != nil {
		return NewsMonthStat{Month: month}
	}
	for _, r := range rows {
		if r.Month == month {
			return r
		}
	}
	return NewsMonthStat{Month: month}
}

// parseNewsMonth reads a "YYYY/MM" or "YYYY-MM" month selector.
func parseNewsMonth(s string) (year, month int, err error) {
	f := strings.FieldsFunc(strings.TrimSpace(s), func(r rune) bool { return r == '/' || r == '-' })
	if len(f) != 2 {
		return 0, 0, fmt.Errorf("bad month %q, want YYYY/MM", s)
	}
	year, month = atoi(f[0]), atoi(f[1])
	if year < 2016 || month < 1 || month > 12 {
		return 0, 0, fmt.Errorf("bad month %q, want YYYY/MM", s)
	}
	return year, month, nil
}

// newsMonthLabel is how a month is spelled in the ledger and the card.
func newsMonthLabel(year, month int) string {
	return fmt.Sprintf("%04d-%02d", year, month)
}

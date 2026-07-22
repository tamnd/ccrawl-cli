package ccrawl

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPartIndexFromURL(t *testing.T) {
	cases := map[string]int{
		"https://data.commoncrawl.org/cc-index/table/cc-main/warc/crawl=CC-MAIN-2026-25/subset=warc/part-00000-b13edba3-e431-43c6-8915-a9f1c955272b.c000.gz.parquet": 0,
		"https://x/part-00042-uuid.c000.gz.parquet": 42,
		"https://x/part-00299-uuid.c000.gz.parquet": 299,
	}
	for url, want := range cases {
		got, ok := partIndexFromURL(url)
		if !ok || got != want {
			t.Errorf("partIndexFromURL(%q) = %d,%v want %d,true", url, got, ok, want)
		}
	}
	if _, ok := partIndexFromURL("https://x/nopart.parquet"); ok {
		t.Error("expected no match for a URL without a part index")
	}
}

func TestParseDomainLine(t *testing.T) {
	// harmonicc_pos harmonicc_val pr_pos pr_val host_rev n_hosts
	row, ok := parseDomainLine("1\t0.95\t3\t0.0001\tcom.example\t7")
	if !ok {
		t.Fatal("expected a parse")
	}
	if row.Domain != "example.com" {
		t.Errorf("domain = %q want example.com", row.Domain)
	}
	if row.HarmonicPos != 1 || row.PagerankPos != 3 || row.NHosts != 7 {
		t.Errorf("positions/hosts wrong: %+v", row)
	}
	if row.HarmonicVal != 0.95 || row.PagerankVal != 0.0001 {
		t.Errorf("values wrong: %+v", row)
	}

	// n_hosts is optional.
	row2, ok := parseDomainLine("2\t0.5\t2\t0.2\torg.wikipedia.en")
	if !ok || row2.Domain != "en.wikipedia.org" || row2.NHosts != 0 {
		t.Errorf("optional n_hosts parse wrong: %+v ok=%v", row2, ok)
	}

	if _, ok := parseDomainLine("#harmonicc_pos\tx"); ok {
		// rankComment is checked by the caller, but a header row also fails the
		// numeric parse, so parseDomainLine must reject it too.
		t.Error("expected header row to fail numeric parse")
	}
	if _, ok := parseDomainLine("too\tfew"); ok {
		t.Error("expected short line to fail")
	}
}

func TestShardRange(t *testing.T) {
	contig := []shard{{Index: 0}, {Index: 1}, {Index: 2}}
	if got := shardRange(5, contig); got != "shards 00000-00002" {
		t.Errorf("contiguous = %q", got)
	}
	single := []shard{{Index: 7}}
	if got := shardRange(3, single); got != "shard 007" {
		t.Errorf("single = %q", got)
	}
	gap := []shard{{Index: 0}, {Index: 2}}
	if got := shardRange(3, gap); got != "shards 000,002" {
		t.Errorf("gap = %q", got)
	}
}

func TestCommitSummary(t *testing.T) {
	shards := []shard{
		{Index: 0, Bytes: 1_000_000_000},
		{Index: 1, Bytes: 400_000_000},
	}
	got := commitSummary("CC-MAIN-2026-25", "url", 5, shards)
	want := "Add CC-MAIN-2026-25 url shards 00000-00001 (2 files, 1.4 GB)"
	if got != want {
		t.Errorf("commitSummary = %q want %q", got, want)
	}
}

func TestURLStatsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stats.csv")
	rows := []URLCrawlStat{
		{Crawl: "CC-MAIN-2026-25", Shards: 300, TotalShards: 300, Rows: 2_800_000_000, ParquetBytes: 1 << 40, Complete: true, FirstCommitted: "2026-07-22T00:00:00Z", LastCommitted: "2026-07-22T01:00:00Z"},
	}
	if err := WriteURLStats(path, rows); err != nil {
		t.Fatal(err)
	}
	rows = UpsertURLStat(rows, URLCrawlStat{Crawl: "CC-MAIN-2026-21", Shards: 10, TotalShards: 300})
	if err := WriteURLStats(path, rows); err != nil {
		t.Fatal(err)
	}
	got, err := ReadURLStats(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 rows, got %d", len(got))
	}
	// Sorted newest first.
	if got[0].Crawl != "CC-MAIN-2026-25" || !got[0].Complete || got[0].Rows != 2_800_000_000 {
		t.Errorf("row0 wrong: %+v", got[0])
	}
}

func TestProgressRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "publish-progress.json")
	m, err := ReadProgress(path)
	if err != nil || len(m) != 0 {
		t.Fatalf("empty read: %v %v", m, err)
	}
	m["CC-MAIN-2026-25"] = ProgressEntry{Shards: 5, Rows: 100, Bytes: 200}
	if err := WriteProgress(path, m); err != nil {
		t.Fatal(err)
	}
	got, err := ReadProgress(path)
	if err != nil {
		t.Fatal(err)
	}
	if got["CC-MAIN-2026-25"].Shards != 5 {
		t.Errorf("progress round-trip wrong: %+v", got)
	}
}

func TestGenerateURLsREADME(t *testing.T) {
	rows := []URLCrawlStat{
		{Crawl: "CC-MAIN-2026-25", Shards: 300, TotalShards: 300, Rows: 2_800_000_000, ParquetBytes: 1 << 40, Complete: true},
	}
	md := GenerateURLsREADME("open-index/ccrawl-urls", rows)
	mustContain(t, md, `path: "data/**/*.parquet"`)
	mustContain(t, md, "config_name: \"CC-MAIN-2026-25\"")
	mustContain(t, md, "license: odc-by")
	mustContain(t, md, "CC-MAIN-2026-25/")
	mustContain(t, md, "part-00000.parquet")
	if strings.Contains(md, "crawl=") {
		t.Error("URLs card must not use Hive-partitioned key=value paths")
	}
	if strings.Contains(md, "—") {
		t.Error("card must not contain em-dashes")
	}
}

func TestGenerateDomainsREADME(t *testing.T) {
	rows := []DomainGraphStat{
		{Graph: "cc-main-2026-mar-apr-may", Shards: 12, Domains: 60_000_000, ParquetBytes: 1 << 30},
	}
	md := GenerateDomainsREADME("open-index/ccrawl-domains", rows)
	mustContain(t, md, `path: "data/**/*.parquet"`)
	mustContain(t, md, "harmonic_pos")
	mustContain(t, md, "cc-main-2026-mar-apr-may/")
	mustContain(t, md, "part-000.parquet")
	if strings.Contains(md, "graph=") {
		t.Error("domains card must not use Hive-partitioned key=value paths")
	}
}

func TestRefreshURLCardIncremental(t *testing.T) {
	dir := t.TempDir()
	statsPath := filepath.Join(dir, "stats.csv")
	o := URLPublishOptions{Repo: "open-index/ccrawl-urls", StageDir: dir}
	base := URLCrawlStat{Crawl: "CC-MAIN-2026-25"}

	// First batch: 16 of 300 shards. The card and ledger must publish now, and
	// the crawl must read as in-progress, not complete.
	stat, ops, err := refreshURLCard(o, "CC-MAIN-2026-25", 300, 16, 150_000_000, 9<<30, base, statsPath)
	if err != nil {
		t.Fatal(err)
	}
	if stat.Complete {
		t.Error("16/300 shards must not be marked complete")
	}
	if len(ops) != 2 || ops[0].PathInRepo != "stats.csv" || ops[1].PathInRepo != "README.md" {
		t.Fatalf("want stats.csv + README.md ops, got %+v", ops)
	}
	for _, op := range ops {
		if _, err := os.Stat(op.LocalPath); err != nil {
			t.Errorf("op file not written: %s: %v", op.LocalPath, err)
		}
	}
	card, err := os.ReadFile(filepath.Join(dir, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	mustContain(t, string(card), "16/300")

	// Final batch: all 300 shards. The same ledger row is updated in place and
	// now reads complete.
	stat, _, err = refreshURLCard(o, "CC-MAIN-2026-25", 300, 300, 2_800_000_000, 1<<40, base, statsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !stat.Complete {
		t.Error("300/300 shards must be complete")
	}
	rows, err := ReadURLStats(statsPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Shards != 300 {
		t.Fatalf("ledger should upsert to a single 300-shard row, got %+v", rows)
	}
}

func TestRefreshDomainCardIncremental(t *testing.T) {
	dir := t.TempDir()
	statsPath := filepath.Join(dir, "stats.csv")
	o := DomainPublishOptions{Repo: "open-index/ccrawl-domains", StageDir: dir, ShardRows: DefaultShardRows}

	// Mid-stream batch: not complete, since the end of the source is unknown.
	stat, ops, err := refreshDomainCard(o, "cc-main-2026-mar-apr-may", 4, 20_000_000, 1<<29, 1<<32, false, statsPath)
	if err != nil {
		t.Fatal(err)
	}
	if stat.Shards != 4 || stat.Domains != 20_000_000 {
		t.Fatalf("stat wrong: %+v", stat)
	}
	if stat.Complete {
		t.Error("a mid-stream batch must not be marked complete")
	}
	if len(ops) != 2 || ops[0].PathInRepo != "stats.csv" || ops[1].PathInRepo != "README.md" {
		t.Fatalf("want stats.csv + README.md ops, got %+v", ops)
	}
	if _, err := os.Stat(filepath.Join(dir, "README.md")); err != nil {
		t.Errorf("README.md not written: %v", err)
	}
	rows, err := ReadDomainStats(statsPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Shards != 4 || rows[0].Complete {
		t.Fatalf("ledger should have a single incomplete 4-shard row, got %+v", rows)
	}

	// Final refresh at end of stream: same row flips to complete.
	if _, _, err := refreshDomainCard(o, "cc-main-2026-mar-apr-may", 5, 24_000_000, 1<<30, 1<<32, true, statsPath); err != nil {
		t.Fatal(err)
	}
	rows, err = ReadDomainStats(statsPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || !rows[0].Complete {
		t.Fatalf("ledger should upsert to a single complete row, got %+v", rows)
	}
}

func TestReadDomainStatsPreCompleteRow(t *testing.T) {
	// A stats.csv written before the complete column existed has seven fields per
	// row. It must still parse, reading as not complete so a partial release is not
	// mistaken for a finished one on resume.
	dir := t.TempDir()
	path := filepath.Join(dir, "stats.csv")
	old := "graph,shards,domains,parquet_bytes,source_bytes,shard_rows,committed_at\n" +
		"cc-main-2026-apr-may-jun,3,15000000,198370539,2388634678,5000000,2026-07-22T15:26:46Z\n"
	if err := os.WriteFile(path, []byte(old), 0o644); err != nil {
		t.Fatal(err)
	}
	rows, err := ReadDomainStats(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0].Shards != 3 || rows[0].Domains != 15_000_000 {
		t.Errorf("row parsed wrong: %+v", rows[0])
	}
	if rows[0].Complete {
		t.Error("a pre-complete row must read as not complete")
	}
}

func TestHFResolveURL(t *testing.T) {
	got := hfResolveURL("open-index/ccrawl-urls", "data/CC-MAIN-2026-25/part-00042.parquet")
	want := "https://huggingface.co/datasets/open-index/ccrawl-urls/resolve/main/data/CC-MAIN-2026-25/part-00042.parquet"
	if got != want {
		t.Errorf("hfResolveURL = %q want %q", got, want)
	}
}

func TestIncompleteAction(t *testing.T) {
	cases := []struct {
		name      string
		doCommit  bool
		newly     int
		remaining int
		wantErr   bool
	}{
		{"whole crawl", true, 300, 0, false},
		{"progress with gaps retries", true, 128, 5, true},
		{"no progress with gaps gives up", true, 0, 5, false},
		{"dry run never retries", false, 0, 5, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := incompleteAction(tc.doCommit, tc.newly, tc.remaining)
			if tc.wantErr && !errors.Is(err, ErrIncomplete) {
				t.Errorf("want ErrIncomplete, got %v", err)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("want nil, got %v", err)
			}
		})
	}
}

func mustContain(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("expected output to contain %q", needle)
	}
}

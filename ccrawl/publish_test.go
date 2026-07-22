package ccrawl

import (
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

func mustContain(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("expected output to contain %q", needle)
	}
}

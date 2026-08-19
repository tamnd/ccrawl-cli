package ccrawl

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseNewsMonth(t *testing.T) {
	ok := []struct {
		in        string
		year, mon int
	}{
		{"2026/07", 2026, 7},
		{"2026-07", 2026, 7},
		{"2016/8", 2016, 8},
	}
	for _, c := range ok {
		y, m, err := parseNewsMonth(c.in)
		if err != nil {
			t.Errorf("parseNewsMonth(%q): %v", c.in, err)
			continue
		}
		if y != c.year || m != c.mon {
			t.Errorf("parseNewsMonth(%q) = %d,%d want %d,%d", c.in, y, m, c.year, c.mon)
		}
	}
	for _, bad := range []string{"", "2026", "2026/13", "2026/00", "2015/07", "july", "2026/xx"} {
		if _, _, err := parseNewsMonth(bad); err == nil {
			t.Errorf("parseNewsMonth(%q) accepted a bad month", bad)
		}
	}
}

func TestNewsMonthLabel(t *testing.T) {
	if got := newsMonthLabel(2026, 7); got != "2026-07" {
		t.Errorf("newsMonthLabel = %q", got)
	}
}

// TestNewsShardPath pins the naming rule the whole resume story rests on: a
// shard is named for the WARC it indexes, so the month's manifest names every
// shard that could exist and nothing has to be listed from the hub.
func TestNewsShardPath(t *testing.T) {
	got := newsShardPath("crawl-data/CC-NEWS/2026/07/CC-NEWS-20260701022501-08467.warc.gz")
	want := "data/2026/07/CC-NEWS-20260701022501-08467.parquet"
	if got != want {
		t.Errorf("newsShardPath = %q, want %q", got, want)
	}
}

func TestNewsStatsRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stats.csv")
	rows := []NewsMonthStat{
		{Month: "2026-06", Files: 340, TotalFiles: 340, Rows: 8_100_000, ParquetBytes: 1 << 30,
			SourceBytes: 360 << 30, Rows2xx: 7_900_000, RowsHTML: 7_800_000, Complete: true,
			FirstCommitted: "2026-07-01T00:00:00Z", LastCommitted: "2026-07-02T00:00:00Z"},
		{Month: "2026-07", Files: 12, TotalFiles: 353, Rows: 290_000, ParquetBytes: 40 << 20,
			SourceBytes: 12 << 30, Rows2xx: 280_000, RowsHTML: 275_000, Complete: false,
			FirstCommitted: "2026-08-01T00:00:00Z", LastCommitted: "2026-08-01T01:00:00Z"},
	}
	if err := WriteNewsStats(path, rows); err != nil {
		t.Fatal(err)
	}
	back, err := ReadNewsStats(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != 2 {
		t.Fatalf("read %d rows, want 2", len(back))
	}
	// Newest first, so the card lists the month a reader cares about at the top.
	if back[0].Month != "2026-07" {
		t.Errorf("first row is %q, want 2026-07", back[0].Month)
	}
	if back[0].Files != 12 || back[0].TotalFiles != 353 || back[0].Rows != 290_000 {
		t.Errorf("row 0 = %+v", back[0])
	}
	if back[0].Complete {
		t.Error("row 0 came back complete")
	}
	if !back[1].Complete || back[1].SourceBytes != 360<<30 {
		t.Errorf("row 1 = %+v", back[1])
	}
}

// TestDecodeNewsStats is the path a search takes: the ledger is fetched over
// HTTPS rather than read off disk, and coverage is decided from it.
func TestDecodeNewsStats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stats.csv")
	if err := WriteNewsStats(path, []NewsMonthStat{{Month: "2026-07", Files: 3, TotalFiles: 353, Rows: 70_000}}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := DecodeNewsStats(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Month != "2026-07" || rows[0].Files != 3 {
		t.Fatalf("decoded %+v", rows)
	}
	if _, err := DecodeNewsStats(nil); err != nil {
		t.Errorf("empty ledger is an error: %v", err)
	}
}

func TestUpsertNewsStat(t *testing.T) {
	rows := []NewsMonthStat{{Month: "2026-06", Files: 1}}
	rows = UpsertNewsStat(rows, NewsMonthStat{Month: "2026-07", Files: 2})
	if len(rows) != 2 {
		t.Fatalf("append: %d rows", len(rows))
	}
	rows = UpsertNewsStat(rows, NewsMonthStat{Month: "2026-07", Files: 9})
	if len(rows) != 2 {
		t.Fatalf("replace grew to %d rows", len(rows))
	}
	if rows[1].Files != 9 {
		t.Errorf("2026-07 files = %d, want 9", rows[1].Files)
	}
}

func TestNewsLangsRoundTripAndMerge(t *testing.T) {
	path := filepath.Join(t.TempDir(), "languages.csv")
	rows := MergeNewsLangs(nil, "2026-07", map[string]int64{"eng": 100, "rus": 40, "deu": 70})
	rows = MergeNewsLangs(rows, "2026-06", map[string]int64{"eng": 5})
	if err := WriteNewsLangs(path, rows); err != nil {
		t.Fatal(err)
	}
	back, err := ReadNewsLangs(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != 4 {
		t.Fatalf("read %d rows, want 4", len(back))
	}
	// Newest month first, biggest language first within a month.
	if back[0].Month != "2026-07" || back[0].Lang != "eng" || back[0].Rows != 100 {
		t.Errorf("row 0 = %+v", back[0])
	}
	if back[1].Lang != "deu" || back[2].Lang != "rus" {
		t.Errorf("languages out of order: %+v", back[:3])
	}

	// Re-merging a month replaces that month and leaves the others alone, which
	// is what keeps a resumed run from double counting.
	back = MergeNewsLangs(back, "2026-07", map[string]int64{"eng": 1})
	var months []string
	for _, r := range back {
		months = append(months, r.Month+"/"+r.Lang)
	}
	if len(back) != 2 {
		t.Fatalf("after remerge: %v", months)
	}
}

func TestNewsSavings(t *testing.T) {
	got := newsSavings(378<<30, 1<<30)
	for _, want := range []string{"WARC", "Parquet", "smaller"} {
		if !strings.Contains(got, want) {
			t.Errorf("newsSavings = %q, missing %q", got, want)
		}
	}
	if newsSavings(0, 0) != "" {
		t.Errorf("newsSavings with nothing read = %q, want empty", newsSavings(0, 0))
	}
}

func TestNewsLangBars(t *testing.T) {
	var langs []NewsLangStat
	for _, c := range []struct {
		code string
		n    int64
	}{{"eng", 500}, {"deu", 200}, {"rus", 100}, {"fra", 50}} {
		langs = append(langs, NewsLangStat{Month: "2026-07", Lang: c.code, Rows: c.n})
	}
	rows := newsLangBars(langs, 1000)
	if len(rows) == 0 {
		t.Fatal("no language rows")
	}
	if rows[0].Code != "eng" {
		t.Errorf("first row = %q, want eng", rows[0].Code)
	}
	if rows[0].Name == "" || rows[0].Name == "eng" {
		t.Errorf("eng has no English name: %q", rows[0].Name)
	}
	if rows[0].Bar == "" {
		t.Error("first row has no bar")
	}
	// 150 of the 1000 rows had no identified language, so there has to be a row
	// saying so rather than a table that silently adds up to less than the total.
	var none bool
	for _, r := range rows {
		if r.Code == "none" {
			none = true
			if !strings.Contains(r.Articles, "150") {
				t.Errorf("none row = %+v, want 150 articles", r)
			}
		}
	}
	if !none {
		t.Error("no row for articles with no identified language")
	}
}

func TestGenerateNewsREADME(t *testing.T) {
	stats := []NewsMonthStat{
		{Month: "2026-07", Files: 353, TotalFiles: 353, Rows: 8_400_000, ParquetBytes: 1100 << 20,
			SourceBytes: 378 << 30, Rows2xx: 8_100_000, RowsHTML: 8_000_000, Complete: true,
			FirstCommitted: "2026-08-01T00:00:00Z", LastCommitted: "2026-08-02T06:00:00Z"},
		{Month: "2026-06", Files: 40, TotalFiles: 340, Rows: 900_000, ParquetBytes: 120 << 20,
			SourceBytes: 42 << 30, Rows2xx: 870_000, RowsHTML: 860_000, Complete: false,
			FirstCommitted: "2026-08-02T06:00:00Z", LastCommitted: "2026-08-02T09:00:00Z"},
	}
	langs := MergeNewsLangs(nil, "2026-07", map[string]int64{"eng": 4_000_000, "deu": 900_000, "rus": 400_000})
	langs = MergeNewsLangs(langs, "2026-06", map[string]int64{"eng": 500_000})

	md := GenerateNewsREADME("open-index/ccrawl-news", stats, langs)

	for _, want := range []string{
		"---",                  // frontmatter
		"license: odc-by",      //
		"config_name: default", //
		"data/2026/07/*.parquet",
		"open-index/ccrawl-news",
		"warc_record_offset",
		"ccrawl fetch",
		"DuckDB",
		"2026-07",
		"2026-06",
		"English",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("README is missing %q", want)
		}
	}
	// A template that failed to resolve a field leaves this behind, and it is
	// silent otherwise because text/template renders it as text.
	if strings.Contains(md, "<no value>") {
		t.Error("README has an unresolved template field")
	}
	if strings.Contains(md, "—") || strings.Contains(md, "–") {
		t.Error("README has a dash character the house style does not use")
	}
	if n := strings.Count(md, "\n---\n"); n < 1 {
		t.Errorf("frontmatter is not closed (%d separators)", n)
	}
}

// TestGenerateNewsREADMEEmpty is the state the repo is in before the first
// commit lands. A card that panics or renders half a table there is a card
// nobody sees until it is too late.
func TestGenerateNewsREADMEEmpty(t *testing.T) {
	md := GenerateNewsREADME("open-index/ccrawl-news", nil, nil)
	if !strings.Contains(md, "open-index/ccrawl-news") {
		t.Error("empty README does not name the repo")
	}
	if strings.Contains(md, "<no value>") {
		t.Error("empty README has an unresolved template field")
	}
}

func TestFinalizeNewsMessage(t *testing.T) {
	done := finalizeNewsMessage(NewsMonthStat{Month: "2026-07", Files: 353, TotalFiles: 353,
		Rows: 8_400_000, ParquetBytes: 1100 << 20, Complete: true})
	if !strings.Contains(done, "complete") || !strings.Contains(done, "353/353") {
		t.Errorf("complete message = %q", done)
	}
	part := finalizeNewsMessage(NewsMonthStat{Month: "2026-07", Files: 12, TotalFiles: 353})
	if !strings.Contains(part, "progress") || !strings.Contains(part, "12/353") {
		t.Errorf("progress message = %q", part)
	}
}

// TestRefreshNewsCardCoverage pins what "complete" means. --files caps the work
// a run does, not the size of the month, so a run that indexed 12 of 353 files
// has to say so. Calling that complete would tell a search it had the whole
// month and quietly turn a partial answer into a wrong one.
func TestRefreshNewsCardCoverage(t *testing.T) {
	dir := t.TempDir()
	statsPath := filepath.Join(dir, "stats.csv")
	langsPath := filepath.Join(dir, "languages.csv")
	o := NewsPublishOptions{Repo: "open-index/ccrawl-news", StageDir: dir}

	part, _, err := refreshNewsCard(o, NewsMonthStat{Month: "2026-07", Files: 12, Rows: 270_000}, nil, 353, statsPath, langsPath)
	if err != nil {
		t.Fatal(err)
	}
	if part.TotalFiles != 353 {
		t.Errorf("total files = %d, want 353", part.TotalFiles)
	}
	if part.Complete {
		t.Error("12 of 353 files was reported complete")
	}

	whole, _, err := refreshNewsCard(o, NewsMonthStat{Month: "2026-07", Files: 353, Rows: 8_000_000}, nil, 353, statsPath, langsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !whole.Complete {
		t.Error("353 of 353 files was not reported complete")
	}
	// The first commit stamp survives a later refresh, so a month keeps the time
	// it was started rather than the time it was last touched.
	if whole.FirstCommitted != part.FirstCommitted {
		t.Errorf("first committed moved from %q to %q", part.FirstCommitted, whole.FirstCommitted)
	}

	// A month with nothing in it is not complete, whatever the arithmetic says.
	empty, _, err := refreshNewsCard(o, NewsMonthStat{Month: "2026-05"}, nil, 0, statsPath, langsPath)
	if err != nil {
		t.Fatal(err)
	}
	if empty.Complete {
		t.Error("a month with no files was reported complete")
	}
}

package ccrawl

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

const (
	vieText = "Trong bối cảnh kinh tế toàn cầu nhiều biến động, Việt Nam vẫn giữ được đà tăng trưởng ổn định nhờ xuất khẩu và đầu tư nước ngoài tăng mạnh trong quý vừa qua. Các chuyên gia cho rằng chính sách tiền tệ linh hoạt đã góp phần kiềm chế lạm phát."
	engText = "The committee published its quarterly report on Tuesday, noting that growth had slowed across most of the region while inflation remained broadly under control. Analysts expect the central bank to hold rates steady until the end of the year."
)

func TestDetectLanguage(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		wantCode string
		minConf  float64
	}{
		{"vietnamese", vieText, "vie", 0.9},
		{"english", engText, "eng", 0.9},
		{"too short", "Hà Nội", "", 0},
		{"empty", "", "", 0},
		{"markdown syntax only", "| a | b |\n| --- | --- |\n# \n* \n", "", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, conf := DetectLanguage(tt.text)
			if code != tt.wantCode {
				t.Fatalf("code = %q, want %q (confidence %.2f)", code, tt.wantCode, conf)
			}
			if conf < tt.minConf {
				t.Fatalf("confidence = %.2f, want at least %.2f", conf, tt.minConf)
			}
		})
	}
}

// TestDetectLanguageIgnoresLinkTargets pins the reason langSample exists. A
// Vietnamese link list carries its prose in the link text and its noise in the
// slugs, and the slugs have no diacritics, so leaving them in drags the answer
// away from Vietnamese. Measured on a real vnexpress.net homepage this was the
// difference between confidence 0.16 and confidence 1.
func TestDetectLanguageIgnoresLinkTargets(t *testing.T) {
	headlines := []struct{ text, slug string }{
		{"Kiến nghị giảm tuần làm việc của lao động doanh nghiệp xuống bốn mươi giờ", "kien-nghi-giam-tuan-lam-viec-cua-lao-dong-doanh-nghiep-xuong-40-44-gio"},
		{"Hà Nội sắp xếp còn mười bốn sở ngành sau đợt cải cách bộ máy hành chính", "ha-noi-sap-xep-con-14-so-nganh"},
		{"Hơn một trăm ba mươi đại học công bố điểm chuẩn, nhiều ngành chạm trần", "diem-chuan-hon-200-dai-hoc-nam-2026"},
		{"Bộ Khoa học và Công nghệ muốn đẩy mạnh nghiên cứu và đổi mới sáng tạo", "bo-khoa-hoc-va-cong-nghe-muon-day-manh-nghien-cuu"},
		{"Thành phố Hải Phòng kiện toàn bộ máy lãnh đạo trong phiên họp sáng nay", "tp-hai-phong-kien-toan-bo-may-lanh-dao"},
		{"Đề nghị cấm hòa giải vụ bạo lực gia đình để bảo vệ nạn nhân tốt hơn", "de-nghi-cam-hoa-giai-vu-bao-luc-gia-dinh-de-bao-ve-nan-nhan"},
	}
	var b strings.Builder
	for _, h := range headlines {
		b.WriteString("- [" + h.text + "](https://vnexpress.net/" + h.slug + "-5107278.html)\n")
	}
	code, conf := DetectLanguage(b.String())
	if code != "vie" {
		t.Fatalf("code = %q, want vie (confidence %.2f)", code, conf)
	}
	if conf < 0.9 {
		t.Fatalf("confidence = %.2f, want at least 0.9", conf)
	}
	if s := LangSample(b.String(), 400); strings.Contains(s, "kien-nghi") {
		t.Fatalf("sample still holds the slug: %q", s)
	}
}

func TestLangMatches(t *testing.T) {
	tests := []struct {
		name             string
		want, got        string
		conf, min        float64
		expect           bool
		explainIfWrongIs string
	}{
		{name: "no filter keeps everything", want: "", got: "eng", conf: 1, min: 0.8, expect: true},
		{name: "no filter keeps the unidentifiable", want: "", got: "", conf: 0, min: 0.8, expect: true},
		{name: "match above the floor", want: "vie", got: "vie", conf: 0.95, min: 0.8, expect: true},
		{name: "match at the floor", want: "vie", got: "vie", conf: 0.8, min: 0.8, expect: true},
		{name: "match below the floor", want: "vie", got: "vie", conf: 0.5, min: 0.8, expect: false},
		{name: "other language", want: "vie", got: "eng", conf: 1, min: 0.8, expect: false},
		{name: "filter drops the unidentifiable", want: "vie", got: "", conf: 0, min: 0.8, expect: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LangMatches(tt.want, tt.got, tt.conf, tt.min); got != tt.expect {
				t.Fatalf("LangMatches(%q, %q, %v, %v) = %v, want %v", tt.want, tt.got, tt.conf, tt.min, got, tt.expect)
			}
		})
	}
}

// langWARC builds a WARC stream of pages in the given languages, one record per
// entry, and returns the bytes.
func langWARC(t *testing.T, langs []string) []byte {
	t.Helper()
	var buf bytes.Buffer
	for i, l := range langs {
		text := engText
		switch l {
		case "vie":
			text = vieText
		case "short":
			text = "Xin chào"
		}
		html := "<html><body><article><p>" + text + "</p></article></body></html>"
		buf.Write(warcMember(t, "https://example.com/"+l+"/"+string(rune('a'+i)), html))
	}
	return buf.Bytes()
}

func TestPackStreamLanguageFilter(t *testing.T) {
	langs := []string{"vie", "eng", "vie", "short", "eng", "vie"}
	out := filepath.Join(t.TempDir(), "out.parquet")
	cfg := MarkdownPackConfig{
		OutPath: out,
		Workers: 2,
		Lang:    "vie",
	}
	stats, err := packStream(context.Background(), bytes.NewReader(langWARC(t, langs)), cfg, MarkdownStats{}, time.Now())
	if err != nil {
		t.Fatalf("packStream: %v", err)
	}
	if stats.Rows != 3 {
		t.Fatalf("Rows = %d, want 3", stats.Rows)
	}
	if stats.LangDropped != 3 {
		t.Fatalf("LangDropped = %d, want 3", stats.LangDropped)
	}
	if got := stats.LangCounts["vie"]; got != 3 {
		t.Fatalf("LangCounts[vie] = %d, want 3", got)
	}
	if got := stats.LangCounts["eng"]; got != 2 {
		t.Fatalf("LangCounts[eng] = %d, want 2", got)
	}
	// The short page has no text worth identifying, so it lands under the empty
	// code and the filter drops it rather than guessing.
	if got := stats.LangCounts[""]; got != 1 {
		t.Fatalf(`LangCounts[""] = %d, want 1`, got)
	}

	rows, err := parquet.ReadFile[MarkdownRow](out)
	if err != nil {
		t.Fatalf("read parquet: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("parquet holds %d rows, want 3", len(rows))
	}
	for _, r := range rows {
		if r.Language != "vie" {
			t.Fatalf("row %s has language %q, want vie", r.URL, r.Language)
		}
		if r.LangConfidence < 0.8 {
			t.Fatalf("row %s has confidence %.2f, below the floor it was filtered on", r.URL, r.LangConfidence)
		}
	}
}

// TestPackStreamNoFilterStillLabels is the other half: without --lang nothing is
// dropped, and every row still carries the language, which is what makes an
// unfiltered v3 shard filterable later without re-extracting it.
func TestPackStreamNoFilterStillLabels(t *testing.T) {
	langs := []string{"vie", "eng", "short"}
	out := filepath.Join(t.TempDir(), "out.parquet")
	stats, err := packStream(context.Background(),
		bytes.NewReader(langWARC(t, langs)),
		MarkdownPackConfig{OutPath: out, Workers: 2},
		MarkdownStats{}, time.Now())
	if err != nil {
		t.Fatalf("packStream: %v", err)
	}
	if stats.Rows != 3 || stats.LangDropped != 0 {
		t.Fatalf("Rows = %d, LangDropped = %d, want 3 and 0", stats.Rows, stats.LangDropped)
	}
	rows, err := parquet.ReadFile[MarkdownRow](out)
	if err != nil {
		t.Fatalf("read parquet: %v", err)
	}
	var seen []string
	for _, r := range rows {
		seen = append(seen, r.Language)
	}
	for _, want := range []string{"vie", "eng", ""} {
		if !slicesContains(seen, want) {
			t.Fatalf("languages %q missing %q", seen, want)
		}
	}
}

// markdownRowV2 is the schema as it stood before the language columns, standing
// in for a reader written against v2 that has not been updated.
type markdownRowV2 struct {
	DocID          string `parquet:"doc_id"`
	URL            string `parquet:"url"`
	Host           string `parquet:"host"`
	CrawlDate      string `parquet:"crawl_date"`
	WARCRecordID   string `parquet:"warc_record_id"`
	HTMLLength     int64  `parquet:"html_length"`
	MarkdownLength int64  `parquet:"markdown_length"`
	Markdown       string `parquet:"markdown"`
}

// TestV2ReaderParsesV3File is the compatibility promise in the issue: a reader
// that only knows the v2 columns reads a v3 file and gets them all. Parquet
// matches columns by name, so appending two at the end costs an old reader
// nothing as long as it never asks for them.
func TestV2ReaderParsesV3File(t *testing.T) {
	out := filepath.Join(t.TempDir(), "v3.parquet")
	if _, err := packStream(context.Background(),
		bytes.NewReader(langWARC(t, []string{"vie", "eng"})),
		MarkdownPackConfig{OutPath: out, Workers: 2},
		MarkdownStats{}, time.Now()); err != nil {
		t.Fatalf("packStream: %v", err)
	}

	v3, err := parquet.ReadFile[MarkdownRow](out)
	if err != nil {
		t.Fatalf("read as v3: %v", err)
	}
	v2, err := parquet.ReadFile[markdownRowV2](out)
	if err != nil {
		t.Fatalf("read as v2: %v", err)
	}
	if len(v2) != len(v3) {
		t.Fatalf("v2 reader saw %d rows, v3 reader saw %d", len(v2), len(v3))
	}
	for i := range v2 {
		if v2[i].DocID != v3[i].DocID || v2[i].URL != v3[i].URL || v2[i].Markdown != v3[i].Markdown {
			t.Fatalf("row %d differs between readers:\n v2 %+v\n v3 %+v", i, v2[i], v3[i])
		}
		if v2[i].MarkdownLength != v3[i].MarkdownLength || v2[i].HTMLLength != v3[i].HTMLLength {
			t.Fatalf("row %d lengths differ between readers", i)
		}
	}

	// And the file really does carry the new columns, so the test above is
	// proving compatibility rather than proving they were never written.
	f, err := os.Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range pf.Schema().Fields() {
		names = append(names, c.Name())
	}
	for _, want := range []string{"language", "language_confidence"} {
		if !slicesContains(names, want) {
			t.Fatalf("schema %q is missing %q", names, want)
		}
	}
}

func slicesContains(hay []string, needle string) bool {
	for _, s := range hay {
		if s == needle {
			return true
		}
	}
	return false
}

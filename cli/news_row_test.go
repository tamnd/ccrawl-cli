package cli

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func sampleNewsRow() ccrawl.NewsRow {
	return ccrawl.NewsRow{
		URLSurtKey:              "ru,kommersant,www)/doc/12345",
		URL:                     "https://www.kommersant.ru/doc/12345",
		URLHostName:             "www.kommersant.ru",
		URLHostRegisteredDomain: "kommersant.ru",
		URLHostTLD:              "ru",
		URLProtocol:             "https",
		FetchTime:               time.Date(2026, 7, 1, 2, 25, 1, 0, time.UTC),
		FetchStatus:             200,
		ContentMIMEType:         "text/html",
		ContentMIMEDetected:     "text/html",
		ContentCharset:          "UTF-8",
		ContentLanguages:        "rus",
		ContentDigest:           "ABCDEF0123456789",
		ContentLength:           20480,
		WARCFilename:            "crawl-data/CC-NEWS/2026/07/CC-NEWS-20260701022501-08467.warc.gz",
		WARCRecordOffset:        71779,
		WARCRecordLength:        20806,
	}
}

// TestNewsRowJSONKeysFeedFetch is the round trip issue 64 asks for: a row that
// news search prints as JSONL has to name its location the way fetch reads it,
// or the index answers a question nobody can act on.
func TestNewsRowJSONKeysFeedFetch(t *testing.T) {
	b, err := json.Marshal(newsRow(sampleNewsRow()).Value)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"warc_filename", "warc_record_offset", "warc_record_length"} {
		if _, ok := got[key]; !ok {
			t.Errorf("news row JSON is missing %q, so it cannot be piped into fetch -", key)
		}
	}
	if got["warc_record_offset"] != float64(71779) {
		t.Errorf("offset = %v, want 71779", got["warc_record_offset"])
	}
	if got["warc_record_length"] != float64(20806) {
		t.Errorf("length = %v, want 20806", got["warc_record_length"])
	}
	// The columnar names are the contract. A key named after the Go field would
	// mean the same data under a name no query written against the dataset uses.
	for _, unwanted := range []string{"WARCFilename", "filename", "offset", "length"} {
		if _, ok := got[unwanted]; ok {
			t.Errorf("news row JSON has a stray key %q", unwanted)
		}
	}
}

func TestNewsRowColumns(t *testing.T) {
	row := newsRow(sampleNewsRow())
	want := []string{"timestamp", "url", "status", "mime", "languages", "digest", "filename", "offset", "length"}
	if !reflect.DeepEqual(row.Cols, want) {
		t.Errorf("columns = %v, want %v", row.Cols, want)
	}
	if len(row.Vals) != len(row.Cols) {
		t.Fatalf("%d values for %d columns", len(row.Vals), len(row.Cols))
	}
	// The timestamp is the CDX one, so a person reading news search output and a
	// person reading index output are reading the same format.
	if row.Vals[0] != "20260701022501" {
		t.Errorf("timestamp = %q", row.Vals[0])
	}
	if row.Vals[2] != "200" {
		t.Errorf("status = %q", row.Vals[2])
	}
	if row.Vals[7] != "71779" || row.Vals[8] != "20806" {
		t.Errorf("span columns = %q,%q", row.Vals[7], row.Vals[8])
	}
}

// TestNewsRowMatchesCDXRow keeps the two search paths readable side by side. The
// news columns are the CDX columns minus the crawl id, which CC-NEWS has no
// equivalent of.
func TestNewsRowMatchesCDXRow(t *testing.T) {
	news := newsRow(sampleNewsRow()).Cols
	cdx := cdxRow(ccrawl.CDXRecord{}).Cols
	if len(cdx) != len(news)+1 {
		t.Fatalf("cdx has %d columns, news has %d", len(cdx), len(news))
	}
	for i := range news {
		if news[i] != cdx[i] {
			t.Errorf("column %d: news %q, cdx %q", i, news[i], cdx[i])
		}
	}
	if cdx[len(cdx)-1] != "crawl" {
		t.Errorf("cdx trailing column = %q, want crawl", cdx[len(cdx)-1])
	}
}

func TestSplitList(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"2026/07", []string{"2026/07"}},
		{"2026/07,2026/06", []string{"2026/07", "2026/06"}},
		{" 2026/07 , 2026/06 ", []string{"2026/07", "2026/06"}},
		{"2026/07,", []string{"2026/07"}},
		{",,", nil},
		{"", nil},
	}
	for _, c := range cases {
		if got := splitList(c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("splitList(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

// TestNewsCommandsRegistered checks the escape hatches are wired up, since a
// command that exists in the source and not in the tree is invisible.
func TestNewsCommandsRegistered(t *testing.T) {
	var names []string
	for _, c := range newsEscapeHatches() {
		names = append(names, c.Use)
	}
	joined := strings.Join(names, " ")
	for _, want := range []string{"download", "search", "publish"} {
		if !strings.Contains(joined, want) {
			t.Errorf("news is missing the %q command, has %v", want, names)
		}
	}
}

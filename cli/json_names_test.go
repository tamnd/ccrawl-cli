package cli

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// The JSON a command writes is a contract: a script reads those keys, and a
// rename breaks it silently, since a missing key in JSON is an empty value
// rather than an error. encoding/json falls back to the Go field name when a
// field has no tag, so an untagged field is a rename waiting to happen, and the
// name it falls back to is not the one the parquet column has.
//
// These tests pin the key set of the two records that go out over parse,
// convert, api, and serve.

func TestWETRecordJSONNames(t *testing.T) {
	want := []string{"content_language", "crawl_id", "date", "record_id", "text", "url"}
	if got := jsonKeys(t, ccrawl.WETRecord{}); !reflect.DeepEqual(got, want) {
		t.Errorf("WET record keys are %v, want %v", got, want)
	}
	assertTagged(t, ccrawl.WETRecord{})
}

func TestWATRecordJSONNames(t *testing.T) {
	want := []string{
		"content_type", "crawl_id", "date", "http_status", "links", "links_count",
		"metas", "record_id", "title", "url",
		"warc_filename", "warc_record_length", "warc_record_offset",
	}
	if got := jsonKeys(t, ccrawl.WATRecord{}); !reflect.DeepEqual(got, want) {
		t.Errorf("WAT record keys are %v, want %v", got, want)
	}
	assertTagged(t, ccrawl.WATRecord{})
}

// TestWARCRecordJSONNames pins the hand-picked WARC object, which parse and
// convert both write and which has to read like the other two.
func TestWARCRecordJSONNames(t *testing.T) {
	want := []string{
		"content_length", "date", "mime", "payload_digest", "record_id", "status", "type", "url",
	}
	if got := jsonKeys(t, warcJSON(ccrawl.WARCRecord{})); !reflect.DeepEqual(got, want) {
		t.Errorf("WARC object keys are %v, want %v", got, want)
	}
}

// jsonKeys marshals a value and returns its top-level keys, sorted.
func jsonKeys(t *testing.T, v any) []string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// assertTagged fails on an exported field with no json tag, which is the way
// one of these records grows a Go field name again.
func assertTagged(t *testing.T, v any) {
	t.Helper()
	typ := reflect.TypeOf(v)
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if !f.IsExported() {
			continue
		}
		if _, ok := f.Tag.Lookup("json"); !ok {
			t.Errorf("%s.%s has no json tag, so it goes out as %q", typ.Name(), f.Name, f.Name)
		}
	}
}

// TestWATRecordFeedsFetch is why the WAT location fields carry the columnar
// names rather than the shorter ones: a WAT record says where the page is, and
// ccrawl fetch reads a location by those names, so the two commands agree on
// what to call the same three numbers.
func TestWATRecordFeedsFetch(t *testing.T) {
	rec := ccrawl.WATRecord{
		URL:        "https://example.com/",
		WARCFile:   "crawl-data/CC-MAIN-2025-05/segments/1/warc/x.warc.gz",
		WARCOffset: 1024,
		WARCLength: 4096,
	}
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	loc, ok := parseLocationLine(string(b))
	if !ok {
		t.Fatalf("fetch did not read a location out of %s", b)
	}
	if loc.Filename != rec.WARCFile || loc.Offset != rec.WARCOffset || loc.Length != rec.WARCLength {
		t.Errorf("location is %+v, want the record's file, offset, and length", loc)
	}
}

// TestIndexDocReadsEverySpellingOfLanguage covers the three ways a corpus line
// names the language: by hand, from ccrawl parse, and from a ccrawl older than
// v0.10.1 that wrote the Go field name.
func TestIndexDocReadsEverySpellingOfLanguage(t *testing.T) {
	for _, key := range []string{"language", "content_language", "ContentLanguage"} {
		var doc indexDoc
		line := `{"url":"https://example.com/","text":"hello","` + key + `":"eng"}`
		if err := json.Unmarshal([]byte(line), &doc); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if doc.lang() != "eng" {
			t.Errorf("a document keyed %q reads as language %q, want eng", key, doc.lang())
		}
	}
}

// TestWETRecordJSONMatchesTheTable is the check the whole change is for: the
// keys a record goes out with and the columns the table prints are one
// vocabulary. The table abbreviates and adds a computed column, so this only
// asks that a column it does share is spelled the same way.
func TestWETRecordJSONMatchesTheTable(t *testing.T) {
	rec := ccrawl.WETRecord{
		RecordID:        "urn:uuid:1",
		CrawlID:         "CC-MAIN-2025-05",
		URL:             "https://example.com/",
		Date:            time.Unix(0, 0).UTC(),
		ContentLanguage: "eng",
		Text:            "hello",
	}
	b, err := json.Marshal(wetRow(rec).Value)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"url":`, `"content_language":`, `"text":`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("the row a command emits is missing %s: %s", want, b)
		}
	}
}

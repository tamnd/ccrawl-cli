package ccrawl

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kgzip "github.com/klauspost/compress/gzip"
	"github.com/parquet-go/parquet-go"
)

// articleHTML is a page with a real article buried in the usual furniture, so
// the engines have something to disagree about: an extractor that works keeps
// the article, and the raw engine keeps all of it.
const articleHTML = `<html><head><title>Quarterly report</title></head><body>
<nav><a href="/home">Home</a> <a href="/about">About</a> <a href="/contact">Contact</a></nav>
<article>
<h1>Quarterly report</h1>
<p>The committee published its quarterly report on Tuesday, noting that growth had slowed across most of the region while inflation remained broadly under control.</p>
<p>Analysts expect the central bank to hold rates steady until the end of the year, and several of them said the pause would last well into the next one.</p>
</article>
<footer>Copyright 2026. All rights reserved. Terms of service. Privacy policy.</footer>
</body></html>`

func TestLookupExtractor(t *testing.T) {
	tests := []struct {
		name       string
		in         string
		wantName   string
		wantSource string
		wantErr    bool
	}{
		{name: "default on empty", in: "", wantName: "h2m", wantSource: "warc"},
		{name: "by name", in: "readability", wantName: "readability", wantSource: "warc"},
		{name: "case and space insensitive", in: "  RAW ", wantName: "raw", wantSource: "warc"},
		{name: "wet reads wet shards", in: "wet", wantName: "wet", wantSource: "wet"},
		{name: "unknown", in: "trafilatura", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ex, err := LookupExtractor(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("LookupExtractor(%q) = %v, want an error", tt.in, ex.Name)
				}
				// The error has to list the options, since the whole point of
				// the flag is that the caller is choosing between them.
				for _, n := range ExtractorNames() {
					if !strings.Contains(err.Error(), n) {
						t.Fatalf("error %q does not mention %q", err, n)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("LookupExtractor(%q): %v", tt.in, err)
			}
			if ex.Name != tt.wantName || ex.SourceKind != tt.wantSource {
				t.Fatalf("got %s reading %s shards, want %s reading %s", ex.Name, ex.SourceKind, tt.wantName, tt.wantSource)
			}
		})
	}
}

// TestExtractorID pins the format the parquet column and the dataset card both
// use. A bare name is not enough: extraction changes between releases, so the
// version is part of the answer.
//
// The version itself reads "unknown" here and only here. A test binary's build
// info carries no dependency list, so this can check the shape but not the
// value; a real binary stamps h2m@v0.2.1.
func TestExtractorID(t *testing.T) {
	for _, name := range []string{"h2m", "readability", "raw"} {
		id := Extractors[name].ID("CC-MAIN-2026-30")
		prefix, version, ok := strings.Cut(id, "@")
		if !ok || prefix != name {
			t.Fatalf("ID = %q, want %s@version", id, name)
		}
		if version == "" {
			t.Fatalf("ID = %q, want a version after the name", id)
		}
	}
	if v := moduleVersion("example.com/nothing/we/import"); v != "unknown" {
		t.Fatalf("moduleVersion of a module we do not depend on = %q, want unknown", v)
	}
	// The WET text is Common Crawl's own and has no version we can read, so the
	// crawl stands in for one.
	if id := Extractors["wet"].ID("CC-MAIN-2026-30"); id != "wet@CC-MAIN-2026-30" {
		t.Fatalf("wet ID = %q, want wet@CC-MAIN-2026-30", id)
	}
	if id := Extractors["wet"].ID(""); id != "wet" {
		t.Fatalf("wet ID with no crawl = %q, want wet", id)
	}
}

// TestExtractorsAgreeOnPagesDisagreeOnText is the done-when from the issue: the
// same shard through three engines gives the same documents and different text.
// Same doc_id set is what makes the corpora comparable; different markdown is
// what makes the choice worth having.
func TestExtractorsAgreeOnPagesDisagreeOnText(t *testing.T) {
	var shard bytes.Buffer
	for i := range 4 {
		shard.Write(warcMember(t, fmt.Sprintf("https://example.com/article/%d", i), articleHTML))
	}

	dir := t.TempDir()
	md := map[string]map[string]string{} // engine -> doc_id -> markdown
	for _, name := range []string{"h2m", "readability", "raw"} {
		out := filepath.Join(dir, name+".parquet")
		stats, err := packStream(context.Background(),
			bytes.NewReader(shard.Bytes()),
			MarkdownPackConfig{OutPath: out, Workers: 2, CrawlID: "CC-MAIN-2026-30", Extractor: Extractors[name]},
			MarkdownStats{}, time.Now())
		if err != nil {
			t.Fatalf("%s: packStream: %v", name, err)
		}
		if stats.Rows != 4 {
			t.Fatalf("%s: Rows = %d, want 4", name, stats.Rows)
		}
		rows, err := parquet.ReadFile[MarkdownRow](out)
		if err != nil {
			t.Fatalf("%s: read parquet: %v", name, err)
		}
		md[name] = map[string]string{}
		for _, r := range rows {
			md[name][r.DocID] = r.Markdown
			if want := Extractors[name].ID("CC-MAIN-2026-30"); r.Extractor != want {
				t.Fatalf("%s: row extractor = %q, want %q", name, r.Extractor, want)
			}
		}
	}

	for _, name := range []string{"readability", "raw"} {
		if len(md[name]) != len(md["h2m"]) {
			t.Fatalf("%s produced %d documents, h2m produced %d", name, len(md[name]), len(md["h2m"]))
		}
		for id := range md["h2m"] {
			if _, ok := md[name][id]; !ok {
				t.Fatalf("%s is missing doc_id %s that h2m produced", name, id)
			}
		}
	}

	// The article survives every engine. Only raw keeps the furniture, which is
	// the difference the flag exists for.
	for _, name := range []string{"h2m", "readability", "raw"} {
		for id, text := range md[name] {
			if !strings.Contains(text, "quarterly report") {
				t.Fatalf("%s dropped the article from %s: %q", name, id, text)
			}
			hasFooter := strings.Contains(text, "Privacy policy")
			if name == "raw" && !hasFooter {
				t.Fatalf("raw dropped the footer, which is the one thing it is for: %q", text)
			}
			if name != "raw" && hasFooter {
				t.Fatalf("%s kept the footer: %q", name, text)
			}
		}
	}
}

// wetShard builds a WET file: WARC conversion records holding plain text.
func wetShard(t *testing.T, pages map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	for uri, text := range pages {
		rec := fmt.Sprintf("WARC/1.0\r\nWARC-Type: conversion\r\nWARC-Target-URI: %s\r\nWARC-Date: 2026-07-10T00:00:00Z\r\nWARC-Record-ID: <urn:uuid:%x>\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n%s\r\n\r\n",
			uri, MarkdownDocID(uri)[:16], len(text), text)
		zw := kgzip.NewWriter(&buf)
		if _, err := zw.Write([]byte(rec)); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return buf.Bytes()
}

// TestPackStreamWETSource covers the other half of --source-kind: a WET shard
// holds no HTML at all, so the pipeline has to read conversion records and pass
// the text through instead of extracting anything.
func TestPackStreamWETSource(t *testing.T) {
	pages := map[string]string{
		"https://example.com/vi": vieText,
		"https://example.com/en": engText,
	}
	out := filepath.Join(t.TempDir(), "wet.parquet")
	stats, err := packStream(context.Background(),
		bytes.NewReader(wetShard(t, pages)),
		MarkdownPackConfig{OutPath: out, Workers: 2, CrawlID: "CC-MAIN-2026-30", Extractor: Extractors["wet"]},
		MarkdownStats{}, time.Now())
	if err != nil {
		t.Fatalf("packStream: %v", err)
	}
	if stats.Rows != 2 {
		t.Fatalf("Rows = %d, want 2", stats.Rows)
	}
	rows, err := parquet.ReadFile[MarkdownRow](out)
	if err != nil {
		t.Fatalf("read parquet: %v", err)
	}
	for _, r := range rows {
		want, ok := pages[r.URL]
		if !ok {
			t.Fatalf("unexpected url %q", r.URL)
		}
		if r.Markdown != want {
			t.Fatalf("%s: text was changed on the way through:\n got %q\nwant %q", r.URL, r.Markdown, want)
		}
		if r.Extractor != "wet@CC-MAIN-2026-30" {
			t.Fatalf("%s: extractor = %q", r.URL, r.Extractor)
		}
		// The language filter reads the same column whatever produced it, which
		// is the reason WET is worth having as a source at all.
		if r.URL == "https://example.com/vi" && r.Language != "vie" {
			t.Fatalf("wet text was not identified: %q at %.2f", r.Language, r.LangConfidence)
		}
	}
}

// TestWETSourceRespectsLangFilter checks the filter still applies when the text
// came from Common Crawl rather than from our own extractor.
func TestWETSourceRespectsLangFilter(t *testing.T) {
	out := filepath.Join(t.TempDir(), "wet.parquet")
	stats, err := packStream(context.Background(),
		bytes.NewReader(wetShard(t, map[string]string{
			"https://example.com/vi": vieText,
			"https://example.com/en": engText,
		})),
		MarkdownPackConfig{OutPath: out, Workers: 2, CrawlID: "CC-MAIN-2026-30", Extractor: Extractors["wet"], Lang: "vie"},
		MarkdownStats{}, time.Now())
	if err != nil {
		t.Fatalf("packStream: %v", err)
	}
	if stats.Rows != 1 || stats.LangDropped != 1 {
		t.Fatalf("Rows = %d, LangDropped = %d, want 1 and 1", stats.Rows, stats.LangDropped)
	}
}

// TestREADMENamesTheEngine covers the second done-when: the dataset card has to
// say which engine produced the data, and it has to say the version too.
func TestREADMENamesTheEngine(t *testing.T) {
	for _, name := range []string{"h2m", "readability", "raw", "wet"} {
		id := Extractors[name].ID("CC-MAIN-2026-30")
		card := GenerateMarkdownREADME(MarkdownDatasetStats{
			CrawlID:         "CC-MAIN-2026-30",
			CommittedShards: 1,
			TotalShards:     1,
			Rows:            100,
			Extractor:       id,
		})
		if !strings.Contains(card, id) {
			t.Fatalf("card for %s never names %q", name, id)
		}
		if !strings.Contains(card, "`extractor`") {
			t.Fatalf("card for %s does not document the extractor column", name)
		}
	}

	// A WET dataset came from a different manifest and skipped extraction, so
	// the steps that describe the input have to say so. Telling a reader to
	// download .warc.gz for a dataset built from WET files is wrong in the one
	// place they go to find out where the text came from.
	wetCard := GenerateMarkdownREADME(MarkdownDatasetStats{
		CrawlID: "CC-MAIN-2026-30", CommittedShards: 1, TotalShards: 1, Rows: 100,
		Extractor: Extractors["wet"].ID("CC-MAIN-2026-30"),
	})
	if !strings.Contains(wetCard, ".warc.wet.gz") || strings.Contains(wetCard, "**Download** raw .warc.gz") {
		t.Fatal("the WET card still describes downloading WARC files")
	}
	if strings.Contains(wetCard, "HTTP 200 responses") {
		t.Fatal("the WET card still describes the response filter, which WET records do not have")
	}

	// And a filtered dataset says so, because "this is the Vietnamese subset" is
	// not something a reader should have to infer from the rows.
	card := GenerateMarkdownREADME(MarkdownDatasetStats{
		CrawlID: "CC-MAIN-2026-30", CommittedShards: 1, TotalShards: 1, Rows: 100, Lang: "vie",
	})
	if !strings.Contains(card, "filtered to `vie`") {
		t.Fatal("card does not mention the language filter")
	}
}

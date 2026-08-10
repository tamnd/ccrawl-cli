package ccrawl

import (
	"strings"
	"testing"
)

func TestColumnarSQL(t *testing.T) {
	q := ColumnarQuery{
		Crawl:  "CC-MAIN-2026-25",
		Domain: "example.com",
		TLD:    "gov",
		MIME:   "application/pdf",
		Status: 200,
		Limit:  10,
	}
	sql := q.SQL(SourceHTTPS)
	for _, want := range []string{
		"url_host_registered_domain = 'example.com'",
		"url_host_tld = 'gov'",
		"content_mime_detected = 'application/pdf'",
		"fetch_status = 200",
		"crawl=CC-MAIN-2026-25",
		"subset=warc",
		"LIMIT 10",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("SQL missing %q in:\n%s", want, sql)
		}
	}
}

func TestColumnarSQLEscaping(t *testing.T) {
	q := ColumnarQuery{Crawl: "CC-MAIN-2026-25", Domain: "o'brien.example"}
	sql := q.SQL(SourceHTTPS)
	if !strings.Contains(sql, "o''brien.example") {
		t.Errorf("single quote not escaped: %s", sql)
	}
}

// A domain query adds a url_surtkey prefix predicate so the engine can prune
// row groups, covering the apex and every subdomain without matching a
// look-alike domain like example2.com.
func TestColumnarSQLSurtkeyDomain(t *testing.T) {
	q := ColumnarQuery{Crawl: "CC-MAIN-2026-25", Domain: "example.com"}
	sql := q.SQL(SourceHTTPS)
	for _, want := range []string{
		"url_surtkey LIKE 'com,example)%'",
		"url_surtkey LIKE 'com,example,%'",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("SQL missing %q in:\n%s", want, sql)
		}
	}
	// The exact registered-domain filter still bounds the result.
	if !strings.Contains(sql, "url_host_registered_domain = 'example.com'") {
		t.Errorf("registered-domain equality dropped:\n%s", sql)
	}
}

// Every negated filter has to spell out IS NULL. In SQL a comparison against a
// null is unknown, so a bare <> quietly drops every unlabelled row, and those
// rows are the whole reason the flag exists.
func TestColumnarSQLNegationKeepsNulls(t *testing.T) {
	q := ColumnarQuery{
		Crawl:     "CC-MAIN-2026-25",
		NotTLD:    "vn",
		NotMIME:   "text/html",
		NotLang:   "vie",
		NotStatus: 200,
	}
	sql := q.SQL(SourceHTTPS)
	for _, want := range []string{
		"(url_host_tld IS NULL OR url_host_tld <> 'vn')",
		"(content_mime_detected IS NULL OR content_mime_detected <> 'text/html')",
		"(content_languages IS NULL OR content_languages NOT LIKE '%vie%')",
		"(fetch_status IS NULL OR fetch_status <> 200)",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("SQL missing %q in:\n%s", want, sql)
		}
	}
}

func TestColumnarSQLSets(t *testing.T) {
	q := ColumnarQuery{
		Crawl:   "CC-MAIN-2026-25",
		Hosts:   []string{"a.example.com", "o'brien.example"},
		Domains: []string{"example.com"},
	}
	sql := q.SQL(SourceHTTPS)
	for _, want := range []string{
		"url_host_name IN ('a.example.com', 'o''brien.example')",
		"url_host_registered_domain IN ('example.com')",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("SQL missing %q in:\n%s", want, sql)
		}
	}
	// A set of ten thousand hosts is one statement, not ten thousand.
	if n := strings.Count(sql, "SELECT"); n != 1 {
		t.Errorf("want a single SELECT, got %d:\n%s", n, sql)
	}
}

func TestColumnarSQLSurtkeyHost(t *testing.T) {
	q := ColumnarQuery{Crawl: "CC-MAIN-2026-25", Host: "www.example.co.uk"}
	sql := q.SQL(SourceHTTPS)
	if want := "url_surtkey LIKE 'uk,co,example,www)%'"; !strings.Contains(sql, want) {
		t.Errorf("SQL missing %q in:\n%s", want, sql)
	}
}

func TestSurtHostKey(t *testing.T) {
	cases := map[string]string{
		"example.com":       "com,example",
		"www.example.com":   "com,example,www",
		"sub.example.co.uk": "uk,co,example,sub",
		"  Example.COM.":    "com,example",
		"":                  "",
	}
	for in, want := range cases {
		if got := surtHostKey(in); got != want {
			t.Errorf("surtHostKey(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestColumnarSourceS3(t *testing.T) {
	src := ColumnarSource("CC-MAIN-2026-25", "warc", SourceS3)
	if !strings.HasPrefix(src, "s3://commoncrawl/") {
		t.Errorf("S3 source = %q", src)
	}
}

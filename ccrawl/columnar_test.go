package ccrawl

import (
	"strings"
	"testing"
)

func TestColumnarSQL(t *testing.T) {
	q := ColumnarQuery{
		Crawl:  "CC-MAIN-2024-51",
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
		"crawl=CC-MAIN-2024-51",
		"subset=warc",
		"LIMIT 10",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("SQL missing %q in:\n%s", want, sql)
		}
	}
}

func TestColumnarSQLEscaping(t *testing.T) {
	q := ColumnarQuery{Crawl: "CC-MAIN-2024-51", Domain: "o'brien.example"}
	sql := q.SQL(SourceHTTPS)
	if !strings.Contains(sql, "o''brien.example") {
		t.Errorf("single quote not escaped: %s", sql)
	}
}

// A domain query adds a url_surtkey prefix predicate so the engine can prune
// row groups, covering the apex and every subdomain without matching a
// look-alike domain like example2.com.
func TestColumnarSQLSurtkeyDomain(t *testing.T) {
	q := ColumnarQuery{Crawl: "CC-MAIN-2024-51", Domain: "example.com"}
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

func TestColumnarSQLSurtkeyHost(t *testing.T) {
	q := ColumnarQuery{Crawl: "CC-MAIN-2024-51", Host: "www.example.co.uk"}
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
	src := ColumnarSource("CC-MAIN-2024-51", "warc", SourceS3)
	if !strings.HasPrefix(src, "s3://commoncrawl/") {
		t.Errorf("S3 source = %q", src)
	}
}

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

func TestColumnarSourceS3(t *testing.T) {
	src := ColumnarSource("CC-MAIN-2024-51", "warc", SourceS3)
	if !strings.HasPrefix(src, "s3://commoncrawl/") {
		t.Errorf("S3 source = %q", src)
	}
}

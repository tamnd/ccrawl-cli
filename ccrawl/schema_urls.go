package ccrawl

import "time"

// URLRow is the projected, faithful subset of the Common Crawl cc-index columnar
// URL table published to open-index/ccrawl-urls. The parquet tags carry Common
// Crawl's own column names, so the output is a subset a cc-index user recognizes.
//
// The same struct is used to read (parquet-go projects only these columns out of
// the ~32 in the source part) and to write (the output shard holds exactly these
// columns in this order). Types mirror the source: fetch_time is a timestamp,
// fetch_status is the source SMALLINT read as int32, and the two warc_record
// fields are the source INTEGER read as int32.
type URLRow struct {
	URLSurtKey              string    `parquet:"url_surtkey"`
	URL                     string    `parquet:"url"`
	URLHostName             string    `parquet:"url_host_name"`
	URLHostRegisteredDomain string    `parquet:"url_host_registered_domain"`
	URLHostTLD              string    `parquet:"url_host_tld"`
	URLProtocol             string    `parquet:"url_protocol"`
	FetchTime               time.Time `parquet:"fetch_time,timestamp(microsecond)"`
	FetchStatus             int32     `parquet:"fetch_status"`
	FetchRedirect           string    `parquet:"fetch_redirect"`
	ContentDigest           string    `parquet:"content_digest"`
	ContentMIMEType         string    `parquet:"content_mime_type"`
	ContentMIMEDetected     string    `parquet:"content_mime_detected"`
	ContentCharset          string    `parquet:"content_charset"`
	ContentLanguages        string    `parquet:"content_languages"`
	ContentTruncated        string    `parquet:"content_truncated"`
	WARCFilename            string    `parquet:"warc_filename"`
	WARCRecordOffset        int32     `parquet:"warc_record_offset"`
	WARCRecordLength        int32     `parquet:"warc_record_length"`
}

// URLColumns is the ordered list of output column names, used by the dataset card
// field table and by tests that assert the schema has not drifted.
var URLColumns = []string{
	"url_surtkey", "url", "url_host_name", "url_host_registered_domain",
	"url_host_tld", "url_protocol", "fetch_time", "fetch_status", "fetch_redirect",
	"content_digest", "content_mime_type", "content_mime_detected", "content_charset",
	"content_languages", "content_truncated", "warc_filename",
	"warc_record_offset", "warc_record_length",
}

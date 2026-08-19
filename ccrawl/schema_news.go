package ccrawl

import "time"

// NewsRow is one captured article in the CC-NEWS index published to
// open-index/ccrawl-news. Common Crawl publishes no index for CC-NEWS, neither
// CDX nor columnar, so unlike the URL index this is not a mirror of anything:
// every row here is read out of the WARC files themselves.
//
// The column names are cc-index's, deliberately. A query written against
// open-index/ccrawl-urls runs here unchanged, and warc_filename with the two
// warc_record columns is the same location triple ccrawl fetch reads, so a row
// out of this dataset fetches the article it describes.
//
// Where the two datasets differ is who computed a column, and that difference is
// worth knowing before trusting one:
//
//   - url_surtkey, the url_host_* columns, content_languages and
//     content_mime_detected do not exist anywhere in a CC-NEWS WARC. They are
//     computed here. cc-index gets its language labels from CLD2 run over the raw
//     HTML; these come from ccrawl's own identifier run over the extracted text,
//     so the two will not always agree even about the same page.
//   - fetch_status, content_mime_type and content_charset are read off the HTTP
//     response the crawler stored, not detected.
//   - content_digest is Common Crawl's own WARC-Payload-Digest, copied across
//     with its "sha1:" prefix removed so it matches the cc-index spelling.
//
// The two warc_record columns are int64 rather than cc-index's int32. A CC-NEWS
// WARC runs to about a gigabyte, which fits in an int32 with under half its range
// to spare, and a schema that is one good month away from silently wrapping to a
// negative offset is not worth the four bytes it saves.
type NewsRow struct {
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
	WARCRecordOffset        int64     `parquet:"warc_record_offset"`
	WARCRecordLength        int64     `parquet:"warc_record_length"`

	// The columns past this point have no cc-index counterpart. They are kept
	// last so the shared prefix above is byte for byte the URL index's schema.

	// ContentLanguageConfidence is what the identifier thought of its own answer,
	// 0 to 1. cc-index has nowhere to put this and throws it away, which is how a
	// coin flip on a nav bar ends up looking like a fact. A row with a language
	// and a low confidence is a guess, and a filter that cares should say so.
	// It is 0 when ContentLanguages is empty, which means there was too little
	// text to ask the question rather than that the answer was bad.
	ContentLanguageConfidence float64 `parquet:"content_language_confidence"`

	// ContentLanguageDeclared is the <html lang> attribute exactly as the page
	// wrote it, so it is BCP-47 ("pt-BR", "en") where ContentLanguages is ISO
	// 639-3 ("por", "eng"). It is what the publisher says the article is, which
	// on a news site is usually right and is occasionally a template default
	// that says en for an entire non-English edition. Kept separate from the
	// detected code rather than merged with it, because the interesting rows are
	// the ones where they disagree.
	ContentLanguageDeclared string `parquet:"content_language_declared"`

	// ContentLength is the size in bytes of the HTTP response body as stored.
	// CC-NEWS keeps the decoded body and moves the original Content-Encoding
	// aside, so this is the decompressed size and it will not match the
	// Content-Length header the origin sent.
	ContentLength int64 `parquet:"content_length"`

	// WARCRecordID is the record's WARC-Record-ID, the urn:uuid the crawler gave
	// it. It is the only identifier that survives independently of where the
	// record sits, so it is what ties a row back to its record if a file is ever
	// republished at a different offset.
	WARCRecordID string `parquet:"warc_record_id"`
}

// NewsColumns is the ordered list of output column names, used by the dataset
// card field table and by the test that asserts the schema has not drifted.
var NewsColumns = []string{
	"url_surtkey", "url", "url_host_name", "url_host_registered_domain",
	"url_host_tld", "url_protocol", "fetch_time", "fetch_status", "fetch_redirect",
	"content_digest", "content_mime_type", "content_mime_detected", "content_charset",
	"content_languages", "content_truncated", "warc_filename",
	"warc_record_offset", "warc_record_length",
	"content_language_confidence", "content_language_declared",
	"content_length", "warc_record_id",
}

package ccrawl

import (
	"context"
	"fmt"
	"strings"
)

// FileURL turns a Common Crawl relative path into a fetchable URL for the given
// source. HTTPS uses the CloudFront mirror; S3 uses the bucket URI.
func FileURL(path string, src Source) string {
	path = strings.TrimPrefix(path, "/")
	if src == SourceS3 {
		return S3BaseURL + path
	}
	return DataBaseURL + path
}

// HTTPSURL always returns the HTTPS mirror URL (used for control-plane fetches
// like manifests and collinfo regardless of the bulk source).
func HTTPSURL(path string) string {
	return DataBaseURL + strings.TrimPrefix(path, "/")
}

// cdxAPIURL is the CDX server endpoint for one crawl.
func cdxAPIURL(crawlID string) string {
	return CDXBaseURL + crawlID + "-index"
}

// pathsURL is the URL of a crawl's gzipped path manifest for a file kind.
func pathsURL(crawlID, kind string) string {
	return DataBaseURL + "crawl-data/" + crawlID + "/" + kind + ".paths.gz"
}

// ColumnarSource returns the parquet glob for one crawl's columnar index subset
// (subset is warc, crawldiagnostics, or robotstxt).
func ColumnarSource(crawlID, subset string, src Source) string {
	base := S3BaseURL
	if src != SourceS3 {
		base = DataBaseURL
	}
	if subset == "" {
		subset = "warc"
	}
	return base + ColumnarPrefix + "crawl=" + crawlID + "/subset=" + subset + "/*.parquet"
}

// ColumnarParquetURLs resolves the columnar index glob into the explicit list of
// parquet file URLs for one crawl and subset. Common Crawl's bucket does not
// allow anonymous listing, so a duckdb run cannot expand the `*.parquet` glob
// over HTTPS (or anonymous S3) on its own. The crawl publishes the full file
// list in cc-index-table.paths.gz, so we read that manifest (cached) and turn
// each entry into a fetchable URL for the chosen source.
func ColumnarParquetURLs(ctx context.Context, h *HTTPClient, cache *Cache, crawlID, subset string, src Source) ([]string, error) {
	if subset == "" {
		subset = "warc"
	}
	paths, err := FetchPaths(ctx, h, cache, crawlID, "cc-index-table")
	if err != nil {
		return nil, err
	}
	marker := "/subset=" + subset + "/"
	var urls []string
	for _, p := range paths {
		if strings.Contains(p, marker) {
			urls = append(urls, FileURL(p, src))
		}
	}
	if len(urls) == 0 {
		return nil, fmt.Errorf("no parquet files for crawl %s subset %s", crawlID, subset)
	}
	return urls, nil
}

// ParquetListLiteral renders parquet URLs as a duckdb list literal, e.g.
// ['https://a', 'https://b'], suitable as the argument to read_parquet.
func ParquetListLiteral(urls []string) string {
	quoted := make([]string, len(urls))
	for i, u := range urls {
		quoted[i] = "'" + sqlEscape(u) + "'"
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

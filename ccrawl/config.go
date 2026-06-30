package ccrawl

import (
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// Common Crawl endpoints.
const (
	CollInfoURL = "https://index.commoncrawl.org/collinfo.json"
	DataBaseURL = "https://data.commoncrawl.org/"
	CDXBaseURL  = "https://index.commoncrawl.org/"
	S3BaseURL   = "s3://commoncrawl/"

	// ColumnarPrefix is the root of the columnar (Parquet) index.
	ColumnarPrefix = "cc-index/table/cc-main/warc/"

	// UserAgent identifies the client politely to Common Crawl's CDN.
	UserAgent = "ccrawl/1.0 (+https://github.com/tamnd/ccrawl-cli)"
)

// Defaults for the client and downloader.
const (
	DefaultTimeout = 120 * time.Second
	DefaultRetries = 5
	DefaultDelay   = 200 * time.Millisecond

	// DefaultBackoff is the base wait before the first retry; later retries grow
	// it exponentially. DefaultBackoffMax caps a single wait so a long stall or a
	// large Retry-After never blocks a run indefinitely.
	DefaultBackoff    = 1 * time.Second
	DefaultBackoffMax = 30 * time.Second
)

// Source selects the transport used for bulk data files.
type Source string

const (
	SourceHTTPS Source = "https"
	SourceS3    Source = "s3"
)

// Config controls library behaviour. The zero value is not usable; call
// DefaultConfig and adjust.
type Config struct {
	DataDir    string
	CacheDir   string
	DBPath     string
	Source     Source
	Workers    int
	Timeout    time.Duration
	Delay      time.Duration
	Retries    int
	Backoff    time.Duration
	BackoffMax time.Duration
	UserAgent  string
	CrawlID    string
}

// DefaultConfig returns a Config rooted at the XDG data/cache directories, with
// the most recent crawl resolved lazily (CrawlID == "latest").
func DefaultConfig() Config {
	return Config{
		DataDir:    dataDir(),
		CacheDir:   cacheDir(),
		DBPath:     filepath.Join(dataDir(), "ccrawl.duckdb"),
		Source:     SourceHTTPS,
		Workers:    defaultWorkers(),
		Timeout:    DefaultTimeout,
		Delay:      DefaultDelay,
		Retries:    DefaultRetries,
		Backoff:    DefaultBackoff,
		BackoffMax: DefaultBackoffMax,
		UserAgent:  UserAgent,
		CrawlID:    "latest",
	}
}

// RawDir is where downloaded archive files land.
func (c Config) RawDir() string { return filepath.Join(c.DataDir, "raw") }

// ParquetDir is where converted Parquet files land.
func (c Config) ParquetDir() string { return filepath.Join(c.DataDir, "parquet") }

func defaultWorkers() int {
	return min(max(runtime.NumCPU(), 1), 8)
}

// dataDir is the root for everything ccrawl writes: downloads, converted
// Parquet, the local DuckDB file, and the cache. It defaults to ~/data/ccrawl so
// all state lives under one predictable, easy-to-find tree. CCRAWL_DATA_DIR
// overrides it.
func dataDir() string {
	if d := os.Getenv("CCRAWL_DATA_DIR"); d != "" {
		return d
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "data", "ccrawl")
}

// cacheDir holds the small cached manifests and collinfo. It sits under the data
// dir by default so the whole footprint is one tree; CCRAWL_CACHE_DIR overrides.
func cacheDir() string {
	if d := os.Getenv("CCRAWL_CACHE_DIR"); d != "" {
		return d
	}
	return filepath.Join(dataDir(), "cache")
}

// ConfigDir returns the directory holding the config file.
func ConfigDir() string {
	if d := os.Getenv("XDG_CONFIG_HOME"); d != "" {
		return filepath.Join(d, "ccrawl")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "ccrawl")
}

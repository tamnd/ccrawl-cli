package ccrawl

import (
	"os"
	"path/filepath"
)

// LibraryDir is the root of the structured dataset library that the --library
// flag downloads into and processes from. It is deliberately separate from the
// data dir: the data dir (see Config) holds ad-hoc downloads, the cache, and the
// local DuckDB file, while the library is a curated, browsable corpus you build
// up over time. CCRAWL_LIBRARY overrides the default of ~/notes/ccrawl.
func LibraryDir() string {
	if d := os.Getenv("CCRAWL_LIBRARY"); d != "" {
		return d
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "notes", "ccrawl")
}

// Library is a structured corpus of Common Crawl archive files for one crawl,
// rooted at Root. The layout is predictable so a directory listing tells you
// exactly what you have:
//
//	<root>/<crawl>/<kind>/<file>.gz               raw downloaded archives
//	<root>/<crawl>/<format>/<kind>/<file>.<ext>   processed output (parquet|jsonl)
//
// Files are stored flat under each kind by their base name. A Common Crawl file
// name already encodes its segment and timestamp and is unique within a crawl,
// so the base name alone is a safe, stable key with no risk of collision.
type Library struct {
	Root  string
	Crawl string
}

// NewLibrary returns a Library rooted at root (or LibraryDir() when root is
// empty) for the given crawl ID.
func NewLibrary(root, crawl string) Library {
	if root == "" {
		root = LibraryDir()
	}
	return Library{Root: root, Crawl: crawl}
}

// CrawlDir is the per-crawl root, the parent of every kind and format directory.
func (l Library) CrawlDir() string { return filepath.Join(l.Root, l.Crawl) }

// RawDir is where downloaded archives of a kind live.
func (l Library) RawDir(kind string) string {
	return filepath.Join(l.Root, l.Crawl, kind)
}

// ProcessedDir is where processed output of a kind lives, grouped by format
// (parquet or jsonl) so the same archives can be materialised more than one way
// side by side.
func (l Library) ProcessedDir(format, kind string) string {
	return filepath.Join(l.Root, l.Crawl, format, kind)
}

// RawPath is the local path a given Common Crawl path maps to under the library.
func (l Library) RawPath(kind, ccPath string) string {
	return filepath.Join(l.RawDir(kind), filepath.Base(ccPath))
}

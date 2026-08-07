//go:build !linux && !darwin

package ccrawl

// currentRSSBytes returns 0 on platforms where we have no way to read the
// resident set size without cgo. The journal writes rss_bytes as 0 there rather
// than reporting a number it made up.
func currentRSSBytes() int64 { return 0 }

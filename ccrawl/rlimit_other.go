//go:build !unix

package ccrawl

// RaiseFileLimit is a no-op on platforms without POSIX rlimits. It reports a
// conventional ceiling so callers have a non-zero number to size against.
func RaiseFileLimit() (uint64, error) { return 1024, nil }

// FileLimit reports a conventional ceiling on platforms without POSIX rlimits.
func FileLimit() uint64 { return 1024 }

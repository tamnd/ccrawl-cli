//go:build unix

package ccrawl

import "syscall"

// RaiseFileLimit lifts the process open-file soft limit to the hard limit so a
// high-concurrency refetch can hold thousands of sockets at once. The default
// soft limit on most Linux hosts is 1024, which caps in-flight connections well
// below what the fetch engine can drive: every live socket, every DNS UDP
// socket, and every open Parquet/WARC file counts against it, so a few hundred
// workers already brush the ceiling and further workers fail with "too many open
// files". Raising the soft limit to the hard limit (commonly 1048576) removes
// that wall without needing root or an ulimit change in the launching shell.
//
// It returns the soft limit now in effect (the value after the raise, or the
// original value if the raise was not possible), so callers can size their
// worker pool to what the OS will actually allow.
func RaiseFileLimit() (uint64, error) {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		return 0, err
	}
	if lim.Cur >= lim.Max {
		return lim.Cur, nil
	}
	want := lim
	want.Cur = want.Max
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &want); err != nil {
		// Leave the original soft limit in place and report it; the caller can
		// still run, just with a lower concurrency ceiling.
		return lim.Cur, err
	}
	// Re-read: some kernels clamp the granted value (notably macOS caps at
	// OPEN_MAX), so report what actually took effect rather than what we asked
	// for.
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		return want.Cur, nil
	}
	return lim.Cur, nil
}

// FileLimit reports the current open-file soft limit without changing it.
func FileLimit() uint64 {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		return 0
	}
	return lim.Cur
}

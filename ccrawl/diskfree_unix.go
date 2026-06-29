//go:build unix

package ccrawl

import "syscall"

// freeDiskBytes returns the bytes available to an unprivileged user on the
// filesystem that holds path. It returns 0 on error, which callers treat as
// "unknown, do not block".
func freeDiskBytes(path string) int64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0
	}
	// Bavail is blocks available to non-root; Bsize is the block size. Both the
	// Linux and Darwin Statfs_t expose these, with different integer widths, so
	// convert through uint64 before multiplying.
	return int64(uint64(st.Bavail) * uint64(st.Bsize))
}

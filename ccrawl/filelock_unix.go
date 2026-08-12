//go:build !windows

package ccrawl

import (
	"os"

	"golang.org/x/sys/unix"
)

// lockFile takes an exclusive advisory lock on f, blocking until it is free.
// The lock is held for a read and a write of sixteen bytes, so the wait is
// microseconds even with a dozen processes on the file.
func lockFile(f *os.File) error {
	for {
		err := unix.Flock(int(f.Fd()), unix.LOCK_EX)
		// Go's runtime preemption signals interrupt the syscall, which is not a
		// failure to lock, so retry rather than degrade the limiter over it.
		if err == unix.EINTR {
			continue
		}
		return err
	}
}

// unlockFile releases the lock taken by lockFile.
func unlockFile(f *os.File) error {
	for {
		err := unix.Flock(int(f.Fd()), unix.LOCK_UN)
		if err == unix.EINTR {
			continue
		}
		return err
	}
}

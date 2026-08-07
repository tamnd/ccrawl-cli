//go:build darwin

package ccrawl

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

// currentRSSBytes returns this process's resident set size, or 0 when it cannot
// be read.
//
// macOS has no /proc, and the call that answers this directly (task_info) is a
// mach trap that needs cgo, which this binary does not use. ps is in the base
// system and reports the same number in kilobytes, and a progress tick asks for
// it every 30 seconds, so the subprocess is not on any hot path. If ps is
// missing the peak RSS from getrusage stands in: it overstates a long run, but
// it is never zero, which is the failure this replaces.
func currentRSSBytes() int64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err == nil {
		if kb, perr := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64); perr == nil {
			return kb * 1024
		}
	}
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	// Darwin reports maxrss in bytes, unlike Linux which uses kilobytes.
	return int64(ru.Maxrss)
}

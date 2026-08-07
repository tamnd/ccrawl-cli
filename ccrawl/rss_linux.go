//go:build linux

package ccrawl

import (
	"os"
	"strconv"
	"strings"
)

// currentRSSBytes returns this process's resident set size, or 0 when it cannot
// be read. Linux publishes it as VmRSS in /proc/self/status, in kilobytes.
func currentRSSBytes() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return kb * 1024
	}
	return 0
}

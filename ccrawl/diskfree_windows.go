//go:build windows

package ccrawl

import "golang.org/x/sys/windows"

// freeDiskBytes returns the bytes available to the calling user on the volume
// that holds path. It returns 0 on error, which callers treat as "unknown, do
// not block". Windows has no statfs, so this asks the Win32 API directly:
// GetDiskFreeSpaceEx reports the free bytes the caller's quota allows, which is
// the right number for the same "will the output fit" check the unix path makes.
func freeDiskBytes(path string) int64 {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0
	}
	var freeForCaller, total, totalFree uint64
	if err := windows.GetDiskFreeSpaceEx(p, &freeForCaller, &total, &totalFree); err != nil {
		return 0
	}
	return int64(freeForCaller)
}

package ccrawl

import "fmt"

// fmtBytes renders a byte count in binary units for dataset cards. A zero or
// negative count renders as a dash so an unknown size reads cleanly.
func fmtBytes(n int64) string {
	const (
		kb = int64(1024)
		mb = kb * 1024
		gb = mb * 1024
		tb = gb * 1024
	)
	switch {
	case n <= 0:
		return "-"
	case n >= tb:
		return fmt.Sprintf("%.1f TB", float64(n)/float64(tb))
	case n >= gb:
		return fmt.Sprintf("%.1f GB", float64(n)/float64(gb))
	case n >= mb:
		return fmt.Sprintf("%.0f MB", float64(n)/float64(mb))
	default:
		return fmt.Sprintf("%.0f KB", float64(n)/float64(kb))
	}
}

// scaleEst projects a per-committed-shard total up to the full shard count, used
// to show an approximate size while a crawl is still publishing.
func scaleEst(total int64, committed, totalShards int) int64 {
	if committed <= 0 {
		return 0
	}
	return total * int64(totalShards) / int64(committed)
}

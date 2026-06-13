package ccrawl

import "strconv"

func atoi64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

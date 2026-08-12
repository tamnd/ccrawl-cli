package cli

import "testing"

// The api server has no authentication, so the question "can anything else on
// the network reach this" decides whether a warning is printed. Getting it
// wrong in the quiet direction is the bad one: a bare ":8080" is every
// interface, and it looks local.
func TestReachableOffThisMachine(t *testing.T) {
	for _, tc := range []struct {
		addr string
		want bool
	}{
		{"127.0.0.1:8080", false},
		{"localhost:8080", false},
		{"[::1]:8080", false},
		{":8080", true},
		{"0.0.0.0:8080", true},
		{"[::]:8080", true},
		{"192.168.1.10:8080", true},
		{"example.com:8080", true},
	} {
		if got := reachableOffThisMachine(tc.addr); got != tc.want {
			t.Errorf("reachableOffThisMachine(%q) = %v, want %v", tc.addr, got, tc.want)
		}
	}
}

func TestDefaultAPIAddrIsLoopback(t *testing.T) {
	if reachableOffThisMachine(defaultAPIAddr) {
		t.Fatalf("the default listen address %s is reachable from other machines", defaultAPIAddr)
	}
}

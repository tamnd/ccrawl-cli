package cli

import "testing"

func TestURLKeep(t *testing.T) {
	cases := []struct {
		url, contains, not string
		want               bool
	}{
		{"https://example.com/blog/post", "", "", true},
		{"https://example.com/blog/post", "/blog/", "", true},
		{"https://example.com/about", "/blog/", "", false},
		{"https://example.com/robots.txt", "", "/robots.txt", false},
		{"https://example.com/blog/post", "/blog/", "/robots.txt", true},
		{"https://example.com/blog/robots.txt", "/blog/", "/robots.txt", false},
	}
	for _, c := range cases {
		if got := urlKeep(c.url, c.contains, c.not); got != c.want {
			t.Errorf("urlKeep(%q, %q, %q) = %v, want %v", c.url, c.contains, c.not, got, c.want)
		}
	}
}

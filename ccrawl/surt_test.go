package ccrawl

import "testing"

func TestSURT(t *testing.T) {
	cases := map[string]string{
		"https://www.example.com/a/b?q=1": "com,example)/a/b?q=1",
		"http://example.com":              "com,example)/",
		"https://sub.example.co.uk/p":     "uk,co,example,sub)/p",
		"example.com/path":                "com,example)/path",
	}
	for in, want := range cases {
		if got := SURT(in); got != want {
			t.Errorf("SURT(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestInferMatchType(t *testing.T) {
	cases := []struct {
		in, url, mt string
	}{
		{"example.com", "example.com", "exact"},
		{"example.com/*", "example.com/", "prefix"},
		{"*.example.com", "example.com", "domain"},
		{"example.com*", "example.com", "prefix"},
	}
	for _, c := range cases {
		url, mt := InferMatchType(c.in)
		if url != c.url || mt != c.mt {
			t.Errorf("InferMatchType(%q) = (%q, %q), want (%q, %q)", c.in, url, mt, c.url, c.mt)
		}
	}
}

func TestHostOf(t *testing.T) {
	if got := HostOf("https://WWW.Example.COM/x"); got != "www.example.com" {
		t.Errorf("HostOf = %q", got)
	}
	if got := HostOf("example.org/p"); got != "example.org" {
		t.Errorf("HostOf bare = %q", got)
	}
}

func TestReverseHost(t *testing.T) {
	if got := reverseHost("www.example.com"); got != "com.example.www" {
		t.Errorf("reverseHost = %q", got)
	}
}

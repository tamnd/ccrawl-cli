package ccrawl

import (
	"net/url"
	"strings"
)

// SURT converts a URL into a Sort-friendly URI Reordering Transform key, the
// canonical form Common Crawl uses to sort and group its index. For example
// "https://www.example.com/a/b?q=1" becomes "com,example,www)/a/b?q=1".
//
// The transform lower-cases the scheme and host, reverses the host labels,
// drops a leading "www.", strips the default port, and keeps the path and query.
func SURT(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return strings.ToLower(raw)
	}

	host := strings.ToLower(u.Hostname())
	host = strings.TrimPrefix(host, "www.")
	labels := strings.Split(host, ".")
	for i, j := 0, len(labels)-1; i < j; i, j = i+1, j-1 {
		labels[i], labels[j] = labels[j], labels[i]
	}
	key := strings.Join(labels, ",") + ")"

	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	key += strings.ToLower(path)
	if u.RawQuery != "" {
		key += "?" + u.RawQuery
	}
	return key
}

// CanonicalURL applies light canonicalization: ensure a scheme, lower-case the
// host, and drop a fragment. It does not reorder query parameters.
func CanonicalURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	u.Host = strings.ToLower(u.Host)
	u.Fragment = ""
	return u.String()
}

// HostOf returns the lower-case host of a URL, or "" if it has none.
func HostOf(raw string) string {
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}

// InferMatchType guesses the CDX matchType from a user-supplied URL pattern.
// "*.example.com" -> domain, "example.com/*" -> prefix, otherwise exact unless
// the caller already chose host/domain/prefix.
func InferMatchType(pattern string) (cleanURL, matchType string) {
	p := strings.TrimSpace(pattern)
	switch {
	case strings.HasPrefix(p, "*."):
		return strings.TrimPrefix(p, "*."), "domain"
	case strings.HasSuffix(p, "/*"):
		return strings.TrimSuffix(p, "*"), "prefix"
	case strings.HasSuffix(p, "*"):
		return strings.TrimSuffix(p, "*"), "prefix"
	default:
		return p, "exact"
	}
}

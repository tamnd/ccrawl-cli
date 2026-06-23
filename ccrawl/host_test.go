package ccrawl

import (
	"testing"
)

func TestHostFromRank(t *testing.T) {
	r := Rank{
		Key:         "www.example.com",
		HarmonicPos: 42,
		HarmonicVal: 1234567.8,
		PageRankPos: 100,
		PageRankVal: 0.00123,
	}
	rec := HostFromRank(r)
	if rec.Host != "www.example.com" {
		t.Errorf("Host = %q, want www.example.com", rec.Host)
	}
	if rec.HostRev != "com.example.www" {
		t.Errorf("HostRev = %q, want com.example.www", rec.HostRev)
	}
	if rec.TLD != "com" {
		t.Errorf("TLD = %q, want com", rec.TLD)
	}
	if rec.HarmonicPos != 42 {
		t.Errorf("HarmonicPos = %d, want 42", rec.HarmonicPos)
	}
}

func TestHostTLD(t *testing.T) {
	cases := []struct{ host, want string }{
		{"www.example.com", "com"},
		{"bbc.co.uk", "uk"},
		{"golang.org", "org"},
		{"example", "example"},
	}
	for _, c := range cases {
		got := hostTLD(c.host)
		if got != c.want {
			t.Errorf("hostTLD(%q) = %q, want %q", c.host, got, c.want)
		}
	}
}

func TestHostCDXAggSQL(t *testing.T) {
	urls := []string{"https://data.commoncrawl.org/part-00000.parquet"}
	sql := HostCDXAggSQL(urls, "CC-MAIN-2026-21", "github.com")
	if sql == "" {
		t.Fatal("empty SQL")
	}
	// Must contain key aggregation expressions
	for _, want := range []string{
		"url_host_name",
		"COUNT(*)",
		"fetch_status",
		"warc_record_length",
		"url_host_registered_domain",
		"CC-MAIN-2026-21",
		"github.com",
	} {
		if !contains(sql, want) {
			t.Errorf("SQL missing %q", want)
		}
	}
}

func TestHostCDXAggSQLNoFilter(t *testing.T) {
	urls := []string{"https://example.com/part.parquet"}
	sql := HostCDXAggSQL(urls, "CC-MAIN-2026-21", "")
	if contains(sql, "url_host_name =") {
		t.Error("SQL should not have host filter when host is empty")
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		func() bool {
			for i := 0; i+len(sub) <= len(s); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
			return false
		}())
}

func TestInt64Val(t *testing.T) {
	row := map[string]any{
		"a": float64(42),
		"b": int64(100),
		"c": nil,
		"d": "not a number",
		"e": "7081731",    // int128 string from DuckDB
		"f": "9876543210", // large int as string
	}
	if v := int64Val(row, "a"); v != 42 {
		t.Errorf("int64Val(float64) = %d, want 42", v)
	}
	if v := int64Val(row, "b"); v != 100 {
		t.Errorf("int64Val(int64) = %d, want 100", v)
	}
	if v := int64Val(row, "c"); v != 0 {
		t.Errorf("int64Val(nil) = %d, want 0", v)
	}
	if v := int64Val(row, "missing"); v != 0 {
		t.Errorf("int64Val(missing) = %d, want 0", v)
	}
	if v := int64Val(row, "e"); v != 7081731 {
		t.Errorf("int64Val(string) = %d, want 7081731", v)
	}
	if v := int64Val(row, "f"); v != 9876543210 {
		t.Errorf("int64Val(large string) = %d, want 9876543210", v)
	}
}

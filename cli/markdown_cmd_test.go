package cli

import (
	"testing"
)

func TestParseShardRange(t *testing.T) {
	total := 1000
	cases := []struct {
		spec string
		want []int
	}{
		{"0", []int{0}},
		{"5", []int{5}},
		{"0-4", []int{0, 1, 2, 3, 4}},
		{"0,2,4", []int{0, 2, 4}},
		{"0-2,5", []int{0, 1, 2, 5}},
		{"3,3,3", []int{3}}, // deduplication
	}
	for _, tc := range cases {
		got, err := parseShardRange(tc.spec, total)
		if err != nil {
			t.Errorf("parseShardRange(%q): unexpected error %v", tc.spec, err)
			continue
		}
		if len(got) != len(tc.want) {
			t.Errorf("parseShardRange(%q) = %v, want %v", tc.spec, got, tc.want)
			continue
		}
		for i, g := range got {
			if g != tc.want[i] {
				t.Errorf("parseShardRange(%q)[%d] = %d, want %d", tc.spec, i, g, tc.want[i])
			}
		}
	}
}

func TestParseShardRangeErrors(t *testing.T) {
	if _, err := parseShardRange("9999", 100); err == nil {
		t.Error("expected error for out-of-bounds shard")
	}
	if _, err := parseShardRange("notanumber", 100); err == nil {
		t.Error("expected error for non-numeric shard")
	}
	if _, err := parseShardRange("", 100); err == nil {
		t.Error("expected error for empty spec")
	}
}

func TestParseShardRangeAll(t *testing.T) {
	got, err := parseShardRange("all", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("all with total=5 gave %d shards, want 5", len(got))
	}
	for i, v := range got {
		if v != i {
			t.Errorf("[%d] = %d, want %d", i, v, i)
		}
	}
}

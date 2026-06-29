package ccrawl

import (
	"context"
	"reflect"
	"testing"
)

// primedCache returns a cache pre-loaded with a collinfo manifest so ListCrawls
// (and ResolveCrawls through it) resolves offline, newest first.
func primedCache(t *testing.T, ids ...string) *Cache {
	t.Helper()
	c := NewCache(t.TempDir(), true)
	var b []byte
	b = append(b, '[')
	for i, id := range ids {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, []byte(`{"id":"`+id+`","name":"`+id+`"}`)...)
	}
	b = append(b, ']')
	c.Put("collinfo", b)
	return c
}

func TestResolveCrawls(t *testing.T) {
	ids := []string{
		"CC-MAIN-2024-10", "CC-MAIN-2023-50", "CC-MAIN-2023-23", "CC-MAIN-2022-05",
	}
	cache := primedCache(t, ids...)
	ctx := context.Background()

	cases := []struct {
		ref  string
		want []string
	}{
		{"latest", []string{"CC-MAIN-2024-10"}},
		{"", []string{"CC-MAIN-2024-10"}},
		{"all", ids},
		{"2", []string{"CC-MAIN-2024-10", "CC-MAIN-2023-50"}},
		{"99", ids}, // clamped to what exists
		{"2023", []string{"CC-MAIN-2023-50", "CC-MAIN-2023-23"}},
		{"2024-10", []string{"CC-MAIN-2024-10"}},
		{"CC-MAIN-2022-05", []string{"CC-MAIN-2022-05"}},
		{"CC-MAIN-2024-10,CC-MAIN-2022-05", []string{"CC-MAIN-2024-10", "CC-MAIN-2022-05"}},
		{"2023,2024-10", []string{"CC-MAIN-2023-50", "CC-MAIN-2023-23", "CC-MAIN-2024-10"}},
		// duplicates collapse, order of first appearance is kept
		{"latest,CC-MAIN-2024-10,2", []string{"CC-MAIN-2024-10", "CC-MAIN-2023-50"}},
	}
	for _, c := range cases {
		got, err := ResolveCrawls(ctx, nil, cache, c.ref)
		if err != nil {
			t.Errorf("ResolveCrawls(%q): %v", c.ref, err)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("ResolveCrawls(%q) = %v, want %v", c.ref, got, c.want)
		}
	}
}

func TestResolveCrawlsErrors(t *testing.T) {
	cache := primedCache(t, "CC-MAIN-2024-10")
	ctx := context.Background()
	for _, ref := range []string{"2019", "0", "nonsense"} {
		if _, err := ResolveCrawls(ctx, nil, cache, ref); err == nil {
			t.Errorf("ResolveCrawls(%q): expected error", ref)
		}
	}
}

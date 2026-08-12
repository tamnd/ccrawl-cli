package ccrawl

import (
	"crypto/sha1"
	"encoding/hex"
	"os"
	"path/filepath"
	"time"
)

// Cache is a tiny on-disk blob cache keyed by an arbitrary string, with a TTL
// per entry. It is safe for the simple single-process use the CLI makes of it.
type Cache struct {
	dir     string
	enabled bool
}

// NewCache returns a cache rooted under dir. If dir is empty or enabled is
// false, all operations are no-ops (cache miss on every Get).
func NewCache(dir string, enabled bool) *Cache {
	return &Cache{dir: dir, enabled: enabled && dir != ""}
}

func (c *Cache) pathFor(key string) string {
	sum := sha1.Sum([]byte(key))
	return filepath.Join(c.dir, hex.EncodeToString(sum[:])+".cache")
}

// Get returns cached bytes for key if present and younger than ttl.
func (c *Cache) Get(key string, ttl time.Duration) ([]byte, bool) {
	data, age, ok := c.GetStale(key)
	if !ok {
		return nil, false
	}
	if ttl > 0 && age > ttl {
		return nil, false
	}
	return data, true
}

// GetStale returns cached bytes for key at any age, along with how old they
// are. It exists for the case where the fresh copy could not be fetched and an
// old answer beats no answer, which the caller then has to own by saying how
// old it is. Anything that just wants a cheap answer wants Get.
func (c *Cache) GetStale(key string) (data []byte, age time.Duration, ok bool) {
	if !c.enabled {
		return nil, 0, false
	}
	p := c.pathFor(key)
	info, err := os.Stat(p)
	if err != nil {
		return nil, 0, false
	}
	data, err = os.ReadFile(p)
	if err != nil {
		return nil, 0, false
	}
	return data, time.Since(info.ModTime()), true
}

// Put stores data under key.
func (c *Cache) Put(key string, data []byte) {
	if !c.enabled {
		return
	}
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		return
	}
	p := c.pathFor(key)
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err == nil {
		_ = os.Rename(tmp, p)
	}
}

// Clear removes every cached entry. It returns the number of files removed.
func (c *Cache) Clear() (int, error) {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	n := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".cache" {
			if os.Remove(filepath.Join(c.dir, e.Name())) == nil {
				n++
			}
		}
	}
	return n, nil
}

// Dir returns the cache directory.
func (c *Cache) Dir() string { return c.dir }

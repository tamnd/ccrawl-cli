package ccrawl

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"sync"
)

// KeyLedger is an append-only record of which items a run has finished, keyed by
// a string rather than by the shard index Ledger uses. A batch fetch works
// through locations, not shards, and there is no small integer that names one.
//
// The file is one key per line, which is the same shape as the publish ledgers
// and readable with grep. Writes go through a buffer and are pushed to disk by
// Sync, because a run of a million records cannot afford an fsync each; the
// caller decides how much work a kill is allowed to cost by choosing when to
// call it.
type KeyLedger struct {
	mu   sync.Mutex
	path string
	done map[string]bool
	f    *os.File
	w    *bufio.Writer
}

// LocationKey names one record for the ledger. A WARC file and an offset
// identify a record on their own, and the length would only add bytes to every
// line for something already unique.
func LocationKey(l Location) string {
	return l.Filename + "@" + strconv.FormatInt(l.Offset, 10)
}

// OpenKeyLedger loads any existing ledger at path and opens it for appending. A
// missing file is an empty ledger, not an error. An empty path gives a nil
// ledger, so a caller can pass the result through without a branch.
func OpenKeyLedger(path string) (*KeyLedger, error) {
	if path == "" {
		return nil, nil
	}
	l := &KeyLedger{path: path, done: make(map[string]bool)}
	if data, err := os.ReadFile(path); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if line = strings.TrimSpace(line); line != "" {
				l.done[line] = true
			}
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l.f = f
	l.w = bufio.NewWriterSize(f, 1<<16)
	return l, nil
}

// Has reports whether the ledger already holds key.
func (l *KeyLedger) Has(key string) bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.done[key]
}

// Mark records key as done. It is not on disk until Sync.
func (l *KeyLedger) Mark(key string) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done[key] {
		return nil
	}
	if _, err := l.w.WriteString(key + "\n"); err != nil {
		return err
	}
	l.done[key] = true
	return nil
}

// Sync flushes the buffered lines and pushes them to disk, so everything marked
// before it survives a kill.
func (l *KeyLedger) Sync() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Sync()
}

// Count returns how many keys the ledger holds.
func (l *KeyLedger) Count() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.done)
}

// Close flushes and closes the file.
func (l *KeyLedger) Close() error {
	if l == nil || l.f == nil {
		return nil
	}
	if err := l.Sync(); err != nil {
		_ = l.f.Close()
		return err
	}
	return l.f.Close()
}

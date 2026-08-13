package ccrawl

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"
)

// ManifestName is the file at the library root that records what is in the
// library. One file for the whole tree rather than one per crawl, because every
// question worth asking of it spans crawls: what do I have, what is corrupt,
// what can I delete, and how much would that free.
const ManifestName = "library.json"

// manifestLockName guards the read, change, write cycle. Two ccrawl processes
// downloading different kinds into one library is an ordinary thing to do, and
// without the lock the second one to finish would write a manifest missing
// everything the first recorded.
const manifestLockName = "library.lock"

// manifestVersion is the format of the file on disk. A reader that meets a
// version it does not know refuses it rather than guessing, since the failure
// mode of guessing is a gc that deletes the wrong thing.
const manifestVersion = 1

// Artifact is one file in the library, as the manifest records it.
type Artifact struct {
	// Path is relative to the library root and always slash separated, so a
	// library written on one platform reads on another.
	Path  string `json:"path"`
	Crawl string `json:"crawl"`
	Kind  string `json:"kind"`
	// Format is empty for a raw archive, and parquet or jsonl for processed
	// output, which is the difference between what was downloaded and what was
	// derived from it.
	Format    string    `json:"format,omitempty"`
	Bytes     int64     `json:"bytes"`
	SHA256    string    `json:"sha256"`
	CreatedAt time.Time `json:"created_at"`
	Producer  string    `json:"producer_version"`
}

// Manifest is the library's record of itself: every artifact, keyed by its path
// under the root.
//
// It is loaded, changed and saved in one pass under a lock rather than kept open
// for the length of a run. A download that writes ten thousand files touches the
// manifest once at the end, so the cost is one read and one write however long
// the run was.
type Manifest struct {
	Root string

	entries map[string]Artifact
}

// LoadManifest reads the manifest at the root of a library. A library with no
// manifest is not an error: it is either new or was built by a version of
// ccrawl that did not write one, and library scan is what turns the second into
// the first.
func LoadManifest(root string) (*Manifest, error) {
	m := &Manifest{Root: root, entries: map[string]Artifact{}}
	data, err := os.ReadFile(ManifestPath(root))
	if err != nil {
		if os.IsNotExist(err) {
			return m, nil
		}
		return nil, err
	}
	var file manifestFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("read %s: %w", ManifestPath(root), err)
	}
	if file.Version != manifestVersion {
		return nil, fmt.Errorf("manifest %s is version %d, this ccrawl reads version %d",
			ManifestPath(root), file.Version, manifestVersion)
	}
	for _, a := range file.Artifacts {
		m.entries[a.Path] = a
	}
	return m, nil
}

// manifestFile is the shape on disk. The artifacts are a list rather than an
// object keyed by path so the file diffs and greps like a list of files, which
// is what a person reaching for it wants.
type manifestFile struct {
	Version   int        `json:"version"`
	UpdatedAt time.Time  `json:"updated_at"`
	Artifacts []Artifact `json:"artifacts"`
}

// ManifestPath is where a library keeps its manifest.
func ManifestPath(root string) string { return filepath.Join(root, ManifestName) }

// Put records an artifact, replacing any earlier record of the same path.
func (m *Manifest) Put(a Artifact) {
	a.Path = filepath.ToSlash(a.Path)
	m.entries[a.Path] = a
}

// Remove drops the record of a path. It does not touch the file.
func (m *Manifest) Remove(path string) {
	delete(m.entries, filepath.ToSlash(path))
}

// Lookup returns the record for a path.
func (m *Manifest) Lookup(path string) (Artifact, bool) {
	a, ok := m.entries[filepath.ToSlash(path)]
	return a, ok
}

// Len is how many artifacts the manifest records.
func (m *Manifest) Len() int { return len(m.entries) }

// Artifacts returns every record, ordered by path so two runs of the same
// command print the same thing in the same order.
func (m *Manifest) Artifacts() []Artifact {
	out := make([]Artifact, 0, len(m.entries))
	for _, a := range m.entries {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// Crawls lists the crawls the manifest has artifacts for, newest first, which is
// the order every other crawl listing in ccrawl uses.
func (m *Manifest) Crawls() []string {
	seen := map[string]bool{}
	var out []string
	for _, a := range m.entries {
		if !seen[a.Crawl] {
			seen[a.Crawl] = true
			out = append(out, a.Crawl)
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(out)))
	return out
}

// Save writes the manifest atomically: a temporary file in the same directory,
// then a rename. A manifest half written is a library that cannot be verified
// or collected, so the file on disk is always a whole one.
func (m *Manifest) Save() error {
	if err := os.MkdirAll(m.Root, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(manifestFile{
		Version:   manifestVersion,
		UpdatedAt: time.Now().UTC().Truncate(time.Second),
		Artifacts: m.Artifacts(),
	}, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	path := ManifestPath(m.Root)
	tmp, err := os.CreateTemp(m.Root, ManifestName+".*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	if err := os.Chmod(name, 0o644); err != nil {
		_ = os.Remove(name)
		return err
	}
	if err := os.Rename(name, path); err != nil {
		_ = os.Remove(name)
		return err
	}
	return nil
}

// UpdateManifest runs change against the library's manifest with the library
// locked, and saves the result. Every writer goes through it, so two ccrawl
// processes materialising into one library cannot lose each other's records.
//
// The lock is held for the read and the write, which is milliseconds. The work
// that produced the artifacts happens outside it.
func UpdateManifest(root string, change func(*Manifest) error) error {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(root, manifestLockName), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	if err := lockFile(lock); err != nil {
		return fmt.Errorf("lock %s: %w", root, err)
	}
	defer func() { _ = unlockFile(lock) }()

	m, err := LoadManifest(root)
	if err != nil {
		return err
	}
	if err := change(m); err != nil {
		return err
	}
	return m.Save()
}

// FileSHA256 is the checksum of a file on disk, hex encoded, which is the form
// the manifest stores and the form every other tool prints.
func FileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// LibraryPath turns an absolute path under the library root into the relative,
// slash separated form the manifest keys on. ok is false for a path that is not
// under the root, which is the caller's signal that it has no business in the
// manifest.
func LibraryPath(root, path string) (string, bool) {
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	return filepath.ToSlash(rel), true
}

// ClassifyPath reads a library relative path back into the crawl, format and
// kind that produced it. It is the inverse of the layout Library builds:
//
//	<crawl>/<kind>/<file>               a raw archive
//	<crawl>/<format>/<kind>/<file>      processed output
//
// ok is false for anything that does not fit, which includes the manifest and
// the lock file themselves.
func ClassifyPath(rel string) (crawl, format, kind string, ok bool) {
	parts := strings.Split(filepath.ToSlash(rel), "/")
	switch len(parts) {
	case 3:
		return parts[0], "", parts[1], true
	case 4:
		if !slices.Contains(processedFormats, parts[1]) {
			return "", "", "", false
		}
		return parts[0], parts[1], parts[2], true
	}
	return "", "", "", false
}

// processedFormats are the formats convert writes into the library, and the
// only second path element that means a format rather than a kind.
var processedFormats = []string{"parquet", "jsonl"}

// WalkLibrary calls fn for every file under root that belongs to the library,
// with the path relative to the root. The manifest, the lock and anything that
// does not fit the layout are skipped: the library is a tree a person also puts
// notes in, and a stray README is not a corrupt artifact.
func WalkLibrary(root string, fn func(rel string, info fs.FileInfo) error) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, ok := LibraryPath(root, path)
		if !ok {
			return nil
		}
		if _, _, _, fits := ClassifyPath(rel); !fits {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		return fn(rel, info)
	})
}

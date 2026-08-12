package cli

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// The library commands: what is in the tree, whether it is intact, what it
// costs, and how to get some of it back.
//
// They read the manifest at the library root rather than the directory listing,
// because the questions worth asking are about provenance and integrity and a
// listing answers neither. A library built before the manifest existed, or one
// somebody copied files into, is brought under it with library scan.
func newLibraryCmd() kit.Command {
	list := &libraryListCmd{}
	du := &libraryDuCmd{}
	verify := &libraryVerifyCmd{}
	gc := &libraryGCCmd{}
	scan := &libraryScanCmd{}
	return kit.Command{
		Use:   "library",
		Short: "Inspect, verify, and collect the dataset library",
		Long: `The dataset library is the curated tree the --library flag downloads into and
processes from, separate from the data dir so scratch state and the files you
keep never mix. Its root is --library-dir, ~/notes/ccrawl by default.

library.json at the root records every artifact: path, crawl, kind, size,
sha256, when it was written, and by which version of ccrawl. Every command that
materialises into the library updates it.

Examples:
  ccrawl library list                  what is in the library
  ccrawl library du                    how much it costs, per crawl
  ccrawl library verify                rehash everything and report what moved
  ccrawl library gc --older-than 90d   free the crawls you are done with
  ccrawl library scan                  record a tree that predates the manifest`,
		Sub: []kit.Command{
			{
				Use:   "list",
				Short: "List the artifacts in the library",
				Long: `List what the manifest records, newest crawl first.

  ccrawl library list
  ccrawl library list -c CC-MAIN-2026-30
  ccrawl library list --kind wet -o url`,
				Flags: list.flags,
				Run:   list.run,
			},
			{
				Use:   "du",
				Short: "Report library size, per crawl",
				Long: `Report what the library costs, one row per crawl and a total.

  ccrawl library du
  ccrawl library du --by kind`,
				Flags: du.flags,
				Run:   du.run,
			},
			{
				Use:   "verify",
				Short: "Rehash every artifact and report what does not match",
				Long: `Read every artifact the manifest records and compare it against the recorded
size and checksum. Reports four kinds of trouble: a file that is gone, one whose
size changed, one whose bytes changed, and one on disk that the manifest has
never heard of.

Exits 1 when anything is wrong, so it can gate a publish run.

  ccrawl library verify
  ccrawl library verify -c CC-MAIN-2026-30
  ccrawl library verify --quick        sizes only, no rehash`,
				Flags: verify.flags,
				Run:   verify.run,
			},
			{
				Use:   "gc",
				Short: "Delete artifacts older than a cutoff",
				Long: `Delete artifacts the library has held longer than --older-than, and drop them
from the manifest.

It is a dry run unless you pass --yes, and the dry run prints exactly the list
the real run deletes.

  ccrawl library gc --older-than 90d           what would go
  ccrawl library gc --older-than 90d --yes     actually go
  ccrawl library gc --crawl CC-MAIN-2025-33 --yes`,
				Flags: gc.flags,
				Run:   gc.run,
			},
			{
				Use:   "scan",
				Short: "Record what is on disk into the manifest",
				Long: `Walk the library tree, hash every artifact, and write the manifest.

This is how a library built by a ccrawl that predated the manifest, or one you
copied files into, comes under management. It reads every byte in the tree, so
it is slow the first time and idempotent after that: an artifact already
recorded with the same size and time is left alone unless --rehash.

  ccrawl library scan
  ccrawl library scan --rehash`,
				Flags: scan.flags,
				Run:   scan.run,
			},
		},
	}
}

// libraryRoot is the library the command works on. The crawl is not resolved
// here: these commands read a local tree and must work with no network, so
// "latest" means every crawl in the library rather than a question for
// collinfo.
func libraryRoot(app *App) string { return app.LibraryDir }

// crawlFilter is the crawl the command was pointed at, or "" for all of them.
// A bare -c latest is the default nobody typed, so it does not filter.
func crawlFilter(app *App) string {
	id := app.Cfg.CrawlID
	if id == "latest" || id == "all" || id == "" {
		return ""
	}
	return id
}

// matches reports whether an artifact is in scope for a crawl filter. A filter
// of CC-MAIN-2026-30 matches exactly; the shorthand 2026-30 matches too, since
// that is what -c takes everywhere else.
func matches(a ccrawl.Artifact, crawl string) bool {
	return crawl == "" || a.Crawl == crawl || a.Crawl == "CC-MAIN-"+crawl
}

type libraryListCmd struct {
	kind   string
	format string
}

func (v *libraryListCmd) flags(f *kit.FlagSet) {
	f.StringVar(&v.kind, "kind", "", "only this kind: warc, wet, wat, ...")
	f.StringVar(&v.format, "format", "", "only this format: raw, parquet, jsonl")
}

func (v *libraryListCmd) run(ctx context.Context, _ []string) error {
	app := appFromCtx(ctx)
	m, err := ccrawl.LoadManifest(libraryRoot(app))
	if err != nil {
		return err
	}
	crawl := crawlFilter(app)
	n := 0
	for _, a := range m.Artifacts() {
		if !matches(a, crawl) || !kindFormatMatch(a, v.kind, v.format) {
			continue
		}
		if app.Limit > 0 && n >= app.Limit {
			break
		}
		n++
		if err := app.Out.Emit(artifactRow(libraryRoot(app), a)); err != nil {
			return err
		}
	}
	if n == 0 {
		return noResults(emptyLibraryMessage(m, libraryRoot(app)))
	}
	return app.Out.Flush()
}

// kindFormatMatch applies the two filters that are not the crawl. A format of
// raw means the archives as downloaded, which the manifest records as no format
// at all.
func kindFormatMatch(a ccrawl.Artifact, kind, format string) bool {
	if kind != "" && a.Kind != kind {
		return false
	}
	switch format {
	case "":
		return true
	case "raw":
		return a.Format == ""
	default:
		return a.Format == format
	}
}

// emptyLibraryMessage separates the two ways a library command finds nothing,
// because the fix is different. An empty manifest over a tree with files in it
// means the manifest was never written, and saying so saves the person working
// out that library scan exists.
func emptyLibraryMessage(m *ccrawl.Manifest, root string) string {
	if m.Len() > 0 {
		return "no artifacts match"
	}
	if hasFiles(root) {
		return "the manifest at " + ccrawl.ManifestPath(root) + " is empty and there are files under " + root + ", run library scan to record them"
	}
	return "the library at " + root + " is empty"
}

// hasFiles reports whether the tree holds anything the library would manage.
func hasFiles(root string) bool {
	found := false
	_ = ccrawl.WalkLibrary(root, func(string, fs.FileInfo) error {
		found = true
		return filepath.SkipAll
	})
	return found
}

func artifactRow(root string, a ccrawl.Artifact) Row {
	format := a.Format
	if format == "" {
		format = "raw"
	}
	cols := []string{"crawl", "kind", "format", "bytes", "created", "path"}
	vals := []string{a.Crawl, a.Kind, format, itoa64(a.Bytes), a.CreatedAt.Format(time.RFC3339), a.Path}
	return Row{
		Cols: cols,
		Vals: vals,
		Value: map[string]any{
			"crawl": a.Crawl, "kind": a.Kind, "format": format,
			"bytes": a.Bytes, "sha256": a.SHA256,
			"created_at": a.CreatedAt, "producer_version": a.Producer,
			"path": a.Path, "url": filepath.Join(root, filepath.FromSlash(a.Path)),
		},
	}
}

type libraryDuCmd struct{ by string }

func (v *libraryDuCmd) flags(f *kit.FlagSet) {
	f.StringVar(&v.by, "by", "crawl", "group by: crawl, kind, format")
}

func (v *libraryDuCmd) run(ctx context.Context, _ []string) error {
	app := appFromCtx(ctx)
	switch v.by {
	case "crawl", "kind", "format":
	default:
		return usageErr("pass --by crawl, --by kind, or --by format")
	}
	root := libraryRoot(app)
	m, err := ccrawl.LoadManifest(root)
	if err != nil {
		return err
	}
	crawl := crawlFilter(app)

	type total struct {
		files int64
		bytes int64
	}
	totals := map[string]*total{}
	var all total
	for _, a := range m.Artifacts() {
		if !matches(a, crawl) {
			continue
		}
		key := a.Crawl
		switch v.by {
		case "kind":
			key = a.Kind
		case "format":
			if key = a.Format; key == "" {
				key = "raw"
			}
		}
		t := totals[key]
		if t == nil {
			t = &total{}
			totals[key] = t
		}
		t.files++
		t.bytes += a.Bytes
		all.files++
		all.bytes += a.Bytes
	}
	if all.files == 0 {
		return noResults(emptyLibraryMessage(m, root))
	}

	keys := make([]string, 0, len(totals))
	for k := range totals {
		keys = append(keys, k)
	}
	// Crawls read newest first, the way every crawl listing in ccrawl reads.
	// Anything else is alphabetical, because a kind has no order of its own.
	if v.by == "crawl" {
		sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	} else {
		sort.Strings(keys)
	}

	emit := func(name string, t total) error {
		return app.Out.Emit(Row{
			Cols: []string{v.by, "files", "bytes", "size"},
			Vals: []string{name, itoa64(t.files), itoa64(t.bytes), humanBytes(t.bytes)},
			Value: map[string]any{
				v.by: name, "files": t.files, "bytes": t.bytes, "size": humanBytes(t.bytes),
			},
		})
	}
	for _, k := range keys {
		if err := emit(k, *totals[k]); err != nil {
			return err
		}
	}
	// The total is a row rather than a footer so it survives -o json and a pipe
	// into anything, which a footer printed on stderr would not.
	if len(keys) > 1 {
		if err := emit("total", all); err != nil {
			return err
		}
	}
	return app.Out.Flush()
}

type libraryVerifyCmd struct {
	quick bool
	kind  string
}

func (v *libraryVerifyCmd) flags(f *kit.FlagSet) {
	f.BoolVar(&v.quick, "quick", false, "check that files exist and are the recorded size, without rehashing")
	f.StringVar(&v.kind, "kind", "", "only this kind: warc, wet, wat, ...")
}

func (v *libraryVerifyCmd) run(ctx context.Context, _ []string) error {
	app := appFromCtx(ctx)
	root := libraryRoot(app)
	m, err := ccrawl.LoadManifest(root)
	if err != nil {
		return err
	}
	crawl := crawlFilter(app)

	var checked, bad int
	// An untracked file is reported but does not fail the run: it is a manifest
	// that is behind, not an artifact that went wrong, and library scan fixes it.
	report := func(state, path, detail string) error {
		if state != "ok" && state != "untracked" {
			bad++
		}
		return app.Out.Emit(Row{
			Cols:  []string{"state", "path", "detail"},
			Vals:  []string{state, path, detail},
			Value: map[string]any{"state": state, "path": path, "detail": detail},
		})
	}

	recorded := map[string]bool{}
	for _, a := range m.Artifacts() {
		if !matches(a, crawl) || !kindFormatMatch(a, v.kind, "") {
			continue
		}
		recorded[a.Path] = true
		if ctx.Err() != nil {
			return ctx.Err()
		}
		checked++
		full := filepath.Join(root, filepath.FromSlash(a.Path))
		info, err := os.Stat(full)
		if err != nil {
			if rerr := report("missing", a.Path, "the manifest records it and it is not there"); rerr != nil {
				return rerr
			}
			continue
		}
		if info.Size() != a.Bytes {
			detail := fmt.Sprintf("%d bytes on disk, %d in the manifest", info.Size(), a.Bytes)
			if rerr := report("resized", a.Path, detail); rerr != nil {
				return rerr
			}
			continue
		}
		if v.quick {
			if rerr := report("ok", a.Path, "size only"); rerr != nil {
				return rerr
			}
			continue
		}
		sum, err := ccrawl.FileSHA256(full)
		if err != nil {
			if rerr := report("unreadable", a.Path, err.Error()); rerr != nil {
				return rerr
			}
			continue
		}
		if sum != a.SHA256 {
			detail := fmt.Sprintf("sha256 %s, the manifest says %s", short(sum), short(a.SHA256))
			if rerr := report("corrupt", a.Path, detail); rerr != nil {
				return rerr
			}
			continue
		}
		if rerr := report("ok", a.Path, short(sum)); rerr != nil {
			return rerr
		}
	}

	// A file on disk that the manifest has never heard of is not corruption, but
	// it is the thing that makes du and gc wrong, so verify is where it is said.
	untracked := 0
	err = ccrawl.WalkLibrary(root, func(rel string, _ fs.FileInfo) error {
		if recorded[rel] {
			return nil
		}
		id, _, _, _ := ccrawl.ClassifyPath(rel)
		if crawl != "" && id != crawl && id != "CC-MAIN-"+crawl {
			return nil
		}
		if _, tracked := m.Lookup(rel); tracked {
			return nil // recorded, but filtered out of this run
		}
		untracked++
		return report("untracked", rel, "on disk, not in the manifest, run library scan")
	})
	if err != nil {
		return err
	}
	if err := app.Out.Flush(); err != nil {
		return err
	}

	if checked == 0 && untracked == 0 {
		return noResults(emptyLibraryMessage(m, root))
	}
	_, _ = fmt.Fprintf(cmdErr, "checked %s, %d ok, %d bad, %d untracked\n",
		plural(checked, "artifact"), checked-bad, bad, untracked)
	if bad > 0 {
		return fmt.Errorf("%s did not verify", plural(bad, "artifact"))
	}
	return nil
}

// short is the first twelve characters of a checksum, which is enough to tell
// two apart on a terminal and short enough to sit in a table.
func short(sum string) string {
	if len(sum) > 12 {
		return sum[:12]
	}
	return sum
}

type libraryGCCmd struct {
	olderThan string
	kind      string
}

func (v *libraryGCCmd) flags(f *kit.FlagSet) {
	f.StringVar(&v.olderThan, "older-than", "", "delete artifacts written longer ago than this: 30d, 12h, 8w")
	f.StringVar(&v.kind, "kind", "", "only this kind: warc, wet, wat, ...")
}

func (v *libraryGCCmd) run(ctx context.Context, _ []string) error {
	app := appFromCtx(ctx)
	root := libraryRoot(app)
	crawl := crawlFilter(app)
	if v.olderThan == "" && crawl == "" && v.kind == "" {
		return usageErr("select what to delete with --older-than, --crawl, or --kind")
	}

	var cutoff time.Time
	if v.olderThan != "" {
		age, err := parseAge(v.olderThan)
		if err != nil {
			return usageErr(err.Error())
		}
		cutoff = time.Now().Add(-age)
	}

	m, err := ccrawl.LoadManifest(root)
	if err != nil {
		return err
	}
	var doomed []ccrawl.Artifact
	var bytes int64
	for _, a := range m.Artifacts() {
		if !matches(a, crawl) || !kindFormatMatch(a, v.kind, "") {
			continue
		}
		if !cutoff.IsZero() && !a.CreatedAt.Before(cutoff) {
			continue
		}
		doomed = append(doomed, a)
		bytes += a.Bytes
	}
	if len(doomed) == 0 {
		return noResults("nothing in the library matches, so there is nothing to delete")
	}

	// The dry run and the real run print the same list from the same slice, so
	// what the dry run showed is what the real run removes. That is the whole
	// contract of the command and the reason the list is built before either.
	for _, a := range doomed {
		if err := app.Out.Emit(artifactRow(root, a)); err != nil {
			return err
		}
	}
	if err := app.Out.Flush(); err != nil {
		return err
	}

	what := fmt.Sprintf("%s, %s", plural(len(doomed), "artifact"), humanBytes(bytes))
	if app.dryRun || !app.yes {
		_, _ = fmt.Fprintf(cmdErr, "would delete %s from %s\nrun it again with --yes to do it\n", what, root)
		return nil
	}

	var freed int64
	var removed int
	err = ccrawl.UpdateManifest(root, func(m *ccrawl.Manifest) error {
		for _, a := range doomed {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			full := filepath.Join(root, filepath.FromSlash(a.Path))
			if rerr := os.Remove(full); rerr != nil && !os.IsNotExist(rerr) {
				return fmt.Errorf("delete %s: %w", a.Path, rerr)
			}
			m.Remove(a.Path)
			freed += a.Bytes
			removed++
		}
		return nil
	})
	// Whatever happened, say what actually went, which after a partial failure is
	// not what the list said.
	_, _ = fmt.Fprintf(cmdErr, "deleted %s, freed %s\n", plural(removed, "artifact"), humanBytes(freed))
	if err != nil {
		return err
	}
	pruneEmptyDirs(root)
	return nil
}

// parseAge reads the ages a person types for a retention window. time.Duration
// stops at hours, and nobody keeps a crawl for 2160h.
func parseAge(s string) (time.Duration, error) {
	unit := map[byte]time.Duration{
		'd': 24 * time.Hour,
		'w': 7 * 24 * time.Hour,
	}
	if len(s) > 1 {
		if mul, ok := unit[s[len(s)-1]]; ok {
			// Atoi rather than Sscanf, which reads 3 out of 3.5d and throws the rest
			// away. A cutoff that quietly means something other than what was typed
			// is the last thing a delete command should do.
			n, err := strconv.Atoi(s[:len(s)-1])
			if err != nil || n < 0 {
				return 0, fmt.Errorf("%s is not a whole number of %s, which is what --older-than %s asks for", s[:len(s)-1], unitName(s[len(s)-1]), s)
			}
			return time.Duration(n) * mul, nil
		}
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("write --older-than as 30d, 8w, or a duration like 12h, not %s", s)
	}
	return d, nil
}

func unitName(c byte) string {
	if c == 'w' {
		return "weeks"
	}
	return "days"
}

// pruneEmptyDirs removes the directories a gc emptied, so a library that has
// been collected down to nothing looks like it. Failures are ignored: a
// directory that will not go is not a reason to fail a run that already did the
// thing it was asked to do.
func pruneEmptyDirs(root string) {
	// Deepest first, so emptying a kind directory lets its crawl directory go in
	// the same pass.
	var dirs []string
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() && path != root {
			dirs = append(dirs, path)
		}
		return nil
	})
	sort.Sort(sort.Reverse(sort.StringSlice(dirs)))
	for _, d := range dirs {
		_ = os.Remove(d)
	}
}

type libraryScanCmd struct{ rehash bool }

func (v *libraryScanCmd) flags(f *kit.FlagSet) {
	f.BoolVar(&v.rehash, "rehash", false, "hash every file again, including ones already recorded")
}

func (v *libraryScanCmd) run(ctx context.Context, _ []string) error {
	app := appFromCtx(ctx)
	root := libraryRoot(app)

	var added, rehashed, kept int
	err := ccrawl.UpdateManifest(root, func(m *ccrawl.Manifest) error {
		return ccrawl.WalkLibrary(root, func(rel string, info fs.FileInfo) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			crawl, format, kind, _ := ccrawl.ClassifyPath(rel)
			old, known := m.Lookup(rel)
			// An artifact already recorded at the same size is left alone unless
			// asked, because the whole cost of this command is reading every byte
			// in a tree that is measured in terabytes.
			if known && !v.rehash && old.Bytes == info.Size() {
				kept++
				return nil
			}
			sum, err := ccrawl.FileSHA256(filepath.Join(root, filepath.FromSlash(rel)))
			if err != nil {
				return err
			}
			created := info.ModTime().UTC()
			if known {
				rehashed++
				// The recorded time is when the artifact was written, and a rescan
				// is not a rewrite, so an artifact that is already known keeps it.
				created = old.CreatedAt
			} else {
				added++
			}
			m.Put(ccrawl.Artifact{
				Path: rel, Crawl: crawl, Kind: kind, Format: format,
				Bytes: info.Size(), SHA256: sum, CreatedAt: created,
				Producer: producerVersion(),
			})
			return nil
		})
	})
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmdErr, "scanned %s: %d new, %d rehashed, %d already recorded\n",
		root, added, rehashed, kept)
	if added+rehashed+kept == 0 {
		return noResults("nothing under " + root + " looks like a library artifact")
	}
	return nil
}

// producerVersion is what goes in the manifest as the thing that wrote an
// artifact, so a file that turns out to be wrong can be traced to a release.
func producerVersion() string { return "ccrawl/" + Version }

// libraryRootFor is the library a command's output belongs to, or "" when the
// output is going somewhere else.
//
// It asks where the files actually land rather than whether --library was
// passed, so an --out that happens to point into the library tree is recorded
// too. A library is defined by its layout, not by the flag that filled it.
func libraryRootFor(app *App, outDir string) string {
	root := app.LibraryDir
	if root == "" || outDir == "" {
		return ""
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return ""
	}
	absOut, err := filepath.Abs(outDir)
	if err != nil {
		return ""
	}
	if _, ok := ccrawl.LibraryPath(absRoot, absOut); !ok {
		return ""
	}
	return absRoot
}

// writtenFile is a file a command just materialised, on its way into the
// manifest.
type writtenFile struct {
	// Path is absolute. A path outside the library root is dropped, so a caller
	// can hand over everything it wrote without checking first.
	Path string
	// SHA256 is what the writer computed on the way past, empty if it did not.
	// Downloads hash as they stream; a converter that wrote through a Parquet
	// library does not, and gets hashed here.
	SHA256 string
	// Reuse says the bytes were already on disk and were not rewritten, which is
	// what a skipped download is. An artifact already recorded at the same size
	// is then left alone rather than reread, so resuming a half finished download
	// of a terabyte does not rehash the half that was already there.
	Reuse bool
}

// recordLibraryFiles records what a run materialised into the library manifest.
// It is the one entry point every writer uses, so a new command that lands files
// in the library gets provenance by calling one function.
//
// A failure to record is reported and does not fail the run: the files are
// there, they are correct, and library scan will pick them up. Losing a
// finished download to a manifest write is the worse trade.
func recordLibraryFiles(root string, files []writtenFile) {
	if len(files) == 0 || root == "" {
		return
	}
	recorded := 0
	err := ccrawl.UpdateManifest(root, func(m *ccrawl.Manifest) error {
		for _, f := range files {
			abs, err := filepath.Abs(f.Path)
			if err != nil {
				continue
			}
			rel, ok := ccrawl.LibraryPath(root, abs)
			if !ok {
				continue
			}
			crawl, format, kind, fits := ccrawl.ClassifyPath(rel)
			if !fits {
				continue
			}
			info, err := os.Stat(abs)
			if err != nil {
				continue
			}
			old, known := m.Lookup(rel)
			if f.Reuse && known && old.Bytes == info.Size() {
				continue
			}
			sum := f.SHA256
			if sum == "" {
				if sum, err = ccrawl.FileSHA256(abs); err != nil {
					continue
				}
			}
			created := info.ModTime().UTC()
			if f.Reuse && known {
				created = old.CreatedAt
			}
			m.Put(ccrawl.Artifact{
				Path: rel, Crawl: crawl, Kind: kind, Format: format,
				Bytes: info.Size(), SHA256: sum, CreatedAt: created,
				Producer: producerVersion(),
			})
			recorded++
		}
		return nil
	})
	if err != nil {
		_, _ = fmt.Fprintf(cmdErr, "ccrawl: could not record %s in %s: %v\n",
			plural(len(files), "artifact"), ccrawl.ManifestPath(root), err)
		return
	}
	if recorded > 0 {
		_, _ = fmt.Fprintf(cmdErr, "recorded %s in %s\n", plural(recorded, "artifact"), ccrawl.ManifestPath(root))
	}
}

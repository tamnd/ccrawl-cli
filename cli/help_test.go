package cli

import (
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The flag surface is a contract. scripts/docs-drift.sh checks one direction of
// it, that nothing documented is missing from the binary, and it cannot see the
// other direction: a flag that quietly disappears in a refactor, or one that
// arrives on the wrong command, leaves the docs happy and the users broken.
//
// So the whole surface is written down here and compared on every run. The
// golden file is generated, not hand written; when a flag really does move, run
//
//	go test ./cli/ -run TestFlagSurface -update
//
// and read the diff before committing it. A diff nobody can explain is the bug
// this test exists to find.

var update = flag.Bool("update", false, "rewrite the golden files")

// cobra generates these two and they are not part of ccrawl's surface.
var generatedCommands = map[string]bool{"help": true, "completion": true}

func TestFlagSurfaceMatchesGolden(t *testing.T) {
	// The globals are on every command, so they are written down once and each
	// command lists only what it adds. A global that goes missing from one
	// command shows up as a -- line under it.
	global := map[string]bool{}
	for _, f := range flagsIn(helpFor(t, nil)) {
		global[f] = true
	}
	// --version is the root's alone; no subcommand carries it.
	delete(global, "--version")

	lines := []string{"ccrawl (global)\n\t" + strings.Join(sortedKeys(global), " ")}
	var walk func(path []string)
	walk = func(path []string) {
		help := helpFor(t, path)
		var own, absent []string
		has := map[string]bool{}
		for _, f := range flagsIn(help) {
			has[f] = true
			if !global[f] && f != "--version" {
				own = append(own, f)
			}
		}
		for g := range global {
			if !has[g] {
				absent = append(absent, "-"+g)
			}
		}
		sort.Strings(absent)
		if len(path) > 0 {
			name := strings.Join(append([]string{"ccrawl"}, path...), " ")
			lines = append(lines, name+"\n\t"+strings.Join(append(own, absent...), " "))
		}
		for _, sub := range subcommandsIn(help) {
			if generatedCommands[sub] {
				continue
			}
			walk(append(append([]string{}, path...), sub))
		}
	}
	walk(nil)
	sort.Strings(lines)
	got := strings.Join(lines, "\n") + "\n"

	golden := filepath.Join("testdata", "flags.golden")
	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(golden, []byte(got), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s", golden)
		return
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("%v (run: go test ./cli/ -run TestFlagSurface -update)", err)
	}
	if got != string(want) {
		t.Fatalf("the flag surface changed:\n%s\nrun: go test ./cli/ -run TestFlagSurface -update",
			firstDiff(string(want), got))
	}
}

// Help has to work for every command, including the ones no other test runs,
// and it has to exit 0 while doing it. A command whose help is an error is one
// nobody can find their way into.
func TestHelpWorksForEveryCommand(t *testing.T) {
	var walk func(path []string)
	walk = func(path []string) {
		r := runNoServer(t, append(path, "--help")...)
		r.wantCode(t, 0)
		if len(r.Out) < 20 {
			t.Fatalf("ccrawl %s --help printed almost nothing:\n%s", strings.Join(path, " "), r.Out)
		}
		for _, sub := range subcommandsIn(r.Out) {
			if generatedCommands[sub] {
				continue
			}
			walk(append(append([]string{}, path...), sub))
		}
	}
	walk(nil)
}

// helpFor returns the help text for a command path, with the colour escapes
// stripped so the golden holds text rather than terminal control codes.
func helpFor(t *testing.T, path []string) string {
	t.Helper()
	r := runNoServer(t, append(append([]string{}, path...), "--help")...)
	r.wantCode(t, 0)
	return stripANSI(r.Out)
}

var (
	reANSI    = regexp.MustCompile("\x1b\\[[0-9;]*[a-zA-Z]")
	reFlag    = regexp.MustCompile(`--[a-z][a-z0-9-]*`)
	reSubName = regexp.MustCompile(`^ {2,}([a-z][a-z0-9-]*)(\s|$)`)
	reSection = regexp.MustCompile(`^\s+(READ |WRITE )?(COMMANDS|FLAGS|USAGE|EXAMPLES)`)
)

func stripANSI(s string) string { return reANSI.ReplaceAllString(s, "") }

// flagsIn pulls the flag names out of a help text's FLAGS section. Only that
// section, because the prose above it mentions flags of other commands, and the
// point here is what this command accepts.
func flagsIn(help string) []string {
	seen := map[string]bool{}
	var out []string
	in := false
	for _, line := range strings.Split(help, "\n") {
		if m := reSection.FindStringSubmatch(line); m != nil {
			in = m[2] == "FLAGS"
			continue
		}
		if !in {
			continue
		}
		for _, f := range reFlag.FindAllString(line, -1) {
			// [--flags] is the usage placeholder, not a flag.
			if f == "--flags" || seen[f] {
				continue
			}
			seen[f] = true
			out = append(out, f)
		}
	}
	sort.Strings(out)
	return out
}

// subcommandsIn lists the immediate subcommands named in a help text.
func subcommandsIn(help string) []string {
	var out []string
	in := false
	for _, line := range strings.Split(stripANSI(help), "\n") {
		if m := reSection.FindStringSubmatch(line); m != nil {
			in = m[2] == "COMMANDS"
			continue
		}
		if !in {
			continue
		}
		if m := reSubName.FindStringSubmatch(line); m != nil {
			out = append(out, m[1])
		}
	}
	sort.Strings(out)
	return out
}

// firstDiff reports the first differing line with a little context, since the
// golden is long and a full dump of it buries the one line that moved.
func firstDiff(want, got string) string {
	w, g := strings.Split(want, "\n"), strings.Split(got, "\n")
	for i := 0; i < len(w) || i < len(g); i++ {
		lw, lg := at(w, i), at(g, i)
		if lw == lg {
			continue
		}
		return "line " + itoa(i+1) + "\n  want: " + lw + "\n  got:  " + lg
	}
	return "(no line differs, only the trailing bytes)"
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func at(lines []string, i int) string {
	if i < len(lines) {
		return lines[i]
	}
	return "(end of file)"
}

package ccrawl

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Settings is a parsed config file: a [default] table that applies to every run,
// and any number of named profile tables that --profile selects between.
//
//	[default]
//	workers = 16
//	global_rate = "500ms"
//
//	[bulk]
//	workers = 64
//	global_rate = "50ms"
//	library_dir = "/data/ccrawl"
//
// The format is the subset of TOML this needs and no more: comments, tables,
// and one scalar per line. Strings may be quoted or bare, and a value is kept as
// written and converted where it is used, so a duration is spelled the way the
// flag spells it. Arrays, nested tables, multi-line strings and dates are not
// understood and are an error rather than a silent skip, because a setting that
// is quietly ignored is worse than one that is refused.
//
// A nil *Settings behaves like an empty file, so callers do not have to check.
type Settings struct {
	Path  string // where it was read from, whether or not it exists
	found bool

	order    []string // profile names in file order
	sections map[string]map[string]settingLine
}

// settingLine is a value and the line it came from, so an error about it can
// point at the place to fix.
type settingLine struct {
	value string
	line  int
}

// DefaultSection is the table every run reads before its profile.
const DefaultSection = "default"

// SettingsPath is the config file a run reads.
func SettingsPath() string { return filepath.Join(ConfigDir(), "config.toml") }

// LoadSettings reads path. A file that is not there is not an error: it means
// every setting comes from the environment, the flags and the defaults, which is
// how ccrawl has always run.
func LoadSettings(path string) (*Settings, error) {
	s := &Settings{Path: path, sections: map[string]map[string]settingLine{}}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	s.found = true

	section := DefaultSection
	sc := bufio.NewScanner(f)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if name, ok := tableName(line); ok {
			if name == "" {
				return nil, fmt.Errorf("%s:%d: a table needs a name", path, n)
			}
			section = name
			if section != DefaultSection && !slices.Contains(s.order, section) {
				s.order = append(s.order, section)
			}
			if s.sections[section] == nil {
				s.sections[section] = map[string]settingLine{}
			}
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("%s:%d: %q is not a key = value line", path, n, line)
		}
		key = strings.TrimSpace(key)
		value, err := scalar(strings.TrimSpace(value))
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %s: %w", path, n, key, err)
		}
		if s.sections[section] == nil {
			s.sections[section] = map[string]settingLine{}
		}
		if prev, dup := s.sections[section][key]; dup {
			return nil, fmt.Errorf("%s:%d: %s is already set on line %d", path, n, key, prev.line)
		}
		s.sections[section][key] = settingLine{value: value, line: n}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return s, nil
}

// tableName reads a [name] header.
func tableName(line string) (string, bool) {
	if !strings.HasPrefix(line, "[") {
		return "", false
	}
	name, ok := strings.CutPrefix(strings.TrimSuffix(line, "]"), "[")
	if !ok || strings.Contains(name, "[") {
		return "", true // a name we cannot read, reported as an unnamed table
	}
	return strings.TrimSpace(name), true
}

// scalar unquotes a value and rejects the parts of TOML this does not do.
func scalar(v string) (string, error) {
	if i := strings.Index(v, " #"); i >= 0 && !strings.HasPrefix(v, `"`) {
		v = strings.TrimSpace(v[:i]) // trailing comment on a bare value
	}
	switch {
	case v == "":
		return "", fmt.Errorf("no value")
	case strings.HasPrefix(v, "["):
		return "", fmt.Errorf("arrays are not read here, one value per key")
	case strings.HasPrefix(v, "{"):
		return "", fmt.Errorf("inline tables are not read here, one value per key")
	case strings.HasPrefix(v, `"`):
		if len(v) < 2 || !strings.HasSuffix(v, `"`) {
			return "", fmt.Errorf("unclosed quote")
		}
		return v[1 : len(v)-1], nil
	case strings.HasPrefix(v, "'"):
		if len(v) < 2 || !strings.HasSuffix(v, "'") {
			return "", fmt.Errorf("unclosed quote")
		}
		return v[1 : len(v)-1], nil
	}
	return v, nil
}

// Exists reports whether there is a config file at all.
func (s *Settings) Exists() bool { return s != nil && s.found }

// Profiles lists the named profiles, in the order the file declares them.
func (s *Settings) Profiles() []string {
	if s == nil {
		return nil
	}
	return slices.Clone(s.order)
}

// HasProfile reports whether the file declares a profile by that name.
func (s *Settings) HasProfile(name string) bool {
	if s == nil {
		return false
	}
	_, ok := s.sections[name]
	return ok && name != DefaultSection
}

// Lookup returns the value for a key and the table it came from: the profile
// when the profile sets it, [default] otherwise.
func (s *Settings) Lookup(profile, key string) (value, from string, ok bool) {
	if s == nil {
		return "", "", false
	}
	if profile != "" && profile != DefaultSection {
		if v, hit := s.sections[profile][key]; hit {
			return v.value, profile, true
		}
	}
	if v, hit := s.sections[DefaultSection][key]; hit {
		return v.value, DefaultSection, true
	}
	return "", "", false
}

// Validate reports the first key the program does not know. A typo in a config
// file is otherwise invisible: the run starts, the setting does nothing, and the
// only symptom is behaviour nobody asked for.
func (s *Settings) Validate(known []string) error {
	if s == nil {
		return nil
	}
	for _, section := range append([]string{DefaultSection}, s.order...) {
		for _, key := range sortedKeys(s.sections[section]) {
			if slices.Contains(known, key) {
				continue
			}
			return fmt.Errorf("%s:%d: unknown setting %q in [%s], the ones this version reads are: %s",
				s.Path, s.sections[section][key].line, key, section, strings.Join(known, ", "))
		}
	}
	return nil
}

func sortedKeys(m map[string]settingLine) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

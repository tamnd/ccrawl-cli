package ccrawl

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// write puts a config file in a fresh directory and returns its path.
func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSettingsReadsDefaultAndProfiles(t *testing.T) {
	s, err := LoadSettings(write(t, `
# the settings every run gets
[default]
workers = 16
global_rate = "500ms"

[bulk]
workers = 64
library_dir = "/data/ccrawl"

[polite]
global_rate = "5s"
`))
	if err != nil {
		t.Fatal(err)
	}
	if !s.Exists() {
		t.Error("Exists() = false for a file that is there")
	}
	if got, want := strings.Join(s.Profiles(), ","), "bulk,polite"; got != want {
		t.Errorf("Profiles() = %q, want %q", got, want)
	}

	// A profile value wins, and a key the profile leaves alone falls through to
	// [default] rather than to nothing.
	cases := []struct{ profile, key, value, from string }{
		{"", "workers", "16", DefaultSection},
		{"bulk", "workers", "64", "bulk"},
		{"bulk", "global_rate", "500ms", DefaultSection},
		{"bulk", "library_dir", "/data/ccrawl", "bulk"},
		{"polite", "global_rate", "5s", "polite"},
	}
	for _, c := range cases {
		v, from, ok := s.Lookup(c.profile, c.key)
		if !ok || v != c.value || from != c.from {
			t.Errorf("Lookup(%q, %q) = %q, %q, %v; want %q, %q, true", c.profile, c.key, v, from, ok, c.value, c.from)
		}
	}
	if _, _, ok := s.Lookup("bulk", "timeout"); ok {
		t.Error("Lookup found a key no table sets")
	}
	if s.HasProfile("nope") || !s.HasProfile("bulk") {
		t.Error("HasProfile disagrees with the file")
	}
	// [default] is a table, not a profile: --profile default would be a way to
	// ask for the settings you already have.
	if s.HasProfile(DefaultSection) {
		t.Error("HasProfile(default) = true, [default] is not a profile")
	}
}

func TestSettingsWithoutAFileIsNotAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	s, err := LoadSettings(path)
	if err != nil {
		t.Fatalf("a missing config file is a normal run, got %v", err)
	}
	if s.Exists() {
		t.Error("Exists() = true for a file that is not there")
	}
	if _, _, ok := s.Lookup("", "workers"); ok {
		t.Error("a missing file has settings in it")
	}
	if s.Path != path {
		t.Errorf("Path = %q, want %q", s.Path, path)
	}
}

// A nil *Settings is what a caller holds when the file could not be read, and it
// has to behave like an empty one rather than panic on the way out.
func TestNilSettingsBehavesLikeAnEmptyFile(t *testing.T) {
	var s *Settings
	if s.Exists() || s.HasProfile("bulk") || s.Profiles() != nil {
		t.Error("a nil *Settings claims to have something in it")
	}
	if _, _, ok := s.Lookup("bulk", "workers"); ok {
		t.Error("a nil *Settings found a setting")
	}
	if err := s.Validate([]string{"workers"}); err != nil {
		t.Errorf("Validate on nil = %v", err)
	}
}

func TestSettingsQuotingAndComments(t *testing.T) {
	s, err := LoadSettings(write(t, `
[default]
quoted = "a value"
single = 'another'
bare = 16
trailing = 8 # how many
hash_in_string = "value # not a comment"
`))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"quoted":         "a value",
		"single":         "another",
		"bare":           "16",
		"trailing":       "8",
		"hash_in_string": "value # not a comment",
	}
	for k, v := range want {
		if got, _, _ := s.Lookup("", k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
}

// The parser refuses what it does not understand instead of skipping it. A
// setting that is silently dropped is the worst outcome here: the run looks
// fine and does something else.
func TestSettingsRefusesWhatItCannotRead(t *testing.T) {
	cases := []struct{ name, body, want string }{
		{"array", "[default]\nworkers = [1, 2]\n", "arrays are not read"},
		{"inline table", "[default]\nworkers = {n = 1}\n", "inline tables are not read"},
		{"unclosed quote", "[default]\ncrawl = \"CC-MAIN\n", "unclosed quote"},
		{"no value", "[default]\nworkers =\n", "no value"},
		{"not a setting line", "[default]\nworkers\n", "is not a key = value line"},
		{"duplicate key", "[default]\nworkers = 1\nworkers = 2\n", "already set on line 2"},
		{"unnamed table", "[]\nworkers = 1\n", "a table needs a name"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := LoadSettings(write(t, c.body))
			if err == nil {
				t.Fatalf("read %q without complaint", c.body)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error is %q, want it to mention %q", err, c.want)
			}
		})
	}
}

// A key the program does not know is nearly always a typo, and the whole value
// of saying so is naming the line and what it could have been.
func TestValidateNamesTheLineAndTheKnownKeys(t *testing.T) {
	path := write(t, "[default]\nworkers = 16\n\n[bulk]\nworkes = 64\n")
	s, err := LoadSettings(path)
	if err != nil {
		t.Fatal(err)
	}
	err = s.Validate([]string{"workers", "timeout"})
	if err == nil {
		t.Fatal("Validate accepted a key the program does not read")
	}
	for _, want := range []string{path + ":5", `"workes"`, "[bulk]", "workers, timeout"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error is %q, want it to mention %q", err, want)
		}
	}
	if err := s.Validate([]string{"workers", "workes"}); err != nil {
		t.Errorf("Validate rejected a key it was told about: %v", err)
	}
}

func TestSettingsPathFollowsTheConfigDir(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CCRAWL_CONFIG_DIR", dir)
	if got, want := SettingsPath(), filepath.Join(dir, "config.toml"); got != want {
		t.Errorf("SettingsPath() = %q, want %q", got, want)
	}
	t.Setenv("CCRAWL_CONFIG_DIR", "")
	t.Setenv("XDG_CONFIG_HOME", dir)
	if got, want := SettingsPath(), filepath.Join(dir, "ccrawl", "config.toml"); got != want {
		t.Errorf("SettingsPath() under XDG_CONFIG_HOME = %q, want %q", got, want)
	}
}

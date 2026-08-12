package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// withConfig puts a config file where the next run will read it. The harness
// otherwise points the config dir at an empty directory, so this is the only way
// a test gets one, and it has to be called before run.
func withConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CCRAWL_CONFIG_DIR", dir)
	return path
}

// shown is one row of config show as it comes out of the default renderer,
// which is JSON Lines because a test has no terminal. Matching the whole row
// rather than the value alone is the point: a value that is right for the wrong
// reason is the failure this command exists to catch.
func shown(key, value, source string) string {
	return `{"key":"` + key + `","source":"` + source + `","value":"` + value + `"}`
}

// The headline of the feature: a profile changes what a command does without
// the command line saying anything but the profile name. The crawl is what this
// checks because it is visible in the output of a real search, so a profile
// that was read but not applied cannot pass.
func TestProfileChangesWhatASearchDoes(t *testing.T) {
	withConfig(t, `
[default]
crawl = "CC-MAIN-2026-30"

[old]
crawl = "CC-MAIN-2025-33"
`)
	r := run(t, "--profile", "old", "search", "example.com/*", "-o", "crawl,url").wantCode(t, 0)
	if strings.Contains(r.Out, `"crawl":"CC-MAIN-2026-30"`) {
		t.Fatalf("the profile did not reach the query:\n%s", r.Out)
	}
	r.wantOut(t, `"crawl":"CC-MAIN-2025-33"`)

	// Without the profile the same file gives the [default] crawl, which is what
	// makes the line above about the profile and not about the file.
	run(t, "search", "example.com/*", "-o", "crawl,url").wantCode(t, 0).wantOut(t, `"crawl":"CC-MAIN-2026-30"`)
}

// A flag beats the profile that beats [default]. This is the whole precedence
// rule in one test, and it is worth having as one: the mechanism is that a
// profile changes what a flag defaults to, so a bug here looks like a flag being
// ignored rather than like a config file problem.
func TestFlagBeatsProfileBeatsDefault(t *testing.T) {
	withConfig(t, `
[default]
workers = 3

[bulk]
workers = 7
`)
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"default", []string{"config", "show"}, shown("workers", "3", "config [default]")},
		{"profile", []string{"--profile", "bulk", "config", "show"}, shown("workers", "7", "config [bulk]")},
		{"flag", []string{"--profile", "bulk", "-j", "9", "config", "show"}, shown("workers", "9", "flag --workers")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			runNoServer(t, c.args...).wantCode(t, 0).wantOut(t, c.want)
		})
	}
}

// The environment sits between the flag and the profile, so a profile cannot
// quietly undo an export in the shell that started the run.
func TestEnvBeatsTheProfile(t *testing.T) {
	withConfig(t, "[default]\nworkers = 3\n\n[bulk]\nworkers = 7\n")
	t.Setenv("CCRAWL_WORKERS", "5")
	runNoServer(t, "--profile", "bulk", "config", "show").wantCode(t, 0).
		wantOut(t, shown("workers", "5", "env CCRAWL_WORKERS"))
	runNoServer(t, "--profile", "bulk", "-j", "9", "config", "show").wantCode(t, 0).
		wantOut(t, shown("workers", "9", "flag --workers"))
}

// config show has to name where every value came from, because the question it
// answers is why a run behaved unexpectedly, and "workers is 7" without "from
// [bulk]" leaves the person exactly where they started.
func TestConfigShowAttributesEveryValue(t *testing.T) {
	path := withConfig(t, "[default]\nworkers = 3\nretries = 2\n\n[bulk]\nworkers = 7\n")
	t.Setenv("CCRAWL_USER_AGENT", "ua-from-env/1")
	r := runNoServer(t, "--profile", "bulk", "-c", "CC-MAIN-2026-30", "config", "show").wantCode(t, 0)
	r.wantOut(t,
		shown("workers", "7", "config [bulk]"),
		shown("retries", "2", "config [default]"),
		shown("user_agent", "ua-from-env/1", "env CCRAWL_USER_AGENT"),
		shown("crawl", "CC-MAIN-2026-30", "flag --crawl"),
		shown("timeout", "2m0s", "default"),
		shown("profile", "bulk", "flag --profile"),
		shown("config_file", path, "derived from config_dir"),
	)

	// Every row says something. A blank source is a row nobody thought about,
	// which is the failure this feature exists to prevent.
	for _, line := range r.Lines() {
		var row struct{ Key, Value, Source string }
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("row %q is not a record: %v", line, err)
		}
		if row.Source == "" {
			t.Errorf("no source for %s", row.Key)
		}
	}
}

// With no config file at all, config show still says so and still attributes
// every value, since that is the state most runs are in.
func TestConfigShowWithoutAFile(t *testing.T) {
	r := runNoServer(t, "config", "show").wantCode(t, 0)
	r.wantOut(t, "(not present)", shown("profile", "none", "default"), shown("retries", "5", "default"))
}

// A profile that is not in the file is a typo, and the fix is nearly always in
// the list of the ones that are, so the error carries it.
func TestUnknownProfileIsAUsageError(t *testing.T) {
	withConfig(t, "[default]\nworkers = 3\n\n[bulk]\nworkers = 7\n\n[polite]\nworkers = 1\n")
	r := runNoServer(t, "--profile", "buk", "config", "show").wantCode(t, 2)
	if !strings.Contains(r.Err, "bulk, polite") {
		t.Errorf("the error does not list the profiles there are:\n%s", r.Err)
	}
}

// A key the program does not read stops the run. The alternative is a config
// file that looks like it is doing something and is not.
func TestUnknownSettingStopsTheRun(t *testing.T) {
	withConfig(t, "[default]\nworkers = 3\nworkes = 4\n")
	r := runNoServer(t, "config", "show").wantCode(t, 2)
	if !strings.Contains(r.Err, "unknown setting") {
		t.Errorf("the error does not say what is wrong:\n%s", r.Err)
	}
}

// The settings that have no flag are the ones most likely to be dropped on the
// way through, because nothing else in the program reads them.
func TestSettingsWithoutAFlagStillReachTheRun(t *testing.T) {
	dir := t.TempDir()
	withConfig(t, "[default]\nbackoff = \"3s\"\nbackoff_max = \"9s\"\ndb_path = \""+dir+"/other.duckdb\"\n")
	runNoServer(t, "config", "show").wantCode(t, 0).
		wantOut(t, shown("backoff", "3s", "config [default]"), shown("backoff_max", "9s", "config [default]"),
			shown("db_path", dir+"/other.duckdb", "config [default]"))
}

// A profile that moves the data dir has to take the cache with it, or a run
// under the profile writes its downloads to one tree and its cached manifests
// to another.
//
// This one is below the command line rather than through it, because the test
// harness exports CCRAWL_DATA_DIR to keep runs off each other's directories and
// the environment rightly beats the profile.
func TestMovingTheDataDirMovesTheCache(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CCRAWL_DATA_DIR", "")
	t.Setenv("CCRAWL_CACHE_DIR", "")
	withConfig(t, "[bulk]\ndata_dir = \""+dir+"\"\n")

	cfg := ccrawl.DefaultConfig()
	loadSettings([]string{"ccrawl", "--profile", "bulk"}).apply(&cfg)
	if cfg.DataDir != dir {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, dir)
	}
	if want := dir + "/cache"; cfg.CacheDir != want {
		t.Errorf("CacheDir = %q, want %q, the cache stayed behind", cfg.CacheDir, want)
	}

	// A cache dir the file names outright is left where it was put.
	other := t.TempDir()
	withConfig(t, "[bulk]\ndata_dir = \""+dir+"\"\ncache_dir = \""+other+"\"\n")
	cfg = ccrawl.DefaultConfig()
	loadSettings([]string{"ccrawl", "--profile", "bulk"}).apply(&cfg)
	if cfg.CacheDir != other {
		t.Errorf("CacheDir = %q, want %q", cfg.CacheDir, other)
	}
}

// A value that is the right key and the wrong type falls back rather than
// failing the run: this is read while the flags are being registered, where
// there is nowhere to return an error, and the run should still start.
func TestABadValueFallsBackAndSaysSo(t *testing.T) {
	withConfig(t, "[default]\nworkers = \"lots\"\ntimeout = \"soon\"\n")
	r := runNoServer(t, "config", "show").wantCode(t, 0)
	if !strings.Contains(r.Err, "not a number") || !strings.Contains(r.Err, "not a duration") {
		t.Errorf("the run did not say what it ignored:\n%s", r.Err)
	}
}

func TestArgvFlagsReadsTheLongAndShortForms(t *testing.T) {
	got := argvFlags([]string{"ccrawl", "-c", "CC-MAIN-2026-30", "--profile=bulk", "-j", "4", "search", "--url-fgrep", "x", "--", "-n"})
	for _, want := range []string{"crawl", "profile", "workers", "url-fgrep"} {
		if !got[want] {
			t.Errorf("did not see --%s in the command line", want)
		}
	}
	// Everything after -- is an argument, not a flag, whatever it looks like.
	if got["limit"] {
		t.Error("read -n after -- as a flag")
	}
}

func TestArgvValueReadsBothSpellings(t *testing.T) {
	for _, argv := range [][]string{
		{"ccrawl", "--profile", "bulk", "search"},
		{"ccrawl", "--profile=bulk", "search"},
	} {
		if got := argvValue(argv, "profile"); got != "bulk" {
			t.Errorf("argvValue(%q) = %q, want bulk", argv, got)
		}
	}
	if got := argvValue([]string{"ccrawl", "search", "--", "--profile", "bulk"}, "profile"); got != "" {
		t.Errorf("read a profile out of the arguments: %q", got)
	}
	if got := argvValue([]string{"ccrawl", "search", "--profile"}, "profile"); got != "" {
		t.Errorf("read a value off the end of the command line: %q", got)
	}
}

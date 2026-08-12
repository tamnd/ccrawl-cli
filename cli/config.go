package cli

import (
	"context"
	"os"
	"path/filepath"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func newConfigCmd() kit.Command {
	return kit.Command{
		Use:   "config",
		Short: "Show resolved configuration and data paths",
		Long:  "Print where ccrawl reads and writes, and the effective client settings, so you can see exactly what a run will do.",
		Sub: []kit.Command{{
			Use:   "show",
			Short: "Print the effective configuration",
			Run:   runConfigShow,
		}},
	}
}

// runConfigShow prints what this run resolved and where each value came from.
// The source column is the point of the command: a run that behaves oddly is
// nearly always a setting arriving from somewhere the person did not look, and
// naming the flag, the environment variable or the profile table ends that
// search in one line.
//
// A row with no source of its own is derived from another (raw_dir and
// parquet_dir hang off the data dir) or is not a setting at all (duckdb is a
// fact about the machine).
func runConfigShow(ctx context.Context, _ []string) error {
	app := appFromCtx(ctx)
	cfg := app.Cfg
	set := app.Settings
	rows := [][3]string{
		{"crawl", cfg.CrawlID, set.source("crawl")},
		{"source", string(cfg.Source), set.source("source")},
		{"data_dir", cfg.DataDir, set.source("data_dir")},
		{"cache_dir", cfg.CacheDir, dirSource(cfg.CacheDir, filepath.Join(cfg.DataDir, "cache"), set.source("cache_dir"))},
		{"config_dir", ccrawl.ConfigDir(), configDirSource()},
		{"config_file", settingsFileWord(set), "derived from config_dir"},
		{"profile", profileWord(set), profileSource(set)},
		{"raw_dir", cfg.RawDir(), "derived from data_dir"},
		{"parquet_dir", cfg.ParquetDir(), "derived from data_dir"},
		{"library_dir", app.LibraryDir, librarySource(set)},
		{"db_path", cfg.DBPath, dirSource(cfg.DBPath, filepath.Join(cfg.DataDir, "ccrawl.duckdb"), set.source("db_path"))},
		{"workers", itoa(cfg.Workers), set.source("workers")},
		{"rate", cfg.Delay.String(), set.source("rate")},
		{"global_rate", cfg.GlobalRate.String(), set.source("global_rate")},
		{"timeout", cfg.Timeout.String(), set.source("timeout")},
		{"retries", itoa(cfg.Retries), set.source("retries")},
		{"backoff", cfg.Backoff.String(), set.source("backoff")},
		{"backoff_max", cfg.BackoffMax.String(), set.source("backoff_max")},
		{"user_agent", cfg.UserAgent, set.source("user_agent")},
		{"duckdb", boolWord(ccrawl.DuckDBAvailable()), "found on PATH"},
	}
	cols := []string{"key", "value", "source"}
	for _, r := range rows {
		if err := app.Out.Emit(Row{
			Cols:  cols,
			Vals:  []string{r[0], r[1], r[2]},
			Value: map[string]any{"key": r[0], "value": r[1], "source": r[2]},
		}); err != nil {
			return err
		}
	}
	return app.Out.Flush()
}

// dirSource names where a path that can either be set or be derived came from.
// The cache dir and the DuckDB file both follow the data dir when nobody named
// them, so reporting "default" for a path that moved because --data-dir moved
// tells the reader the opposite of what happened, and this column exists to end
// that kind of question rather than start one.
func dirSource(got, derived, named string) string {
	if named == "default" && got == derived {
		return "derived from data_dir"
	}
	return named
}

// settingsFileWord is the config file, or a note that there is none, which is
// the first thing to know when a setting is not being picked up.
func settingsFileWord(s *settings) string {
	path := ccrawl.SettingsPath()
	if s == nil || !s.file.Exists() {
		return path + " (not present)"
	}
	return path
}

func profileWord(s *settings) string {
	if s == nil || s.profile == "" {
		return "none"
	}
	return s.profile
}

func profileSource(s *settings) string {
	switch {
	case s == nil || s.profile == "":
		return "default"
	case s.argv["profile"]:
		return "flag --profile"
	default:
		return "env CCRAWL_PROFILE"
	}
}

// configDirSource covers the one path that is settled before the config file is
// read, and so cannot come from it.
func configDirSource() string {
	if os.Getenv("CCRAWL_CONFIG_DIR") != "" {
		return "env CCRAWL_CONFIG_DIR"
	}
	if os.Getenv("XDG_CONFIG_HOME") != "" {
		return "env XDG_CONFIG_HOME"
	}
	return "default"
}

// librarySource is library_dir under the name its flag has.
func librarySource(s *settings) string {
	if s != nil && s.argv["library-dir"] {
		return "flag --library-dir"
	}
	return s.source("library_dir")
}

func boolWord(b bool) string {
	if b {
		return "available"
	}
	return "not found"
}

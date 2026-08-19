package cli

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// The settings a run can take from the config file, and the environment
// variable that beats each one. A key that is not on this list is refused when
// the file is read, so a typo says so instead of doing nothing.
//
// The order is the order config show prints.
var knownSettings = []struct {
	key string
	env string
}{
	{"crawl", "CCRAWL_CRAWL"},
	{"source", "CCRAWL_SOURCE"},
	{"data_dir", "CCRAWL_DATA_DIR"},
	{"cache_dir", "CCRAWL_CACHE_DIR"},
	{"library_dir", "CCRAWL_LIBRARY"},
	{"db_path", "CCRAWL_DB_PATH"},
	{"workers", "CCRAWL_WORKERS"},
	{"rate", "CCRAWL_RATE"},
	{"global_rate", "CCRAWL_GLOBAL_RATE"},
	{"timeout", "CCRAWL_TIMEOUT"},
	{"retries", "CCRAWL_RETRIES"},
	{"backoff", "CCRAWL_BACKOFF"},
	{"backoff_max", "CCRAWL_BACKOFF_MAX"},
	{"user_agent", "CCRAWL_USER_AGENT"},
	{"urls_repo", "CCRAWL_URLS_REPO"},
	{"domains_repo", "CCRAWL_DOMAINS_REPO"},
	{"news_repo", "CCRAWL_NEWS_REPO"},
	{"collinfo_endpoint", "CCRAWL_COLLINFO_ENDPOINT"},
	{"data_endpoint", "CCRAWL_DATA_ENDPOINT"},
	{"cdx_endpoint", "CCRAWL_CDX_ENDPOINT"},
}

// The flag that beats each setting, where the two are not spelled the same. A
// setting with no flag of its own is left out and can never read as coming from
// one.
var settingFlags = map[string]string{
	"library_dir":  "library-dir",
	"urls_repo":    "repo",
	"domains_repo": "repo",
	"news_repo":    "repo",
}

// settings is what a run resolved out of the config file, and where each value
// came from. It is read while flags are being registered, because a profile
// works by changing what a flag defaults to: kit then overwrites the default
// with the flag when one was given, which is the precedence the whole thing
// needs and none of the code it would otherwise take.
type settings struct {
	file    *ccrawl.Settings
	profile string
	// argv holds the long names of the global flags this command line carries,
	// so config show can say a value came from a flag. cobra knows this and does
	// not share it: a kit command is handed a context and its arguments, not the
	// command it was parsed from, so the answer is read off os.Args instead.
	argv map[string]bool
	// err is a config file that could not be read or a profile that is not in
	// it. NewApp has nowhere to return an error, so it is carried here and
	// raised when the run builds its client, which is before any command runs.
	err error
}

// current is the config file this process resolved. The flag registrations that
// want a setting are spread across the command tree and are called with no
// builder within reach, so the resolved file is parked here for them, in the
// same place and for the same reason they already reach for the environment.
var current *settings

// setting is the default for a flag that a config file may set, for the
// registrations that cannot see the builder.
func setting(key, def string) string { return current.str(key, def) }

// loadSettings reads the config file and works out which profile is in force.
func loadSettings(argv []string) *settings {
	s := &settings{argv: argvFlags(argv)}
	f, err := ccrawl.LoadSettings(ccrawl.SettingsPath())
	if err != nil {
		// A config file the program cannot read is the same class of mistake as a
		// flag it cannot read, so it exits the same way.
		s.err = usageErr(err.Error())
		return s
	}
	known := make([]string, 0, len(knownSettings))
	for _, k := range knownSettings {
		known = append(known, k.key)
	}
	if err := f.Validate(known); err != nil {
		s.err = usageErr(err.Error())
		return s
	}
	s.file = f
	s.profile = argvValue(argv, "profile")
	if s.profile == "" {
		s.profile = os.Getenv("CCRAWL_PROFILE")
	}
	if s.profile != "" && !f.HasProfile(s.profile) {
		s.err = usageErr(profileMiss(f, s.profile))
	}
	return s
}

// profileMiss is the error for --profile naming something the file does not
// declare. It lists what there is, because the answer is nearly always a
// spelling and the list is short.
func profileMiss(f *ccrawl.Settings, name string) string {
	if !f.Exists() {
		return fmt.Sprintf("--profile %s: there is no config file at %s", name, f.Path)
	}
	have := f.Profiles()
	if len(have) == 0 {
		return fmt.Sprintf("--profile %s: %s declares no profiles, only [default]", name, f.Path)
	}
	return fmt.Sprintf("--profile %s: %s declares %s", name, f.Path, strings.Join(have, ", "))
}

// value resolves one setting the way the whole program resolves settings:
// environment first, then the profile, then [default]. The flag is not in here
// because the flag beats all of it and kit applies it afterwards.
func (s *settings) value(key string) (string, string, bool) {
	if s == nil {
		return "", "", false
	}
	for _, k := range knownSettings {
		if k.key != key || k.env == "" {
			continue
		}
		if v, ok := os.LookupEnv(k.env); ok && v != "" {
			return v, "env " + k.env, true
		}
	}
	v, from, ok := s.file.Lookup(s.profile, key)
	if !ok {
		return "", "", false
	}
	if from == ccrawl.DefaultSection {
		return v, "config [default]", true
	}
	return v, "config [" + from + "]", true
}

// str resolves a string setting, falling back to def.
func (s *settings) str(key, def string) string {
	if v, _, ok := s.value(key); ok {
		return v
	}
	return def
}

// duration resolves a duration setting. A value that does not parse falls back
// and says so: this runs while flags are being registered, where there is no
// error to return, and a config file typo should not cost a run.
func (s *settings) duration(key string, def time.Duration) time.Duration {
	v, from, ok := s.value(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ccrawl: %s from %s is %q, which is not a duration, using %s\n", key, from, v, def)
		return def
	}
	return d
}

// integer resolves an int setting, with the same forgiveness as duration.
func (s *settings) integer(key string, def int) int {
	v, from, ok := s.value(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ccrawl: %s from %s is %q, which is not a number, using %d\n", key, from, v, def)
		return def
	}
	return n
}

// source names where a value ends up coming from, for config show. The flag is
// checked first because the flag wins, and the rest follows value.
func (s *settings) source(key string) string {
	flag := settingFlags[key]
	if flag == "" {
		flag = strings.ReplaceAll(key, "_", "-")
	}
	if s != nil && s.argv[flag] {
		return "flag --" + flag
	}
	if _, from, ok := s.value(key); ok {
		return from
	}
	return "default"
}

// apply folds the config file into the defaults every flag is registered
// against, and into the endpoints the run talks to.
func (s *settings) apply(def *ccrawl.Config) {
	def.DataDir = s.str("data_dir", def.DataDir)
	def.CacheDir = s.str("cache_dir", def.CacheDir)
	def.DBPath = s.str("db_path", def.DBPath)
	def.CrawlID = s.str("crawl", def.CrawlID)
	def.UserAgent = s.str("user_agent", def.UserAgent)
	def.Workers = s.integer("workers", def.Workers)
	def.Retries = s.integer("retries", def.Retries)
	def.Delay = s.duration("rate", def.Delay)
	def.GlobalRate = s.duration("global_rate", def.GlobalRate)
	def.Timeout = s.duration("timeout", def.Timeout)
	def.Backoff = s.duration("backoff", def.Backoff)
	def.BackoffMax = s.duration("backoff_max", def.BackoffMax)
	if src := s.str("source", string(def.Source)); src == string(ccrawl.SourceS3) {
		def.Source = ccrawl.SourceS3
	}
	// A cache dir that was not named follows the data dir, or a profile that
	// moves the whole tree would leave the cache behind in the old one.
	if _, _, ok := s.value("cache_dir"); !ok {
		if _, _, moved := s.value("data_dir"); moved {
			def.CacheDir = def.DataDir + "/cache"
		}
	}
	ccrawl.Endpoints.CollInfo = s.str("collinfo_endpoint", ccrawl.Endpoints.CollInfo)
	ccrawl.Endpoints.Data = s.str("data_endpoint", ccrawl.Endpoints.Data)
	ccrawl.Endpoints.CDX = s.str("cdx_endpoint", ccrawl.Endpoints.CDX)
}

// argvFlags collects the long names of the flags a command line carries. Short
// forms are mapped to the long name they stand for, and only the globals are
// listed, because this exists to attribute settings and nothing else uses it.
func argvFlags(argv []string) map[string]bool {
	short := map[string]string{"c": "crawl", "j": "workers", "n": "limit", "o": "output", "y": "yes", "q": "quiet", "v": "verbose"}
	out := map[string]bool{}
	for _, a := range argv {
		if a == "--" {
			break
		}
		name, ok := strings.CutPrefix(a, "--")
		if !ok {
			if s, isShort := strings.CutPrefix(a, "-"); isShort && s != "" && !strings.HasPrefix(s, "-") {
				name = short[strings.SplitN(s, "=", 2)[0]]
				if name == "" {
					continue
				}
				out[name] = true
			}
			continue
		}
		out[strings.SplitN(name, "=", 2)[0]] = true
	}
	return out
}

// argvValue reads the value of a long flag off the command line, for the two
// settings that have to be known before cobra has parsed anything: which
// profile to load, and therefore what every other flag defaults to.
func argvValue(argv []string, name string) string {
	for i, a := range argv {
		if a == "--" {
			return ""
		}
		if v, ok := strings.CutPrefix(a, "--"+name+"="); ok {
			return v
		}
		if a == "--"+name && i+1 < len(argv) {
			return argv[i+1]
		}
	}
	return ""
}

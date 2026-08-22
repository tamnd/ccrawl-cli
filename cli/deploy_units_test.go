package cli

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The fleet units are configuration rather than code, and configuration that is
// wrong fails on a machine at three in the morning instead of in CI. These are
// the three ways it can be wrong that nothing else would catch.

// unitVar matches ${CCRAWL_...} as it appears in an ExecStart line.
var unitVar = regexp.MustCompile(`\$\{(CCRAWL_[A-Z_]+)\}`)

// envAssign matches a NAME=value line in an environment file.
var envAssign = regexp.MustCompile(`(?m)^(CCRAWL_[A-Z_]+)=`)

func readDeploy(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "deploy", name))
	if err != nil {
		t.Fatalf("reading %s: %v", name, err)
	}
	return string(b)
}

func namesIn(re *regexp.Regexp, s string) map[string]bool {
	out := map[string]bool{}
	for _, m := range re.FindAllStringSubmatch(s, -1) {
		out[m[1]] = true
	}
	return out
}

func sorted(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestUnitsAndEnvAgree is about a systemd failure that is quiet in the worst
// way. An unset variable in an ExecStart expands to nothing rather than to an
// error, so a typo in CCRAWL_WORKERS does not stop the unit, it starts a crawl
// with "--workers --writers 4" and the flag parser reads the next flag as the
// worker count.
//
// The check runs both ways. A variable a unit reads and no env file sets is the
// bug above. A variable an env file sets and no unit reads is dead
// configuration, which is worse than useless because somebody will tune it and
// wonder why nothing changed.
func TestUnitsAndEnvAgree(t *testing.T) {
	units := namesIn(unitVar, readDeploy(t, "systemd/ccrawl-recrawl@.service")+
		readDeploy(t, "systemd/ccrawl-publish@.service"))
	if len(units) == 0 {
		t.Fatal("the units reference no environment variables at all, so this test is checking nothing")
	}

	for _, kind := range []string{"domains", "urls"} {
		env := namesIn(envAssign, readDeploy(t, "env/recrawl-"+kind+".env.example"))
		for name := range units {
			if !env[name] {
				t.Errorf("the units read %s and recrawl-%s.env.example does not set it, so systemd would expand it to nothing", name, kind)
			}
		}
		for name := range env {
			if !units[name] {
				t.Errorf("recrawl-%s.env.example sets %s and no unit reads it", kind, name)
			}
		}
		if t.Failed() {
			t.Logf("units read %v", sorted(units))
			t.Logf("recrawl-%s.env.example sets %v", kind, sorted(env))
		}
	}
}

// TestInstallRewritesTheLinesThatAreThere is the one that would really have
// happened.
//
// install.sh gives each machine its shard number by running sed over the
// example env file. sed does not fail when a pattern matches nothing, it copies
// the file through unchanged, so a reworded example line means all three
// machines install as shard 0 of 3 and quietly crawl the same third of the work
// list while the other two thirds are never fetched. Nothing downstream would
// notice: three machines all busy, all publishing, a third of the corpus
// covered.
func TestInstallRewritesTheLinesThatAreThere(t *testing.T) {
	install := readDeploy(t, "install.sh")

	// The anchors install.sh rewrites, exactly as its sed expressions see them.
	for _, line := range []string{"CCRAWL_SHARD=", "CCRAWL_SHARDS=", "CCRAWL_SERVER="} {
		if !strings.Contains(install, "s/^"+line+".*/") {
			t.Fatalf("install.sh no longer rewrites %s, so this test is out of date", line)
		}
		for _, kind := range []string{"domains", "urls"} {
			env := readDeploy(t, "env/recrawl-"+kind+".env.example")
			if !regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(line)).MatchString(env) {
				t.Errorf("recrawl-%s.env.example has no line starting %s, so install.sh would leave every machine on the default", kind, line)
			}
		}
	}
}

// TestUnitsRestartOnTheRightThings pins the two exit behaviours the fleet
// depends on and neither of which is the systemd default.
//
// A finished work list exits 0 and the crawl unit has to stop rather than start
// again, or a machine that has crawled everything it was given spends the rest
// of the month restarting into an empty run. Exit 75 is the opposite: it is the
// binary asking to be recycled, and it has to be a restart rather than a
// failure.
func TestUnitsRestartOnTheRightThings(t *testing.T) {
	crawl := readDeploy(t, "systemd/ccrawl-recrawl@.service")
	if !strings.Contains(crawl, "Restart=on-failure") {
		t.Error("the crawl unit does not restart on failure only, so a finished work list would restart forever")
	}
	if !strings.Contains(crawl, "RestartForceExitStatus=75") || !strings.Contains(crawl, "SuccessExitStatus=75") {
		t.Error("the crawl unit does not treat exit 75 as a retry, which is what the binary uses to ask for one")
	}
	if !strings.Contains(crawl, "StartLimitBurst=") {
		t.Error("the crawl unit has no start limit, so a broken binary would restart forever instead of being noticed")
	}

	pub := readDeploy(t, "systemd/ccrawl-publish@.service")
	if !strings.Contains(pub, "Restart=always") {
		t.Error("the publisher does not always restart, and with --watch set it has no finished state to reach")
	}
	// The token belongs to the publisher and to nothing else.
	if !strings.Contains(pub, "/etc/ccrawl/hf.env") {
		t.Error("the publisher does not read the token file, so it could not commit")
	}
	if strings.Contains(crawl, "hf.env") {
		t.Error("the crawl unit reads the token file and has no use for a credential")
	}
}

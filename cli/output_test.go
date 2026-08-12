package cli

import (
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/tamnd/any-cli/kit/render"
)

// TestAnUnknownOutputFormatIsRefused pins the behaviour the renderer does not
// have: a format with no encoder behind it used to fall through to JSON Lines
// and exit 0, so a typo in -o produced a file full of the wrong thing with a
// name that said otherwise.
func TestAnUnknownOutputFormatIsRefused(t *testing.T) {
	for _, argv := range [][]string{
		{"crawls", "latest", "-o", "csvv"},
		{"crawls", "latest", "-o=csvv"},
		{"crawls", "latest", "-ocsvv"},
		{"crawls", "latest", "--output", "csvv"},
		{"crawls", "latest", "--output=csvv"},
	} {
		t.Run(strings.Join(argv[2:], " "), func(t *testing.T) {
			r := runNoServer(t, argv...).wantCode(t, 2)
			// On stderr, and stdout stays empty: a script that redirects the
			// run to a file gets an empty file rather than an error inside it.
			for _, want := range []string{"unknown output format", "csv, tsv"} {
				if !strings.Contains(r.Err, want) {
					t.Errorf("stderr does not contain %q\nstderr:\n%s", want, r.Err)
				}
			}
			if strings.TrimSpace(r.Out) != "" {
				t.Errorf("stdout is not empty:\n%s", r.Out)
			}
		})
	}
}

// TestEveryAdvertisedOutputFormatIsAccepted is the other half. Refusing a value
// that works would be a worse bug than the one being fixed, so every format the
// help offers is run through the check.
func TestEveryAdvertisedOutputFormatIsAccepted(t *testing.T) {
	for _, f := range knownOutputs() {
		if err := checkOutput([]string{"ccrawl", "crawls", "latest", "-o", f}); err != nil {
			t.Errorf("-o %s was refused: %v", f, err)
		}
	}
}

// TestTheOutputFormatsMatchTheHelp is what makes keeping the list here
// defensible. The valid set belongs to the framework, and hardcoding a copy of
// it means the day any-cli adds a format ccrawl starts refusing a value that
// works. So the copy is checked against the framework's own --output help,
// which is generated from the formats it actually implements, and this fails
// rather than a user finding out.
func TestTheOutputFormatsMatchTheHelp(t *testing.T) {
	out := runNoServer(t, "--help").wantCode(t, 0).Out
	line := regexp.MustCompile(`(?i)output format: ([a-z|]+)`).FindStringSubmatch(out)
	if line == nil {
		t.Fatalf("no --output help found, this test can no longer read the format list:\n%s", out)
	}
	advertised := strings.Split(line[1], "|")

	want := slices.Clone(outputFormats)
	for _, f := range render.RegisteredFormats() {
		want = append(want, string(f))
	}
	slices.Sort(want)
	got := slices.Clone(advertised)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("the help advertises %v, this package accepts %v; update outputFormats", advertised, want)
	}
}

// TestOutputIsNotReadPastTheSeparator checks that an argument to a command is
// not mistaken for the flag. Everything after -- belongs to the command, so a
// literal "-o nonsense" in a template or a query is none of this check's
// business.
func TestOutputIsNotReadPastTheSeparator(t *testing.T) {
	if err := checkOutput([]string{"ccrawl", "search", "--", "-o", "csvv"}); err != nil {
		t.Errorf("an argument past -- was read as the flag: %v", err)
	}
	// A bundled short flag is left to the flag parser rather than guessed at.
	if err := checkOutput([]string{"ccrawl", "crawls", "latest", "-qo", "csvv"}); err != nil {
		t.Errorf("a bundled short flag was guessed at: %v", err)
	}
}

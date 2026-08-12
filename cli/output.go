package cli

import (
	"fmt"
	"slices"
	"strings"

	"github.com/tamnd/any-cli/kit/render"
)

// The renderer dispatches on the format name and its switch ends in a default
// that writes JSON Lines, so a format nobody registered is not refused, it is
// quietly rendered as something else:
//
//	ccrawl search 'example.com/*' -o csvv > captures.csv
//
// That writes JSON Lines into a file called captures.csv and exits 0, and the
// only way to find out is to open the file. A typo in the one flag whose whole
// job is to say what the bytes look like should not be the quietest mistake in
// the program.
//
// The check runs off os.Args before the command tree is built, the same way the
// profile is read, because the format is not on kit.Config and there is nowhere
// later to stand. Refusing a value the renderer would have accepted is the risk
// that comes with keeping the list here rather than upstream, so the list is not
// invented: it is the one the --output help advertises, and
// TestTheOutputFormatsMatchTheHelp reads that help off the built tree and fails
// the day the framework adds a format this does not know about.

// outputFormats is what kit's --output help advertises.
var outputFormats = []string{
	"auto", "table", "markdown", "list", "json", "jsonl", "csv", "tsv", "url", "raw",
}

// outputAliases are the spellings the renderer folds onto one of the above, plus
// "template", which the renderer implements and the help does not mention. They
// work, so refusing them would be the wrong kind of strict.
var outputAliases = []string{"md", "section", "sections", "template"}

// knownOutputs is every value -o accepts in this binary, including the formats
// registered at init time, which is how parquet gets in.
func knownOutputs() []string {
	out := slices.Concat(outputFormats, outputAliases)
	for _, f := range render.RegisteredFormats() {
		if !slices.Contains(out, string(f)) {
			out = append(out, string(f))
		}
	}
	return out
}

// checkOutput refuses a format the renderer does not have an encoder for. An
// empty value is left alone: -o with nothing after it is the flag parser's
// complaint to make, and it makes a better one than this could.
func checkOutput(argv []string) error {
	v := argvOutput(argv)
	if v == "" {
		return nil
	}
	known := knownOutputs()
	if slices.Contains(known, v) {
		return nil
	}
	return usageErr(fmt.Sprintf("-o %s: unknown output format, use one of %s", v, strings.Join(known, ", ")))
}

// argvOutput reads the value of -o off the command line. It covers the four
// spellings a person writes by hand and stops at --, since everything past that
// belongs to the command. A bundled short flag like -qo csv is left to the flag
// parser: guessing at one and getting it wrong would refuse a run that works.
func argvOutput(argv []string) string {
	for i, a := range argv {
		if a == "--" {
			return ""
		}
		for _, p := range []string{"--output=", "-o="} {
			if v, ok := strings.CutPrefix(a, p); ok {
				return v
			}
		}
		if v, ok := strings.CutPrefix(a, "-o"); ok && v != "" && !strings.HasPrefix(v, "-") {
			return v
		}
		if (a == "--output" || a == "-o") && i+1 < len(argv) && !strings.HasPrefix(argv[i+1], "-") {
			return argv[i+1]
		}
	}
	return ""
}

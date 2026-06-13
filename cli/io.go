package cli

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mattn/go-isatty"
)

// cmdOut and cmdErr are the standard streams, isolated so tests can swap them.
var (
	cmdOut io.Writer = os.Stdout
	cmdErr io.Writer = os.Stderr
)

// stderrTTY reports whether stderr is an interactive terminal (controls progress).
func stderrTTY() bool {
	return isatty.IsTerminal(os.Stderr.Fd())
}

// confirm prompts on stderr unless assumeYes is set. It returns true to proceed.
func confirm(assumeYes bool, prompt string) bool {
	if assumeYes {
		return true
	}
	if !stderrTTY() {
		// Non-interactive without --yes: refuse the potentially costly action.
		fmt.Fprintln(cmdErr, prompt+" (refusing without --yes)")
		return false
	}
	fmt.Fprint(cmdErr, prompt+" [y/N] ")
	sc := bufio.NewScanner(os.Stdin)
	if !sc.Scan() {
		return false
	}
	ans := strings.ToLower(strings.TrimSpace(sc.Text()))
	return ans == "y" || ans == "yes"
}

// readLines reads non-empty trimmed lines from r (used for "-" stdin args).
func readLines(r io.Reader, fn func(string) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if err := fn(line); err != nil {
			return err
		}
	}
	return sc.Err()
}

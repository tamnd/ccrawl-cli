package cli

import (
	"context"
	"fmt"
	"runtime"

	"github.com/tamnd/any-cli/kit"
)

// versionCmd holds the --short flag for the version command.
type versionCmd struct {
	short bool
}

func newVersionCmd() kit.Command {
	v := &versionCmd{}
	return kit.Command{
		Use:   "version",
		Short: "Print version information",
		Long: `Print the version, the commit it was built from, when it was built, and the platform and Go toolchain behind it.

The default is the one-line form people read. -o renders the same six fields as data, for a CI job that wants one of them without a regular expression:

  ccrawl version -o json | jq -r .commit`,
		Flags: v.flags,
		Run:   v.run,
	}
}

func (v *versionCmd) flags(f *kit.FlagSet) {
	f.BoolVar(&v.short, "short", false, "print just the version number")
}

func (v *versionCmd) run(ctx context.Context, _ []string) error {
	if v.short {
		_, _ = fmt.Fprintln(cmdOut, Version)
		return nil
	}
	// auto stays the sentence, on a terminal and in a pipe alike. Everywhere
	// else auto means "a table for a person, JSON Lines for a pipe", but this
	// line is what `ccrawl version` has always printed and what a script that
	// greps it expects, and changing that to help a script that does not exist
	// yet is the wrong trade. Asking for a format is how you say you want data.
	if st := kit.FromContext(ctx); st == nil || st.Output.Format == "" || st.Output.Format == "auto" {
		_, _ = fmt.Fprintf(cmdOut, "ccrawl %s (commit %s, built %s, %s/%s, %s)\n",
			Version, Commit, Date, runtime.GOOS, runtime.GOARCH, runtime.Version())
		return nil
	}

	app := appFromCtx(ctx)
	cols := []string{"version", "commit", "built", "os", "arch", "go"}
	vals := []string{Version, Commit, Date, runtime.GOOS, runtime.GOARCH, runtime.Version()}
	value := map[string]any{}
	for i, c := range cols {
		value[c] = vals[i]
	}
	if err := app.Out.Emit(Row{Cols: cols, Vals: vals, Value: value}); err != nil {
		return err
	}
	return app.Out.Flush()
}

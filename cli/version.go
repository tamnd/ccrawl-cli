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
		Flags: v.flags,
		Run:   v.run,
	}
}

func (v *versionCmd) flags(f *kit.FlagSet) {
	f.BoolVar(&v.short, "short", false, "print just the version number")
}

func (v *versionCmd) run(_ context.Context, _ []string) error {
	if v.short {
		_, _ = fmt.Fprintln(cmdOut, Version)
		return nil
	}
	_, _ = fmt.Fprintf(cmdOut, "ccrawl %s (commit %s, built %s, %s/%s, %s)\n",
		Version, Commit, Date, runtime.GOOS, runtime.GOARCH, runtime.Version())
	return nil
}

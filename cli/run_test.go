package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/any-cli/kit/errs"
	"github.com/tamnd/ccrawl-cli/internal/fakecc"
)

// The harness for running the whole command tree in process, the way main does.
//
// Unit tests of the pieces below the CLI already exist and they do not catch the
// failures this layer actually has: a flag that never reaches the query, a
// subcommand registered under the wrong parent, an exit code that says success
// on an empty result. Those only appear when a command is run the way a person
// runs it, from an argv to bytes on stdout and a process exit code, so that is
// what this does.

// result is what one command run produced.
type result struct {
	Code   int
	Out    string
	Err    string
	Server *fakecc.Server
}

// Lines splits stdout into non-empty lines, which is what most assertions want.
func (r result) Lines() []string {
	var out []string
	for _, l := range strings.Split(r.Out, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	return out
}

// run executes ccrawl with the given arguments against a fresh fake Common Crawl
// and returns the exit code with everything the run wrote.
//
// Two rate flags are forced on every run. --global-rate 0 switches off the
// host-wide lock file, which a test has no business touching, and --rate 1ns
// keeps the per-process delay from adding two hundred milliseconds to every
// request; it cannot be 0 because kit reads only a positive --rate.
func run(t *testing.T, args ...string) result {
	t.Helper()
	return runStdin(t, "", args...)
}

// runStdin is run with something on stdin, for the commands that read "-".
func runStdin(t *testing.T, stdin string, args ...string) result {
	t.Helper()
	srv := fakecc.Start(t)
	full := append([]string{"ccrawl", "--rate", "1ns", "--global-rate", "0"}, args...)
	code, out, errOut := invoke(t, stdin, full)
	return result{Code: code, Out: out, Err: errOut, Server: srv}
}

// runNoServer is run without a fake Common Crawl behind it, for the commands
// that never make a request. A test that uses it and then hits the network gets
// a connection error rather than a silent live request.
func runNoServer(t *testing.T, args ...string) result {
	t.Helper()
	code, out, errOut := invoke(t, "", append([]string{"ccrawl"}, args...))
	return result{Code: code, Out: out, Err: errOut}
}

// invoke isolates the process state one run touches, drives kit.Run, and puts it
// all back. os.Stdout is redirected as well as cmdOut, because the renderer
// writes to os.Stdout directly and cobra prints help and usage there.
func invoke(t *testing.T, stdin string, argv []string) (int, string, string) {
	t.Helper()

	// Every run gets its own data and cache directory. The cache is real, so
	// sharing one between tests would let a manifest fetched by one test satisfy
	// another and hide the request it was meant to make.
	dir := t.TempDir()
	t.Setenv("CCRAWL_DATA_DIR", dir)
	t.Setenv("CCRAWL_CACHE_DIR", dir+"/cache")
	// The library root is read at flag-registration time from the environment,
	// so it has to be set before NewApp rather than passed as a flag. A test that
	// needs two runs to share one library sets it first, the way the config dir
	// works below, since every invoke otherwise gets a temp dir of its own.
	if os.Getenv("CCRAWL_LIBRARY") == "" {
		t.Setenv("CCRAWL_LIBRARY", dir+"/library")
	}
	// An empty config dir, so a developer's own ~/.config/ccrawl/config.toml
	// cannot reach a test. It would not be a subtle failure either: a profile is
	// allowed to move the endpoints, which would send a test past the fake
	// Common Crawl and at the real one. A test that wants a config file calls
	// withConfig first, which is why an already-set value is left alone.
	if os.Getenv("CCRAWL_CONFIG_DIR") == "" {
		t.Setenv("CCRAWL_CONFIG_DIR", dir+"/config")
	}

	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	oldOut, oldErr, oldIn, oldArgs := os.Stdout, os.Stderr, os.Stdin, os.Args
	oldCmdOut, oldCmdErr := cmdOut, cmdErr
	os.Stdout, os.Stderr = outW, errW
	cmdOut, cmdErr = outW, errW
	os.Stdin = stdinFile(t, stdin)
	os.Args = argv

	// Drain both pipes while the command runs. A command that writes more than
	// the pipe buffer would otherwise block forever on its own output.
	outCh, errCh := make(chan string, 1), make(chan string, 1)
	go func() { b, _ := io.ReadAll(outR); outCh <- string(b) }()
	go func() { b, _ := io.ReadAll(errR); errCh <- string(b) }()

	// The same two lines main has, for the same reason: a config file the program
	// could not read is settled before there is a command tree to run.
	code := 0
	app, err := NewApp()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ccrawl: %v\n", err)
		code = errs.ExitCode(err)
	} else {
		code = kit.Run(context.Background(), app)
	}

	_ = outW.Close()
	_ = errW.Close()
	os.Stdout, os.Stderr, os.Stdin, os.Args = oldOut, oldErr, oldIn, oldArgs
	cmdOut, cmdErr = oldCmdOut, oldCmdErr

	return code, <-outCh, <-errCh
}

// stdinFile writes s to a temporary file and hands back a reader for it. A file
// rather than a pipe, because the commands that read stdin read it to the end
// and a pipe with no writer left open never reaches one.
func stdinFile(t *testing.T, s string) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "stdin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(s); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// wantCode fails with the run's output attached, since a bare "exit 1, want 0"
// says nothing about what went wrong.
func (r result) wantCode(t *testing.T, want int) result {
	t.Helper()
	if r.Code != want {
		t.Fatalf("exit %d, want %d\nstdout:\n%s\nstderr:\n%s", r.Code, want, r.Out, r.Err)
	}
	return r
}

// wantOut fails unless every fragment appears on stdout.
func (r result) wantOut(t *testing.T, fragments ...string) result {
	t.Helper()
	for _, f := range fragments {
		if !strings.Contains(r.Out, f) {
			t.Fatalf("stdout does not contain %q\nstdout:\n%s\nstderr:\n%s", f, r.Out, r.Err)
		}
	}
	return r
}

// wantNotOut fails if any fragment appears on stdout, which is how a filter is
// shown to have removed something rather than merely to have run.
func (r result) wantNotOut(t *testing.T, fragments ...string) result {
	t.Helper()
	for _, f := range fragments {
		if strings.Contains(r.Out, f) {
			t.Fatalf("stdout contains %q and should not\nstdout:\n%s", f, r.Out)
		}
	}
	return r
}

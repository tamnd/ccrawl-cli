package cli

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/any-cli/kit/errs"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// TestClassifyGivesTransportFailuresTheirCode pins the mapping the exit codes
// page describes: a request that never got an answer is worth running again and
// says so with 8, and a request the server answered badly is not.
func TestClassifyGivesTransportFailuresTheirCode(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"nothing went wrong", nil, 0},
		{"the dial failed", fmt.Errorf("fetch collinfo: %w: connection refused", ccrawl.ErrTransport), 8},
		{"the server answered badly", errors.New("all 6 attempts failed: HTTP 503"), 1},
		{"a usage error keeps its own kind", usageErr("name a kind"), 2},
		{"an empty result keeps its own kind", noResults("nothing matched"), 3},
		{"a transport failure already classified is left alone", errs.Wrap(errs.KindNeedAuth, ccrawl.ErrTransport, "no token"), 4},
		// The retry loop only sees a request that never got an answer, so a body
		// that stops halfway arrives with nothing attached to it. It is the same
		// failure and it gets the same code: the rank table is 262 million rows
		// and losing the connection 77 seconds in is worth trying again.
		{"the connection went away mid stream", fmt.Errorf("stream rank table: %w", opErr(syscall.EHOSTUNREACH)), 8},
		{"the read timed out mid stream", fmt.Errorf("stream vertices: %w", opErr(errTimeout{})), 8},
		{"a reset while reading", fmt.Errorf("stream edges: %w", syscall.ECONNRESET), 8},
		{"a broken pipe while writing", fmt.Errorf("push shard: %w", syscall.EPIPE), 8},
		// os.PathError has Timeout and Temporary methods, so it satisfies the
		// net.Error interface. A missing local file is not a network failure and
		// asking for the interface rather than the socket errors would say it is.
		{"a file that is not there", fmt.Errorf("open shard: %w", &fs.PathError{Op: "open", Path: "/absent", Err: syscall.ENOENT}), 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := errs.ExitCode(classify(c.err)); got != c.want {
				t.Errorf("exit code %d, want %d", got, c.want)
			}
		})
	}
}

// opErr builds the error the net package hands back when a read on an open
// connection fails, which is the shape the real failure had: a *net.OpError
// wrapping the reason, reading "read tcp [addr]->[addr]: read: no route to
// host".
func opErr(cause error) error {
	return &net.OpError{Op: "read", Net: "tcp", Err: cause}
}

// errTimeout is a cause that reports itself as a timeout, since a deadline
// passing mid-body is the other way a long stream ends and it arrives as a
// different concrete type each time.
type errTimeout struct{}

func (errTimeout) Error() string   { return "i/o timeout" }
func (errTimeout) Timeout() bool   { return true }
func (errTimeout) Temporary() bool { return true }

// TestClassifyKeepsTheMessage checks that giving an error a kind does not
// rewrite what the user reads. The message is the only thing that says which
// host was unreachable.
func TestClassifyKeepsTheMessage(t *testing.T) {
	in := fmt.Errorf("fetch collinfo: all 6 attempts failed for https://index.commoncrawl.org/collinfo.json: %w: connection refused", ccrawl.ErrTransport)
	out := classify(in)
	if out.Error() != in.Error() {
		t.Errorf("message changed:\n got %q\nwant %q", out.Error(), in.Error())
	}
	if !errors.Is(out, ccrawl.ErrTransport) {
		t.Error("the cause is no longer reachable through errors.Is")
	}
}

// TestClassifyCmdWrapsTheWholeTree checks that a subcommand three levels down
// is classified too, since that is where most of the escape hatches live.
func TestClassifyCmdWrapsTheWholeTree(t *testing.T) {
	fail := func(context.Context, []string) error {
		return fmt.Errorf("get: %w: no route to host", ccrawl.ErrTransport)
	}
	tree := classifyCmd(kit.Command{
		Use: "parent",
		Sub: []kit.Command{{
			Use: "child",
			Sub: []kit.Command{{Use: "grandchild", Run: fail}},
		}},
	})
	run := tree.Sub[0].Sub[0].Run
	if got := errs.ExitCode(run(context.Background(), nil)); got != 8 {
		t.Errorf("grandchild exit code %d, want 8", got)
	}
}

// TestEveryHandlerIsClassified is the reason the shims exist. Registering a
// command straight through kit means its errors skip the classification and
// every network failure it hits comes back as exit 1, which is the bug this
// replaced. The walk is the only way to catch it: kit.App builds its cobra tree
// privately, so a test cannot ask the built binary what it registered.
func TestEveryHandlerIsClassified(t *testing.T) {
	fset := token.NewFileSet()
	banned := map[string]string{
		"kit.Handle":          "handle",
		"app.AddCommand":      "addCmd",
		"app.AddCommandUnder": "addCmdUnder",
	}

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") || path == "classify.go" {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			ident, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			name := ident.Name + "." + sel.Sel.Name
			if use, bad := banned[name]; bad {
				t.Errorf("%s: %s registers a handler whose errors are never classified, use %s", fset.Position(call.Pos()), name, use)
			}
			return true
		})
	}

	// The walk is worthless if it stopped finding the registrations, so count
	// the ones that went through the shim and fail on a suspiciously small
	// number rather than passing an empty search.
	var shims int
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); ok && (id.Name == "handle" || id.Name == "addCmd" || id.Name == "addCmdUnder") {
				shims++
			}
			return true
		})
	}
	if shims < 40 {
		t.Fatalf("only %d classified registrations found, the walk is not finding them", shims)
	}
	t.Logf("checked %d classified registrations", shims)
}

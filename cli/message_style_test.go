package cli

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// The CLI prints errors through fang, which runs the message through its own
// titleFirstWord before drawing the box:
//
//	words := strings.Fields(s)
//	words[0] = cases.Title(language.AmericanEnglish).String(words[0])
//
// On a sentence that capitalizes the first letter and nothing else, which is
// what it is for. On anything else it rewrites the word: "--table" comes out as
// "--Table", "--crawl-a" as "--Crawl-A", "CDX" as "Cdx", "HTTP" as "Http". A
// message that opens with a flag then names a flag the command will reject, and
// one that opens with an acronym prints a word nobody wrote.
//
// The renderer is fang's and we do not get a vote on it, so the rule is ours to
// keep: an error message may not start with a word that title-casing damages.
func titleFirstWord(s string) string {
	words := strings.Fields(s)
	if len(words) == 0 {
		return s
	}
	words[0] = cases.Title(language.AmericanEnglish).String(words[0])
	return strings.Join(words, " ")
}

// errCtors are the functions whose first string literal becomes a user-visible
// message. errs.Wrap and fmt.Errorf take the format string second and first
// respectively, so the check reads the first string literal in the call rather
// than a fixed argument position.
var errCtors = map[string]bool{
	"usageErr": true, "noResults": true, "needAuth": true, "networkErr": true,
	"Errorf": true, "New": true, "Wrap": true,
	"Usage": true, "NoResults": true, "NeedAuth": true, "RateLimited": true,
	"NotFound": true, "Unsupported": true, "Network": true,
}

// TestErrorMessagesSurviveTheRenderer walks the source of both packages and
// fails on a message whose first word the renderer would rewrite.
//
// It reads the literal, not the rendered string, so a message that starts with
// a format verb is skipped: what "%s" holds is not knowable from the source.
// Those are worth avoiding for the same reason, and a reviewer has to catch
// them.
func TestErrorMessagesSurviveTheRenderer(t *testing.T) {
	fset := token.NewFileSet()
	var checked int

	for _, dir := range []string{".", "../ccrawl"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range files {
			if strings.HasSuffix(path, "_test.go") {
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
				var name string
				switch fn := call.Fun.(type) {
				case *ast.Ident:
					name = fn.Name
				case *ast.SelectorExpr:
					name = fn.Sel.Name
				}
				if !errCtors[name] {
					return true
				}
				msg, pos, ok := firstStringLit(call, fset)
				if !ok {
					return true
				}
				first, _, _ := strings.Cut(msg, " ")
				if first == "" {
					return true
				}
				// A format that is nothing but "%s" is a pass-through, not a
				// message. errs.NoResults("%s", msg) and errs.Usage("%s", msg)
				// are the shims that give a message its exit code, and the
				// message they carry is written at its own call site, which
				// this walk checks there.
				if msg == "%s" {
					return true
				}
				checked++
				// A message that opens with a verb is the worst case, not an
				// exempt one. What the verb holds is not knowable here, but a
				// URL is: "%s: %w" on a parquet URL rendered as
				// Https://Data.commoncrawl.org/Cc-Index/... , which is not a
				// URL that resolves, in an error whose whole job was to name
				// the file that failed. There is always a word to put in front.
				if strings.Contains(first, "%") {
					t.Errorf("%s: %s(%q) starts with a value, and the renderer title-cases whatever lands there; put a word first", pos, name, msg)
					return true
				}
				if got := titleFirstWord(msg); got != capFirst(msg) {
					t.Errorf("%s: %s(%q) renders as %q", pos, name, msg, got)
				}
				return true
			})
		}
	}
	if checked < 100 {
		t.Fatalf("only %d messages checked, the walk is not finding them", checked)
	}
	t.Logf("checked %d error messages", checked)
}

// flagCtors are the flag registrations whose last string argument is the line
// of help the user reads. pflag puts the usage string last in every one of
// them, whatever else the signature takes.
var flagCtors = map[string]bool{
	"StringVar": true, "StringVarP": true, "BoolVar": true, "BoolVarP": true,
	"IntVar": true, "IntVarP": true, "Int64Var": true, "Int64VarP": true,
	"Float64Var": true, "Float64VarP": true, "DurationVar": true, "DurationVarP": true,
	"StringSliceVar": true, "StringSliceVarP": true, "IntSliceVar": true, "IntSliceVarP": true,
	"UintVar": true, "UintVarP": true,
}

// TestFlagHelpSurvivesTheRenderer is the same rule as above for the other place
// the renderer title-cases a first word: the help line beside a flag. It runs
// over both ways ccrawl declares one, the pflag call and the kit struct tag.
//
// This one is worse than an error message, because a help line is where the
// user looks up what to type. "Auto|Duckdb|Native|Print" is a list of four
// values none of which the flag accepts.
//
// It only reaches the flags declared here. Two of the framework globals are
// damaged the same way and have to be fixed in any-cli: --fields prints
// "Comma-Separated columns to show" and --timeout prints "Per-Request
// timeout".
func TestFlagHelpSurvivesTheRenderer(t *testing.T) {
	fset := token.NewFileSet()
	var checked int

	check := func(help string, pos token.Position, what string) {
		first, _, _ := strings.Cut(help, " ")
		if first == "" || strings.Contains(first, "%") {
			return
		}
		checked++
		if got := titleFirstWord(help); got != capFirst(help) {
			t.Errorf("%s: %s %q renders as %q", pos, what, help, got)
		}
		// A line that opens with a column name loses nothing to title-casing,
		// because an underscore is not a word boundary, but it still gets its
		// first letter capitalized, and a column name with a capital on it is
		// not the column name. The columnar filters all used to open this way.
		if strings.Contains(first, "_") {
			t.Errorf("%s: %s %q opens with the identifier %q, which prints as %q", pos, what, help, first, capFirst(first))
		}
	}

	for _, dir := range []string{".", "../ccrawl"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range files {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			ast.Inspect(file, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.CallExpr:
					sel, ok := node.Fun.(*ast.SelectorExpr)
					if !ok || !flagCtors[sel.Sel.Name] {
						return true
					}
					if help, pos, ok := lastStringLit(node, fset); ok {
						check(help, pos, sel.Sel.Name)
					}
				case *ast.Field:
					if node.Tag == nil {
						return true
					}
					tag, err := strconv.Unquote(node.Tag.Value)
					if err != nil {
						return true
					}
					if help := reflect.StructTag(tag).Get("help"); help != "" {
						check(help, fset.Position(node.Tag.Pos()), "help tag")
					}
				}
				return true
			})
		}
	}
	if checked < 200 {
		t.Fatalf("only %d help lines checked, the walk is not finding them", checked)
	}
	t.Logf("checked %d flag help lines", checked)
}

// lastStringLit returns the last plain string literal argument of a call, which
// for a flag registration is the usage line.
func lastStringLit(call *ast.CallExpr, fset *token.FileSet) (string, token.Position, bool) {
	for i := len(call.Args) - 1; i >= 0; i-- {
		lit, ok := call.Args[i].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			continue
		}
		s, err := strconv.Unquote(lit.Value)
		if err != nil {
			return "", token.Position{}, false
		}
		return s, fset.Position(lit.Pos()), true
	}
	return "", token.Position{}, false
}

// firstStringLit returns the first plain string literal argument of a call.
func firstStringLit(call *ast.CallExpr, fset *token.FileSet) (string, token.Position, bool) {
	for _, arg := range call.Args {
		lit, ok := arg.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			continue
		}
		s, err := strconv.Unquote(lit.Value)
		if err != nil {
			return "", token.Position{}, false
		}
		return s, fset.Position(lit.Pos()), true
	}
	return "", token.Position{}, false
}

// capFirst is what the renderer is supposed to do to a message: upper-case the
// first letter and leave the rest alone.
func capFirst(s string) string {
	r := []rune(s)
	if len(r) == 0 {
		return s
	}
	return strings.ToUpper(string(r[0])) + string(r[1:])
}

// TestTitleFirstWordMatchesTheRenderer pins the cases the rule exists for, so a
// reader of the tests above can see what it is protecting against without going
// to read fang.
func TestTitleFirstWordMatchesTheRenderer(t *testing.T) {
	cases := []struct{ in, want string }{
		{"--table is required", "--Table is required"},
		{"--crawl-a and --crawl-b are required", "--Crawl-A and --crawl-b are required"},
		{"CDX page 3 failed", "Cdx page 3 failed"},
		{"point --table at a rank table", "Point --table at a rank table"},
		{"auto|duckdb|native|print", "Auto|Duckdb|Native|Print"},
		{"query engine: auto|duckdb|native|print", "Query engine: auto|duckdb|native|print"},
	}
	for _, c := range cases {
		if got := titleFirstWord(c.in); got != c.want {
			t.Errorf("titleFirstWord(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

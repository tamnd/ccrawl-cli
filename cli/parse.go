package cli

import (
	"bufio"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

type parseFlags struct {
	format   string // warc|wat|wet (auto)
	wtype    string // WARC-Type filter
	status   string
	mime     string
	lang     string
	urlRe    string
	links    bool
	text     bool
	markdown bool
	meta     bool
}

func newParseCmd(app *App) *cobra.Command {
	pf := &parseFlags{}
	cmd := &cobra.Command{
		Use:   "parse <file|->",
		Short: "Decode a local WARC/WAT/WET file into records",
		Long: `Parse an archive file into structured records.

The format is detected from the file name (.warc.gz, .warc.wat.gz, .warc.wet.gz)
and can be forced with --format. Read from a file or from stdin with "-".

Examples:
  ccrawl parse file.warc.gz -o jsonl
  ccrawl parse file.warc.wat.gz --links -o jsonl
  ccrawl parse file.warc.wet.gz --lang eng -o jsonl
  ccrawl parse file.warc.gz --type response --status 200 --markdown -o jsonl`,
		Args: cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runParse(app, c, args[0], pf)
		},
	}
	f := cmd.Flags()
	f.StringVar(&pf.format, "format", "", "force format: warc|wat|wet")
	f.StringVar(&pf.wtype, "type", "", "WARC-Type filter (response|request|metadata|...)")
	f.StringVar(&pf.status, "status", "", "HTTP status filter")
	f.StringVar(&pf.mime, "mime", "", "HTTP MIME filter (substring)")
	f.StringVar(&pf.lang, "lang", "", "language filter (WET)")
	f.StringVar(&pf.urlRe, "url", "", "keep records whose URL contains this substring")
	f.BoolVar(&pf.links, "links", false, "flatten WAT links into one row each")
	f.BoolVar(&pf.text, "text", false, "emit extracted text for response/conversion records")
	f.BoolVar(&pf.markdown, "markdown", false, "convert response bodies to Markdown")
	f.BoolVar(&pf.meta, "meta", false, "emit record metadata only")
	return cmd
}

// errLimit is returned by a limited emitter once the requested number of rows
// has been written, to stop the record iterator early.
var errLimit = errors.New("limit reached")

// limitedEmit wraps app.Out.Emit and honours the global -n limit: once that many
// rows have been emitted it returns errLimit so iteration stops. A limit of 0
// means unlimited.
func limitedEmit(app *App) func(Row) error {
	n := app.Limit
	emitted := 0
	return func(row Row) error {
		if err := app.Out.Emit(row); err != nil {
			return err
		}
		emitted++
		if n > 0 && emitted >= n {
			return errLimit
		}
		return nil
	}
}

func runParse(app *App, c *cobra.Command, path string, pf *parseFlags) error {
	r, name, closeFn, err := openInput(path)
	if err != nil {
		return err
	}
	defer closeFn()

	format := pf.format
	if format == "" {
		format = detectFormat(name)
	}
	id, _ := app.Crawl(c.Context())

	switch format {
	case "wat":
		return parseWAT(app, r, id, pf)
	case "wet":
		return parseWET(app, r, id, pf)
	default:
		return parseWARC(app, r, pf)
	}
}

func parseWARC(app *App, r io.Reader, pf *parseFlags) error {
	count := 0
	emit := limitedEmit(app)
	err := ccrawl.IterateWARC(r, func(rec ccrawl.WARCRecord) error {
		if !warcMatches(rec, pf) {
			return nil
		}
		count++
		switch {
		case pf.links && rec.Header.Type == "response":
			body := ccrawl.HTTPBody(rec.Block)
			for _, l := range ccrawl.ExtractLinks(rec.Header.TargetURI, body) {
				if err := emit(linkRow(l)); err != nil {
					return err
				}
			}
			return nil
		case pf.text && rec.Header.Type == "response":
			text := ccrawl.ExtractText(ccrawl.HTTPBody(rec.Block))
			return emit(textRow(rec.Header.TargetURI, "", text))
		case pf.markdown && rec.Header.Type == "response":
			md, _ := ccrawl.ExtractMarkdown(ccrawl.HTTPBody(rec.Block))
			return emit(textRow(rec.Header.TargetURI, "", md))
		default:
			return emit(warcRow(rec))
		}
	})
	if err != nil && err != errLimit {
		return err
	}
	if err := app.Out.Flush(); err != nil {
		return err
	}
	if count == 0 {
		return noResults("no matching records")
	}
	return nil
}

func parseWAT(app *App, r io.Reader, id string, pf *parseFlags) error {
	emit := limitedEmit(app)
	err := ccrawl.IterateWAT(r, id, func(w ccrawl.WATRecord) error {
		if pf.urlRe != "" && !strings.Contains(w.URL, pf.urlRe) {
			return nil
		}
		if pf.links {
			for _, l := range w.Links {
				if err := emit(linkRow(l)); err != nil {
					return err
				}
			}
			return nil
		}
		return emit(watRow(w))
	})
	if err != nil && err != errLimit {
		return err
	}
	return app.Out.Flush()
}

func parseWET(app *App, r io.Reader, id string, pf *parseFlags) error {
	emit := limitedEmit(app)
	err := ccrawl.IterateWET(r, id, func(w ccrawl.WETRecord) error {
		if pf.urlRe != "" && !strings.Contains(w.URL, pf.urlRe) {
			return nil
		}
		if pf.lang != "" && !strings.Contains(w.ContentLanguage, pf.lang) {
			return nil
		}
		return emit(wetRow(w))
	})
	if err != nil && err != errLimit {
		return err
	}
	return app.Out.Flush()
}

func warcMatches(rec ccrawl.WARCRecord, pf *parseFlags) bool {
	h := rec.Header
	if pf.wtype != "" && h.Type != pf.wtype {
		return false
	}
	if pf.status != "" && strconv.Itoa(h.HTTPStatus) != pf.status {
		return false
	}
	if pf.mime != "" && !strings.Contains(h.HTTPMIME, pf.mime) {
		return false
	}
	if pf.urlRe != "" && !strings.Contains(h.TargetURI, pf.urlRe) {
		return false
	}
	if (pf.links || pf.text || pf.markdown) && h.Type != "response" {
		return false
	}
	return true
}

func textRow(url, lang, text string) Row {
	return Row{
		Cols:  []string{"url", "language", "length", "text"},
		Vals:  []string{url, lang, itoa(len(text)), text},
		Value: map[string]any{"url": url, "language": lang, "length": len(text), "text": text},
	}
}

// openInput opens a path or stdin, transparently decompressing a .gz stream.
func openInput(path string) (io.Reader, string, func(), error) {
	if path == "-" {
		br := bufio.NewReader(os.Stdin)
		return br, "stdin.warc.gz", func() {}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, "", nil, err
	}
	closeFn := func() { _ = f.Close() }
	return f, path, closeFn, nil
}

func detectFormat(name string) string {
	switch {
	case strings.Contains(name, ".wat."):
		return "wat"
	case strings.Contains(name, ".wet."):
		return "wet"
	default:
		return "warc"
	}
}

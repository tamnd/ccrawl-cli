package cli

import (
	"github.com/spf13/cobra"
	"github.com/tamnd/any-cli/kit/render"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// contentMode selects how a fetched WARC record is rendered.
type contentMode struct {
	raw      bool
	headers  bool
	body     bool
	text     bool
	markdown bool
	links    bool
	meta     bool
}

func addContentFlags(cmd *cobra.Command, m *contentMode) {
	f := cmd.Flags()
	f.BoolVar(&m.raw, "raw", false, "print the full raw WARC record")
	f.BoolVar(&m.headers, "headers", false, "print only the captured HTTP response headers")
	f.BoolVar(&m.body, "body", false, "print the response body (default)")
	f.BoolVar(&m.text, "text", false, "extract readable plain text")
	f.BoolVar(&m.markdown, "markdown", false, "convert the HTML body to Markdown")
	f.BoolVar(&m.links, "links", false, "extract outbound links")
	f.BoolVar(&m.meta, "meta", false, "print the record metadata instead of content")
}

// render writes a WARC record to out according to the selected mode. It emits
// rows for the structured selectors (links/meta) and writes raw bytes otherwise.
func (m contentMode) render(out *render.Renderer, rec ccrawl.WARCRecord) error {
	switch {
	case m.raw:
		return writeAll(out, rec.Block)
	case m.headers:
		return writeAll(out, append(ccrawl.HTTPHeaders(rec.Block), '\n'))
	case m.meta:
		return out.Emit(warcRow(rec))
	case m.links:
		body := ccrawl.HTTPBody(rec.Block)
		for _, l := range ccrawl.ExtractLinks(rec.Header.TargetURI, body) {
			if err := out.Emit(linkRow(l)); err != nil {
				return err
			}
		}
		return nil
	case m.text:
		return writeAll(out, []byte(ccrawl.ExtractText(ccrawl.HTTPBody(rec.Block))+"\n"))
	case m.markdown:
		md, err := ccrawl.ExtractMarkdown(ccrawl.HTTPBody(rec.Block))
		if err != nil {
			return err
		}
		return writeAll(out, []byte(md+"\n"))
	default: // body
		return writeAll(out, ccrawl.HTTPBody(rec.Block))
	}
}

// writeAll writes a raw content blob to the renderer, which passes it straight
// through to its underlying writer.
func writeAll(out *render.Renderer, b []byte) error {
	_, err := out.Write(b)
	return err
}

// structured reports whether the mode emits Rows (links/meta) versus raw bytes,
// so callers know whether to Flush the output.
func (m contentMode) structured() bool { return m.links || m.meta }

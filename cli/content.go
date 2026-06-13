package cli

import (
	"github.com/spf13/cobra"
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

// render writes a WARC record to out according to the selected mode. It returns
// rows for the structured selectors (links/meta) and writes raw bytes otherwise.
func (m contentMode) render(out *Output, rec ccrawl.WARCRecord) error {
	switch {
	case m.raw:
		return out.Raw(rec.Block)
	case m.headers:
		return out.Raw(append(ccrawl.HTTPHeaders(rec.Block), '\n'))
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
		return out.Raw([]byte(ccrawl.ExtractText(ccrawl.HTTPBody(rec.Block)) + "\n"))
	case m.markdown:
		md, err := ccrawl.ExtractMarkdown(ccrawl.HTTPBody(rec.Block))
		if err != nil {
			return err
		}
		return out.Raw([]byte(md + "\n"))
	default: // body
		return out.Raw(ccrawl.HTTPBody(rec.Block))
	}
}

// structured reports whether the mode emits Rows (links/meta) versus raw bytes,
// so callers know whether to Flush the output.
func (m contentMode) structured() bool { return m.links || m.meta }

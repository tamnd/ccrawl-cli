package cli

import (
	"errors"
	"fmt"
	"os"

	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// pageLosses collects the index pages a query could not read. A wide query is
// thousands of pages and the index truncates or refuses one often enough that
// losing the whole run to it is the wrong trade: an hour of pages that did
// arrive is worth more than an error message. So the default is to say which
// page was lost, on stderr where it cannot be mistaken for a result, and keep
// going, and --strict is there for the caller who would rather have nothing
// than a result with a hole in it.
type pageLosses struct {
	cmd    string // the command name, for the stderr prefix
	strict bool
	pages  int
	crawls int

	// otherThanTransport counts the losses that were not the transport giving
	// up. A run where every loss was the transport is an outage rather than a
	// bad query, and the caller turns that into exit 8 so a supervisor knows to
	// come back later. One 404 or one truncated page in the mix and it is not
	// that story any more.
	otherThanTransport int
}

// handler returns the callback for CDXQuery.OnPageError. In strict mode it
// returns nil, which is how the stream is told to fail on the first page it
// cannot read.
func (p *pageLosses) handler() func(string, int, error) error {
	if p == nil || p.strict {
		return nil
	}
	return func(crawlID string, page int, err error) error {
		if !errors.Is(err, ccrawl.ErrTransport) {
			p.otherThanTransport++
		}
		if page < 0 {
			p.crawls++
			fmt.Fprintf(os.Stderr, "%s: %s: %v, skipping the crawl\n", p.cmd, crawlID, err)
			return nil
		}
		p.pages++
		fmt.Fprintf(os.Stderr, "%s: %s: %v, skipping the page\n", p.cmd, crawlID, err)
		return nil
	}
}

// everyLossWasTransport reports whether the run lost something and every last
// bit of it was the bytes not arriving.
func (p *pageLosses) everyLossWasTransport() bool {
	return p.total() > 0 && p.otherThanTransport == 0
}

// total is how many pages and whole crawls the run gave up on.
func (p *pageLosses) total() int {
	if p == nil {
		return 0
	}
	return p.pages + p.crawls
}

// report prints what the run lost, once, after it has finished. It is the line
// that stops a partial result from passing for a whole one, so it says the
// result is incomplete in those words rather than leaving it to be inferred
// from the per page lines that scrolled past an hour ago.
func (p *pageLosses) report() {
	if p == nil || p.pages+p.crawls == 0 {
		return
	}
	var what string
	switch {
	case p.crawls == 0:
		what = plural(p.pages, "index page")
	case p.pages == 0:
		what = plural(p.crawls, "crawl")
	default:
		what = fmt.Sprintf("%s and %s", plural(p.pages, "index page"), plural(p.crawls, "crawl"))
	}
	fmt.Fprintf(os.Stderr, "%s: the result is incomplete, %s could not be read; run it again or pass --strict to fail instead\n", p.cmd, what)
}

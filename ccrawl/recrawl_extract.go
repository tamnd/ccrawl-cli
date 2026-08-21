package ccrawl

import (
	"strings"
	"time"
)

// Rendering the page while it is still in hand.
//
// The obvious way to get Markdown out of a recrawl is the way we already get it
// out of Common Crawl: publish the captures, then run a conversion pass over
// them. That pass is what markdown.go does and it is good at it. It is also the
// wrong shape here, for two reasons that are both about scale.
//
// The first is that a second pass re-reads the corpus. The URL work list is 6.3
// billion pages and the domain list is 363 million, so a pass over the captures
// is another few hundred terabytes off the network and back, to look at bodies
// that were in memory a moment ago the first time round.
//
// The second is that the recrawl has the CPU spare and knows it. The run is
// network bound and the timing says so out loud: at 256 workers against the live
// domain list a run reports fetching at 7 percent of the pool, writing at 0, and
// the rest idle. Extraction is the one thing in this pipeline that is neither
// waiting on a socket nor holding a lock, so it fits in the gap the fetches
// leave. That is only true while it stays out of the write mutex, which is why
// it happens here in the worker and not inside the sink.
//
// So the columns are filled on the way past. The names match open-markdown-v3
// where they overlap, deliberately, so somebody who has a query for that dataset
// can point it at a recrawl shard and have it run.

// pageExtractor renders captured pages, or is nil when the run was asked not to.
//
// It also carries the extractor ID, which is name@version, resolved once at
// construction rather than per page: it is the same string for every row in the
// run and reading a module version is not free.
type pageExtractor struct {
	ex *Extractor
	id string
}

// newPageExtractor resolves an extractor name into something that can fill the
// text columns. An empty name means the default engine.
func newPageExtractor(name, crawlID string) (*pageExtractor, error) {
	ex, err := LookupExtractor(name)
	if err != nil {
		return nil, err
	}
	return &pageExtractor{ex: ex, id: ex.ID(crawlID)}, nil
}

// fill renders one capture into the text columns.
//
// A page that is not HTML is left alone. Running an HTML extractor over a PDF or
// a JPEG produces a column full of mojibake that looks like text to a query and
// is not, and a corpus is better off with the column empty and the body still
// there for anybody who wants to do better later.
//
// A page that is HTML but yields nothing gets the extractor stamp anyway. That
// is the difference between "we did not try" and "we tried and there was
// nothing here", and a reader counting coverage needs to tell them apart.
func (p *pageExtractor) fill(c *Capture) {
	if p == nil || len(c.Body) == 0 || !isHTMLContent(c.ContentType) {
		return
	}
	c.Extractor = p.id

	md := p.ex.Convert(c.Body, c.URL)
	if md != "" {
		c.Markdown = md
		c.MarkdownLength = int64(len(md))
		// Fingerprint and language come off the Markdown rather than the body,
		// the same way the conversion pipeline does it, so the two producers
		// agree about what a page is. The body would fingerprint the navigation
		// and the ad slots along with the article.
		c.Simhash = Simhash(md)
		c.Language, c.LangConfidence = DetectLanguage(md)
	}

	tr := ExtractContent(c.Body)
	c.Title = tr.Title
	c.Text = tr.Body
	c.TextLength = int64(len(tr.Body))
	c.WordCount = int64(tr.WordCount)
	if c.Language == "" {
		// Nothing rendered, so fall back to what the document claims about
		// itself. It is weaker evidence than the detector and it is better than
		// an empty column.
		c.Language = tr.Language
	}
}

// isHTMLContent reports whether a Content-Type is worth handing to an HTML
// extractor. The parameters after the semicolon are the charset and friends and
// have no say in it.
func isHTMLContent(contentType string) bool {
	t := contentType
	if i := strings.IndexByte(t, ';'); i >= 0 {
		t = t[:i]
	}
	t = strings.ToLower(strings.TrimSpace(t))
	return t == "text/html" || t == "application/xhtml+xml" || t == ""
}

// captureRowSink is a sink that takes a row somebody else has built, so columns
// filled outside it survive.
//
// It is an assertion rather than a method on CaptureSink because only the
// Parquet sink has these columns. A WARC has nowhere to put Markdown, and a run
// writing WARC should not pay for rendering it would then throw away, so the
// assertion failing is also what turns extraction off.
type captureRowSink interface {
	Write(Capture) error
}

// writeCapture renders one fetch and hands it to the writer goroutine.
//
// The rendering stays here, in the worker, and does not travel with the job.
// That is the whole point of doing it on this side: rendering is the one phase
// that is CPU rather than network, so 256 workers doing it in the gaps their
// fetches leave is parallel, and one writer doing it is a queue with a hundred
// milliseconds of HTML parsing in it. What goes over the channel is a finished
// row.
//
// The write timer now covers the handoff rather than the sink. Usually that is
// nothing at all, and when it is not, it is the sink falling behind the pool and
// the queue backing up into the workers, which is exactly the moment worth
// seeing. The sink's own time is counted separately, on the timing line, because
// it belongs to one goroutine and not to the pool.
func (r *Recrawler) writeCapture(res *CrawlResult, seq int64) {
	j := writeJob{seq: seq}
	if r.rows != nil && r.ex != nil {
		start := time.Now()
		c := NewCapture(res)
		r.ex.fill(&c)
		r.timers.add(&r.timers.extract, start)
		j.row, j.rows = c, true
	} else {
		j.res = res
	}

	// A plain send, with no escape. The writer drains the queue even after a
	// write has failed, and the queue is closed only once the pool has stopped,
	// so there is no arrangement in which this blocks forever.
	start := time.Now()
	r.wq <- j
	r.timers.add(&r.timers.write, start)
}

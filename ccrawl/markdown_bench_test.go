package ccrawl

import (
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"net/url"

	"github.com/tamnd/yomi/extract"
	"github.com/tamnd/yomi/mdconv"
	"golang.org/x/net/html/charset"
)

// htmlToMarkdownReadability is the engine open-markdown-v2 originally shipped:
// yomi's go-readability extraction plus mdconv rendering. It is kept here only
// so TestEngineComparison can measure it head to head against the h2m
// (trafilatura, FavorRecall) engine that htmlToMarkdown now uses.
func htmlToMarkdownReadability(body []byte, pageURL string) string {
	if len(body) == 0 {
		return ""
	}
	r, err := charset.NewReader(strings.NewReader(string(body)), "")
	if err != nil {
		return ""
	}
	utf8Body, err := io.ReadAll(r)
	if err != nil || len(utf8Body) == 0 {
		return ""
	}
	art, err := extract.FromHTML(utf8Body, pageURL)
	if err != nil || art.Node == nil {
		return ""
	}
	base, _ := url.Parse(pageURL)
	md, err := mdconv.Convert(art.Node, mdconv.Options{Base: base})
	if err != nil {
		return ""
	}
	return strings.TrimSpace(md)
}

type engineRun struct {
	name    string
	rows    int
	htmlIn  int64 // html bytes of pages that produced output
	mdOut   int64
	elapsed time.Duration
}

func (e engineRun) ratio() float64 {
	if e.htmlIn == 0 {
		return 0
	}
	return float64(e.mdOut) / float64(e.htmlIn) * 100
}

// TestEngineComparison runs the same WARC shard through both conversion engines
// and prints a side-by-side quality and speed comparison. It is skipped unless
// CCRAWL_WARC points at a local .warc.gz file. Cap the record count with
// CCRAWL_LIMIT (default 5000) to keep a manual run quick.
//
//	CCRAWL_WARC=~/data/cc/shard0.warc.gz CCRAWL_LIMIT=20000 \
//	  go test ./ccrawl -run TestEngineComparison -v -timeout 30m
func TestEngineComparison(t *testing.T) {
	path := os.Getenv("CCRAWL_WARC")
	if path == "" {
		t.Skip("set CCRAWL_WARC to a local .warc.gz file to run the engine comparison")
	}
	limit := 5000
	if v := os.Getenv("CCRAWL_LIMIT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}

	type page struct {
		url  string
		html []byte
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open WARC: %v", err)
	}
	defer func() { _ = f.Close() }()

	var pages []page
	var totalHTML int64
	err = IterateWARC(f, func(rec WARCRecord) error {
		if len(pages) >= limit {
			return io.EOF
		}
		if rec.Header.Type != "response" || rec.Header.HTTPStatus != 200 {
			return nil
		}
		if !isHTMLMIME(rec.Header.HTTPMIME) {
			return nil
		}
		body := HTTPBody(rec.Block)
		if len(body) == 0 {
			return nil
		}
		b := make([]byte, len(body))
		copy(b, body)
		pages = append(pages, page{url: rec.Header.TargetURI, html: b})
		totalHTML += int64(len(b))
		return nil
	})
	if err != nil && err != io.EOF {
		t.Fatalf("iterate WARC: %v", err)
	}
	if len(pages) == 0 {
		t.Fatal("no HTML response records found in WARC")
	}

	engines := []struct {
		name string
		conv func([]byte, string) string
	}{
		{"h2m (trafilatura, FavorRecall)", htmlToMarkdown},
		{"v2 (yomi readability + mdconv)", htmlToMarkdownReadability},
	}

	var runs []engineRun
	for _, eng := range engines {
		run := engineRun{name: eng.name}
		t0 := time.Now()
		for _, p := range pages {
			md := eng.conv(p.html, p.url)
			if md == "" {
				continue
			}
			run.rows++
			run.htmlIn += int64(len(p.html))
			run.mdOut += int64(len(md))
		}
		run.elapsed = time.Since(t0)
		runs = append(runs, run)
	}

	t.Logf("WARC: %s", path)
	t.Logf("HTML response records read: %d (%0.1f MB raw)", len(pages), float64(totalHTML)/1e6)
	t.Logf("%-34s %8s %10s %10s %8s %9s", "engine", "rows", "html MB", "md MB", "md/html", "elapsed")
	for _, r := range runs {
		t.Logf("%-34s %8d %10.1f %10.1f %7.1f%% %9s",
			r.name, r.rows, float64(r.htmlIn)/1e6, float64(r.mdOut)/1e6, r.ratio(),
			r.elapsed.Round(time.Millisecond))
	}
	if len(runs) == 2 {
		base, cmp := runs[1], runs[0] // v2 baseline vs h2m
		t.Logf("h2m vs v2: rows %+.1f%%, md bytes %+.1f%%",
			pctDelta(base.rows, cmp.rows), pctDelta64(base.mdOut, cmp.mdOut))
	}

	// CCRAWL_DUMP=N writes the first N pages' output from both engines to a
	// directory so the text quality can be compared side by side.
	if n, _ := strconv.Atoi(os.Getenv("CCRAWL_DUMP")); n > 0 {
		dir := os.Getenv("CCRAWL_DUMP_DIR")
		if dir == "" {
			dir = "/tmp/h2m-compare"
		}
		_ = os.MkdirAll(dir, 0o755)
		dumped := 0
		for i, p := range pages {
			if dumped >= n {
				break
			}
			a := htmlToMarkdown(p.html, p.url)
			b := htmlToMarkdownReadability(p.html, p.url)
			if a == "" || b == "" {
				continue
			}
			_ = os.WriteFile(dir+"/"+strconv.Itoa(i)+".h2m.md", []byte("URL: "+p.url+"\n\n"+a), 0o644)
			_ = os.WriteFile(dir+"/"+strconv.Itoa(i)+".v2.md", []byte("URL: "+p.url+"\n\n"+b), 0o644)
			dumped++
		}
		t.Logf("dumped %d page pairs to %s", dumped, dir)
	}
}

func pctDelta(base, cmp int) float64 {
	if base == 0 {
		return 0
	}
	return (float64(cmp) - float64(base)) / float64(base) * 100
}

func pctDelta64(base, cmp int64) float64 {
	if base == 0 {
		return 0
	}
	return (float64(cmp) - float64(base)) / float64(base) * 100
}

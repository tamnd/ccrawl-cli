package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// closeWith closes c and returns the first non-nil of the existing error or the
// close error, so a write failure is never masked by a successful close and a
// close failure is never silently dropped.
func closeWith(c io.Closer, err error) error {
	if cerr := c.Close(); cerr != nil && err == nil {
		return cerr
	}
	return err
}

func newConvertCmd() *cobra.Command {
	var to string
	var outPath string
	var markdown bool

	cmd := &cobra.Command{
		Use:   "convert <file|dir>",
		Short: "Convert WARC/WAT/WET archives to Parquet or JSONL",
		Long: `Convert one archive file or a directory of them to columnar Parquet (zstd,
dictionary-encoded) or JSONL. WARC response bodies can be converted to Markdown
inline with --markdown.

Examples:
  ccrawl convert file.warc.wet.gz --to jsonl
  ccrawl convert file.warc.gz --to parquet -O out.parquet --markdown
  ccrawl convert ./warc/ --to parquet --out ./parquet
  ccrawl convert wet --library --to parquet   process the library's WET files

With --library the argument is a kind: ccrawl reads every archive of that kind
from the library and writes the output under <crawl>/<format>/<kind>/, beside
the raw files.`,
		Args: cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			app := appFromCtx(c.Context())
			return runConvert(app, c, args[0], to, outPath, markdown)
		},
	}
	cmd.Flags().StringVar(&to, "to", "parquet", "output format: parquet|jsonl")
	cmd.Flags().StringVarP(&outPath, "out", "O", "", "output file (single input) or directory")
	cmd.Flags().BoolVar(&markdown, "markdown", false, "convert WARC HTML bodies to Markdown")
	return cmd
}

func runConvert(app *App, c *cobra.Command, input, to, outPath string, markdown bool) error {
	if app.UseLibrary {
		// In library mode the argument is a kind: read every archive of that kind
		// from the library and write the processed output back into the library
		// under <crawl>/<format>/<kind>/, beside the raw files.
		lib, err := app.Library(c.Context())
		if err != nil {
			return err
		}
		kind := input
		input = lib.RawDir(kind)
		if outPath == "" {
			outPath = lib.ProcessedDir(to, kind)
		}
	}
	info, err := os.Stat(input)
	if err != nil {
		return err
	}
	var files []string
	if info.IsDir() {
		entries, _ := os.ReadDir(input)
		for _, e := range entries {
			if strings.Contains(e.Name(), ".warc") {
				files = append(files, filepath.Join(input, e.Name()))
			}
		}
	} else {
		files = []string{input}
	}
	if len(files) == 0 {
		return noResults("no archive files found")
	}

	for _, f := range files {
		out := outPath
		if out == "" || info.IsDir() {
			base := strings.TrimSuffix(filepath.Base(f), ".gz")
			dir := outPath
			if dir == "" {
				dir = "."
			}
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return err
			}
			out = filepath.Join(dir, base+"."+to)
		}
		if err := convertOne(app, c, f, out, to, markdown); err != nil {
			return err
		}
		_, _ = fmt.Fprintln(cmdErr, "wrote "+out)
	}
	return nil
}

func convertOne(app *App, c *cobra.Command, in, out, to string, markdown bool) error {
	r, name, closeFn, err := openInput(in)
	if err != nil {
		return err
	}
	defer closeFn()
	format := detectFormat(name)
	id, _ := app.Crawl(c.Context())

	if to == "jsonl" {
		return convertJSONL(r, format, id, out)
	}
	return convertParquet(r, format, id, out, markdown)
}

func convertJSONL(r interface{ Read([]byte) (int, error) }, format, id, out string) (err error) {
	f, err := os.Create(out)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	enc := json.NewEncoder(f)
	switch format {
	case "wat":
		return ccrawl.IterateWAT(r, id, func(w ccrawl.WATRecord) error { return enc.Encode(w) })
	case "wet":
		return ccrawl.IterateWET(r, id, func(w ccrawl.WETRecord) error { return enc.Encode(w) })
	default:
		return ccrawl.IterateWARC(r, func(rec ccrawl.WARCRecord) error {
			return enc.Encode(map[string]any{
				"type": rec.Header.Type, "url": rec.Header.TargetURI,
				"status": rec.Header.HTTPStatus, "mime": rec.Header.HTTPMIME,
				"date": rec.Header.Date, "payload_digest": rec.Header.PayloadDigest,
			})
		})
	}
}

func convertParquet(r interface{ Read([]byte) (int, error) }, format, id, out string, markdown bool) error {
	switch format {
	case "wet":
		w, err := ccrawl.NewParquetWriter[ccrawl.WETParquetRow](out)
		if err != nil {
			return err
		}
		werr := ccrawl.IterateWET(r, id, func(rec ccrawl.WETRecord) error {
			return w.Write(ccrawl.WETParquetRow{
				RecordID: rec.RecordID, CrawlID: rec.CrawlID, URL: rec.URL,
				Date: rec.Date, ContentLanguage: rec.ContentLanguage,
				TextLength: int32(len([]rune(rec.Text))), Text: rec.Text,
			})
		})
		return closeWith(w, werr)
	case "wat":
		w, err := ccrawl.NewParquetWriter[ccrawl.WATParquetRow](out)
		if err != nil {
			return err
		}
		werr := ccrawl.IterateWAT(r, id, func(rec ccrawl.WATRecord) error {
			links, _ := json.Marshal(rec.Links)
			metas, _ := json.Marshal(rec.Metas)
			return w.Write(ccrawl.WATParquetRow{
				RecordID: rec.RecordID, CrawlID: rec.CrawlID, URL: rec.URL, Date: rec.Date,
				HTTPStatus: int32(rec.HTTPStatus), ContentType: rec.ContentType, Title: rec.Title,
				LinksCount: int32(rec.LinksCount), Links: string(links), Metas: string(metas),
			})
		})
		return closeWith(w, werr)
	default:
		w, err := ccrawl.NewParquetWriter[ccrawl.WARCParquetRow](out)
		if err != nil {
			return err
		}
		werr := ccrawl.IterateWARC(r, func(rec ccrawl.WARCRecord) error {
			h := rec.Header
			row := ccrawl.WARCParquetRow{
				RecordID: h.RecordID, CrawlID: id, WARCType: h.Type, TargetURI: h.TargetURI,
				Date: h.Date, IPAddress: h.IPAddress, PayloadDigest: h.PayloadDigest,
				ContentType: h.ContentType, ContentLength: h.ContentLength, Truncated: h.Truncated,
				HTTPStatus: int32(h.HTTPStatus), HTTPMIME: h.HTTPMIME, Language: h.Language,
			}
			if markdown && h.Type == "response" && strings.Contains(h.HTTPMIME, "html") {
				body := ccrawl.HTTPBody(rec.Block)
				row.Title = ccrawl.ExtractTitle(body)
				md, _ := ccrawl.ExtractMarkdown(body)
				row.Markdown = md
			}
			return w.Write(row)
		})
		return closeWith(w, werr)
	}
}

package cli

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// registerSeed attaches the seed command group. It turns a CDX URL-index shard
// into a plain seed (a list of URLs with a digest and a few hints) that any
// recrawler can consume. The output carries nothing specific to its origin: it
// is just url, digest, host, mime, lang, status, and time columns.
func registerSeed(app *kit.App) {
	app.CommandGroup("seed", "Export a recrawl seed from a CDX URL-index shard")
	app.AddCommandUnder("seed", newSeedExportCmd())
	app.AddCommandUnder("seed", newSeedCCCmd())
	app.AddCommandUnder("seed", newSeedPublishCmd())
}

// seedExportCmd holds the flags for `ccrawl seed export`.
type seedExportCmd struct {
	format  string
	outPath string
	status  int
	mime    string
	lang    string
	limit   int64
}

func newSeedExportCmd() kit.Command {
	v := &seedExportCmd{}
	return kit.Command{
		Use:   "export <shard.parquet|url>",
		Short: "Project a CDX URL-index shard into a generic recrawl seed",
		Long: `Read one CDX URL-index Parquet shard and write a seed: one row per capture
with the URL, its content digest, and a few hints (host, MIME, language,
status, time). The shard argument may be a local Parquet file or an http(s)
URL, which is downloaded first.

By default only captures with fetch_status 200 are kept, since a recrawl
usually starts from the URLs that resolved before. The output is generic: it
names nothing about where the list came from, so a downstream crawler reads
"url" and "digest" and treats the rest as opaque metadata.

Examples:
  ccrawl seed export shard.parquet -O seed.parquet
  ccrawl seed export shard.parquet --to jsonl -O seed.jsonl.gz
  ccrawl seed export shard.parquet --to lines --mime text/html -O urls.txt
  ccrawl seed export https://host/path/shard.parquet -O seed.parquet --limit 1000000`,
		Args:  kit.ExactArgs(1),
		Flags: v.flags,
		Run:   v.run,
	}
}

func (v *seedExportCmd) flags(f *kit.FlagSet) {
	f.StringVar(&v.format, "to", "parquet", "output format: parquet|jsonl|lines")
	f.StringVarP(&v.outPath, "out", "O", "", "output file (default: stdout for lines/jsonl, required for parquet)")
	f.IntVar(&v.status, "status", 200, "keep only this fetch_status (0 = any)")
	f.StringVar(&v.mime, "mime", "", "keep only captures whose detected MIME contains this")
	f.StringVar(&v.lang, "lang", "", "keep only captures whose languages contain this")
	f.Int64Var(&v.limit, "limit", 0, "stop after this many rows (0 = all)")
}

func (v *seedExportCmd) run(ctx context.Context, args []string) error {
	app := appFromCtx(ctx)

	shard := args[0]
	if strings.HasPrefix(shard, "http://") || strings.HasPrefix(shard, "https://") {
		tmp, err := os.CreateTemp("", "ccrawl-seed-*.parquet")
		if err != nil {
			return err
		}
		tmpPath := tmp.Name()
		_ = tmp.Close()
		defer func() { _ = os.Remove(tmpPath) }()
		fmt.Fprintf(os.Stderr, "downloading shard %s\n", shard)
		if err := ccrawl.DownloadToFile(ctx, app.HTTP, shard, tmpPath); err != nil {
			return fmt.Errorf("download shard: %w", err)
		}
		shard = tmpPath
	}

	opt := ccrawl.SeedExportOptions{
		Status: int32(v.status),
		MIME:   v.mime,
		Lang:   v.lang,
		Limit:  v.limit,
	}

	write, closeFn, err := v.openSink()
	if err != nil {
		return err
	}

	st, exportErr := ccrawl.ExportSeedFromCDX(ctx, shard, opt, write)
	if cerr := closeFn(); cerr != nil && exportErr == nil {
		exportErr = cerr
	}
	if exportErr != nil {
		return exportErr
	}
	fmt.Fprintf(os.Stderr, "seed: scanned %d captures, wrote %d rows\n", st.Scanned, st.Written)
	return nil
}

// openSink builds the row writer for the chosen format and returns a close func
// that flushes and closes any underlying file.
func (v *seedExportCmd) openSink() (write func(ccrawl.SeedRow) error, closeFn func() error, err error) {
	switch v.format {
	case "parquet":
		if v.outPath == "" {
			return nil, nil, fmt.Errorf("parquet output needs --out/-O")
		}
		if err := os.MkdirAll(filepath.Dir(v.outPath), 0o755); err != nil {
			return nil, nil, err
		}
		w, err := ccrawl.NewParquetWriter[ccrawl.SeedRow](v.outPath)
		if err != nil {
			return nil, nil, err
		}
		return w.Write, w.Close, nil

	case "jsonl":
		out, closeOut, err := openSeedFile(v.outPath)
		if err != nil {
			return nil, nil, err
		}
		var gz *gzip.Writer
		enc := json.NewEncoder(out)
		if strings.HasSuffix(v.outPath, ".gz") {
			gz = gzip.NewWriter(out)
			enc = json.NewEncoder(gz)
		}
		write = func(r ccrawl.SeedRow) error { return enc.Encode(r) }
		closeFn = func() error {
			if gz != nil {
				if err := gz.Close(); err != nil {
					_ = closeOut()
					return err
				}
			}
			return closeOut()
		}
		return write, closeFn, nil

	case "lines":
		out, closeOut, err := openSeedFile(v.outPath)
		if err != nil {
			return nil, nil, err
		}
		write = func(r ccrawl.SeedRow) error {
			_, werr := fmt.Fprintln(out, r.URL)
			return werr
		}
		return write, closeOut, nil

	default:
		return nil, nil, fmt.Errorf("unknown format %q (want parquet, jsonl, or lines)", v.format)
	}
}

// openSeedFile opens path for writing, or returns stdout (with a no-op close)
// when path is empty.
func openSeedFile(path string) (*os.File, func() error, error) {
	if path == "" {
		return os.Stdout, func() error { return nil }, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, nil, err
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	return f, f.Close, nil
}

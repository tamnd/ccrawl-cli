package cli

import (
	"context"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func newConfigCmd() kit.Command {
	return kit.Command{
		Use:   "config",
		Short: "Show resolved configuration and data paths",
		Long:  "Print where ccrawl reads and writes, and the effective client settings, so you can see exactly what a run will do.",
		Sub: []kit.Command{{
			Use:   "show",
			Short: "Print the effective configuration",
			Run:   runConfigShow,
		}},
	}
}

func runConfigShow(ctx context.Context, _ []string) error {
	app := appFromCtx(ctx)
	cfg := app.Cfg
	rows := [][2]string{
		{"crawl", cfg.CrawlID},
		{"source", string(cfg.Source)},
		{"data_dir", cfg.DataDir},
		{"cache_dir", cfg.CacheDir},
		{"config_dir", ccrawl.ConfigDir()},
		{"raw_dir", cfg.RawDir()},
		{"parquet_dir", cfg.ParquetDir()},
		{"library_dir", app.LibraryDir},
		{"db_path", cfg.DBPath},
		{"workers", itoa(cfg.Workers)},
		{"rate", cfg.Delay.String()},
		{"timeout", cfg.Timeout.String()},
		{"retries", itoa(cfg.Retries)},
		{"user_agent", cfg.UserAgent},
		{"duckdb", boolWord(ccrawl.DuckDBAvailable())},
	}
	for _, r := range rows {
		if err := app.Out.Emit(Row{
			Cols:  []string{"key", "value"},
			Vals:  []string{r[0], r[1]},
			Value: map[string]any{"key": r[0], "value": r[1]},
		}); err != nil {
			return err
		}
	}
	return app.Out.Flush()
}

func boolWord(b bool) string {
	if b {
		return "available"
	}
	return "not found"
}

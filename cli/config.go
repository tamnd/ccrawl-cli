package cli

import (
	"github.com/spf13/cobra"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func newConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Show resolved configuration and data paths",
		Long:  "Print where ccrawl reads and writes, and the effective client settings, so you can see exactly what a run will do.",
	}
	cmd.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "Print the effective configuration",
		RunE: func(c *cobra.Command, _ []string) error {
			app := appFromCtx(c.Context())
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
		},
	})
	return cmd
}

func boolWord(b bool) string {
	if b {
		return "available"
	}
	return "not found"
}

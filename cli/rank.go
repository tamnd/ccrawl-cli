package cli

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

func newRankCmd(app *App) *cobra.Command {
	var rankURL string
	cmd := &cobra.Command{
		Use:   "rank",
		Short: "Look up host and domain ranks from the web graph",
		Long: `Look up harmonic-centrality and PageRank positions from Common Crawl's
web-graph rank tables.

Rank tables are large and their exact URL changes per web-graph release, so pass
the gzipped table URL with --table. The lookup streams it once and caches the
answer.

Examples:
  ccrawl rank domain example.com --table https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-feb-mar-may/domain/cc-main-2024-feb-mar-may-domain-ranks.txt.gz
  ccrawl rank top --tld gov --table <url> -n 100`,
	}
	cmd.PersistentFlags().StringVar(&rankURL, "table", "", "URL of a gzipped rank table")

	cmd.AddCommand(
		&cobra.Command{
			Use:   "domain <domain>",
			Short: "Rank of a registered domain",
			Args:  cobra.ExactArgs(1),
			RunE: func(c *cobra.Command, args []string) error {
				return runRankLookup(app, c, rankURL, args[0])
			},
		},
		&cobra.Command{
			Use:   "host <host>",
			Short: "Rank of a host",
			Args:  cobra.ExactArgs(1),
			RunE: func(c *cobra.Command, args []string) error {
				return runRankLookup(app, c, rankURL, args[0])
			},
		},
	)

	topCmd := &cobra.Command{
		Use:   "top",
		Short: "Top-ranked hosts or domains",
		RunE: func(c *cobra.Command, _ []string) error {
			if rankURL == "" {
				return usageErr("--table is required (URL of a gzipped rank table)")
			}
			n := app.Limit
			if n == 0 {
				n = 50
			}
			ranks, err := ccrawl.RankTop(c.Context(), app.HTTP, rankURL, topTLD, n)
			if err != nil {
				return err
			}
			for _, r := range ranks {
				if err := app.Out.Emit(rankRow(r)); err != nil {
					return err
				}
			}
			return app.Out.Flush()
		},
	}
	topCmd.Flags().StringVar(&topTLD, "tld", "", "restrict to a TLD")
	cmd.AddCommand(topCmd)
	return cmd
}

var topTLD string

func runRankLookup(app *App, c *cobra.Command, url, key string) error {
	if url == "" {
		return usageErr("--table is required (URL of a gzipped rank table)")
	}
	r, err := ccrawl.RankLookup(c.Context(), app.HTTP, url, key)
	if err != nil {
		return noResults(err.Error())
	}
	if err := app.Out.Emit(rankRow(r)); err != nil {
		return err
	}
	return app.Out.Flush()
}

func rankRow(r ccrawl.Rank) Row {
	return Row{
		Cols:  []string{"key", "harmonic_pos", "harmonic_val", "pagerank_pos", "pagerank_val"},
		Vals:  []string{r.Key, itoa64(r.HarmonicPos), fmt.Sprintf("%g", r.HarmonicVal), itoa64(r.PageRankPos), fmt.Sprintf("%g", r.PageRankVal)},
		Value: r,
	}
}

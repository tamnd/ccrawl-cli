package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// dedupCmd holds the flags for the dedup command.
type dedupCmd struct {
	distance int
	top      int
	asJSON   bool
}

func newDedupCmd() kit.Command {
	v := &dedupCmd{}
	return kit.Command{
		Use:   "dedup <parquet-file|dir>...",
		Short: "Report exact and near duplicate documents in a Parquet corpus",
		Long: `Read a Parquet file or a directory of them and report how much of the corpus is
redundant. Exact duplicates are documents whose Markdown is byte identical. Near
duplicates are documents whose simhash fingerprints differ in at most --distance
bits, which is the same article with different navigation, a changed date line,
or one extra paragraph.

This reports, it does not rewrite. Which copy of a page to keep is a decision
about what the corpus is for, so dedup gives you the numbers and the clusters and
leaves the deleting to a query you write. To drop byte identical payloads while
building instead, pass --dedup-digest to markdown export or markdown refetch.

Only the url, markdown, and simhash columns are read, so this is cheap on a
dataset with eleven columns. Files written before the simhash column existed are
fingerprinted while they are read, so the report works on any vintage.

  ccrawl dedup ./parquet/
  ccrawl dedup shard-000.parquet --distance 6 --top 40
  ccrawl dedup ./parquet/ --json | jq '.near_duplicates'`,
		Args:  kit.MinimumNArgs(1),
		Flags: v.flags,
		Run:   v.run,
	}
}

func (v *dedupCmd) flags(f *kit.FlagSet) {
	f.IntVar(&v.distance, "distance", ccrawl.DefaultNearDistance, "max simhash bit distance for a near duplicate (0 to 64)")
	f.IntVar(&v.top, "top", 10, "how many of the largest clusters to print (0 for all)")
	f.BoolVar(&v.asJSON, "json", false, "print the report as JSON")
}

func (v *dedupCmd) run(ctx context.Context, args []string) error {
	if v.distance < 0 || v.distance > 64 {
		return usageErr(fmt.Sprintf("distance must be between 0 and 64, got %d", v.distance))
	}
	rep, err := ccrawl.AnalyzeDedup(args, v.distance, v.top)
	if err != nil {
		return err
	}
	if v.asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	}
	fmt.Print(rep.Summary())
	return nil
}

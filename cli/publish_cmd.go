package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// obsoleteRepos are the first-generation dataset repos superseded by
// open-index/ccrawl-urls and open-index/ccrawl-domains. delete-obsolete removes
// them so the account does not carry stale, fragmented datasets.
var obsoleteRepos = []string{
	"open-index/cc-host-dataset",
	"open-index/commoncrawl-urls",
}

// registerPublish attaches the `publish` command group for cross-dataset
// maintenance that does not belong to a single dataset.
func registerPublish(app *kit.App) {
	app.CommandGroup("publish", "Maintenance for the published Common Crawl datasets")
	app.AddCommandUnder("publish", newDeleteObsoleteCmd())
}

type deleteObsoleteCmd struct {
	yes bool
}

func newDeleteObsoleteCmd() kit.Command {
	v := &deleteObsoleteCmd{}
	return kit.Command{
		Use:   "delete-obsolete",
		Short: "Delete the superseded first-generation dataset repos",
		Long: `Delete the obsolete dataset repos that the ccrawl-urls and ccrawl-domains
datasets replace:

  open-index/cc-host-dataset
  open-index/commoncrawl-urls

This is irreversible and removes the repos and all their data on HuggingFace.
It requires --yes to run. HF_TOKEN (or HUGGINGFACE_TOKEN) must be set.`,
		Args:  kit.NoArgs,
		Flags: v.flags,
		Run:   v.run,
	}
}

func (v *deleteObsoleteCmd) flags(f *kit.FlagSet) {
	f.BoolVar(&v.yes, "yes", false, "confirm the irreversible deletion")
}

func (v *deleteObsoleteCmd) run(ctx context.Context, args []string) error {
	if !v.yes {
		return usageErr("this deletes repos permanently; pass --yes to confirm")
	}
	hf := ccrawl.NewHFClient("")
	if !hf.Valid() {
		return fmt.Errorf("HF_TOKEN (or HUGGINGFACE_TOKEN) is not set")
	}
	for _, repo := range obsoleteRepos {
		if err := hf.DeleteDatasetRepo(ctx, repo); err != nil {
			return fmt.Errorf("delete %s: %w", repo, err)
		}
		fmt.Fprintf(os.Stderr, "deleted https://huggingface.co/datasets/%s\n", repo)
	}
	return nil
}

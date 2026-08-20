package cli

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tamnd/any-cli/kit"
	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// recrawlRepos are the two datasets a recrawl publishes into. They are separate
// repos because a domain recrawl and a URL recrawl finish on completely
// different schedules and nobody wants a card that averages the two.
var recrawlRepos = map[string]string{
	"domains": "open-index/ccrawl-recrawl-domains",
	"urls":    "open-index/ccrawl-recrawl-urls",
}

type recrawlPublishIn struct {
	App         *App          `kit:"inject"`
	Dir         string        `kit:"flag" help:"capture directory to publish from, the same one recrawl run writes into"`
	Repo        string        `kit:"flag" help:"dataset repo to publish to (default: the open-index repo for --kind)"`
	Kind        string        `kit:"flag" default:"domains" help:"which recrawl this is: domains or urls"`
	Server      string        `kit:"flag" help:"name of this machine, which is what keeps its shards and ledger apart from the rest of the fleet"`
	Shard       int           `kit:"flag" help:"which partition of the work list this machine took, 0-based"`
	Shards      int           `kit:"flag" default:"1" help:"how many machines are splitting the work list"`
	State       string        `kit:"flag" help:"the crawl's checkpoint file, read so the card can report progress"`
	CommitEvery int           `kit:"flag,name=commit-every" default:"4" help:"shards per HuggingFace commit"`
	Watch       time.Duration `kit:"flag" help:"keep watching the directory and publish shards as they close (0 = one pass)"`
	Keep        bool          `kit:"flag" help:"keep local shards after commit instead of deleting them"`
	Private     bool          `kit:"flag" help:"create the dataset repo private"`
	NoPush      bool          `kit:"flag,name=no-push" help:"stage and report but skip the upload"`
}

func registerRecrawlPublish(app *kit.App) {
	handle(app, kit.OpMeta{
		Name:    "publish",
		Parent:  "recrawl",
		Summary: "Publish closed capture shards to a HuggingFace dataset as they land",
		Long: `Watch a recrawl's output directory and commit each shard to a dataset repo as it
closes, refreshing the ledger and the dataset card in the same commit, then
deleting the local file. Run it alongside recrawl run, not after it, so disk
stays flat and the dataset is readable from the first hour of a crawl that takes
months.

A shard still being written is named .parquet.tmp and is renamed only once its
footer is on disk, so this command can never pick up a half-written file.

Each machine writes exactly one ledger file, ledger/<server>-shard<i>of<n>.csv,
and never touches another's, so three machines committing at the same moment
cannot lose each other's numbers. The card is generated from the union of every
ledger file on the hub, so it corrects itself on the next commit from any
machine.

Shards are named by a hash of their contents, which makes republishing one a
no-op instead of a duplicate. Resume asks the hub what is already published
rather than trusting local state, so a wiped staging directory does not restart
the numbering.

HF_TOKEN (or HUGGINGFACE_TOKEN) must be set to push. Examples:
  ccrawl recrawl publish --dir captures/ --kind domains --server server1 --shard 0 --shards 3 --watch 30s
  ccrawl recrawl publish --dir captures/ --kind urls --server server2 --shard 1 --shards 3 --state recrawl.json
  ccrawl recrawl publish --dir captures/ --no-push   # rehearse, upload nothing`,
	}, func(ctx context.Context, in recrawlPublishIn, emit func(ccrawl.RecrawlStat) error) error {
		kind := strings.ToLower(strings.TrimSpace(in.Kind))
		repo := in.Repo
		if repo == "" {
			r, ok := recrawlRepos[kind]
			if !ok {
				return usageErr(fmt.Sprintf("--kind %s is neither domains nor urls, so name the dataset with --repo", in.Kind))
			}
			repo = r
		}
		server := in.Server
		if server == "" {
			// Falling back to the hostname is right for a fleet of named boxes and
			// wrong for three containers that all think they are the same machine,
			// so say which one was picked rather than picking it silently.
			h, err := os.Hostname()
			if err != nil || h == "" {
				return usageErr("name this machine with --server, this host has no usable name")
			}
			server = h
			fmt.Fprintf(os.Stderr, "recrawl publish: publishing as %s, pass --server to override\n", server)
		}

		push := !in.NoPush && !in.App.dryRun
		hf := ccrawl.NewHFClient("")
		cfg := ccrawl.RecrawlPublishConfig{
			Dir:         in.Dir,
			Repo:        repo,
			Kind:        kind,
			Server:      server,
			Shard:       in.Shard,
			Shards:      in.Shards,
			StatePath:   in.State,
			CommitEvery: in.CommitEvery,
			Poll:        in.Watch,
			Keep:        in.Keep,
			DoCommit:    push,
			Private:     in.Private,
		}
		if err := cfg.Validate(); err != nil {
			return usageErr(err.Error())
		}
		if push {
			fmt.Fprintf(os.Stderr, "recrawl publish: %s to https://huggingface.co/datasets/%s\n", in.Dir, repo)
		} else {
			fmt.Fprintf(os.Stderr, "recrawl publish: rehearsing %s against %s, uploading nothing\n", in.Dir, repo)
		}

		stat, err := ccrawl.PublishRecrawl(ctx, hf, cfg)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "recrawl publish: %s has %d shards, %d pages, %s on the hub, at part %d row %d\n",
			stat.Server, stat.Files, stat.Rows, humanBytes(stat.Bytes), stat.Part, stat.Row)
		return emit(stat)
	})
}

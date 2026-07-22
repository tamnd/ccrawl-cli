package cli

import "github.com/tamnd/any-cli/kit"

// registerEscapeHatches attaches the commands that do not fit the emit-records
// shape of a kit operation: byte fetches, the columnar DuckDB console, archive
// parsing, dataset conversion, and the cache and config utilities. The crawls
// and news groups host a kit operation (list) alongside these, so their extra
// verbs attach under the same parent with AddCommandUnder.
func registerEscapeHatches(app *kit.App) {
	app.CommandGroup("crawls", "Discover Common Crawl collections")
	app.CommandGroup("news", "Work with the CC-NEWS dataset")
	for _, c := range crawlsEscapeHatches() {
		app.AddCommandUnder("crawls", c)
	}
	for _, c := range newsEscapeHatches() {
		app.AddCommandUnder("news", c)
	}

	registerMarkdown(app)
	registerURLs(app)
	registerDomains(app)
	registerPublish(app)

	app.AddCommand(newGetCmd())
	app.AddCommand(newFetchCmd())
	app.AddCommand(newExportCmd())
	app.AddCommand(newDownloadCmd())
	app.AddCommand(newPathsCmd())
	app.AddCommand(newParseCmd())
	app.AddCommand(newExtractCmd())
	app.AddCommand(newTableCmd())
	app.AddCommand(newDBCmd())
	app.AddCommand(newConvertCmd())
	app.AddCommand(newConfigCmd())
	app.AddCommand(newCacheCmd())
	app.AddCommand(newVersionCmd())
}

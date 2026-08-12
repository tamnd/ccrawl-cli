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
		addCmdUnder(app, "crawls", c)
	}
	for _, c := range newsEscapeHatches() {
		addCmdUnder(app, "news", c)
	}

	registerMarkdown(app)
	registerURLs(app)
	registerDomains(app)
	registerPublish(app)

	addCmd(app, newGetCmd())
	addCmd(app, newFetchCmd())
	addCmd(app, newExportCmd())
	addCmd(app, newDownloadCmd())
	addCmd(app, newPathsCmd())
	addCmd(app, newParseCmd())
	addCmd(app, newExtractCmd())
	addCmd(app, newTableCmd())
	addCmd(app, newDBCmd())
	addCmd(app, newConvertCmd())
	addCmd(app, newDedupCmd())
	addCmd(app, newLibraryCmd())
	addCmd(app, newConfigCmd())
	addCmd(app, newCacheCmd())
	addCmd(app, newVersionCmd())
}

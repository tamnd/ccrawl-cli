---
title: "CLI"
description: "Every command and subcommand, with the flags that matter."
weight: 10
---

```
ccrawl <command> [subcommand] [flags]
```

Run `ccrawl <command> --help` for the full flag list on any command.

## Commands

| Command | What it does |
|---|---|
| `crawls` | List, resolve, and inspect the monthly crawls |
| `search` | Query the URL index (CDX) for captures of a URL or pattern |
| `get` | Fetch what Common Crawl captured for a URL |
| `fetch` | Retrieve WARC records by explicit location, or from stdin |
| `export` | Write matching captures into WARC files with provenance |
| `download` | Download whole archive files for a crawl |
| `paths` | List the archive file paths for a crawl |
| `parse` | Decode a local WARC/WAT/WET file into records |
| `extract` | Pull text, links, title, or Markdown from a captured page |
| `content` | Live-fetch content signals: text, outlinks, quality |
| `news` | Work with the continuous CC-NEWS dataset |
| `columnar` | Query the columnar Parquet index |
| `markdown` | Build Markdown Parquet datasets from CC WARCs and publish them |
| `rank` | Look up host and domain ranks from the web graph |
| `host` | Enumerate and enrich hosts from the CC web graph |
| `urls` | Mirror the Common Crawl URL index to a HuggingFace dataset |
| `domains` | Mirror the Common Crawl domain ranks to a HuggingFace dataset |
| `publish` | Maintenance for the published Common Crawl datasets |
| `crawl` | Recrawl engine: seed, fetch, and write WARC output |
| `sched` | Recrawl scheduling: tier assignment and differential CDX analysis |
| `index` | Build and query a local BM25 full-text search index |
| `api` | Start the v2 REST API server |
| `db` | Build and query a local DuckDB database |
| `convert` | Convert WARC/WAT/WET archives to Parquet or JSONL |
| `library` | Inspect, verify, and collect the dataset library |
| `dedup` | Report exact and near duplicate documents in a Markdown Parquet dataset |
| `stats` | Show the shape of a crawl: file counts per archive kind |
| `serve` | Serve the operations over HTTP as NDJSON |
| `mcp` | Run as an MCP server over stdio |
| `config` | Show resolved configuration and data paths |
| `cache` | Inspect and clear the on-disk cache |
| `version` | Print the version and exit |

---

## crawls

| Subcommand | Does |
|---|---|
| `crawls list` | List the monthly crawls, newest first |
| `crawls latest` | Print the newest crawl ID |
| `crawls resolve <ref>` | Resolve a year or `latest` to a crawl ID |
| `crawls info [id]` | File counts per archive kind for a crawl |

`crawls info` and `stats` are the same command under two names, so they count the same kinds and answer with the same rows: `crawl`, `kind`, `files`.
Both take `--kinds` to narrow the list, and both honour `-o`, so `crawls info -o csv` writes CSV the way every other read command does.
The one difference is how you name the crawl: `crawls info` takes it as a positional argument, `stats` reads `-c`, and either one falls back to the configured crawl when you leave it out.
A kind whose manifest could not be fetched comes back with `files` of `-1` rather than being dropped, so a row is never silently missing.

---

## search

```
ccrawl search <url|pattern> [flags]
```

A trailing `/*` matches everything under a path.
Filters: `--mime`, `--status`, `--from`, `--to`, `--filter`.
URL filters: `--url-contains`, `--url-not-contains`.
Pick the capture closest to a date with `--at` (for example `--at 2023-06`).
Order with `--sort newest|oldest`.
Estimate the size of a result instead of listing it with `--estimate`.
Shaping: `--fields`, `--template`, `-o`, `-n`.
Alias: `cdx`.

### Memory over a wide query

`--at`, `--latest-only` and `--dedup` each have to remember something about every URL the query touches, and a wildcard over a large domain across every crawl touches hundreds of millions of them.
The three of them share one budget, `--max-buffer`, which is how many records they hold in memory before the run starts writing to a temporary file in `TMPDIR` instead.
The default is 5,000,000 records, which is a few hundred megabytes, and the temporary files are removed when the command exits however it exits.

| Flag | Description |
| --- | --- |
| `--max-buffer` | Records `--at`, `--latest-only` and `--dedup` hold in memory before spilling to disk (default 5000000) |

A CDX response is sorted by urlkey and every capture of a URL sits in one urlkey group, so `--at` and `--latest-only` stay exact past the budget: they reduce each group as it goes by and merge the per crawl runs afterwards.
`--dedup` is the exception, because payload digests arrive in no order at all.
Past the budget it forgets the digests it has not seen for the longest and says so on stderr, which costs you a duplicate that gets through rather than a unique record that gets dropped.

One thing does change past the budget. `--at` normally sorts its result newest first; when the result itself will not fit in the buffer it comes out in index order instead, and the command says so on stderr.
That sort gets the same budget again, so `--at` can hold twice `--max-buffer` records at the moment it hands the result over.

### A page the index will not serve

A wide query is thousands of index pages, and the index truncates or refuses one often enough that a long run used to end on it and throw away everything that had already arrived.

A page whose body stops early is read again, up to `--retries` times, so a truncated response costs a second request rather than the records it was carrying.
A page that fails every attempt is named on stderr with its crawl and page number, and the run carries on with the next page:

```
search: CC-MAIN-2026-30: CDX page 252: HTTP 503, skipping the page
search: the result is incomplete, 1 index page could not be read; run it again or pass --strict to fail instead
```

The summary line is printed once at the end of the run, so a partial result never passes for a whole one.
Pass `--strict` to get the old behaviour, where the first page that cannot be read ends the command.

| Flag | Description |
| --- | --- |
| `--strict` | Fail the run if an index page cannot be read, rather than skipping it |

`export` takes `--strict` and reports the same way, for the same reason.

---

## get

```
ccrawl get <url> [flags]
```

Content flags (pick one): `--text`, `--markdown`, `--links`, `--headers`.
With none, prints the raw HTTP response body.

---

## fetch

```
ccrawl fetch [-] [flags]
```

Locate a record with `--file`, `--offset`, `--length`, or stream JSONL locations on stdin with `-`.
Content flags: `--body` (default), `--text`, `--markdown`, `--links`, `--headers`, `--meta`.
Write one file per record with `--dir` and `--out-dir`.

### Batch mode

One record per HTTP request is fine for a few thousand locations and hopeless for a few million.
`--batch` sorts the locations by file and offset, coalesces the ones that sit close together, and reads each run of them in a single ranged GET.
Records that share a request are sliced back apart by their own offset and length and parsed individually, so the output is byte for byte what the one at a time path produces.

| Flag | Description |
| --- | --- |
| `--batch` | Coalesce nearby records in the same WARC file into shared ranged GETs |
| `--gap` | Coalesce records at most this many bytes apart (default 1 MiB) |
| `--max-span` | Never read more than this in one GET (default 16 MiB) |
| `--order` | `input` or `file`: emit in the order given or the order on disk (default `file`) |
| `--ledger` | File of finished locations, to skip on a resume |
| `--lookahead` | Ranged GETs allowed to run ahead of the writer (default 64) |

```sh
ccrawl columnar locations --tld vn -o jsonl | ccrawl fetch - --batch --ledger fetched.txt --dir
```

### Choosing a gap

`--gap` is the price you are willing to pay in wasted bytes to save one request.
Every merge drops one round trip and reads the hole between the two records, so the flag is that trade written down as a number.
Add `--dry-run` and nothing is fetched: the grouping is pure arithmetic on the locations, so it reports both halves of the trade and you can try a few values for free.

```
$ ccrawl fetch - --batch --dry-run --gap 65536 < locations.jsonl
1000000 locations in 612078 requests, 1.6x fewer than one at a time; 10.1 GB read for 1.6 GB of records, 6.3x amplification

$ ccrawl fetch - --batch --dry-run < locations.jsonl
1000000 locations in 101238 requests, 9.9x fewer than one at a time; 116.0 GB read for 1.6 GB of records, 71.5x amplification
```

Those are real numbers from a million robots.txt locations, which is close to the worst case: tiny records scattered ten to a WARC file.
The 71x amplification looks alarming and is still the right call, because a round trip to `data.commoncrawl.org` costs far more than a megabyte of transfer does.
On 486 real records packed into 20 files the default gap turned 486 requests into 20 and finished in 6.6 seconds against 97.4 seconds for the one at a time path, a 14.7x speedup while reading 34x the bytes.
Lower `--gap` when bandwidth is what you are paying for, raise it when latency is.
`--max-span` is the separate ceiling that stops one dense file from becoming a single enormous read.

`--order file` streams: groups are written out in the order they sit on disk and no more than `--lookahead` of them are ever in flight.
`--order input` has to put back an ordering the grouping destroyed, so it holds finished records in memory until their turn comes, and a single slow group early in the input will hold everything behind it.
Use it when something downstream is lining the output up against the input, and leave it alone otherwise.

Pass `--ledger` and every finished location is appended to that file, flushed after each group.
Rerunning the same command with the same ledger skips what is already in it, so a killed run picks up where it stopped rather than starting over.
The ledger is one `filename@offset` per line, so it is greppable and safe to trim by hand.

---

## export

```
ccrawl export <url-or-pattern|-> [flags]
```

Run a query, pull each matching capture, and write them into one or more `.warc.gz` files.
Each file opens with a `warcinfo` record carrying provenance (the tool and version, the prefix, and the exact command line), so the output is self-describing.
Pass a URL or wildcard pattern to run a query, or `-` to read location records (filename, offset, length) as JSONL on stdin, exactly what `search --locations` and `columnar locations` produce.

Naming: `--prefix`, `--subprefix`. Rotation: `--size` (bytes, default 1 GB). Destination: `--out-dir`.
Provenance: `--creator`, `--operator`.
Query filters mirror `search`: `--match`, `--from`, `--to`, `--status`, `--mime`, `--lang`, `--filter`.
URL filters: `--url-fgrep`, `--url-fgrepv`.

```sh
ccrawl export example.com/* --prefix example
ccrawl search example.com --locations | ccrawl export - --prefix example
```

---

## download

```
ccrawl download <kind|-> [flags]
```

Kinds: `warc`, `wat`, `wet`, `robotstxt`, `non200responses`, `cc-index`, `cc-index-table`.
Use `-` to read paths on stdin.
`--out` sets the directory, `--flat` drops the source tree, `-j/--workers` sets concurrency.

---

## paths

```
ccrawl paths <kind> [flags]
```

Kinds: `warc`, `wat`, `wet`, `robotstxt`, `non200responses`, `cc-index`, `cc-index-table`, `segment`.
`--kinds` lists them.
`-o url` prints full URLs.

---

## parse

```
ccrawl parse <file|-> [flags]
```

Force the format with `--format` (`warc|wat|wet`).
Filters: `--type`, `--status`, `--mime`, `--lang`, `--url`.
Content flags: `--links`, `--text`, `--markdown`, `--meta`.

---

## extract

| Subcommand | Does |
|---|---|
| `extract title <url>` | The page title |
| `extract text <url>` | Readable plain text |
| `extract markdown <url>` | HTML converted to Markdown |
| `extract links <url>` | Outbound links |

---

## content

Live-fetch a URL and compute content signals.
Unlike `extract`, these commands use the v2 crawler config (10 MB body limit, brotli support, redirect following).

| Subcommand | Does |
|---|---|
| `content extract <url\|->` | Clean text, title, description, canonical URL, language, word count |
| `content outlinks <url\|->` | Outbound links as `(source, url, host)` rows, without anchor text |
| `content quality <url\|->` | Quality signals: word count, title length, main-content flag, spam score, parked detection |
| `content lang <url\|->` | The language the markdown pipelines would detect, with the confidence and the text it judged |

```sh
ccrawl content extract https://golang.org/
ccrawl content quality https://example.com/ -o json
ccrawl content outlinks https://news.ycombinator.com/ -n 20
ccrawl content lang https://vnexpress.net/ -o json
```

All four take `-` in place of the URL and read a list from stdin, one URL per line or JSONL with a `url` field, which is what `search`, `columnar` and `crawl fetch` produce:

```sh
ccrawl content quality - -o jsonl < seeds.txt
ccrawl search 'example.com/*' -n 100 -o jsonl | ccrawl content quality - -o jsonl
```

Over a stream a URL that cannot be fetched is named on stderr and the rest of the list carries on. A run that scored nothing because every URL failed exits 1, and empty stdin exits 3. [Content signals](/guides/content-signals/) has the field tables and the pipelines.

### content lang

`content lang` runs the same identifier `markdown export --lang` applies, on one URL at a time, so a document that was kept or dropped can be asked about directly.

```
$ ccrawl content lang https://vnexpress.net/ -o json
{
  "url": "https://vnexpress.net/",
  "language": "vie",
  "confidence": 1,
  "cc_language": "vi",
  "chars": 24196,
  "sample": "Báo tiếng Việt nhiều người xem nhất Thứ hai, 3/8/2026 ..."
}
```

`language` is detected in the extracted Markdown, not in the raw HTML and not read off the page's own `lang` attribute, because the Markdown is what the pipelines keep and filter on. `cc_language` is what the page declares, printed next to it so a disagreement is visible instead of silent. `sample` is the text the identifier actually saw, truncated; when an answer looks wrong it is almost always the input that is wrong.

---

## news

| Subcommand | Does |
|---|---|
| `news list` | List CC-NEWS files for `--year`/`--month` |
| `news download` | Download CC-NEWS files |
| `news search <host>` | Stream and match a host (no index) |

---

## columnar

Aliases: `table`, `athena`.

| Subcommand | Does |
|---|---|
| `columnar urls` | Matching URLs |
| `columnar locations` | Record locations, ready for `fetch` |
| `columnar count` | Count of matching captures |
| `columnar langs` | Breakdown by content language |
| `columnar mimes` | Breakdown by MIME type |
| `columnar sql` | Build the SQL from the filter flags and print it |
| `columnar query <sql>` | Run raw SQL (`ccindex` is the source) |
| `columnar schema` | The columns of the index |

Filters: `--domain`, `--host`, `--tld`, `--mime`, `--status`, `--lang`, `--path-prefix`, `--subset`.
Negated filters: `--not-tld`, `--not-mime`, `--not-lang`, `--not-status`. A row where the column is missing counts as a match, so `--not-lang vie` returns the captures Common Crawl never labelled as well as the ones it labelled something else.
Set filters: `--hosts-file` and `--domains-file` read one value per line, skipping blank lines and `#` comments, and turn the whole list into a single query.
Engine: `--engine` (`auto|duckdb|native|print`). See [the columnar engines](/reference/columnar-engines/) for what each one can answer and how fast.

```sh
ccrawl columnar count --tld vn --not-lang vie
ccrawl columnar urls --hosts-file hosts.txt --not-tld vn -o url
```

---

## markdown

Build Markdown Parquet datasets from Common Crawl WARC files and commit them to a HuggingFace dataset repo.

| Subcommand | Does |
|---|---|
| `markdown export` | Convert the HTML Common Crawl captured to Markdown Parquet |
| `markdown refetch` | Re-fetch every URL in a shard live, then convert to Markdown Parquet |

```sh
ccrawl markdown export --shards 0 --push=false --out ./md
ccrawl markdown export --shards all --parallel 4 --commit-batch 10 --repo org/name
ccrawl markdown refetch --shards 0-9 --fetch-workers 400 --repo org/name
```

Both take `--shards` (`N`, `N-M`, `N,M`, or `all`), resume from a ledger at `<out>/.committed`, and need `HF_TOKEN` unless `--push=false`.
The [markdown reference](/reference/markdown/) has both schemas, the HuggingFace path layout, and the tuning flags.

### Choosing an extractor

`--extractor` picks the engine that turns a captured page into the text in the row.

| Engine | Reads | Does |
|---|---|---|
| `h2m` (default) | WARC | go-trafilatura tuned for recall, rendered as GitHub-flavored Markdown |
| `readability` | WARC | go-readability extraction plus mdconv, the engine `open-markdown-v2` shipped |
| `raw` | WARC | the whole document as Markdown, no boilerplate removal at all |
| `wet` | WET | the plain text Common Crawl already extracted, passed through unchanged |

```sh
ccrawl markdown export --shards 0 --extractor readability --push=false --out ./md
ccrawl markdown export --shards 0 --extractor raw --push=false --out ./md
ccrawl markdown export --shards 0 --source-kind wet --extractor wet --push=false --out ./md
```

Which engine you use is a corpus quality decision, so it is a flag and not a build time constant. The same shard through two engines is two different corpora, and the only way to find out which one suits a downstream task is to build both and compare. Every row records the engine that produced it in the `extractor` column, as `name@version`, because extraction changes between releases and a name alone cannot explain why two shards built months apart disagree about the same page.

`raw` keeps the nav bars and the footer, which sounds useless and is not. Every extractor is a lossy judgement about what a page was for, and on the pages where that judgement goes wrong the output alone does not say so. Raw is the control you measure the others against.

`--source-kind` picks the manifest a run reads: `warc` takes `warc.paths.gz` and extracts the HTML itself, `wet` takes `wet.paths.gz` and uses the text Common Crawl already extracted. It defaults to whatever the extractor needs, so it rarely has to be given. The two are not interchangeable, since a WET file holds no HTML and a WARC holds no pre-extracted text, and asking for a pairing that cannot work is a usage error rather than a silently reinterpreted run:

```
$ ccrawl markdown export --shards 0 --source-kind wet --extractor h2m
ERROR  Extractor h2m reads warc shards, so it cannot be used with --source-kind wet.
```

`markdown refetch` takes `--extractor` too, but only the WARC engines: a live fetch returns HTML, and there is no Common Crawl text to pass through.

WET is much cheaper than any of the extractors, since the text is already there and the files are a fraction of the size of the WARCs. It is also somebody else's extraction decision, boilerplate and all, and it is plain text rather than Markdown, so headings and links are gone.

### Language filtering

Both subcommands label every row with a detected language and can keep only one of them.

| Flag | Default | Does |
|---|---|---|
| `--lang` | (off) | Keep only documents detected as this ISO 639-3 language, for example `vie` |
| `--min-lang-confidence` | `0.8` | Confidence a document has to clear for `--lang` |

```sh
ccrawl markdown export --shards 0 --lang vie --push=false --out ./md
ccrawl markdown export --shards 0 --lang vie --min-lang-confidence 0.9 --push=false --out ./md
```

The language is detected in the extracted Markdown, so it describes the text in the row rather than whatever the page declared. Without `--lang` nothing is dropped and every row still carries `language` and `language_confidence`, which is what makes an unfiltered shard filterable later without extracting it again.

A document with too little text to identify is dropped by `--lang` rather than kept: a filtered export asks for documents known to be in one language, and "we could not tell" is not that. The run prints both the drop rate and the detected mix, so a filter that threw away more than expected says so:

```
language: --lang vie kept 21482 of 43110 documents, dropped 21628 (50.2%)
language: detected vie=21482 eng=17903 unknown=2011 zho=884 ...
```

`--lang` cannot be combined with `markdown refetch --fetch-only`, since fetch-only never produces the Markdown the identifier reads.

This is a coarse pre-filter. A trigram identifier tells Vietnamese from Malay well enough to cut a corpus down to something worth looking at, and it is not a substitute for a language specific classifier.

### Deduplication

| Flag | Default | Does |
|---|---|---|
| `--dedup-digest` | `false` | Skip records whose payload digest was already seen in this shard |

```sh
ccrawl markdown export --shards 0 --dedup-digest --push=false --out ./md
```

The check runs before extraction, so a duplicate costs a hash lookup rather than an HTML parse, and the scope is one shard rather than the whole run. On one shard of `CC-MAIN-2026-30` it dropped 83 duplicate payloads, which an independent scan of the same WARC confirmed exactly.

Every row also carries a `simhash` fingerprint whether or not the flag is set. Near duplicates are reported by `ccrawl dedup` rather than dropped during the run, because deciding which of two near identical copies is the good one is not a decision a converter should make on its own. See [Deduplication](/reference/markdown/#deduplication).

### Converting a location set

`markdown export --locations` reads exactly the records a stream of index locations points at, instead of downloading whole shards.

| Flag | Default | Does |
|---|---|---|
| `--locations` | (off) | Convert the records in this JSONL location stream, `-` for stdin |
| `--part-size` | `50000` | Locations per Parquet part |
| `--gap` | 1 MiB | Coalesce records closer together than this many bytes into one ranged read |
| `--max-span` | 16 MiB | Cap on the size of one coalesced ranged read |

```sh
ccrawl columnar locations --crawl CC-MAIN-2026-30 --lang vie -o jsonl \
  | ccrawl markdown export --locations - --lang vie --dedup-digest --push=false --out ./md
ccrawl markdown export --locations missed.jsonl --part-size 10000 --push=false --out ./md
```

This is the recovery pass: a columnar query picks out the pages you are missing, and the export turns those pages and nothing else into Parquet, with the same extractor, language filter, dedup, and schema a full export uses. Reading whole shards to reach a few thousand scattered pages would move something like a thousand times the bytes those pages are worth, so the records are fetched with ranged GETs and the neighbours coalesced.

A part is to a location run what a shard is to a full export: the unit that gets one Parquet file, one ledger entry, and one digest dedup set. The stream is cut in order, so an interrupted run resumes where the ledger says it stopped. Locations that will not fetch are skipped rather than failing the part, because a recovery pass runs against an index that can disagree with the archive.

`--locations` bypasses `--shards`, `--source-kind`, and the manifest fetch entirely, and it cannot be combined with the `wet` extractor, since WET files have no record offsets to point at. See [Converting a location set instead of whole shards](/reference/markdown/#converting-a-location-set-instead-of-whole-shards).

---

## rank

| Subcommand | Does |
|---|---|
| `rank domain <domain>` | Rank of a registered domain |
| `rank host <host>` | Rank of a host |
| `rank top` | Top-ranked hosts |
| `rank all` | Stream every host in a rank table, most central first |

All four read the newest web-graph release without being told where it is. `--graph <release-id>` pins a release, the way the `host` commands take it, and `--table <url>` points at a table directly and skips the lookup.
`rank domain` reads the domain ranks and the other three read the host ranks, which are separate tables with separate positions in them: `wikipedia.org` is domain 14 and host 864 in the same release.
The newest release for a domain lookup is the newest one whose domain table is published, which is not always the newest release, because a release is listed as soon as its host tables land.

`rank top` and `rank all` take `--tld` to filter by TLD.
The rank table is sorted by harmonic centrality, so `rank all -n 1000` is the top 1000 hosts without any sorting on your side.

```sh
ccrawl rank host wikipedia.org
ccrawl rank all --tld com -n 1000
ccrawl rank all --graph cc-main-2026-mar-apr-may -o jsonl > hosts.jsonl
```

---

## host

Enumerate and enrich hosts from the CC web graph.
All subcommands accept `--graph <release-id>` to pin a specific web-graph release (default: latest).

| Subcommand | Does |
|---|---|
| `host top` | Top hosts by harmonic centrality, streamed from the rank table |
| `host get <hostname>` | Enriched profile for one host |
| `host vertices` | Stream the vertex ID to hostname mapping |
| `host degrees` | Compute in-degree and out-degree from edge files (~7.7 GB) |
| `host cdx` | Aggregate CDX statistics per host via DuckDB |
| `host enrich` | Full enrichment pipeline: rank + degrees + CDX |

### host top

```sh
ccrawl host top -n 20 -o table
ccrawl host top --graph cc-main-2026-mar-apr-may -n 1000 -o jsonl > top1k.jsonl
```

### host get

```sh
ccrawl host get golang.org -o json
```

### host vertices

```sh
ccrawl host vertices --graph cc-main-2026-mar-apr-may -n 5
```

### host degrees

Streams all edge files to compute per-host in/out-degree.
Requires ~7.7 GB of edge data.

```sh
ccrawl host degrees --graph cc-main-2026-mar-apr-may -n 100 -o jsonl
```

### host cdx

Runs a DuckDB `GROUP BY url_host_name` over the columnar Parquet index.
Without `--filter` this scans ~184 GB of Parquet.

```sh
ccrawl host cdx --filter example.com -o json
ccrawl host cdx -n 100 -o jsonl
```

| Flag | Meaning |
|---|---|
| `--filter` | Restrict to one host (`url_host_name`) |

### host enrich

Runs all enrichment phases in sequence.
Phases 3 and 4 are opt-in because they require large data transfers.

```sh
ccrawl host enrich -n 20
ccrawl host enrich --graph cc-main-2026-mar-apr-may -n 100
ccrawl host enrich --degrees --cdx -o jsonl > enriched.jsonl
```

| Flag | Meaning |
|---|---|
| `--graph` | Web-graph release ID (default: latest) |
| `--degrees` | Phase 3: compute in/out-degree from edge files (~7.7 GB) |
| `--cdx` | Phase 4: aggregate CDX statistics via DuckDB (~184 GB) |

## urls

Mirror the Common Crawl columnar URL index to a HuggingFace dataset, one output Parquet shard per original source part.
Nothing is aggregated, deduplicated, or filtered: the rows and their order match the source, projected down to the URL-level columns.
The run is idempotent from remote truth, so shards already on the hub are skipped and each local shard is deleted right after it commits.

| Subcommand | Does |
|---|---|
| `urls publish` | Mirror the URL index to a HuggingFace dataset, shard for shard |
| `urls recount` | Repair drifted URL and byte totals in `stats.csv` from the hub |

### urls publish

```sh
ccrawl urls publish -c CC-MAIN-2026-25
ccrawl urls publish -c 2 --commit-every 32
ccrawl urls publish -c CC-MAIN-2026-25 --no-push   # scan and report, upload nothing
```

`HF_TOKEN` (or `HUGGINGFACE_TOKEN`) must be set to push.

| Flag | Meaning |
|---|---|
| `--repo` | HuggingFace dataset repo (default: `open-index/ccrawl-urls`, or `CCRAWL_URLS_REPO`) |
| `--commit-every` | Shards per HuggingFace commit (default 16) |
| `--workers` | Download-and-convert workers (0 picks a default from CPU count) |
| `--whole` | Download each part whole before reading (fallback for range-hostile mirrors) |
| `--private` | Create the dataset repo private |
| `--keep` | Keep local shards after commit instead of deleting them |
| `--min-free-gb` | Pause new downloads when free disk is under this many GB |
| `--max-stall` | Restart the run (exit 75) after this long with no progress |
| `--no-push` | Scan and stage but skip the upload |

## domains

Stream the web-graph domain ranks top to bottom and republish them as rank-ordered Parquet shards on a HuggingFace dataset.
The one edit to the data is un-reversing the source host key (`com.example` becomes `example.com`); rows stay in rank order, so `part-000` holds the highest-centrality domains.

| Subcommand | Does |
|---|---|
| `domains publish` | Mirror the domain ranks to a HuggingFace dataset, in rank order |
| `domains recount` | Repair drifted release totals in `stats.csv` from the hub |
| `domains diff` | Count domains added, removed, and shared between two published releases |

### domains publish

```sh
ccrawl domains publish
ccrawl domains publish --no-push   # scan and report, upload nothing
```

| Flag | Meaning |
|---|---|
| `--repo` | HuggingFace dataset repo (default: `open-index/ccrawl-domains`, or `CCRAWL_DOMAINS_REPO`) |
| `--commit-every` | Shards per HuggingFace commit |
| `--private` | Create the dataset repo private |
| `--keep` | Keep local shards after commit instead of deleting them |
| `--min-free-gb` | Pause new work when free disk is under this many GB |
| `--max-stall` | Restart the run (exit 75) after this long with no progress |
| `--no-push` | Scan and stage but skip the upload |

### domains diff

Compare two web-graph domain releases already published to the dataset and report how many domains are new in the later release, how many dropped out of the earlier one, and how many the two share.
It reads only the domain column of each shard straight from the hub, so it never downloads the rank fields.
With no ids it diffs the two most recent complete releases in the dataset, older against newer.

```sh
ccrawl domains diff
ccrawl domains diff --from cc-main-2026-mar-apr-may --to cc-main-2026-apr-may-jun
ccrawl domains diff --added-out new-domains.txt
```

| Flag | Meaning |
|---|---|
| `--repo` | HuggingFace dataset repo (default: `open-index/ccrawl-domains`, or `CCRAWL_DOMAINS_REPO`) |
| `--from` | Older web-graph release id (default: second-newest published) |
| `--to` | Newer web-graph release id (default: newest published) |
| `--added-out` | Write the domains new in the later release to this file, one per line |
| `--workers` | Concurrent shard readers (0 picks a default from CPU count) |

## publish

Maintenance for the published Common Crawl datasets.

| Subcommand | Does |
|---|---|
| `publish verify` | Check that the published shards are readable, complete, and the schema they claim |
| `publish delete-obsolete` | Delete the superseded first-generation dataset repos |

### publish verify

Publishing only ever asks whether a shard's path exists, so an upload that was cut off part way through leaves an object the resume path skips forever, and nothing notices until somebody reads the dataset and gets an error out of a Parquet library.
`publish verify` reads each shard's footer over ranged requests and asks what publishing never asks: does the file parse, is it the schema the dataset promises, do the row groups add up to the row count the footer claims, and does every column chunk sit inside the bytes the hub is holding.
The totals are then reconciled against the `stats.csv` ledger the dataset card is built from, and a disagreement is reported even when every shard passes, because it means the numbers the dataset advertises are not the numbers it holds.

```sh
ccrawl publish verify -c CC-MAIN-2026-25
ccrawl publish verify -c CC-MAIN-2026-25 --sample 64
ccrawl publish verify -c CC-MAIN-2026-25 --repair
ccrawl publish verify --graph cc-main-2026-mar-apr-may --json
```

Verifying the 300 shard `CC-MAIN-2026-25` crawl reads 269 MB against a dataset holding 150.5 GB, which is 0.175 percent of it, so a full check costs about as much as a listing.

| Flag | Meaning |
|---|---|
| `--repo` | HuggingFace dataset repo (default: the dataset the unit belongs to) |
| `--graph` | Verify a web-graph release of the domains dataset instead of URL crawls |
| `--sample` | Rows to decode from each shard's last row group (0 reads the footer alone) |
| `--workers` | Shards checked at once (0 picks a default from CPU count) |
| `--repair` | Rebuild and re-upload the shards that fail |
| `--no-push` | With `--repair`, rebuild locally but skip the upload |
| `--json` | Print the report as JSON |

Each shard comes back `ok`, or `missing`, `unreadable`, `truncated`, `schema`, `empty`, `corrupt`, or `no-access`.
The last one is not a verdict on the data: the shards are read with plain ranged GETs because the published datasets are public, so a repo that will not serve them reports `no-access` rather than being called corrupt.
The exit status is non-zero when a shard fails and `--repair` was not passed.

`--sample` decodes rows out of each shard as well as reading its footer, which is the only way to catch a page whose bytes are wrong rather than missing.
It reads a page of every column instead of a footer, so it costs a great deal more than the default check and is worth running on a crawl you have a specific reason to distrust.

`--repair` works on the URL dataset, where a shard is the projection of exactly one source part and can be rebuilt on its own from the part that made it.
A domain shard is a cut of one sequential rank stream, so rebuilding one means reading the source up to it: `publish verify --graph` reports the bad shards and leaves the rebuild to `ccrawl domains publish`.

### publish delete-obsolete

Delete the obsolete dataset repos that the `ccrawl-urls` and `ccrawl-domains` datasets replaced.
It removes `open-index/cc-host-dataset` and `open-index/commoncrawl-urls`, and asks for confirmation unless `--yes` is passed.

```sh
ccrawl publish delete-obsolete          # prompt before deleting
ccrawl publish delete-obsolete --yes    # delete without prompting
```

## crawl

Recrawl engine commands for seeding and fetching live URLs.

| Subcommand | Does |
|---|---|
| `crawl seed` | Generate seed URLs from the web-graph rank table |
| `crawl fetch <url>` | Crawl a single URL with robots.txt checking and content digest |
| `crawl run` | Run a crawl from a seed file, with a resumable frontier and WARC output |
| `crawl status` | Show daily crawl budget allocation across the five recrawl tiers |

### crawl seed

Streams the rank table and emits one seed URL per host.
Use `--max-tier` to restrict to high-priority hosts: tier 2 is roughly the top million by harmonic rank, tier 5 is everything.
Tier 1 wants a change rate above 0.8 and a seed carries no measured change rate, so `--max-tier 1` is refused rather than answered with nothing.

```sh
ccrawl crawl seed -n 100 -o table
ccrawl crawl seed --max-tier 2 -n 1000000 -o jsonl > seeds.jsonl
ccrawl crawl seed --graph cc-main-2026-mar-apr-may --max-tier 3 -n 5000000
```

| Flag | Meaning |
|---|---|
| `--graph` | Web-graph release ID (default: latest) |
| `--max-seeds` | Maximum hosts to emit (default 10 000 000) |
| `--max-tier` | Skip hosts with tier higher than this (2-5, default 5 = all; 1 is unreachable from a seed) |

### crawl fetch

Fetches one URL with the v2 crawler config: polite user-agent, brotli support, redirect following (up to 5 hops), 10 MB body limit, SHA-1 digest.

```sh
ccrawl crawl fetch https://golang.org/ -o json
ccrawl crawl fetch https://example.com/ --robots -o json
ccrawl crawl fetch https://example.com/ --warc-dir warc/
```

| Flag | Meaning |
|---|---|
| `--robots` | Check robots.txt before fetching |
| `--warc-dir` | Write the fetch to a WARC file in this directory |

With `--warc-dir` the fetch is archived as an ISO 28500 WARC/1.0 file: a warcinfo record, then a request and response pair linked with `WARC-Concurrent-To`, with `WARC-Block-Digest` and `WARC-Payload-Digest` as `sha1:` base32, `WARC-IP-Address`, and `WARC-Truncated: length` when the body cap trips.
The stored headers describe the stored body, so a decoded or dechunked response gets a rewritten `Content-Length` and loses the encoding headers that no longer apply.
The path written is reported in the record as `warc_file`.

### crawl run

Drives a whole crawl from a seed file: the frontier hands out URLs in priority order, one host at a time, robots.txt is fetched once per host and enforced, every fetch is written to WARC, and outlinks go back in the queue up to `--max-depth`.

```sh
ccrawl crawl seed -n 100000 -o jsonl > seeds.jsonl
ccrawl crawl run --seeds seeds.jsonl --out warc/ --state crawl.db --max-pages 100000 -j 64
ccrawl crawl run --seeds - --out warc/ --state crawl.db --max-depth 2 --same-host
```

| Flag | Meaning |
|---|---|
| `--seeds` | Seed file: `crawl seed` JSONL or one URL per line, `-` for stdin |
| `--out` | Directory to write WARC files into (empty fetches without archiving) |
| `--state` | Frontier state file, so a run resumes after a restart |
| `--delay` | Minimum spacing between two requests to the same host |
| `--max-depth` | How far from a seed to follow links (default 0, the seeds only) |
| `--max-pages` | Stop after this many fetches (0 = no limit) |
| `--same-host` | Stay on the hosts the seeds named |
| `--no-robots` | Do not check robots.txt |
| `--robots` | Check robots.txt, which is already the default |
| `--warc-size` | Rotate to a new WARC file past this many bytes |
| `--prefix` | File name prefix for the WARC output |

The frontier in `--state` is the whole resume story: a run that is killed leaves its queue, its politeness clocks and its seen set on disk, and the next run over the same file picks up where the last one stopped rather than refetching what is done.
Politeness is per host and it is the longer of `--delay` and the host's own `Crawl-delay`, so raising `--workers` adds hosts in flight and never adds requests to one host.

### crawl status

Prints the daily page budget across the five recrawl tiers assuming 10 000 pages/s sustained throughput.

```sh
ccrawl crawl status -o table
```

---

## sched

Recrawl scheduling commands.
`sched diff` requires DuckDB on PATH.

| Subcommand | Does |
|---|---|
| `sched assign` | Assign crawl tiers to hosts by harmonic rank and change rate |
| `sched diff` | Compare two crawls and compute per-host content change rates |

### sched assign

```sh
ccrawl sched assign -n 20 -o table
ccrawl sched assign --graph cc-main-2026-mar-apr-may --change-rate 0.5 -o jsonl
```

| Flag | Meaning |
|---|---|
| `--graph` | Web-graph release ID (default: latest) |
| `--change-rate` | Assumed change rate for all hosts (0-1, default 0.5) |

Tier assignment:

| Tier | Recrawl interval | Criteria |
|---|---|---|
| 1 | 24 h | harmonic rank <= 100 K and change rate > 0.8 |
| 2 | 3 days | rank <= 1 M and change rate >= 0.5 |
| 3 | 7 days | rank <= 5 M and change rate >= 0.2 |
| 4 | 30 days | rank <= 10 M |
| 5 | on-demand | everything else |

### sched diff

Joins two CDX Parquet indexes on URL and compares `content_digest` to compute per-host change rates.
Requires DuckDB on PATH.
Scans ~368 GB of Parquet (184 GB per crawl).

```sh
ccrawl sched diff --crawl-a CC-MAIN-2026-17 --crawl-b CC-MAIN-2026-21 -n 20
ccrawl sched diff --crawl-a CC-MAIN-2026-12 --crawl-b CC-MAIN-2026-17 -o jsonl > changes.jsonl
```

| Flag | Meaning |
|---|---|
| `--crawl-a` | Older crawl ID |
| `--crawl-b` | Newer crawl ID |

---

## index

Build and query a local BM25 full-text search index over a JSONL page corpus.
This is a reference implementation with a corpus ceiling of a few hundred thousand documents, not a production search engine; the [search index guide](/guides/search-index/) has the measured numbers.

| Subcommand | Does |
|---|---|
| `index build` | Build a BM25 inverted index from JSONL documents or a list of URLs |
| `index search <query>` | Query the index; results ranked by BM25 score |

### index build

Reads JSONL documents from `--input`, or fetches and extracts the pages named by `--urls`, tokenizes them, and writes a BM25 inverted index with per-document length normalization.
Each `--input` line is a JSON object with a `url` and the `text` to index, and optionally a `title` and a language, which is the shape `ccrawl parse` writes for a WET file.
The language key can be `language`, which is what a file written by hand tends to say, or `content_language`, which is what `ccrawl parse wet -o jsonl` writes; a file written by a ccrawl older than v0.10.1 says `ContentLanguage` and is still read.
The index directory contains `terms.dat`, `postings.dat`, `forward.jsonl`, and `stats.dat`, and a rebuild replaces all four.
One of `--input` or `--urls` is required; without either the command exits 2 rather than writing an empty index.

```sh
ccrawl index build --dir /data/idx --input docs.jsonl
ccrawl parse file.warc.wet.gz --lang eng -o jsonl | ccrawl index build --dir /data/idx --input -
ccrawl index build --dir /data/idx --urls https://golang.org/,https://pkg.go.dev/ -o json
```

| Flag | Meaning |
|---|---|
| `--dir` | Directory to write the index into (default: `~/data/ccrawl/index`) |
| `--input` | JSONL file of documents to index, or `-` for stdin |
| `--urls` | Comma-separated URLs to fetch and index |
| `-j`, `--workers` | Fetch concurrency for `--urls` (default 8) |

### index search

Queries the local index using BM25 scoring with per-document length normalization.
Query terms are ORed: a document matches if it holds any of them, and the ones holding more score higher.
A query that matches nothing exits 3.

```sh
ccrawl index search "golang web server"
ccrawl index search "machine learning" --dir /data/idx -n 20 -o json
```

| Flag | Meaning |
|---|---|
| `--dir` | Index directory to search (default: `~/data/ccrawl/index`) |
| `-n`, `--limit` | Documents to return (default 100) |

---

## api

Start the v2 HTTP REST API server.
This is a local exploration tool with no authentication, no rate limiting, no request log and no pagination, so it binds loopback by default and warns if pointed anywhere else; see the [API server guide](/guides/api-server/).
The host store is loaded from the web-graph rank table on startup (top 1 M hosts) and a load that fails is fatal.
Full-text search is available when `--index-dir` points to a built index, and answers 503 without it.

```
GET /v2/host/{host}       host profile from the rank table
GET /v2/hosts?tld=&n=     top N hosts, optional TLD filter
GET /v2/search?q=&k=      BM25 full-text search (requires --index-dir)
GET /v2/health            liveness, and which stores are loaded
```

```sh
ccrawl api
ccrawl api --addr 127.0.0.1:9090 --index-dir /data/idx
```

| Flag | Meaning |
|---|---|
| `--addr` | Listen address (default `127.0.0.1:8080`) |
| `--index-dir` | Path to a built inverted index directory |

---

## db

| Subcommand | Does |
|---|---|
| `db load` | Load matching index records into local DuckDB |
| `db sql <query>` | Run SQL against the local database |
| `db shell` | Open an interactive DuckDB shell |
| `db path` | Print the database file path |

`db load` takes the same filter flags as `table`.

---

## convert

```
ccrawl convert <file|dir> [flags]
```

`--to parquet|jsonl` (default `parquet`).
`-O/--out` sets the output file or directory.
`--markdown` converts HTML bodies on the way.

---

## library

```sh
ccrawl library list
ccrawl library du
ccrawl library verify
ccrawl library gc --older-than 90d
ccrawl library scan
```

The dataset library is the tree `--library` downloads into and processes from, `~/notes/ccrawl` by default and moved with `--library-dir` or `CCRAWL_LIBRARY`.
It is separate from the data dir so scratch state and the files you keep never mix, and `library` is how you find out what is in it.

`library.json` at the root of the tree records every artifact: its path, crawl, kind, format, size, sha256, when it was written, and which version of ccrawl wrote it.
Every command that materialises into the library updates it, and the checksum for a download is computed as the bytes stream past rather than by reading the file back.
Concurrent runs take a lock on `library.lock` for the read-change-write, so two ccrawl processes filling one library do not lose each other's records.

| Subcommand | Does |
|---|---|
| `library list` | List the artifacts the manifest records |
| `library du` | Report library size, per crawl |
| `library verify` | Rehash every artifact and report what does not match |
| `library gc` | Delete artifacts older than a cutoff |
| `library scan` | Record what is on disk into the manifest |

### library list

| Flag | Default | Does |
|---|---|---|
| `--kind` | all | Only this kind: `warc`, `wet`, `wat`, and so on |
| `--format` | all | Only this format: `raw`, `parquet`, `jsonl` |

`-c` narrows to one crawl and `-n` caps the rows.

### library du

`--by crawl|kind|format` picks the grouping, `crawl` by default.
A `total` row follows the groups when there is more than one, as a row rather than a footer so it survives `-o json` and a pipe.

### library verify

Reads every artifact and compares it against the manifest.
Four kinds of trouble are reported: `missing` for a file that is gone, `resized` for one whose size changed, `corrupt` for one whose bytes changed, and `untracked` for a file on disk the manifest has never heard of.
The first three exit 1, so `library verify` can gate a publish run; an untracked file is reported but is not a failure, since `library scan` is the fix.

| Flag | Default | Does |
|---|---|---|
| `--quick` | `false` | Check existence and size only, no rehash |
| `--kind` | all | Only this kind |

### library gc

Deletes artifacts and drops them from the manifest.
It needs something to select on: `--older-than`, `-c`, or `--kind`.
`--older-than` takes `30d`, `8w`, or any Go duration such as `12h`.

It is a dry run unless you pass `--yes`, and the dry run prints exactly the list the real run deletes.
Directories the collection emptied are removed with it.

### library scan

Walks the tree, hashes what it finds, and writes the manifest.
This is how a library built by a ccrawl that predated the manifest, or one you copied files into, comes under management.
It reads every byte the first time and is a no-op after that: an artifact already recorded at the same size is left alone unless you pass `--rehash`.

Only files that fit the library layout are recorded, `<crawl>/<kind>/<file>` for a raw archive and `<crawl>/<format>/<kind>/<file>` for processed output, so a README you left in the tree is not mistaken for a corrupt artifact.

---

## dedup

```
ccrawl dedup <parquet-file|dir>... [flags]
```

Reports duplicate documents in a dataset written by `markdown export` or `markdown refetch`. It reads and prints, it never rewrites the input, so it is safe to point at a published dataset.

| Flag | Default | Does |
|---|---|---|
| `--distance` | `3` | Hamming distance between fingerprints that still counts as a near duplicate, 0 to 64 |
| `--top` | `10` | How many of the largest clusters to list, 0 for none |
| `--json` | `false` | Emit the report as JSON instead of a table |

```sh
ccrawl dedup ./md
ccrawl dedup ./md --distance 6 --top 20
ccrawl dedup ./md/part-000.parquet ./md/part-001.parquet --json
```

```
20,861 rows in 1 files
  exact duplicates          165 in 113 clusters, 139.7 kB
  near duplicates            24 in 23 clusters, 171.0 kB  (distance <= 3)
  redundant                 189  (0.9% of rows)
  no fingerprint              2  (too short to hash, left alone)
```

Exact clusters are documents with identical Markdown. Near clusters are grouped by the `simhash` column, and a file written before that column existed is fingerprinted on the fly, so an older dataset still works.

Only three columns are read, which is why a 20k row shard takes well under a second. Raising `--distance` widens the net and chains clusters together, so 6 finds more real template duplicates and also more junk. Documents under 512 bytes are left out of the near pass because a 64 bit fingerprint over that little text is decided by noise. See [Deduplication](/reference/markdown/#deduplication) for what the numbers mean and how they were measured.

---

## serve

Serve the record-stream operations over HTTP as NDJSON.
Every kit operation gets an endpoint, so anything the CLI can list, the server can stream.

```sh
ccrawl serve --addr :8080
ccrawl serve --addr :8080 --allow-writes
```

| Flag | Meaning |
|---|---|
| `--addr` | Listen address (default `:8080`) |
| `--allow-writes` | Expose the write operations, which are hidden by default |

`serve` is the generic operation server; `api` is the purpose-built v2 REST API with the host store and search.

---

## mcp

Run as an MCP server over stdio, exposing the same operations as tools to an MCP client.

```sh
ccrawl mcp
```

There are no command-specific flags.
The global flags apply, so `-c`, `--data-dir`, and `--no-cache` all set the server's defaults.

---

## config

```sh
ccrawl config show
```

`config show` prints every setting a run resolved, the value it ended up with, and where that value came from. The source column is the reason to run it: a run that behaves oddly is nearly always a setting arriving from somewhere you did not look, and `workers 7 config [bulk]` ends that search in one line.

```
key          value                        source
crawl        CC-MAIN-2026-30              flag --crawl
workers      7                            config [bulk]
retries      2                            config [default]
user_agent   my-crawler/1                 env CCRAWL_USER_AGENT
timeout      2m0s                         default
profile      bulk                         flag --profile
config_file  ~/.config/ccrawl/config.toml derived from config_dir
```

### The config file

ccrawl reads `~/.config/ccrawl/config.toml` if it is there. `CCRAWL_CONFIG_DIR` names the directory outright, and `XDG_CONFIG_HOME` moves it the usual way. There is no file by default and nothing needs one.

The `[default]` table applies to every run. Any other table is a profile, and `--profile <name>` layers it on top of `[default]` for that run.

```toml
[default]
workers = 8
global_rate = "500ms"

[bulk]
workers = 64
global_rate = "50ms"
library_dir = "/data/ccrawl"

[polite]
global_rate = "5s"
retries = 8
```

```sh
ccrawl --profile bulk markdown build -c 2026-30
ccrawl --profile polite search '*.gov' --limit 1000
```

`CCRAWL_PROFILE` selects a profile from the environment, for a shell or a systemd unit that should run everything one way.

Precedence is flag, then environment, then profile, then `[default]`, then the built-in default. A profile cannot undo an `export` in the shell that started the run, and a flag on the command line always wins.

The settings, with the environment variable that beats each one:

| Setting | Env | What it sets |
|---|---|---|
| `crawl` | `CCRAWL_CRAWL` | Default crawl, same values as `-c` |
| `source` | `CCRAWL_SOURCE` | `https` or `s3` |
| `data_dir` | `CCRAWL_DATA_DIR` | Root data directory |
| `cache_dir` | `CCRAWL_CACHE_DIR` | Cache directory, follows `data_dir` when unset |
| `library_dir` | `CCRAWL_LIBRARY` | Dataset library root |
| `db_path` | `CCRAWL_DB_PATH` | Local DuckDB file |
| `workers` | `CCRAWL_WORKERS` | Concurrency |
| `rate` | `CCRAWL_RATE` | Per-process delay between requests |
| `global_rate` | `CCRAWL_GLOBAL_RATE` | Host-wide gap between Common Crawl requests |
| `timeout` | `CCRAWL_TIMEOUT` | Per-request timeout |
| `retries` | `CCRAWL_RETRIES` | Retry attempts |
| `backoff` | `CCRAWL_BACKOFF` | Base wait before the first retry |
| `backoff_max` | `CCRAWL_BACKOFF_MAX` | Cap on a single retry wait |
| `user_agent` | `CCRAWL_USER_AGENT` | User agent sent to Common Crawl |
| `urls_repo` | `CCRAWL_URLS_REPO` | HuggingFace dataset for `urls publish` |
| `domains_repo` | `CCRAWL_DOMAINS_REPO` | HuggingFace dataset for `domains publish` |
| `collinfo_endpoint` | `CCRAWL_COLLINFO_ENDPOINT` | Where the crawl list comes from |
| `data_endpoint` | `CCRAWL_DATA_ENDPOINT` | Where manifests, WARC files and the columnar index come from |
| `cdx_endpoint` | `CCRAWL_CDX_ENDPOINT` | Where the URL index comes from |

Durations are written the way the flags are written, `"500ms"` or `"5s"`, and have to be quoted. The endpoints are there for a mirror or a local proxy; the defaults are Common Crawl's own hosts.

A setting ccrawl does not read stops the run and names the line, since the alternative is a config file that looks like it is doing something and is not. So does `--profile` naming a table the file does not declare, and the error lists the ones it does. A value of the right key and the wrong type, `workers = "lots"`, is reported on stderr and the run continues on the default: that is read while the flags are being registered, where there is nowhere to return an error.

---

## Global flags

These apply to every command.

| Flag | Short | Meaning | Default |
|---|---|---|---|
| `--crawl` | `-c` | Crawl ID, year (all crawls of that year), `latest`, `all`, an integer for the newest N, or a comma list | `latest` |
| `--output` | `-o` | Output format: `auto`, `table`, `json`, `jsonl`, `csv`, `tsv`, `url`, `raw`, `parquet` | `auto` |
| `--limit` | `-n` | Maximum records (0 = unlimited) | `0` |
| `--workers` | `-j` | Concurrency for downloads and scans | `8` |
| `--source` | | Bulk data source: `https` or `s3` | `https` |
| `--rate` | | Minimum delay between requests, for this process alone | `0s` |
| `--global-rate` | | Minimum gap between Common Crawl requests across every ccrawl process on this host (0 disables) | `200ms` |
| `--timeout` | | Per-request timeout | `0s` |
| `--no-cache` | | Bypass the on-disk cache | false |
| `--fields` | | Comma-separated columns to show | |
| `--template` | | Go template applied per record | |
| `--library` | | Read and write under the dataset library | false |
| `--library-dir` | | Library root | `~/notes/ccrawl` |
| `--data-dir` | | Root data directory | |
| `--dry-run` | | Print actions, do not perform them | false |
| `--quiet` | `-q` | Suppress progress output | false |
| `--verbose` | `-v` | Increase verbosity (repeatable) | |
| `--color` | | Color output: `auto`, `always`, `never` | `auto` |
| `--no-header` | | Omit the header row in table output | false |
| `--db` | | Tee every record into a store (e.g. `out.db`, `postgres://...`) | |
| `--profile` | | Named profile to load | |
| `--progress` | | Progress reporting for long runs: `text`, `json`, `none` | text on a terminal, json otherwise |
| `--journal` | | Append run events as JSON Lines to this file | `run.jsonl` beside the ledger |
| `--metrics-addr` | | Serve Prometheus metrics for the run on this address, e.g. `:9090` | off |

See [run journal](/reference/run-journal/) for the event schema, the metric names, and the queries worth keeping.

### The crawl list outlives the index server

Turning `latest` into a crawl ID needs `collinfo.json` from `index.commoncrawl.org`. It is cached for six hours, and when the fetch fails the cached copy is used at any age, with a line on stderr saying how old it is:

```
crawls: the index server is unreachable, using the crawl list cached 19h0m0s ago; pass -c to name a crawl instead of resolving it
```

Common Crawl publishes about six crawls a year, so a day-old list is almost always the list a fresh fetch would return, and the index server has gone away for three days at a stretch. Most of what ccrawl does reads `data.commoncrawl.org`, which stays up through those outages, and only touches the index server to resolve that one word. Without the fallback `paths`, `columnar` and `download` all failed on a crawl ID sitting in the cache.

Naming a crawl with `-c CC-MAIN-2026-30` skips the lookup entirely, and is the right move for a scheduled job that should not depend on it. With no cached copy and the server unreachable the run still fails with exit 8: guessing a crawl list is worse than saying nothing.

`--no-cache` turns the whole cache off, the fallback with it.

### The shared request budget

`--rate` spaces the requests one ccrawl process makes. That is not the number Common Crawl sees. Running the URL publish, the domain publish, and a Markdown export at once means three processes each pacing themselves politely and three times the traffic arriving at a nonprofit that serves this for free.

`--global-rate` is the gap between requests summed over every ccrawl process on the host. The processes coordinate through a small lock file at `<data-dir>/ratelimit.lock`: taking a slot means locking the file, reading the time the next slot comes free, pushing it forward by one interval, and sleeping until the slot you were handed. The lock is held for sixteen bytes of read and write, so processes queue on the timestamps rather than on the lock.

It covers `index.commoncrawl.org`, `data.commoncrawl.org`, `commoncrawl.org`, and the `commoncrawl` S3 bucket. Requests a recrawl makes to arbitrary sites are not Common Crawl's bandwidth, so they pay `--rate` and nothing else. Columnar scans are also exempt, for the reason `--rate` does not apply to them either: a scan is thousands of few-kilobyte footer reads, and pushing those through a five per second budget turns a thirty second query into an hour.

The default is `200ms`, five requests per second, which is what a single process used to take on its own. So one process behaves exactly as it did before, and three processes now split that budget instead of tripling it. Raise the gap to be gentler, or pass `--global-rate 0` to switch the shared limiter off and go back to a per process delay. `CCRAWL_GLOBAL_RATE` sets it from the environment.

Processes sharing a budget must share a data dir, since that is where the lock file lives. When the file cannot be created or locked, which is what a read-only or exotic filesystem looks like, ccrawl prints one warning and falls back to the per process delay rather than failing the run. Pass `-v` to have any run print the rate it is actually working under.

Measured on one host, three concurrent pipelines walking twenty crawls each and fetching real path manifests: `--global-rate 2s` served 60 requests at 0.499 per second combined against a configured 0.500, and the same 60 requests with `--global-rate 0` went out at 2.316 per second.

## Exit codes

`0` success, `1` error, `2` usage error, `3` the query matched nothing, `4` a credential is needed and is not set, `8` transport failure so Common Crawl could not be reached at all, `75` temporary failure so run it again. See [exit codes](/reference/exit-codes/) for what to branch on and how to supervise a publish run.

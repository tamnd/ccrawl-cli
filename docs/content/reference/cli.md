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
| `crawls info <id>` | File counts per archive kind for a crawl |

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
| `content extract <url>` | Clean text, title, description, canonical URL, language, word count |
| `content outlinks <url>` | Structured outbound links with anchor text |
| `content quality <url>` | Quality signals: word count, spam score, parked detection, short-content flag |
| `content lang <url>` | The language the markdown pipelines would detect, with the confidence and the text it judged |

```sh
ccrawl content extract https://golang.org/
ccrawl content quality https://example.com/ -o json
ccrawl content outlinks https://news.ycombinator.com/ -n 20
ccrawl content lang https://vnexpress.net/ -o json
```

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

---

## rank

| Subcommand | Does |
|---|---|
| `rank domain <domain>` | Rank of a registered domain |
| `rank host <host>` | Rank of a host |
| `rank top` | Top-ranked hosts or domains (requires `--table <url>`) |
| `rank all` | Stream every host in a rank table, most central first (requires `--table <url>`) |

`rank top` and `rank all` take `--tld` to filter by TLD.
The rank table is sorted by harmonic centrality, so `rank all -n 1000` is the top 1000 hosts without any sorting on your side.

```sh
ccrawl rank all --table <url> --tld com -n 1000
ccrawl rank all --table <url> -o jsonl > hosts.jsonl
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
| `crawl status` | Show daily crawl budget allocation across the five recrawl tiers |

### crawl seed

Streams the rank table and emits one seed URL per host.
Use `--max-tier` to restrict to high-priority hosts (tier 1 = top 100 K by harmonic rank, tier 5 = all).

```sh
ccrawl crawl seed -n 100 -o table
ccrawl crawl seed --max-tier 2 -n 1000000 -o jsonl > seeds.jsonl
ccrawl crawl seed --graph cc-main-2026-mar-apr-may --max-tier 3 -n 5000000
```

| Flag | Meaning |
|---|---|
| `--graph` | Web-graph release ID (default: latest) |
| `--max-seeds` | Maximum hosts to emit (default 10 000 000) |
| `--max-tier` | Skip hosts with tier higher than this (1-5, default 5 = all) |

### crawl fetch

Fetches one URL with the v2 crawler config: polite user-agent, brotli support, redirect following (up to 5 hops), 10 MB body limit, SHA-1 digest.

```sh
ccrawl crawl fetch https://golang.org/ -o json
ccrawl crawl fetch https://example.com/ --robots -o json
```

| Flag | Meaning |
|---|---|
| `--robots` | Check robots.txt before fetching |

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

Build and query a local BM25 full-text search index over any set of URLs.

| Subcommand | Does |
|---|---|
| `index build` | Fetch URLs, extract text, and build a BM25 inverted index |
| `index search <query>` | Query the index; results ranked by BM25 score |

### index build

Fetches each URL in parallel (8 workers by default), extracts clean text, tokenizes it, and writes a BM25 inverted index with per-document length normalization.
The index directory contains `terms.dat`, `postings.dat`, `forward.jsonl`, and `stats.dat`.

```sh
ccrawl index build --urls https://golang.org/,https://pkg.go.dev/ -o json
ccrawl index build --dir /data/idx --urls https://example.com/ --workers 16
ccrawl index build --dir /data/idx --input docs.jsonl
```

| Flag | Meaning |
|---|---|
| `--dir` | Directory to write the index into (default: `~/data/ccrawl/index`) |
| `--urls` | Comma-separated URLs to fetch and index |
| `--input` | JSONL file of `ForwardDoc` records to index directly |
| `--workers` | Parallel fetch workers (default 8) |

### index search

Queries the local index using BM25 scoring with per-document length normalization and optional link-graph boost.

```sh
ccrawl index search "golang web server"
ccrawl index search "machine learning" --dir /data/idx -n 20 -o json
```

| Flag | Meaning |
|---|---|
| `--dir` | Index directory to search (default: `~/data/ccrawl/index`) |

---

## api

Start the v2 HTTP REST API server.
The host store is loaded from the web-graph rank table on startup (top 1 M hosts).
Full-text search is available when `--index-dir` points to a built index.

```
GET /v2/host/{host}       enriched host profile
GET /v2/hosts?tld=&n=     top N hosts, optional TLD filter
GET /v2/search?q=&k=      BM25 full-text search (requires --index-dir)
GET /v2/health            health check
```

```sh
ccrawl api --addr :8080
ccrawl api --addr :8080 --index-dir /data/idx
```

| Flag | Meaning |
|---|---|
| `--addr` | Listen address (default `:8080`) |
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

## Global flags

These apply to every command.

| Flag | Short | Meaning | Default |
|---|---|---|---|
| `--crawl` | `-c` | Crawl ID, year (all crawls of that year), `latest`, `all`, an integer for the newest N, or a comma list | `latest` |
| `--output` | `-o` | Output format: `auto`, `table`, `json`, `jsonl`, `csv`, `tsv`, `url`, `raw`, `parquet` | `auto` |
| `--limit` | `-n` | Maximum records (0 = unlimited) | `0` |
| `--workers` | `-j` | Concurrency for downloads and scans | `8` |
| `--source` | | Bulk data source: `https` or `s3` | `https` |
| `--rate` | | Minimum delay between requests | `0s` |
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

## Exit codes

`0` success, `1` error, `2` usage error, `3` the query matched nothing, `75` temporary failure so run it again. See [exit codes](/reference/exit-codes/) for what to branch on and how to supervise a publish run.

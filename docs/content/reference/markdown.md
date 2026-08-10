---
title: "Markdown pipelines"
description: "The markdown export and refetch commands: schemas, HuggingFace layout, resume behaviour, and the three knobs that set throughput."
weight: 15
---

The `markdown` group turns Common Crawl WARC files into Markdown Parquet datasets and commits them to a HuggingFace dataset repo.
There are two pipelines and they answer different questions.

| Command | Source of the HTML | Use it when |
|---|---|---|
| `markdown export` | the HTML Common Crawl already captured | you want the crawl as it was, no live traffic |
| `markdown refetch` | a live fetch of every URL in the shard | you want today's page plus response metadata |

Both walk the same unit of work: one WARC file from a crawl's manifest is one shard, one shard becomes one Parquet file, and one Parquet file lands at a fixed path in the repo.

```sh
ccrawl markdown export --shards 0 --repo open-index/open-markdown-v3
ccrawl markdown refetch --shards 0 --repo open-index/open-markdown-refetch-v1
```

Set `HF_TOKEN` before either command, or pass `--push=false` to write Parquet locally and skip HuggingFace entirely.

## Selecting shards

`--shards` takes a single index, an inclusive range, a comma list, a mix of the two, or `all`.
Indices are 0-based positions in the crawl's `warc.paths.gz` manifest, which has around 90 000 entries per crawl.

```sh
ccrawl markdown export --shards 0
ccrawl markdown export --shards 0-49
ccrawl markdown export --shards 1,3,5
ccrawl markdown export --shards 0-9,20
ccrawl markdown export --shards all --limit 200
```

An out-of-range index is a usage error, not a silent skip.
`--limit` trims the resolved list after expansion, so `--shards all --limit 200` means the first 200 shards of the manifest.

## Converting a location set instead of whole shards

`--locations` replaces `--shards` as the source for `markdown export`.
Instead of downloading whole WARC files, it reads exactly the records a stream of index locations points at, using coalesced ranged GETs, and runs the same conversion pipeline over them.
This is the recovery pass shape: a columnar query picks out the pages you missed, and the export turns those pages and nothing else into Parquet.

```sh
ccrawl columnar locations --crawl CC-MAIN-2026-30 --lang vie -o jsonl \
  | ccrawl markdown export --locations - --lang vie --dedup-digest --push=false --out ./md
```

The input is the JSONL that `columnar locations` and `index` emit: one object per line with `filename`, `offset`, `length`, and `url`.
`--locations -` reads standard input, anything else is a path.
Lines that are not location records are skipped, so a mixed stream is fine.

A **part** is to a location run what a shard is to a full export: the unit that gets one Parquet file, one ledger entry, and one digest dedup set.
`--part-size` sets how many locations go into a part, defaulting to 50 000, which is a few hundred megabytes of Markdown.
Smaller parts mean more files and worse compression, larger parts mean a resumed run redoes more.
The stream is cut in order, so the same input always cuts the same way and a ledger from an interrupted run still means what it said.

`--gap` and `--max-span` tune the coalescing exactly as they do for [batch fetch](/reference/cli/#batch-mode): two records closer together than `--gap` are pulled in one request, and a coalesced range never grows past `--max-span`.
Records that will not fetch are counted and skipped rather than failing the part, because a recovery pass runs against an index that can disagree with the archive.

Two things do not combine with `--locations`.
The `wet` extractor reads WET shards, which have no record offsets to point at, so the pair is a usage error.
`--shards`, `--source-kind`, and the manifest fetch are all bypassed, since there is no manifest involved.

The `warc_bytes` a location run reports is what the ranged reads actually pulled off the wire, holes between coalesced records included, rather than a shard size.
Comparing it against the size of the shards those records live in is the whole argument for the flag.

## Output schemas

### open-markdown-v3, written by `markdown export`

| Column | Type | Meaning |
|---|---|---|
| `doc_id` | string | stable SHA-256 of the URL, first 16 bytes as hex |
| `url` | string | the URL Common Crawl captured |
| `host` | string | hostname taken from the URL |
| `crawl_date` | string | `WARC-Date` of the record, `YYYY-MM-DD` |
| `warc_record_id` | string | `WARC-Record-ID` of the source record |
| `html_length` | int | raw HTML body bytes before conversion |
| `markdown_length` | int | converted Markdown bytes |
| `markdown` | string | the converted Markdown |
| `language` | string | ISO 639-3 language detected in the Markdown, empty when there was too little text |
| `language_confidence` | double | the identifier's confidence, 0 to 1 |
| `extractor` | string | the engine that produced the text, as `name@version` |
| `simhash` | uint64 | 64 bit near duplicate fingerprint of the Markdown, 0 when there was too little text |

v3 appends the last four columns and changes nothing else, so a reader written against v2 loads a v3 file and gets every column it asks for.
The default repo moved to `open-index/open-markdown-v3` to keep the two schemas in separate repos.

### open-markdown-refetch-v1, written by `markdown refetch`

Everything the export schema has, plus the live response and its timings.
`doc_id` is computed the same way, so the two datasets join on it directly.

| Column | Type | Meaning |
|---|---|---|
| `doc_id` | string | stable SHA-256 of the URL, first 16 bytes as hex |
| `url` | string | the URL taken from the Common Crawl shard |
| `final_url` | string | URL after redirects |
| `host` | string | hostname |
| `ip_address` | string | server IP the fetch landed on |
| `crawl_id` | string | the CC crawl the URL came from, for example `CC-MAIN-2026-25` |
| `crawl_date` | string | `WARC-Date` of the original record |
| `warc_record_id` | string | `WARC-Record-ID` of the original record |
| `fetched_at` | int64 | Unix milliseconds of the live fetch |
| `status` | int | HTTP status code |
| `content_type` | string | `Content-Type` response header |
| `fetch_duration_ms` | int | whole fetch wall clock |
| `ttfb_ms` | int | time to first byte |
| `etag` | string | `ETag` response header |
| `last_modified` | string | `Last-Modified` response header |
| `resp_headers` | string | full response head, status line and headers |
| `body_length` | int | raw body bytes |
| `digest` | string | SHA-1 hex of the raw body |
| `html_length` | int | HTML body bytes, set only when the response is HTML |
| `markdown_length` | int | converted Markdown bytes |
| `markdown` | string | the converted Markdown |
| `language` | string | ISO 639-3 language detected in the Markdown |
| `language_confidence` | double | the identifier's confidence, 0 to 1 |
| `extractor` | string | the engine that produced the text, as `name@version` |
| `simhash` | uint64 | 64 bit near duplicate fingerprint of the Markdown, 0 when there was too little text |
| `error` | string | fetch error, empty on success |

A failed fetch still produces a row.
That is deliberate: a shard of Common Crawl URLs contains a lot of dead hosts, and knowing a URL is gone is a result worth keeping.
Filter on `error = ''` when you only want live pages.

## HuggingFace path layout

Both pipelines write the same layout, so the two repos load with the same code.

```
data/crawl=CC-MAIN-2026-25/000000.parquet
data/crawl=CC-MAIN-2026-25/000001.parquet
data/crawl=CC-MAIN-2026-25/000042.parquet
```

The number is the shard index, zero padded to six digits, so a file name maps straight back to a manifest position.
The `crawl=` component is a Hive partition, which means DuckDB and the `datasets` library both see `crawl` as a column without any extra configuration.

```sql
SELECT host, count(*)
FROM read_parquet('data/crawl=*/*.parquet', hive_partitioning=1)
WHERE crawl = 'CC-MAIN-2026-25'
GROUP BY host ORDER BY 2 DESC LIMIT 20;
```

Parquet is written with zstd compression.

## Resuming

Every committed shard is appended to a ledger file, `<out>/.committed` unless `--ledger` moves it.
On start the run reads the ledger and skips anything already in it, then reports what it found:

```
markdown: ledger /Users/you/data/ccrawl/markdown/CC-MAIN-2026-25/.committed already records 412 committed shards
```

So a killed run resumes by re-running the exact same command.
Nothing is recomputed and nothing is committed twice.

Local Parquet files are deleted after they commit, which is what keeps a `--shards all` run inside a fixed disk budget.
Pass `--keep-parquet` to hold on to them, and watch the disk if you do.
`--min-free-gb` (default 2) pauses new downloads when free space drops below the threshold rather than failing mid-shard.

A per-shard conversion failure is logged and counted, and the run keeps going.
Only a fatal commit failure or a cancelled context aborts, and the final line always reports the split:

```
markdown: 48 committed, 2 skipped, 0 failed of 50 | 1843229 rows | html=61.2 GiB md=8.4 GiB parquet=2.1 GiB | 41m18s elapsed (72.6 shards/hour)
```

### Exit codes

The markdown pipelines resume from the ledger, so they do not use the supervised-restart exit code.
They exit 0 on success and 1 on a fatal error, and you re-run the same command to continue.

`urls publish` and `domains publish` are the commands that exit 75, because those runs commit into a single growing dataset and have a stall clock.
See [exit codes](/reference/exit-codes/) for the full contract.

## Extractors

`--extractor` chooses the engine that turns a captured page into the text in the `markdown` column.

| Engine | Source | What it does |
|---|---|---|
| `h2m` (default) | WARC | go-trafilatura tuned for recall, rendered as GitHub-flavored Markdown |
| `readability` | WARC | go-readability plus mdconv, the engine `open-markdown-v2` shipped |
| `raw` | WARC | the whole document as Markdown, no boilerplate removal |
| `wet` | WET | the plain text Common Crawl already extracted, passed through unchanged |

```sh
ccrawl markdown export --shards 0 --extractor readability --push=false --out ./md
ccrawl markdown export --shards 0 --source-kind wet --extractor wet --push=false --out ./md
```

Which engine you use is a corpus quality decision, so it is a flag rather than a build time constant.
The same shard through two engines is two different corpora, and the only way to find out which one suits a downstream task is to build both and compare them.

Every row records the engine in the `extractor` column as `name@version`, and the dataset card names it too.
The version is there because extraction changes between releases: a dataset that only says `h2m` cannot explain why two shards built months apart disagree about the same page.
The WET engine has no version of ours to record, so it is stamped with the crawl instead, as `wet@CC-MAIN-2026-30`.

`--source-kind` picks the manifest a run reads.
`warc` takes `warc.paths.gz` and extracts the HTML itself, `wet` takes `wet.paths.gz` and uses the text Common Crawl already extracted.
It defaults to whatever the extractor needs, so it rarely has to be given, and a pairing that cannot work is a usage error rather than a silently reinterpreted run:

```
$ ccrawl markdown export --shards 0 --source-kind wet --extractor h2m
ERROR  Extractor h2m reads warc shards, so it cannot be used with --source-kind wet.
```

`markdown refetch` takes `--extractor` too, but only the WARC engines.
A live fetch returns HTML, so there is no Common Crawl text for the WET engine to pass through, and asking for it exits 2.

### What the engines actually cost

Shard 0 of `CC-MAIN-2026-30`, the same 3.5 GB of HTML through each engine on the same machine:

| Engine | Rows | Markdown | Parquet | Mean doc | Wall clock |
|---|---|---|---|---|---|
| `h2m` | 20861 | 91.9 MB | 46.8 MB | 3990 B | 1m49s |
| `readability` | 20666 | 108.5 MB | 51.8 MB | 4865 B | 1m58s |
| `raw` | 20806 | 446.2 MB | 328.2 MB | 20724 B | 2m55s |
| `wet` | 21165 | 161.9 MB | 80.0 MB | 6604 B | 15s |

`raw` produces roughly five times the text of `h2m` from the same pages, which is a fair measure of how much of a web page is not the page.
It keeps the nav bars and the footers, which sounds useless and is not: every extractor is a lossy judgement about what a page was for, and the output alone never says where that judgement went wrong.
Raw is the control you measure the others against.

The three WARC engines agree on which pages are there and disagree on what is on them, which is the property that makes them comparable.
20353 documents are in all three, 97.6 percent of what `h2m` produced, and of the documents `h2m` and `readability` share only 284 have byte identical text.
The rest of the row count is pages one engine extracted nothing from and skipped rather than writing an empty row, and the engines do not give up on quite the same pages.

`wet` is in a different cost class, since the text is already extracted and a WET file is a fraction of the size of the WARC it came from.
It is also somebody else's extraction decision, boilerplate included, and it is plain text rather than Markdown, so headings, links, and lists are gone.
Use it when you want volume cheaply and the structure does not matter.

## Language filtering

Every row is labelled with a detected language, and `--lang` keeps only one of them.

```sh
ccrawl markdown export --shards 0-9 --lang vie --push=false --out ./md
ccrawl markdown refetch --shards 0-9 --lang vie --min-lang-confidence 0.9
```

The identifier reads the extracted Markdown, not the raw HTML and not the page's own `lang` attribute.
That is the whole point of doing it here: the columnar index already carries CLD2 labels computed by Common Crawl over the raw HTML, and those labels are unreliable for low resource languages and missing outright on a good share of rows.
Running the identifier over the Markdown asks the question about the text that ends up in the dataset.

`--min-lang-confidence` defaults to 0.8, which is the identifier's own reliability threshold.
Raise it when precision matters more than volume.

Documents with too little text to identify come out with an empty `language`, and a `--lang` filter drops them.
A filtered export asks for documents known to be in one language, and "we could not tell" is not that.
Without `--lang` nothing is dropped at all and the columns are still filled in, so an unfiltered shard can be filtered later without extracting it again.

Both commands report the drop rate and the detected mix at the end of the run.
Three shards of `CC-MAIN-2026-30` filtered to Vietnamese look like this:

```
markdown: 3 committed, 0 skipped, 0 failed of 3 | 647 rows | html=110.1 MB md=3.7 MB parquet=1.5 MB | 4m46s elapsed
language: --lang vie kept 647 of 62824 documents, dropped 62177 (99.0%)
language: detected eng=24266 fra=4165 deu=3915 rus=3786 spa=3388 cmn=2984 jpn=2702 por=1814
```

Those two lines only print in text mode, so the same numbers ride on the journal events as `lang_dropped` and `lang_counts` for a run whose progress is JSON.
The breakdown is worth reading even when you did not filter, because it tells you what the crawl actually holds: Vietnamese is roughly 1 percent of a general Common Crawl shard, so a `--lang vie` run throws away 99 percent of it by design.

The failure mode to know about is mojibake.
A page served as UTF-8 but labelled Latin-1 comes out of extraction as garbled text, and garbled Cyrillic in particular carries enough accented Latin characters to read as Vietnamese.
In a 200 document manual sample of the run above, 199 were Vietnamese and the one miss was a mojibake Ukrainian page.
Romanized Vietnamese written without tone marks is detected as Vietnamese, which is usually what you want and worth knowing if it is not.

Two limits worth knowing before you rely on it.
`--lang` cannot be combined with `markdown refetch --fetch-only`, because fetch-only never produces the Markdown the identifier reads, and asking for both is a usage error rather than a silently ignored flag.
And this is a coarse pre-filter: a trigram identifier separates Vietnamese from Malay well enough to cut a corpus down to something worth looking at, and it is not a substitute for a language specific classifier.

`ccrawl content lang <url>` runs the same identifier on a single URL and prints the text it judged, which is the way to find out why a page was kept or dropped.

## Deduplication

A crawl shard holds the same page more than once, and there are two different problems hiding in that sentence.
The cheap one is byte identical payloads, which is what `--dedup-digest` drops.
The expensive one is pages that differ only in a session id or a timestamp, which is what the `simhash` column exists to let you find later.

```sh
ccrawl markdown export --shards 0 --dedup-digest --push=false --out ./md
ccrawl markdown refetch --shards 0 --dedup-digest --push=false --out ./md
ccrawl dedup ./md
```

`--dedup-digest` skips a record whose payload digest has already been seen.
It runs before extraction, so a duplicate costs a hash lookup instead of an HTML parse, and on export it uses the digest the WARC already carries rather than recomputing one.
The scope is one shard, not the whole run.
That is deliberate: shards are converted in parallel, so a run wide set would make the choice of which copy survives depend on scheduling, and over `--shards all` the set would grow without a bound anyone set.
On refetch the drop happens in the single writer loop, and failed fetches are exempt, because two dead hosts share an empty body and collapsing them would hide the failure.

One shard of `CC-MAIN-2026-30` measured both ways:

| Run | Rows | `digest_dropped` | Parquet |
|---|---|---|---|
| without `--dedup-digest` | 20861 | | 49.7 MB |
| with `--dedup-digest` | 20819 | 83 | 48.3 MB |

An independent scan of the same WARC found 21278 HTML response records holding 21195 distinct payloads, so 83 duplicate payloads in 28 groups, which is the pipeline's number exactly and zero false drops.
Only 42 of those 83 show up as fewer rows because the other 41 extract to nothing and would never have been rows in the first place.
Under 1 percent is the honest expectation for a single shard: byte identical duplicates within one WARC are rare, and the interesting redundancy on Common Crawl is across shards and across crawls, which this flag does not attempt.

The `simhash` column is a 64 bit fingerprint over overlapping three word shingles of the Markdown, and two documents that differ in a few phrases differ in a few bits.
It is stored rather than acted on.
Dropping near duplicates during a run means deciding, once and irreversibly, which copy is the good one, and that decision belongs to whoever knows what the dataset is for.
The column costs a hash per row and makes the decision available afterwards.

`ccrawl dedup` reads it back and reports, without rewriting anything:

```
20,861 rows in 1 files
  exact duplicates          165 in 113 clusters, 139.7 kB
  near duplicates            24 in 23 clusters, 171.0 kB  (distance <= 3)
  redundant                 189  (0.9% of rows)
  no fingerprint              2  (too short to hash, left alone)
```

That is 0.68 seconds over the shard, because it reads three columns and leaves the rest of the parquet on disk.
The exact count is higher than the 83 above for a reason worth knowing: different HTML can extract to identical Markdown, and identical HTML at two URLs can extract to different Markdown, since relative links are resolved against the page URL.
Payload identity and text identity are not the same question, and `--dedup-digest` answers the first while `ccrawl dedup` answers the second.

Two things were learned building this and are worth writing down.

Charikar's original simhash weights each feature by how often it occurs, and on web text that is a trap.
A mojibake page, one where UTF-8 was served as Latin-1, extracts to a handful of replacement sequences repeated thousands of times, and a count weighted fingerprint is then decided almost entirely by those few features.
Two such pages measured here shared 6.5 percent of their shingles and still came out one bit apart, which pulled 68 unrelated documents into a single cluster by chaining.
Counting each distinct shingle once, however often it occurs, put that pair 30 bits apart and cut the shard's near duplicate count from 67 to 26.

The other is that a 64 bit fingerprint decided by fewer than 64 features is decided by noise, so the near pass ignores documents under 512 bytes of Markdown.
Short pages still get a fingerprint and still cluster as exact duplicates, which is the only claim worth making about a stub.

## Tuning: parallel, workers, commit-batch

Three flags set throughput, and they control three different resources.

| Flag | Resource | Default (export) | Default (refetch) |
|---|---|---|---|
| `--parallel` | network and disk, shards in flight | 3 | 2 |
| `--workers` | CPU, HTML to Markdown conversion | `NumCPU` | `NumCPU` |
| `--commit-batch` | HuggingFace round trips per commit | 1 | 1 |

`--parallel` is how many shards download and process at once.
Raise it when the download is the bottleneck, which it usually is on a fast machine with a slow link.
Each in-flight shard holds a WARC on disk, so `--parallel 8` on 1 GiB shards means roughly 8 GiB of working set.

`--workers` is a single conversion pool shared across every in-flight shard, not a per-shard pool.
That is the point: raising `--parallel` does not oversubscribe the cores.
Leave it at 0 to get `NumCPU` unless you are sharing the box.

`--commit-batch` is how many finished Parquet files go into one HuggingFace commit.
The commit round trip is slow and the HF API takes one commit at a time per repo, so committing one file at a time throttles a fast run.
A background committer batches finished shards and commits them off the critical path, which means a larger batch trades commit frequency for throughput.
Use 1 for a short run where you want each shard durable immediately, 10 or more for a long `--shards all` run.

A reasonable starting point on a machine with a fast link:

```sh
ccrawl markdown export --shards all --parallel 4 --commit-batch 10
```

### Refetch has a fourth knob

`markdown refetch` also drives a live fetch, so it adds `--fetch-workers`, the number of concurrent requests per shard.
Left at 0 it derives a value from the process file descriptor limit divided across `--parallel`, capped at 3000, and prints what it picked:

```
refetch: fd limit 1048576
refetch: auto fetch-workers=3000 (per shard, 2 shards in parallel)
```

The command raises its own soft `RLIMIT_NOFILE` at startup, so you do not need a shell `ulimit` change.
`--rate` sets a per-host request rate in requests per second, 0 meaning unlimited, and `--max-redirects` caps redirect hops at 5 by default.

`--fetch-only` stores the raw HTML and skips conversion, so the fetch runs at full speed and you convert offline later over the `html` column.
That is the right shape when you are fetch-bound and want the network phase to finish as fast as the hosts allow.

Downloaded WARC shards are cached under `<data-dir>/ami/warc` so a re-run skips the download.
Move it with `--warc-cache-dir` or turn it off with `--no-warc-cache`.

Refetch reports where the time went, which is the fastest way to find out what to tune:

```
phase totals: extract=61s fetch=1840s convert=402s export=88s publish=120s
phase avg/shard: extract=30s fetch=920s convert=201s export=44s | fetch-only 431 pages/s (urls/fetch-sec)
failures: 184203 total | dns=91002 timeout=64881 refused=19422 skip=1102 other=7796
```

## Flags

Shared by `markdown export` and `markdown refetch`:

| Flag | Meaning |
|---|---|
| `--shards` | Shard range: `N`, `N-M`, `N,M`, or `all` |
| `--repo` | HuggingFace dataset repo, `org/name` |
| `--out` | Directory for Parquet files |
| `--push` | Commit each shard to HuggingFace (default true, `--push=false` to stay local) |
| `--limit` | Process at most this many shards (0 = all) |
| `--parallel` | Shards in flight at once |
| `--workers` | Conversion workers shared across shards (0 = `NumCPU`) |
| `--commit-batch` | Parquet files per HuggingFace commit |
| `--keep-parquet` | Keep local Parquet after it commits |
| `--min-free-gb` | Pause new downloads below this much free disk |
| `--ledger` | Resume ledger path (default `<out>/.committed`) |
| `--lang` | Keep only documents detected as this ISO 639-3 language, for example `vie` |
| `--min-lang-confidence` | Confidence a document has to clear for `--lang` (default 0.8) |
| `--extractor` | Conversion engine: `h2m`, `readability`, `raw`, or `wet` (default `h2m`, and `wet` is export only) |
| `--dedup-digest` | Skip records whose payload digest was already seen in this shard |

`markdown export` only:

| Flag | Meaning |
|---|---|
| `--skip-errors` | Continue past per-shard failures instead of aborting |
| `--source-kind` | Manifest to read, `warc` or `wet` (default: whatever the extractor needs) |
| `--locations` | Convert the records in this JSONL location stream instead of whole shards, `-` for stdin |
| `--part-size` | Locations per Parquet part for `--locations` (default 50000) |
| `--gap` | Coalesce `--locations` records closer together than this many bytes |
| `--max-span` | Cap on the size of one coalesced ranged read |

`markdown refetch` only:

| Flag | Meaning |
|---|---|
| `--fetch-workers` | Concurrent fetches per shard (0 = auto from the fd limit) |
| `--fetch-only` | Store raw HTML and skip conversion |
| `--rate` | Per-host request rate limit in req/s (0 = unlimited) |
| `--max-redirects` | Maximum redirect hops per fetch (default 5) |
| `--warc-cache-dir` | Where to cache downloaded WARC shards |
| `--no-warc-cache` | Do not cache downloaded shards |

For a worked run, see the [Markdown corpus guide](/guides/markdown-corpus/).

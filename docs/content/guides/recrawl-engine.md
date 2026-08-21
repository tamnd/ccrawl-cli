---
title: "Building a recrawl engine"
description: "What the crawl group does today: pick seed hosts from the web graph, run a resumable crawl that writes WARC, and see the tier budget."
weight: 70
---

`ccrawl crawl` is the recrawl side of the tool: decide which hosts are worth crawling again, and fetch pages the way a well-behaved crawler should.

What ships right now is the whole path: pick the seeds, run the crawl, get WARC out.
`crawl run` drives a resumable frontier, so a run that is killed picks up where it stopped.
The [planned](#planned) section at the bottom says what is still missing.

## What ships today

| Command | Does |
|---|---|
| `crawl seed` | Stream the web-graph host rank table and emit one seed URL per host |
| `crawl fetch <url>` | Fetch one URL with the crawler config, optionally checking robots.txt |
| `crawl run` | Crawl a seed list through a resumable frontier and write WARC |
| `crawl status` | Show the daily page budget across the five recrawl tiers |

Tier assignment itself lives in `ccrawl sched`, covered in [recrawl scheduling](/guides/scheduling/).

## Seeding from the host rank table

`crawl seed` streams the host rank table from a web-graph release and emits one seed per host: the host, `https://{host}/`, its tier, and its harmonic centrality as the priority.
Hosts come out in rank order, so the most central hosts arrive first and you can stop reading whenever you have enough.

```bash
ccrawl crawl seed -n 100 -o table
ccrawl crawl seed --max-tier 2 -n 1000000 -o jsonl > seeds.jsonl
ccrawl crawl seed --graph cc-main-2026-mar-apr-may --max-tier 3 -o jsonl > seeds.jsonl
```

| Flag | Default | Purpose |
|---|---|---|
| `--max-tier` | 5 | Skip hosts at a tier above this (2 = top million, 5 = everything) |
| `--max-seeds` | 10 000 000 | Hard cap on hosts emitted |
| `--graph` | latest | Web-graph release to read ranks from |

Note the seed is the host root, one URL per host, not every URL the host has in the index.
If you want per-URL seeds, that comes from the [columnar index](/guides/columnar-index/) instead.

One thing worth knowing about the tier column here: `crawl seed` has ranks but no measured change rates, so it assumes 0.5 for every host.
Tier 1 needs a change rate above 0.8, so nothing seeded this way lands in tier 1, and `--max-tier 1` says so instead of reading the whole table to emit nothing.
Feed real change rates in with `ccrawl sched diff` if the tier split matters to you.

The `-n` limit and `--max-seeds` do the same job from different ends.
`-n` is the global record limit that every ccrawl command has, `--max-seeds` is the pipeline's own cap.
Either one alone is fine.

Tier 1 is around 100 000 hosts, tier 2 around a million, tier 5 is the whole 262 million host tail.
Start at `--max-tier 2` and grow.

## Fetching one URL

`crawl fetch` takes exactly one URL argument.
It fetches with the crawler user agent (`CCrawl/2.0`), follows up to 5 redirects, caps the body at 10 MB, and reports the status, the final URL, the content type, a SHA-1 digest of the body, and the outbound link count.

```bash
ccrawl crawl fetch https://golang.org/ -o json
ccrawl crawl fetch example.com                  # scheme is added if you leave it off
ccrawl crawl fetch https://example.com/ --robots
ccrawl crawl fetch https://example.com/ --warc-dir warc/
```

`--robots` fetches and parses the host's `robots.txt` first and refuses the fetch if the path is disallowed.
It is off by default because a single manual fetch of a URL you already chose does not need it.
Turn it on for anything automated.

The parser follows [RFC 9309](https://www.rfc-editor.org/rfc/rfc9309.html): `*` and `$` wildcards, longest match wins with an allow breaking ties, the most specific user agent group taking precedence over `*`, and `Sitemap` lines collected.
A missing `robots.txt` leaves the host open, and a `robots.txt` that could not be fetched at all, whether the host returned a 5xx or never answered, disallows the whole host until the fetch is retried a few minutes later.
That last rule is the spec's and it is deliberate: a site whose robots endpoint is down cannot tell you to stop, so you stop.

### Archiving the fetch

`--warc-dir` writes the fetch into a `.warc.gz` file in that directory and prints the path in the record.
What comes out is a WARC/1.0 file the same shape as the ones Common Crawl publishes: a warcinfo record naming the tool and the exact command, then a request record and a response record linked to each other with `WARC-Concurrent-To`.
Every record carries a `WARC-Block-Digest`, every response also carries a `WARC-Payload-Digest` over the HTTP body, both `sha1:` and base32, and the address the server answered from goes in `WARC-IP-Address`.
When the 10 MB body cap cuts a response short the record says so with `WARC-Truncated: length` rather than pretending the page was that size.

One thing to know about what is stored. Go decodes gzip and undoes chunking while it reads, so the bytes in the record are the decoded body and not the bytes that crossed the wire.
The stored headers are rewritten to match: `Content-Length` is the length of the body actually stored, and a `Content-Encoding` or `Transfer-Encoding` that no longer describes anything is dropped.
The record is self consistent, which is what a reader needs, but it is not a byte for byte capture of the connection.

Each file opens at `ccrawl-crawl-00000.warc.gz` and takes the first sequence number the directory does not already have, so fetching into the same directory twice adds files rather than overwriting them.
Read the output back with `ccrawl parse`, or with anything else that reads WARC:

```bash
ccrawl crawl fetch https://example.com/ --warc-dir warc/
ccrawl parse warc/ccrawl-crawl-00000.warc.gz --type response -o jsonl
warcio check -v warc/ccrawl-crawl-00000.warc.gz
```

The digest is the useful part for recrawl work.
Fetch a URL, compare the digest with the `content_digest` the columnar index has for the same URL, and you know whether the page changed since the crawl without diffing any text.

Because it is one URL per invocation, driving a list means driving the loop yourself:

```bash
# fetch the top 50 seeds, one at a time, politely
ccrawl crawl seed -n 50 -o jsonl \
  | jq -r .url \
  | while read -r u; do ccrawl crawl fetch "$u" --robots -o jsonl; sleep 1; done \
  > pages.jsonl
```

That is a shell loop, not a crawler.
It has no shared frontier, no per-host politeness across workers, and no resume.
It is fine for a few hundred URLs and wrong for a few million, which is what `crawl run` is for.

## Running a crawl

`crawl run` is the loop: it reads a seed file, walks the frontier, fetches, archives, and puts the outlinks it finds back in the queue.

```bash
ccrawl crawl seed -n 100000 -o jsonl > seeds.jsonl
ccrawl crawl run --seeds seeds.jsonl --out warc/ --state crawl.db --max-pages 100000 -j 64
```

`--seeds` takes the JSONL `crawl seed` writes, so the harmonic centrality in it becomes the queue priority and the most central hosts are crawled first.
It also takes a plain list of URLs, one per line, and `-` for stdin.

Three things are worth knowing before you point it at the open web.

Politeness is per host, and it is the longer of `--delay` and whatever the host's own `Crawl-delay` asks for.
Workers share one frontier, and the frontier hands out at most one URL per host per delay, so `--workers 64` means 64 hosts in flight and never 64 requests at one host.

robots.txt is fetched once per host, cached, and enforced.
How long it is cached is the host's decision where it makes one: a `Cache-Control` header on the response sets the lifetime, floored at a minute so a `no-store` does not mean a fetch per page, and capped at a day so a host asking to be remembered for a year does not get it.
A host that says nothing gets the day.
A host that answers 5xx or does not answer at all is treated as fully disallowed for five minutes, which is what RFC 9309 asks for, so an outage is not read as an invitation.
`--no-robots` turns the whole check off on `crawl run`, and you should have a reason.
`recrawl run` is the other way round: the check is off there unless you pass `--robots`, because a recrawl of a domain corpus pays it on every page rather than once per host, and a third of those hosts never answer it.

The cache is bounded by both an entry count and a byte budget, and it evicts least recently used, because a fleet run walks more hosts than any machine can hold parsed rules for.
The defaults are 200 000 entries and 64 MB, and a run that walks 50 000 hosts through a cache sized for 1000 holds 1000 and grows its heap by tens of megabytes rather than by the number of hosts.
Every run prints what robots cost it, since the check is an extra request per host and on a domain corpus that is one request for every page:

```
recrawl run: robots: 258 hosts fetched, 0 saved by the cache, 16 refused, 39 unreachable
```

Refused and unreachable are counted apart on purpose.
Both stop the page, but one is a site telling you no and the other is a site that could not be asked, and reading a log where those are the same number tells you nothing about which you are looking at.

When anything was unreachable, a second line says why:

```
recrawl run: robots failures: dns 12, timeout 61, refused 18, 5xx 2, other 0 of 93 unreachable
```

That line only matters when the first one looks wrong, and then it is the whole diagnosis.
A run where a tenth of the hosts could not be asked and a run where two thirds of them could not be asked print the same shape of summary, and the difference between them is entirely in this breakdown.
A name that did not resolve is ours to fix, a connection the kernel would not open is the machine, a timeout is the host or the link to it, and a 5xx is a site that is up and telling us nothing.

## DNS is a per page cost

On a link crawl the frontier keeps coming back to hosts it already knows, so DNS is paid once and amortised over everything after it.
On a recrawl of a domain corpus every row is a different host, so every page is a fresh name, and the resolver is in the hot path for the whole run.
Handing 256 concurrent lookups to the system stub does not work: it drops about a third of them without saying so, and a dropped lookup arrives at the crawl as a host that would not talk to us, which is indistinguishable in a log from a host that is genuinely gone.

The run therefore does its own resolution, bounded and cached, and it says what it did:

```
recrawl run: dns: 1607 hosts looked up, 141 answered from the cache, 56 unresolved of which 37 do not exist, 32 open at the busiest
```

Four things are going on in that line.

Lookups are bounded, and the bound is what does the work rather than anything clever layered on top of it.
The default is one eighth of the worker count, floored at 16 and capped at 128, so `--workers 256` asks at most 32 questions at once no matter how many workers want an answer.

Each lookup that misses the cache is raced across the Go resolver and three public resolvers, and the first usable answer wins.
Racing on its own does not help, and measured on its own it is no better than the stub; racing under the bound is what turns a five second pass with a third of the names lost into a one and a half second pass with none of them lost.

Answers are cached for the run, which is what makes robots.txt and the page one lookup rather than two.
Failures are cached only when every resolver in the pool agreed the name does not exist, because negative caching a lookup that was merely dropped writes a live host off for the rest of the run.

The last number is the one to read when a run is slower than it should be.
It is the most lookups that were ever open at once, and against the bound it says whether DNS has become a queue.
A peak that sits exactly on the bound for a whole run means workers are waiting for a lookup slot rather than for the web, and the fix is a resolver with more headroom, which in a fleet means a caching resolver on the machine rather than three public ones over the internet.

One connection layer is shared by robots.txt and the page, sharded by host so both requests land on the same pool.
They used to go through different transports, so every page paid a fresh TCP and TLS handshake to a host the run had finished talking to a second earlier.

## The sink is not on the worker path

Every worker used to write its own row: take the mutex, hand the capture over, let go.
That is correct and it is also the shape that stops a wide pool getting any wider.
Measured on the live domain list at 256 workers, writing was 47 percent of the pool, and the run reported that 100 percent of that was time spent waiting for the mutex rather than time the sink was doing anything.
At 512 workers it was 56 percent, again all of it queueing, and the run held fewer pages a second than the narrower one did.

So the sink has its own goroutine and the workers have a channel.
A worker renders the page, hands the finished row over, and goes back to the network, which is the only thing it is any good at.
The timing line splits the two costs, because they have different fixes:

```
worker time: robots 0%, host clock 0%, fetching 61%, extracting 3%, queueing to write 1%, idle 6% of 256 workers, with the writer busy 8% of the run
```

Queueing to write is what the pool paid to hand rows over, and it is normally near zero.
When it is not, the sink has fallen behind the pool and the queue between them has backed up, and that is the moment to make the sink faster.
The writer figure is the sink's own time as a share of the run, and it answers the other half of the question: a pool queueing to reach a sink that is idle wants a wider writer, and a pool queueing to reach a sink that is busy every second of the run wants a faster one.

Rendering stays in the worker rather than travelling with the row.
It is the one phase that is CPU rather than network, so 256 workers doing it in the gaps their fetches leave is parallel, and one writer doing it is a queue with a hundred milliseconds of HTML parsing in it.

The resume promise moved along with the write.
A row is retired from the flight set when it is safely with the sink, and the checkpoint never steps past an unretired row, so retiring happens in the writer and not in the worker.
Retiring in the worker and writing later would let a checkpoint step over a row that is still sitting in the channel, and a kill at that moment loses the page with nothing anywhere saying so.
It does mean a kill replays more than one batch, because the pool now runs further ahead of the position the checkpoint can safely name, and `replayBound` in the engine writes down exactly how much: the batch, the buffer between the reader and the pool, one row per worker, and the writer queue.
At the fleet settings that is a fifth of a minute's fetching and it replays rather than skips, which is the direction this is built to fail in.

## One writer is a ceiling too

Taking the sink off the worker path did not make the sink faster, and once the pool stopped queueing for the mutex the writer became the thing everything waits for.
At 256, 512 and 1024 workers the same run holds 33 to 34 pages a second with the writer busy 91 to 94 percent of the wall clock, and a rate that does not move when you triple the pool is a rate set somewhere else.

What that goroutine does with the time is the interesting part.
Measured under `/usr/bin/time -v`, the sink costs 6.0 ms of CPU a page, and at 34 pages a second it is holding 27 ms of wall clock to spend it.
Four fifths of the writer's time is waiting, for a core on a box that is running two other crawlers and for the disk at the end of a shard, and waiting is the kind of work that parallelises.

So `--writers` opens more than one shard at a time, each with its own encoder, its own buffer and its own goroutine, and rows go round them as they arrive.
Nothing in a Parquet file requires the row beside it to be in the same file, and the published corpus was always a directory of independently readable shards, so a reader sees more files of much the same size and nothing else changes.

The parts rotate together rather than each on its own, and that is a checkpoint decision rather than a filing one.
A checkpoint may only move when everything behind it is readable, and a Parquet shard is unreadable until its footer is written, so parts rotating independently would have to be caught empty at the same instant for a sync to ever report durable.
At fleet speed that never happens, and a run whose checkpoint never moves replays from row zero after every restart.
One part reaching `--shard-size` therefore seals all of them, which costs a few percent of the shard size on the parts that were not yet full and buys back a checkpoint that advances once per rotation exactly as the single writer's does.

Measured on server3 against the live domain list, 20 000 rows a run from a fresh offset each time, on an afternoon when the box was carrying a load average around 25:

| writers | pages a second | queueing to write | idle | peak RSS |
| --- | --- | --- | --- | --- |
| 1 | 10.6 | 36% | 13% | 3.1 GB |
| 2 | 24.3 | 22% | 24% | 4.0 GB |
| 4 | 29.0 | 13% | 26% | 5.4 GB |
| 8 | 29.1 | 10% | 27% | 7.7 GB |

The knee is at four, and what moves with it is the shape of the run rather than only the number: workers queueing to reach the sink fall from 36 percent of the pool to 10, and idle rises from 13 percent to 27, which is the pool going back to waiting on the network like it is supposed to.

Then the same pair of settings on the same box two hours later, load average around 14, alternating so neither gets the good half of the afternoon:

| writers | pages a second | writer share | peak RSS |
| --- | --- | --- | --- |
| 4 | 27.4 | 24% each | 4.1 GB |
| 1 | 25.2 | 65% | 2.7 GB |
| 4 | 25.6 | 23% each | 4.5 GB |
| 1 | 24.1 | 71% | 2.3 GB |

Six percent, which on this box is noise, for about two gigabytes of resident memory.

Both of those are the same flag doing exactly what it says, and the difference between them is the whole rule for using it.
The fanout buys back time the writer was spending waiting, so it is worth the memory when the writer is pinned in the high eighties or above and worth nothing at all when it is not.
That is why the default is one and why the figure to read is the writer share on the timing line rather than the page rate: the rate tells you the run is slow, and the writer share tells you whether this is the reason.
Raise it one step at a time, since every part is an open encoder with its own zstd workers and its own row buffer, and the memory is real on a box that has none spare.

The frontier lives in `--state`, and it is the resume story.
The queue, the seen set and the per-host clocks are all in that file, committed as the crawl goes, so a run that is killed halfway through 100 000 pages restarts on the remainder rather than on the whole list.
Point a second run at the same state file with the same seeds and it crawls what is left.

```bash
# stop it, restart it, and watch it carry on rather than start over
ccrawl crawl run --seeds seeds.jsonl --out warc/ --state crawl.db -j 64
```

What comes out in `--out` is ISO 28500 WARC, the same records `crawl fetch --warc-dir` writes, rotated at `--warc-size` and named with `--prefix`.
Every page is also emitted as a record on stdout, so `-o jsonl` gives you a log of the crawl next to the archive.

Two limits keep a run bounded: `--max-pages` stops after that many fetches, and `--max-depth` decides how far from a seed links are followed, with the default of 0 meaning the seeds and nothing else.
`--same-host` keeps the crawl to the hosts the seeds named, which is what you want when the seed list is the point rather than a starting position.

## Crawl budget

`crawl status` prints the daily page budget across the five tiers, assuming 10 000 pages per second sustained, which is 864 million pages a day.

```bash
ccrawl crawl status -o table
```

It is a planning tool, not a measurement.
It tells you what a full recrawl at each tier interval would cost so you can size the thing before building it.

## Feeding a search index

`ccrawl index build` builds a local BM25 index. It takes a list of URLs to fetch itself, or a JSONL file of documents you already have:

```bash
ccrawl index build --dir idx/ --urls "$(ccrawl crawl seed -n 20 -o jsonl | jq -r .url | paste -sd,)"
ccrawl index search --dir idx/ "golang concurrency"
```

With `--urls` it does its own fetching and text extraction, with `-j` for concurrency.
With `--input docs.jsonl`, or `--input -` for stdin, it reads documents that already carry their text.
Note that `crawl fetch` output is not one of them: those rows are fetch metadata and hold no page text.
It is a reference implementation with a corpus ceiling of a few hundred thousand documents.
See the [search index guide](/guides/search-index/) for the details.

## Planned

`crawl run` crawls, and there is a list of things it does not do yet:

- no distributed frontier, so a run is one machine and one state file
- no sitemap discovery, so the only way in is the seed list and the links on the pages
- no per-tier scheduling inside a run, so `sched assign` picks the tiers and you pass the seeds yourself
- no CDX or index write on the way out, so the WARC files are indexed after the fact with `ccrawl index`

If the URL set is fixed up front rather than discovered from links, [`ccrawl markdown refetch`](/guides/markdown-corpus/) is the better fit.
It takes the URL list from a WARC shard, fetches every page live at high concurrency, and writes Parquet.

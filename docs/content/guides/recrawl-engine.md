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

robots.txt is fetched once per host, cached for a day, and enforced.
A host that answers 5xx or does not answer at all is treated as fully disallowed for five minutes, which is what RFC 9309 asks for, so an outage is not read as an invitation.
`--no-robots` turns the whole check off, and you should have a reason.

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

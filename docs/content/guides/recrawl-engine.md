---
title: "Building a recrawl engine"
description: "What the crawl group does today: pick seed hosts from the web graph, fetch a URL politely, and see the tier budget."
weight: 70
---

`ccrawl crawl` is the recrawl side of the tool: decide which hosts are worth crawling again, and fetch pages the way a well-behaved crawler should.

Read this first, because it decides whether the guide is useful to you today.
What ships right now is the seeding half and a single-URL fetcher that can write WARC.
There is no bulk crawl loop yet and no shared frontier behind a command.
The [planned](#planned) section at the bottom says what is coming and where to follow it.

## What ships today

| Command | Does |
|---|---|
| `crawl seed` | Stream the web-graph host rank table and emit one seed URL per host |
| `crawl fetch <url>` | Fetch one URL with the crawler config, optionally checking robots.txt |
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
| `--max-tier` | 5 | Skip hosts at a tier above this (1 = top 100 K only, 5 = everything) |
| `--max-seeds` | 10 000 000 | Hard cap on hosts emitted |
| `--graph` | latest | Web-graph release to read ranks from |

Note the seed is the host root, one URL per host, not every URL the host has in the index.
If you want per-URL seeds, that comes from the [columnar index](/guides/columnar-index/) instead.

One thing worth knowing about the tier column here: `crawl seed` has ranks but no measured change rates, so it assumes 0.5 for every host.
Tier 1 needs a change rate above 0.8, so nothing seeded this way lands in tier 1.
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
It is fine for a few thousand URLs and wrong for a few million, which is exactly the gap the planned `crawl run` fills.

## Crawl budget

`crawl status` prints the daily page budget across the five tiers, assuming 10 000 pages per second sustained, which is 864 million pages a day.

```bash
ccrawl crawl status -o table
```

It is a planning tool, not a measurement.
It tells you what a full recrawl at each tier interval would cost so you can size the thing before building it.

## Feeding a search index

`ccrawl index build` builds a local BM25 index, and it takes URLs directly rather than reading crawl output on stdin:

```bash
ccrawl index build --dir idx/ --urls "$(ccrawl crawl seed -n 20 -o jsonl | jq -r .url | paste -sd,)"
ccrawl index search --dir idx/ "golang concurrency"
```

`index build` does its own fetching and text extraction, with `--workers` for concurrency.
It also accepts `--input docs.jsonl` for a JSONL file of documents you already have.
See the [search index guide](/guides/search-index/) for the details.

## Planned

The library already has the pieces a real crawl loop needs, and they compile and are tested:

- `Frontier`, a SQLite-backed priority queue with a per-host politeness delay, resumable across a restart
- `RobotsCache`, a per-host cache of RFC 9309 rules with a TTL
- `WARCWriter`, the ISO 28500 writer behind `--warc-dir`, rotating at a size target
- a shared connection pool, 200 idle connections and 10 per host

None of it is reachable from a command yet.
Wiring it into a `ccrawl crawl run` that takes a seed list, walks the frontier, honours robots, and writes WARC is tracked in [#54](https://github.com/tamnd/ccrawl-cli/issues/54).

Until those land, if you need a bulk pipeline over Common Crawl URLs today, use [`ccrawl markdown refetch`](/guides/markdown-corpus/).
It takes the URL list from a WARC shard, fetches every page live at high concurrency, and writes Parquet.
It is a different shape from a general crawler, since the URL set is fixed up front rather than discovered from links, but it is the one that exists and it is fast.

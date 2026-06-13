---
title: "Fetching content"
description: "Pull the exact bytes Common Crawl captured for a URL, as text, Markdown, links, or the raw HTTP response."
weight: 20
---

Once you know a capture exists, you want what is in it. ccrawl has two commands
for that: `get` for the common case of a URL, and `fetch` for when you already
hold a record location.

## get: the curl for Common Crawl

`ccrawl get <url>` looks the URL up in the index, fetches the single WARC
record with a byte-range request, and extracts what you ask for. One command,
one round trip to the data:

```bash
ccrawl get example.com --text       # readable plain text
ccrawl get example.com --markdown   # the page as Markdown
ccrawl get example.com --links      # outbound links
ccrawl get example.com --headers    # the captured HTTP response headers
```

With no extraction flag you get the raw HTTP response body, exactly as Common
Crawl stored it. Pick a crawl with `-c` just like `search`:

```bash
ccrawl get example.com --text -c 2024-51
```

Because a WARC record is its own gzip member, `get` downloads only that record,
not the file around it. That is what makes it feel like fetching a live page
rather than mining an archive.

## fetch: when you have a location

`ccrawl fetch` reads records by explicit location. Point it at a record with
flags, or stream a list of locations on stdin with `-`. This is the other half
of the pipelines that `search` and `table locations` start:

```bash
# fetch records named on stdin (JSONL with filename/offset/length)
ccrawl search 'example.com/*' -o jsonl | ccrawl fetch -

# the same, written one file per record
ccrawl search 'example.com/*' -o jsonl | ccrawl fetch - --dir --out-dir out/

# a single record by exact location
ccrawl fetch --file crawl-data/.../CC-MAIN-...warc.gz --offset 698683535 --length 1262
```

`fetch` takes the same content flags as `get`, so you can transform on the way
through:

```bash
ccrawl search 'example.com/*' -o jsonl | ccrawl fetch - --markdown
ccrawl search 'example.com/*' -o jsonl | ccrawl fetch - --links -o url
```

## extract: content from a page

When you only want one piece of a captured page, `ccrawl extract` is a thin
shortcut over `get`:

```bash
ccrawl extract title example.com    # just the <title>
ccrawl extract text example.com     # readable text
ccrawl extract markdown example.com # Markdown
ccrawl extract links example.com    # outbound links
```

## Picking the right tool

- You have a **URL** and want its content: `ccrawl get`.
- You have a **list of locations** (from `search` or `table locations`):
  `ccrawl fetch -`.
- You want a **whole archive file**, not single records: see
  [bulk and archives](/guides/archives/).

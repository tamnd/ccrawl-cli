---
title: "Finding pages"
description: "Query the URL index for captures of a URL or a path pattern, and filter the results."
weight: 10
---

`ccrawl search` queries the URL index (the CDX server) for captures of a URL.
This is how you find what Common Crawl saw, and where each capture lives, before you fetch anything.

## A single URL

```bash
ccrawl search example.com
```

Each row is one capture.
The default output adapts to where it is going: an aligned table when you are looking at a terminal, JSONL when the output is piped.
Force it with `-o`:

```bash
ccrawl search example.com -o table   # columns for reading
ccrawl search example.com -o jsonl   # one JSON object per line
ccrawl search example.com -o json    # a single JSON array
ccrawl search example.com -o csv     # spreadsheet friendly
ccrawl search example.com -o url     # just the URL column
```

## Path and host patterns

A trailing `/*` matches everything under a path.
This is the fastest way to enumerate a site as Common Crawl indexed it:

```bash
ccrawl search 'example.com/*'              # every capture under the host
ccrawl search 'example.com/blog/*' -o url  # every URL under /blog
```

## Filtering

Narrow the matches with the capture fields:

```bash
ccrawl search 'example.com/*' --mime application/pdf   # only PDFs
ccrawl search 'example.com/*' --status 200             # only successful fetches
ccrawl search 'example.com/*' --from 2023 --to 2024    # captures in a date range
ccrawl search 'example.com/*' --url-contains /blog/    # URL substring match
ccrawl search 'example.com/*' --url-not-contains /tag/ # skip a URL substring
```

Pick the capture closest to a moment in time with `--at`, and order the results with `--sort`:

```bash
ccrawl search example.com --at 2023-06            # the capture nearest June 2023
ccrawl search 'example.com/*' --sort oldest       # oldest captures first
```

To size a result before pulling it, ask for an estimate instead of the rows:

```bash
ccrawl search 'example.com/*' --estimate          # rough page and record counts
```

## Choosing a crawl

`search` runs against the latest crawl unless you say otherwise.
`-c` takes a full crawl ID, a year (every crawl of that year), `latest`, `all`, an integer for the newest N crawls, or a comma-separated list:

```bash
ccrawl search example.com -c 2024-51        # one specific crawl
ccrawl search example.com -c 2024           # every 2024 crawl
ccrawl search example.com -c 3              # the three newest crawls
ccrawl search example.com -c 2024-51,2023-50 # an explicit list
ccrawl search example.com -c all            # across every crawl
```

## Shaping the rows

Keep only the columns you care about, or template each row into whatever shape you need downstream:

```bash
ccrawl search example.com --fields url,status,length
ccrawl search example.com --template '{{.URL}} {{.Status}}'
```

`--limit` (or `-n`) caps the number of results; `0` means unlimited.

## From a match to the bytes

The point of finding a capture is usually to read it.
The `url`, `filename`, `offset`, and `length` on each row are exactly what the fetcher needs, so `search` composes straight into `fetch`:

```bash
ccrawl search 'example.com/*' --mime application/pdf -o jsonl \
  | ccrawl fetch - --dir --out-dir pdfs/
```

For the same question asked across a whole crawl at once, the columnar index is faster and cheaper than the CDX server.
See [the columnar index](/guides/columnar-index/).

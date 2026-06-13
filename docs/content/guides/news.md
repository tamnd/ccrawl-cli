---
title: "Scanning the news"
description: "Work with the continuous CC-NEWS dataset, which has no URL index."
weight: 70
---

CC-NEWS is Common Crawl's continuous news crawl: news articles collected around
the clock and published as WARC files, organised by year and month. Unlike the
monthly crawls, it has no URL index, so you cannot look a URL up; you stream the
files and match as they go by.

## Listing files

```bash
ccrawl news list --year 2026 --month 5         # the month's WARC files
ccrawl news list --year 2026 --month 5 -n 10   # just the first ten
```

## Downloading

```bash
ccrawl news download --year 2026 --month 5 -n 1   # one news WARC file
```

The files land under `<data-dir>/raw` like any other download, and you can
parse them with `ccrawl parse` exactly as you would a crawl WARC.

## Searching by host

Because there is no index, searching means streaming the month and keeping
records whose target host matches. It is slower than an indexed `search`, and
`--workers` parallelises it across files:

```bash
ccrawl news search bbc.co.uk --year 2026 --month 5 -n 50
```

This downloads and scans real WARC data, so be patient and use `-n` to stop
once you have enough. For anything that exists in a monthly crawl, prefer
[the indexed search](/guides/finding-pages/); reach for `news` only when you
specifically want the continuous news feed.

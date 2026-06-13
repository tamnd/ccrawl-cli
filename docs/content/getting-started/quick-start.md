---
title: "Quick start"
description: "From an empty terminal to reading a real captured page, in a handful of commands."
weight: 30
---

This walks the core loop: find the newest crawl, look a URL up in the index,
and read the page Common Crawl captured. Every command here hits live data and
finishes in a second or two.

## 1. Find the newest crawl

```bash
ccrawl crawls latest
```

```
CC-MAIN-2026-21
```

ccrawl defaults to the latest crawl, so you rarely need to pass one. When you
do, `-c` takes a full ID, a year, or `latest`:

```bash
ccrawl crawls list -n 3      # the three most recent crawls
ccrawl crawls resolve 2024   # the newest 2024 crawl: CC-MAIN-2024-51
```

## 2. Find a URL in the index

```bash
ccrawl search example.com -n 3
```

Each row is one capture: when it was seen, its status and MIME, and where the
record lives (file, offset, length). Pick the output that suits you:

```bash
ccrawl search example.com -o url     # just the URLs
ccrawl search example.com -o table   # aligned columns for reading
ccrawl search 'example.com/*'        # every capture under a path
```

## 3. Read the page

`ccrawl get` does the index lookup and the byte-range fetch in one step, then
extracts what you ask for:

```bash
ccrawl get example.com --text
```

```
Example Domain
This domain is for use in illustrative examples in documents. You may use this
domain in literature without prior coordination or asking for permission.
More information...
```

The same capture, other ways to read it:

```bash
ccrawl get example.com --markdown    # the page as Markdown
ccrawl get example.com --links -o url # outbound links, one per line
ccrawl get example.com --headers     # the captured HTTP response headers
```

## 4. Compose

Output that pipes is the point. Find every PDF on a domain and fetch them:

```bash
ccrawl search 'example.com/*' --mime application/pdf -o jsonl \
  | ccrawl fetch - --dir --out-dir pdfs/
```

Count the words on the latest capture of a page:

```bash
ccrawl get example.com --text | wc -w
```

## Where to next

You have the core loop. From here:

- [Finding pages](/guides/finding-pages/) goes deep on the index and filters.
- [Fetching content](/guides/fetching-content/) covers `get`, `fetch`, and the
  byte-range model.
- [The columnar index](/guides/columnar-index/) answers dataset-wide questions
  with SQL.
- The [CLI reference](/reference/cli/) lists every command and flag.

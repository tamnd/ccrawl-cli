---
title: "Introduction"
description: "What Common Crawl is, how it is laid out, and the model ccrawl uses to make it feel small."
weight: 10
---

[Common Crawl](https://commoncrawl.org) is a free, open archive of the web.
A new crawl ships most months and runs to petabytes: billions of pages, the full HTTP response for each one, plus extracted metadata and plain text.
It is one of the most useful public datasets there is, and one of the most awkward to poke at, because everything about it is built for batch processing on a cluster, not for answering a quick question from your laptop.

ccrawl closes that gap.
It is a single binary that treats Common Crawl the way `curl` treats a web server: you ask for something, it fetches exactly that, and it gets out of your way.

## How a crawl is laid out

Each monthly crawl ships a few kinds of files:

- A **URL index** that maps a URL to the WARC file, byte offset, and length where its capture lives. It comes in two forms: the **CDX** server you query over HTTP, and a **columnar Parquet** copy you query with SQL.
- **WARC** files holding the full HTTP request and response for every capture.
- **WAT** files with extracted metadata and links.
- **WET** files with the plain text of each page.

There is also the continuous **CC-NEWS** dataset (news articles, with no URL index) and the **web graph** with host and domain ranks.

## The load-bearing trick

Each record in a WARC file is its own gzip member, compressed independently of the rest.
That means a single record can be fetched and decompressed on its own with an HTTP byte-range request, without downloading the file it lives in.

This is the whole reason ccrawl feels instant.
When you run `ccrawl get example.com --text`, it looks the URL up in the index, reads the one record at its offset and length, decompresses just that member, and extracts the text.
You get the page Common Crawl saw without touching the other 100,000 records in that WARC.

## Scope

ccrawl is a read-only client over data Common Crawl already publishes.
It reads that public data and shapes it for you, and stops there: no crawling the web, no building or serving an index, no running any part of the Common Crawl pipeline.
That narrow scope is what keeps it a single small binary with no database, no daemon, and no setup.

Common Crawl is the only data source.
Other web archives, including the Internet Archive and its CDX server, are out of scope: ccrawl speaks Common Crawl's layout and conventions on purpose, and a second backend would dilute that focus.

Next: [install it](/getting-started/installation/), then take the [quick start](/getting-started/quick-start/).

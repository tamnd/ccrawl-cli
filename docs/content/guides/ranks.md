---
title: "Host and domain ranks"
description: "Look up harmonic-centrality and PageRank positions from the Common Crawl web graph."
weight: 60
---

Alongside the crawls, Common Crawl publishes a **web graph**: who links to whom,
distilled into rank tables for hosts and for registered domains. `ccrawl rank`
reads those tables and tells you where something sits.

## Looking something up

```bash
ccrawl rank domain example.com --table <url>   # rank of a registered domain
ccrawl rank host www.example.com --table <url> # rank of a single host
```

Each result carries the harmonic-centrality position and value and the PageRank
position and value. Harmonic centrality is the rank Common Crawl sorts by; it
tends to track real-world importance more closely than raw PageRank.

## The top of the graph

```bash
ccrawl rank top --table <url> -n 20            # the 20 highest-ranked domains
ccrawl rank top --table <url> --tld gov -n 20  # the top .gov domains
```

`top` reads from the head of the table, which is already sorted by rank, so it
returns quickly even though the table itself is large.

## Choosing a table

The rank tables are big and their exact URL changes with each web-graph
release, so you pass the gzipped table URL with `--table`. A current one looks
like this:

```bash
ccrawl rank top -n 10 --table \
  https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2025-jan-feb-mar/domain/cc-main-2025-jan-feb-mar-domain-ranks.txt.gz
```

Releases come and go, so if a URL returns a 404, check the
[web graph release list](https://commoncrawl.org/web-graphs) for the current
one. The domain table and the host table live side by side under each release;
use the `domain` file for `rank domain ... --table` and the `host` file for
`rank host`.

The first lookup streams the table once and caches it, so later lookups against
the same table are fast.

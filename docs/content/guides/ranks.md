---
title: "Host and domain ranks"
description: "Look up harmonic-centrality and PageRank positions from the Common Crawl web graph."
weight: 60
---

Alongside the crawls, Common Crawl publishes a **web graph**: who links to whom, distilled into rank tables for hosts and for registered domains.
`ccrawl rank` reads those tables and tells you where something sits.

## Looking something up

```bash
ccrawl rank domain example.com     # rank of a registered domain
ccrawl rank host www.example.com   # rank of a single host
```

Each result carries the harmonic-centrality position and value and the PageRank position and value.
Harmonic centrality is the rank Common Crawl sorts by; it tends to track real-world importance more closely than raw PageRank.

## The top of the graph

```bash
ccrawl rank top -n 20            # the 20 highest-ranked hosts
ccrawl rank top --tld gov -n 20  # the top .gov hosts
```

`top` reads from the head of the table, which is already sorted by rank, so it returns quickly even though the table itself is large.
`rank all` reads the same table all the way down, so `ccrawl rank all -o jsonl > hosts.jsonl` gives you the whole graph in rank order.

## Choosing a table

By default each command finds the table itself: the newest web-graph release, the domain file for `rank domain` and the host file for the rest.
The two are different tables with different positions in them, so `wikipedia.org` comes back as domain 14 and host 864 in the same release.
For a domain lookup the newest release is the newest one that has published its domain table, which is not always the newest release, since a release is listed as soon as its host tables land.

Pin a release with `--graph`, or give a table outright with `--table`:

```bash
ccrawl rank top -n 10 --graph cc-main-2026-mar-apr-may
ccrawl rank top -n 10 --table \
  https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2025-jan-feb-mar/domain/cc-main-2025-jan-feb-mar-domain-ranks.txt.gz
```

Releases come and go, so if a URL you passed returns a 404, check the [web graph release list](https://commoncrawl.org/web-graphs) for the current one.

The first lookup streams the table once and caches it, so later lookups against the same table are fast.

---
title: "How far each part goes"
description: "Which subsystems are finished, which are running in production, and which are reference implementations."
weight: 45
---

ccrawl grew in three directions at once: a client for the five Common Crawl surfaces, a set of long-running pipelines that publish datasets, and the beginnings of a crawler and a search engine.
Those are not at the same stage, and a table that says so is worth more than a page of even prose.
Each row says what the part is for, where it stands, and where the detail lives.

| Part | Commands | Where it stands |
|---|---|---|
| Read path | `crawls`, `search`, `get`, `fetch`, `paths`, `stats`, `download`, `parse`, `convert`, `export`, `extract` | Finished. This is the product; it is tested against live Common Crawl on every release. |
| Columnar index | `columnar`, `db` | Finished, with one external dependency. Needs `duckdb` on PATH for SQL, answers what it can natively, and prints the SQL when it cannot run it. |
| Archive formats | `pkg/warc`, `pkg/wat`, `pkg/wet` behind `parse` and `convert` | Finished. The highest covered code in the repo. |
| Web graph | `rank`, `host` | Finished. `host cdx` and the `--cdx` enrichment need `duckdb`. |
| CC-NEWS | `news` | Finished, and slow by nature. CC-NEWS has no index, so a search streams WARC files. |
| Dataset library | `library`, `dedup` | Finished. Verified against 1.1 GB of real archives, checksum by checksum. |
| Publish pipelines | `urls`, `domains`, `markdown`, `publish` | Running in production. Multi-day runs, resume from what is already on the hub, stall detection, disk floors, exit 75 for a supervisor. Fragile in the sense that any long pipeline is: it is watched, not fired and forgotten. |
| Recrawl scheduling | `sched assign`, `sched diff` | Shipped. `sched diff` reads two crawls and needs `duckdb`; the tier it derives is provisional until `sched assign` adds a rank. `crawl status` is arithmetic on a throughput assumption, not a measurement of a running crawl. |
| Crawl engine | `crawl seed`, `crawl run`, `crawl fetch` | Shipped for one machine. Frontier, robots cache, politeness and WARC output all work and resume; there is no distributed frontier, no sitemap discovery, no per-tier scheduling inside a run, and no index written on the way out. See [the recrawl engine](/guides/recrawl-engine/). |
| Content signals | `content lang`, `content quality`, `content outlinks`, `content extract` | Shipped. Heuristics with measured thresholds, not classifiers. See [content signals](/guides/content-signals/). |
| Full-text index | `index build`, `index search` | Reference implementation. BM25 built in memory, with a ceiling around 600,000 documents on a 16 GB machine. It is not a search engine to put behind a service. See [the search index guide](/guides/search-index/). |
| API server | `api`, `serve` | Local exploration tools. No authentication, no rate limiting, no pagination, never run against hostile traffic. `api` binds loopback by default and warns when you widen it. See [the API server guide](/guides/api-server/). |
| MCP server | `mcp` | Shipped, and it is stdio only, so it inherits the trust boundary of whatever launched it. |

## What "finished" and "reference implementation" mean here

**Finished** means the shape is settled, the failure modes are known and documented, and a change to it is a bug fix rather than a redesign.
It does not mean it will never gain a flag.

**Running in production** means it does real work on real data over days, and it also means it has operational edges: it needs a token, it needs disk, and it wants a supervisor that understands exit 75.

**Reference implementation** means it is complete enough to read, to learn from, and to run over a corpus that fits on your machine, and it is not what you should serve traffic from.
Where that is the case the command's own `--help` says so too, so nobody finds out from a load test.

Nothing in ccrawl sits between those states without a line saying which one it is.
If you find a command whose docs promise more than it does, that is a bug worth filing.

---
title: "What each command needs"
description: "The runtime dependencies outside the binary: network, DuckDB, tokens, credentials, disk, and ports."
weight: 18
---

`ccrawl` is one static binary with no shared libraries and no runtime to install, and most of it needs nothing else.
Five things live outside the binary, and this page says which commands need each one, what happens when it is missing, and how to run the same command without it.

## The binary and nothing else

These read local files or local state and never open a socket:

| Command | Reads |
|---|---|
| `parse` | A local WARC, WAT, or WET file, or stdin |
| `convert` | A local archive file or directory |
| `dedup` | Local Parquet files |
| `index build --input`, `index search` | A local JSONL corpus and the index directory it wrote |
| `library list`, `library du`, `library verify`, `library scan`, `library gc` | The dataset library on disk |
| `cache dir`, `cache info`, `cache clear` | The on-disk cache |
| `config show`, `db path`, `version` | Resolved settings |

Everything else in this section is about the commands that do reach out.

## A network route to Common Crawl

Three hosts cover the whole tool:

- `index.commoncrawl.org` serves the collection registry and the CDX URL index, so `crawls`, `search`, `export`, and any command resolving `-c latest` or `-c all` need it.
- `data.commoncrawl.org` serves the archives, the file manifests, the columnar Parquet index, and the web graph, so `get`, `fetch`, `download`, `paths`, `stats`, `columnar`, `rank`, `host`, `news`, `markdown`, and `sched` need it.
- `huggingface.co` is only for the publish commands, covered below.

`crawl fetch`, `crawl run` and the four `content` commands are the exception: they fetch the live web rather than Common Crawl, so they need a route to whatever hosts you point them at and nothing from Common Crawl at all.
`crawl run` reads `robots.txt` for each host first, unless you pass `--no-robots`.
`crawl fetch` checks it only when you pass `--robots`, and the `content` commands do not check it at all.
None of them draw on `--global-rate` either, which is a budget for Common Crawl's servers and not for anyone else's.
That is fine for the handful of URLs those commands were built for, and it is not fine for a long list on stdin: `crawl run` is the command for crawling a site, and it is the one that paces itself and asks permission first.

When Common Crawl itself is having a bad day, [troubleshooting](/reference/troubleshooting/) covers what the retries do and what a partial result looks like.

## DuckDB on PATH

The binary never links DuckDB.
The commands that run SQL shell out to a `duckdb` on your `PATH`, which you install from [duckdb.org](https://duckdb.org/docs/installation).

| Command | Without DuckDB |
|---|---|
| `db load`, `db sql`, `db shell` | Exit 2 with a message naming the install page |
| `sched diff` | Exit 2 |
| `host cdx`, `host get --cdx`, `host enrich --cdx` | Exit 1, the rest of `host get` and `host enrich` still runs |
| `columnar` (any subcommand) | The native engine answers what it can, and anything needing SQL is printed instead of run, exit 0 either way |

`columnar` is the one that degrades rather than fails, because printed SQL is a useful answer: paste it into Athena, Spark, Trino, or a DuckDB somewhere better connected than your laptop.
`--engine duckdb` turns the degrade off and insists on a local binary, `--engine print` asks for the SQL on a machine that has one, and `--engine native` answers the queries ccrawl can compute itself with no DuckDB in the picture at all.
See [columnar engines](/reference/columnar-engines/) for which queries the native engine can take.

## A HuggingFace token

`HF_TOKEN`, or `HUGGINGFACE_TOKEN`, is needed by every command that writes to the hub, and by nothing else:

| Command | Opt out |
|---|---|
| `urls publish`, `urls recount` | `--no-push` |
| `domains publish`, `domains recount` | `--no-push` |
| `markdown export`, `markdown refetch` | `--push=false` |
| `publish verify --repair` | `--no-push` |
| `publish delete-obsolete` | None, deleting is the whole command |

The token is checked before any work starts, so a missing one costs a message and not an afternoon.
Without it the command exits 4.
The opt out flags turn each of these into a local run that writes Parquet to disk and pushes nothing, which is also the way to try a pipeline before you have an account.

Reading a published dataset needs no token at all, so `domains diff` and a plain `publish verify` work anonymously.

## AWS credentials, for `--source s3` only

The default source is the free HTTPS mirror, which needs no credentials and no AWS account.
`--source s3` reads the same bytes from the `commoncrawl` bucket, which stopped allowing anonymous reads and answers AccessDenied without a signature, so it needs `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in the environment or a profile in `~/.aws/credentials`.
`AWS_PROFILE` and `AWS_SHARED_CREDENTIALS_FILE` are honored; instance roles are not.
Nothing is charged for the objects themselves, the credentials are there to sign the request.

Without them the read fails with a message naming both variables, and `download` exits 8.
S3 is worth it from inside `us-east-1`, where the read pays no egress on a dataset measured in terabytes per crawl; from anywhere else the HTTPS mirror is usually the better deal.

## Free disk

The long pipelines write before they upload, so they watch the disk and pause rather than filling it.
`markdown export` and `markdown refetch` stop starting new shards below `--min-free-gb` (2 GiB by default) and resume when space comes back.
`download` and `library` write whatever you asked for, so size those yourself: a full WARC shard set is measured in terabytes, and [`ccrawl stats`](/reference/cli/) sizes a crawl before you commit to it.

## A port to bind

`api` binds `127.0.0.1:8080` by default and `serve` binds `:8080`, both take `--addr`, and `--metrics-addr` binds another port for Prometheus on any long run.
`mcp` speaks over stdio and binds nothing.
Both servers are local tools with no authentication, so read the [API server guide](/guides/api-server/) before you point one at an address other machines can reach.

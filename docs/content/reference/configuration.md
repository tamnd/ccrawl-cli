---
title: "Configuration"
description: "The data directory, environment variables, and global flags, with their defaults."
weight: 20
---

ccrawl needs almost no configuration.
The defaults are chosen so the common case needs none, every option is a flag or an environment variable, and there is a config file for the settings you got tired of typing.

## The data directory

ccrawl keeps all of its state under one tree, `~/data/ccrawl` by default: the on-disk cache, downloaded archives, converted Parquet, and the local DuckDB file.
See the resolved paths any time:

```bash
ccrawl config show
```

```
data_dir     ~/data/ccrawl                 default
cache_dir    ~/data/ccrawl/cache           default
raw_dir      ~/data/ccrawl/raw             derived from data_dir
parquet_dir  ~/data/ccrawl/parquet         derived from data_dir
db_path      ~/data/ccrawl/ccrawl.duckdb   default
```

The third column is where the value came from, which is the answer to nearly every question about why a run did something unexpected.

Point the whole tree somewhere else with `CCRAWL_DATA_DIR`, or per-command with `--data-dir`.

## The config file

ccrawl reads `~/.config/ccrawl/config.toml` if it is there, and nothing needs one to exist.
`CCRAWL_CONFIG_DIR` names the directory outright, `XDG_CONFIG_HOME` moves it the usual way, and `ccrawl config show` reports both the directory and the file.

The `[default]` table applies to every run.
Any other table is a profile, and `--profile <name>` layers it on top of `[default]` for that run.

```toml
[default]
workers = 8
global_rate = "500ms"

[bulk]
workers = 64
global_rate = "50ms"
library_dir = "/data/ccrawl"

[polite]
global_rate = "5s"
retries = 8
```

```sh
ccrawl --profile bulk markdown build -c 2026-30
ccrawl --profile polite search '*.gov' --limit 1000
```

`CCRAWL_PROFILE` selects a profile from the environment, for a shell or a systemd unit that runs everything one way.

Precedence is flag, then environment, then profile, then `[default]`, then the built-in default.
So a profile cannot undo an `export` in the shell that started the run, and a flag on the command line always wins.
See [the CLI reference](/reference/cli/#the-config-file) for the full list of settings and the environment variable that beats each one.

## The dataset library

The `--library` flag (see [bulk and archives](/guides/archives/)) reads and writes a curated corpus of archive files in a tree of its own, separate from the data dir so scratch state and the files you keep never mix.
It defaults to `~/notes/ccrawl` and reports as `library_dir` in `ccrawl config show`:

```
library_dir  ~/notes/ccrawl
```

Move it with `CCRAWL_LIBRARY` or per-command with `--library-dir`.
Inside it, raw archives live under `<crawl>/<kind>/` and processed output under `<crawl>/<format>/<kind>/`.

## Environment variables

| Variable | Used for |
|---|---|
| `CCRAWL_DATA_DIR` | Root data directory (overrides the default `~/data/ccrawl`) |
| `CCRAWL_LIBRARY` | Dataset library root (overrides the default `~/notes/ccrawl`) |
| `CCRAWL_CACHE_DIR` | Cache directory (overrides the default under the data dir) |
| `CCRAWL_CONFIG_DIR` | Config file directory (overrides `~/.config/ccrawl`) |
| `CCRAWL_PROFILE` | Profile to load from the config file, the same as `--profile` |
| `HF_TOKEN` | HuggingFace write token, required by the publishing commands (`HUGGINGFACE_TOKEN` also works) |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | Credentials for `--source s3` (see [the source flag](#the-source-flag)) |
| `AWS_PROFILE`, `AWS_SHARED_CREDENTIALS_FILE` | Which profile and file to read credentials from when the variables above are unset |
| `AWS_REGION` | Where the run is, used to warn about cross-region egress under `--source s3` |

## Publishing to HuggingFace

`urls publish`, `domains publish`, `markdown export`, and `markdown refetch` commit their output to a HuggingFace dataset repo, and they need `HF_TOKEN` set to a token with write access to it.

The commit is spoken directly by the binary: preupload, then LFS multipart to object storage, then the commit itself.
Nothing else has to be installed.
The `CCRAWL_HF_COMMIT=python` escape hatch that v0.6.0 shipped is gone as of v0.7.0, along with the embedded helper script and the uv lookup.

## Global flags

| Flag | Default | Meaning |
|---|---|---|
| `-c, --crawl` | `latest` | Crawl ID, a year (all crawls of that year), `latest`, `all`, an integer for the newest N, or a comma list |
| `-o, --output` | auto | `table`, `json`, `jsonl`, `csv`, `tsv`, `url`, `raw`, `parquet` |
| `-n, --limit` | `0` | Maximum results; `0` is unlimited |
| `-j, --workers` | per command | Concurrency for downloads and scans |
| `--source` | `https` | Which URL scheme to name for bulk data: `https` or `s3` (see [below](#the-source-flag)) |
| `--rate` | `200ms` | Minimum delay between requests, to stay polite |
| `--retries` | `5` | Retry attempts on 403, 429, and 5xx |
| `--timeout` | `2m` | Per-request timeout |
| `--no-cache` | off | Bypass the on-disk cache for this run |
| `--data-dir` | `~/data/ccrawl` | Root data directory |
| `--library` | off | Read and write under the dataset library |
| `--library-dir` | `~/notes/ccrawl` | Dataset library root |
| `--fields` | all | Comma-separated columns to show |
| `--template` | none | Go text/template applied per row |
| `--no-header` | off | Omit the header row in table/csv output |
| `--color` | auto | `auto`, `always`, or `never` |
| `-q, --quiet` | off | Suppress progress output |
| `-v, --verbose` | off | Increase verbosity (repeatable) |
| `--dry-run` | off | Print actions without performing them |
| `--profile` | none | Profile to load from the config file |

## The source flag

`--source` picks which base a Common Crawl path is read from: `https://data.commoncrawl.org/` or `s3://commoncrawl/`.
Both serve the same bytes. The difference is the bill.

Reads of `s3://commoncrawl` from inside `us-east-1`, where the bucket lives, do not pay egress.
The HTTPS mirror does, and one crawl is measured in terabytes, so a bulk job running on EC2 in that region wants `--source s3` and everything else wants the default.

```sh
ccrawl download warc --source s3 --crawl CC-MAIN-2026-30 -n 10
ccrawl get example.com --source s3 --text
ccrawl markdown export --source s3 --crawl CC-MAIN-2026-30 --repo you/your-dataset
```

Every command that reads bulk data honors it: `get`, `fetch`, `download`, `export`, `news`, `markdown export`, `markdown refetch`, and the columnar projection behind `urls publish`.
It also still changes the URLs ccrawl writes down, which is what you want when the SQL is going somewhere else:

| Command | Effect |
|---|---|
| `paths` | Prints `s3://commoncrawl/...` paths instead of HTTPS ones |
| `columnar sql`, `columnar *`, `db load` | The generated SQL reads `s3://commoncrawl/cc-index/...` |
| `host cdx`, `host enrich`, `sched diff` | The Parquet file list handed to DuckDB uses S3 URIs |

### S3 needs AWS credentials

The bucket used to allow anonymous reads and does not any more: an unsigned request gets `AccessDenied`.
Nothing is charged for the objects themselves, but the request has to be signed, so `--source s3` needs credentials.

ccrawl reads them from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`, and falls back to the profile named by `AWS_PROFILE` (or `default`) in `~/.aws/credentials`.
Instance roles are not used, because a run that takes hours wants a key it was handed rather than one that expires halfway through.
Without credentials the command fails immediately, saying so, rather than retrying a 403 five times.

An S3 error that names the credentials, `InvalidAccessKeyId`, `SignatureDoesNotMatch`, `ExpiredToken`, is reported as-is and not retried.

If you pass `--source s3` from outside `us-east-1`, ccrawl prints one warning on the first read, because that is the combination that costs money for nothing.
It stays quiet when it cannot tell where it is running.

DuckDB reads the S3 URIs in the generated SQL with its own credentials, not ccrawl's, so `columnar` queries under `--source s3` need `httpfs` configured on the DuckDB side.

Making ccrawl fetch over S3 was tracked in [#44](https://github.com/tamnd/ccrawl-cli/issues/44).

## Output auto-detection

The default output format adapts to where it is going: an aligned table when the output is a terminal, JSONL when it is piped.
That keeps interactive use readable and scripted use parseable without you setting `-o` either time.
See [output formats](/reference/output/) for the full set.

## Caching and politeness

ccrawl caches small index responses and manifests on disk so repeated commands do not re-fetch them.
`--rate` keeps a minimum gap between requests so a busy session stays a good citizen against the public data.
`cache info`, `cache dir`, and `cache clear` manage the cache.

When a request is throttled, ccrawl retries 403, 429, and 5xx responses with exponential backoff and jitter, and honors a `Retry-After` header when the CDN sends one.
`--retries` sets the attempt count; the wait grows from `backoff` (1s) up to `backoff_max` (30s), both of which `ccrawl config show` reports.

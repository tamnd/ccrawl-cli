---
title: "Installation"
description: "Install ccrawl from a release, with go install, or from source. DuckDB is optional."
weight: 20
---

## Prebuilt binaries

Every [release](https://github.com/tamnd/ccrawl-cli/releases) carries archives
for Linux, macOS, and Windows on amd64 and arm64, plus deb, rpm, and apk
packages for Linux. Download, unpack, put `ccrawl` on your `PATH`, done. The
`checksums.txt` on each release is signed with keyless
[cosign](https://docs.sigstore.dev/) if you want to verify before running.

## With Go

```bash
go install github.com/tamnd/ccrawl-cli/cmd/ccrawl@latest
```

That puts `ccrawl` in `$(go env GOPATH)/bin`, which is `~/go/bin` unless you
moved it. Make sure that directory is on your `PATH`.

## From source

```bash
git clone https://github.com/tamnd/ccrawl-cli
cd ccrawl-cli
make build        # produces ./bin/ccrawl
./bin/ccrawl version
```

## Optional: DuckDB

The columnar index commands (`ccrawl table`, `ccrawl db`) run SQL against the
public Parquet index. If a `duckdb` binary is on your `PATH`, ccrawl uses it to
run the queries directly. With no `duckdb` on your `PATH`, ccrawl prints the SQL
so you can paste it into DuckDB, Athena, Spark, or Trino yourself. Either way the ccrawl binary
never links DuckDB, so the install stays small and pure Go.

Install DuckDB from [duckdb.org](https://duckdb.org/docs/installation) if you
want local execution. Everything else in ccrawl works without it.

## Requirements

- **Go 1.26 or later** to build. The released binary has no Go requirement.
- **A `duckdb` binary** only if you want to run columnar queries locally.

That is the whole list. No config file, no database to provision, no daemon.

## Checking the install

```bash
ccrawl version
```

prints the version and exits. Then confirm it can reach Common Crawl:

```bash
ccrawl crawls latest
```

should print the newest crawl ID, for example `CC-MAIN-2026-21`. If you see
that, you are ready for the [quick start](/getting-started/quick-start/).

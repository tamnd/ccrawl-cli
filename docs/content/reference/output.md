---
title: "Output formats"
description: "Every output format, how to narrow columns, and how to template rows."
weight: 30
---

Every list command renders through the same formatter.
Pick a format with `-o`, or let ccrawl choose: a table when writing to a terminal, JSONL when piped.

## Formats

```bash
ccrawl search example.com -o table   # aligned columns for reading
ccrawl search example.com -o jsonl   # one JSON object per line, for piping
ccrawl search example.com -o json    # a single JSON array
ccrawl search example.com -o csv     # spreadsheet friendly
ccrawl search example.com -o tsv     # tab-separated
ccrawl search example.com -o url     # just the URL column
ccrawl search example.com -o raw     # the underlying bytes, unformatted
ccrawl search example.com -o parquet > out.parquet   # columnar, for analytics
```

| Format | Best for |
|---|---|
| `table` | Reading on a terminal |
| `jsonl` | Piping into another tool, one object at a time |
| `json` | Loading a whole result as an array |
| `csv` / `tsv` | Spreadsheets and quick column math |
| `url` | Feeding URLs into other commands |
| `raw` | The unformatted bytes (response bodies, file contents) |
| `parquet` | Columnar output for analytics, written to a file or pipe |

`parquet` writes a zstd-compressed Parquet stream where every projected column is a UTF-8 string.
It works with any list command, so you can turn a search or a host listing straight into a Parquet file for DuckDB, Spark, or pandas:

```bash
ccrawl search '*.gov/*' --status 200 -o parquet > gov.parquet
ccrawl host top -n 100000 -o parquet > top_hosts.parquet
```

## Narrowing columns

Keep only the fields you want:

```bash
ccrawl search example.com --fields url,status,length
```

`--no-header` drops the header row in `table` and `csv` output, which is handy when a downstream tool expects bare rows.

## Templating rows

For full control over each line, apply a Go text/template.
Fields are the JSON keys, capitalised:

```bash
ccrawl search example.com --template '{{.URL}} {{.Status}}'
ccrawl search example.com --template '{{.URL}}	{{.Length}} bytes'
```

## Why auto-detection helps

Because the default adapts to the destination, the same command reads well by hand and parses cleanly in a pipe:

```bash
ccrawl search example.com            # a table, because this is a terminal
ccrawl search example.com | wc -l    # JSONL, because this is a pipe
```

You only reach for `-o` when you want something other than that default.

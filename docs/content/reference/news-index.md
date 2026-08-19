---
title: "CC-NEWS index"
description: "The index Common Crawl does not publish for CC-NEWS: how it is built, what the columns mean, and how a search reads it."
weight: 16
---

Common Crawl publishes an index for its main crawls twice over, as CDX files and as a columnar table.
CC-NEWS gets neither.
A month of CC-NEWS is a directory of WARC files and a list of their names, and that is the whole of it, so the only way to answer "what did this publisher put out in July" has been to decompress every file in the month.

`ccrawl news publish` builds the missing index and mirrors it to a HuggingFace dataset.
`ccrawl news search` reads that index when the month is indexed and falls back to the scan when it is not.

```sh
ccrawl news publish --months 2026/07
ccrawl news search bbc.co.uk --year 2026 --month 7
```

## What one month costs

| | |
|---|---|
| WARC files in a month | around 350 |
| Size of one file | roughly 1 GB |
| Bytes read to index a month | a few hundred GB, once |
| Parquet the month produces | a small fraction of that |

The archives are never written to disk.
They are decompressed, indexed, and dropped as they stream, so a run holds one output shard per worker and nothing else, whatever the size of the month.

Use `--files N` to index the first N files of a month when proving a setup, which is the cheap way to see real output before committing to the whole thing.

## Layout

One source WARC becomes one Parquet shard, named for the archive it indexes.

```
data/2026/07/CC-NEWS-20260701022501-08467.parquet
data/2026/07/CC-NEWS-20260701052811-08468.parquet
...
stats.csv        one row per month: files, articles, bytes read, whether it is complete
languages.csv    one row per month and detected language
README.md        the dataset card, rebuilt from the two ledgers on every commit
```

The naming is what makes resume cheap.
A shard is named for its source, so the month's own `warc.paths` manifest names every shard that could exist, and a run works out what is left to do by asking the hub which of those paths are already there.
A killed run picks up where it stopped, and nothing is counted twice.

## Schema

The column names are cc-index's, deliberately: a query written against `open-index/ccrawl-urls` runs here unchanged.

| Column | Type | Where it comes from |
|---|---|---|
| `url_surtkey` | string | computed here (SURT of the URL) |
| `url` | string | the record's `WARC-Target-URI` |
| `url_host_name` | string | computed here |
| `url_host_registered_domain` | string | computed here, via the public suffix list |
| `url_host_tld` | string | computed here |
| `url_protocol` | string | computed here |
| `fetch_time` | timestamp | the record's `WARC-Date` |
| `fetch_status` | int32 | the stored HTTP status line |
| `fetch_redirect` | string | the stored `Location` header, on a 3xx |
| `content_digest` | string | the record's `WARC-Payload-Digest`, `sha1:` trimmed |
| `content_mime_type` | string | the stored `Content-Type` header |
| `content_mime_detected` | string | computed here, by sniffing the body |
| `content_charset` | string | the stored `Content-Type` header |
| `content_languages` | string | computed here, ISO 639-3 |
| `content_truncated` | string | the record's `WARC-Truncated` |
| `warc_filename` | string | the source archive path |
| `warc_record_offset` | int64 | computed here |
| `warc_record_length` | int64 | computed here |
| `content_language_confidence` | double | computed here, 0 to 1 |
| `content_language_declared` | string | the page's own `<html lang>`, BCP-47 |
| `content_length` | int64 | size of the stored response body |
| `warc_record_id` | string | the record's `WARC-Record-ID` |

Two things are worth knowing before trusting a column.

**Most of the useful ones are computed, not reported.** A CC-NEWS record carries no `WARC-Identified-Content-Language` and no `WARC-Identified-Payload-Type`, and there are no metadata records at all, because CC-NEWS is raw crawler output rather than a processed crawl. So every host, language and detected MIME column above was worked out from the bytes rather than read off a label. cc-index gets its language labels from CLD2 run over raw HTML; these come from ccrawl's identifier run over the extracted text, so the two will not always agree about the same page.

**The two `warc_record` columns are int64, not cc-index's int32.** A gigabyte fits in an int32 with under half its range to spare, and a schema one good month away from wrapping to a negative offset is not worth the four bytes.

## The location triple

`warc_filename`, `warc_record_offset` and `warc_record_length` are the same triple `ccrawl fetch` reads, so a row out of this dataset fetches the article it describes.

```sh
ccrawl news search bbc.co.uk --year 2026 --month 7 -o jsonl | ccrawl fetch - --text
```

or by hand, from any row:

```sh
ccrawl fetch \
  --file crawl-data/CC-NEWS/2026/07/CC-NEWS-20260701022501-08467.warc.gz \
  --offset 71779 --length 20806 --text
```

The offsets are byte spans in the compressed file.
A WARC is a multi-member gzip stream with one record per member, so a span is a self-contained gzip member and a range request for it decompresses on its own.

## Why a search is fast

A shard is Parquet, so a reader can pull one column without reading the rest of the file.
A host query reads `url_host_name` out of each shard, and only opens the shards where that column matched.
Most shards in a month hold nothing for a given publisher, so most of them cost a footer and a small column chunk rather than the whole file.

`news search` says on stderr which path it took.
A month that is indexed but still building is searched from the part that is published, and the shortfall is reported rather than passed off as the whole month.
`--no-index` forces the scan, which is the behaviour the command had before the index existed.

## Reading it directly

```sql
SELECT url, fetch_time, content_languages
FROM 'https://huggingface.co/datasets/open-index/ccrawl-news/resolve/main/data/2026/07/*.parquet'
WHERE url_host_registered_domain = 'bbc.co.uk'
ORDER BY fetch_time;
```

```sql
-- where the publisher's own label and the detected language disagree
SELECT content_language_declared, content_languages, count(*) AS n
FROM 'https://huggingface.co/datasets/open-index/ccrawl-news/resolve/main/data/2026/07/*.parquet'
WHERE content_language_declared <> '' AND content_languages <> ''
  AND left(content_language_declared, 2) <> left(content_languages, 2)
GROUP BY 1, 2 ORDER BY n DESC LIMIT 20;
```

## Settings

| Setting | Environment | Default |
|---|---|---|
| `news_repo` | `CCRAWL_NEWS_REPO` | `open-index/ccrawl-news` |

Both `news publish` and `news search` take `--repo`, which wins over either.

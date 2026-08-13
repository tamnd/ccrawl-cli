---
title: "Columnar engines"
linkTitle: "Columnar engines"
description: "The two engines behind ccrawl columnar, what each one can answer, and which one runs when."
weight: 17
---

`ccrawl columnar` answers bulk questions against Common Crawl's Parquet index. There are two engines behind it, and `--engine` picks between them.

| Engine | What it is | Needs |
|---|---|---|
| `duckdb` | The SQL is handed to a local duckdb binary reading the Parquet over HTTPS | duckdb on PATH |
| `native` | ccrawl reads the Parquet files itself over ranged HTTP | nothing |
| `print` | The SQL is printed for you to run in Athena, Spark, Trino, or duckdb yourself | nothing |
| `auto` | native for anything native can answer, duckdb for the rest | nothing |

The default is `auto`. It takes the native engine for every query the native engine can answer, whether or not duckdb is installed, so the same command does the same thing on every box. What is left is the arbitrary SQL, and that goes to duckdb where duckdb exists and gets printed where it does not. `--engine duckdb` forces the old path on a box that has both.

## What the native engine can answer

`urls`, `locations`, `count`, `langs`, `mimes` and `schema`, with any combination of the filter flags: `--domain`, `--host`, `--tld`, `--mime`, `--lang`, `--status`, `--path-prefix` and `--subset`, plus the negated forms `--not-tld`, `--not-mime`, `--not-lang`, `--not-status` and the set forms `--hosts-file` and `--domains-file`.

`columnar query` and `columnar sql` take arbitrary SQL, so they need duckdb and always will. A hand written engine that accepts arbitrary SQL is a database, and this is a command line tool. Asking for `--engine native` on one of those is an error rather than a silent fallback, so a script cannot end up quietly running somewhere else.

## How it reads less

A crawl's `warc` subset is thousands of Parquet parts. Reading all of them to count the captures on one TLD would be absurd, so the native engine does what a query planner does.

It opens each part over ranged HTTP and reads only the footer. From the footer it reads the page index for the columns the filters mention, and compares each page's minimum and maximum against the filter. A part whose pages all fall outside the filter is dropped without a single data page being read. Because the files are sorted by `url_surtkey`, a `--domain` or `--host` filter also gets a prefix predicate on that column, which is the one that prunes hardest. `--hosts-file` and `--domains-file` get it too, one prefix per value, so a list of hosts reads no more of the index than the same hosts asked for one at a time.

A list longer than 64 collapses to the one prefix every value shares. Ten thousand subdomains of one company still prune to that company; a list spread across many top level domains shares nothing and gets no prefix, which is the honest answer, since there is no one stretch of the index that holds them. Grouping a large list by top level domain and running it in a few passes reads less than one pass over the lot.

What survives is read a column at a time, cheapest column first, and each column narrows the set of rows still alive. A row group where the first filter kills everything never has the expensive columns touched at all. Only then are the output columns read, and only for the rows that matched.

The upshot on a real crawl: opening a part and deciding it holds nothing costs about 200 KiB out of an 8.7 MiB file.

## What negation costs

A negated filter prunes nothing. Page statistics record a page's minimum and maximum and say nothing at all about how many nulls are in it, so a page where the language is `vie` from top to bottom can still hold rows with no language, and those rows are exactly what `--not-lang vie` is looking for. Skipping the page on its bounds would drop them. The predicate is therefore applied on the read instead, which means a query with only negated filters reads every page of every part.

Pair a negation with something positive when you can. `--tld vn --not-lang vie` prunes on the TLD first and only then applies the negation to what survives, which is a different amount of work from `--not-lang vie` on its own.

A set filter does prune, on the smallest and largest member of the whole set rather than one comparison per member, so a ten thousand host list costs the same page decision as a single host. It is conservative: a page holding none of the hosts but sorting between two that it does will still be read.

## Speed

Counting the Vietnamese TLD across the 300 parts of one crawl's `robotstxt` subset, on a home connection:

| Engine | Wall clock |
|---|---|
| native, `-j 16` | 15s |
| native, default `-j 8` | 22s |
| duckdb | 39s |

Being faster than duckdb is not the point and will not hold for every query. duckdb is a vectorized engine and will win where a query actually has to read a lot of rows. The point is that the no-dependency path is not a consolation prize.

The native engine does not apply the `--rate` inter-request delay. That delay is priced for requests that mean something, a CDX page or a WARC range or a whole file, and a columnar scan is thousands of few-kilobyte reads of footers and column indexes. duckdb, which answers the same queries by default, applies no delay either.

## Differences to know about

Rows come back in whichever order the parts finish, which is also what duckdb over an unordered file list does. Add your own sort if you need one.

`langs` and `mimes` order by count descending and break ties by value, so repeated runs agree with each other.

An empty string and a missing value are two different groups in a breakdown, under both engines.

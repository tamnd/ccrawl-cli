---
title: "Content signals"
description: "Extract text, score quality, identify language, and map outlinks, for one URL or for a list of them."
weight: 100
---

`ccrawl content` fetches a page and says something about it: what the text is, whether the page is worth keeping, what language it is in, and what it links to.
There are four commands, they all take the same argument, and they all fetch the live web rather than reading Common Crawl.
That is the point of them: a Common Crawl capture is months old, and these answer for the page as it is now.

```
ccrawl content extract <url>    clean text, title, canonical URL, word count
ccrawl content quality <url>    five signals a corpus can be filtered on
ccrawl content lang <url>       the language, the way the Markdown pipelines decide it
ccrawl content outlinks <url>   every outbound link as a row
```

Every one of them takes `-` in place of the URL and reads a list from stdin instead, which is the form that scales past a spot check.

## Extracting content

```bash
ccrawl content extract https://go.dev/blog/ -o json
```

| Field | What it is |
|---|---|
| `url` | The URL that answered, after up to five redirects |
| `canon_url` | The page's own `rel=canonical`, resolved against `url`, or `url` when it declares none |
| `title` | The `<title>` text |
| `description` | The meta description, absent when there is none |
| `language` | The `lang` attribute the page declares, which is a claim and not a measurement |
| `word_count` | Words in the extracted text |
| `doc_id` | A 64-bit FNV hash of `canon_url`, the same ID the full-text index uses |
| `snippet` | The first 500 characters of the extracted text |

The extracted text is the page with `script`, `style`, `noscript`, `nav`, `header`, `footer` and `aside` removed.
`snippet` is a sample, not the document; for the whole thing use `ccrawl get --text` or `ccrawl get --markdown`.

## Scoring quality

```bash
ccrawl content quality https://example.com/ -o json
```

| Field | What it is |
|---|---|
| `url` | The URL that answered |
| `word_count` | Words in the extracted text |
| `title_length` | Characters in the title, 0 for a page with none |
| `has_main_content` | `word_count` is at least 50 |
| `spam_score` | 0 to 1, one tenth for each of sixteen English sales phrases the page contains, capped at 1 |
| `is_parked` | A page under 150 words that says it is for sale, parked, coming soon, or under construction |

These are cheap and blunt on purpose.
`spam_score` reads English only, and a page that scores 0 is not thereby clean.
Use them to throw out the obvious floor of a crawl, not to rank what is left.

```bash
ccrawl content quality - -o jsonl < seeds.txt \
  | jq -c 'select(.has_main_content and .spam_score < 0.2 and (.is_parked | not))'
```

## Identifying language

`content lang` runs the identifier that `markdown export --lang` filters on, so a document that was kept or dropped can be asked about one URL at a time.

```bash
ccrawl content lang https://vnexpress.net/ -o json
```

| Field | What it is |
|---|---|
| `url` | The URL that answered |
| `language` | The detected language, ISO 639-3, so `eng` and `vie` rather than `en` and `vi` |
| `confidence` | 0 to 1 from the trigram identifier |
| `cc_language` | What the page's own `lang` attribute claims, shown alongside so a disagreement is visible |
| `chars` | Characters of Markdown the identifier saw |
| `sample` | The first 200 characters of that text |

The language is detected in the extracted Markdown, not in the raw HTML and not taken from the page's declaration, because that is what the pipelines filter on.
When an answer looks wrong, read `sample` first: it is usually the input that is wrong and not the identifier.

## Mapping outlinks

```bash
ccrawl content outlinks https://go.dev/blog/ -o jsonl
```

Each row is `source`, the page the link was found on, `url`, the link target, and `host`, its hostname.
The links are every `http` and `https` anchor href on the page, resolved against the page's own address.
They are not deduplicated and they carry no anchor text, so a nav bar linked from every article shows up once per page.
`ccrawl get --links` gives the same targets as a plain text list.

```bash
ccrawl content outlinks - -o jsonl < seeds.txt > links.jsonl
jq -r .host links.jsonl | sort | uniq -c | sort -rn | head
```

## Reading a list of URLs

`-` reads stdin. A line is either a bare URL or a JSON object with a `url` field, which is what `search`, `columnar` and `crawl fetch` write with `-o jsonl`, so a query feeds a content command with no glue in between:

```bash
# a plain list
ccrawl content quality - -o jsonl < seeds.txt

# what one page links to, scored
ccrawl content outlinks https://go.dev/blog/ -o jsonl | jq -r .url | ccrawl content quality - -o jsonl

# straight from a Common Crawl query
ccrawl search 'example.com/*' -n 100 -o jsonl | ccrawl content quality - -o jsonl
```

The URLs are fetched one at a time, in the order they arrive, with no concurrency, no per host delay, and no `robots.txt` check.
`--global-rate` does not apply, because it is a budget for Common Crawl's servers and these requests do not go there.
A list of ten thousand URLs on one host is a load test of that host.
When the list gets long enough for that to matter, `ccrawl crawl run` is the command that paces itself per host and reads `robots.txt` first.

A URL that cannot be fetched is named on stderr and the rest of the list carries on:

```
fetch https://nx.invalid/: dial tcp: lookup nx.invalid: no such host, skipping it
1 of the 3 URLs on stdin could not be fetched
```

The exit code says which kind of run it was.
A run that scored at least one page exits 0 even if some URLs failed.
A run where every URL failed exits 1, because nothing was learned about those pages, they were never seen.
Empty stdin exits 3, an empty result.
A single URL that fails exits 1, since that run has nothing else to do.

Each page is fetched with a 120 second timeout, at most five redirects, and at most 10 MB of body.

## Stored WARC files

The content commands fetch, they do not read archives.
For a WARC, WAT or WET file you already have, `ccrawl parse` is the command, and it emits the text directly:

```bash
ccrawl parse file.warc.wet.gz --lang eng -o jsonl
ccrawl parse file.warc.gz --type response --status 200 --markdown -o jsonl
```

To score a stored capture with `content quality` instead, take the URLs out of the archive and pass them in.
That refetches the page as it is today, which is a different thing from the capture, and is usually what you want when the question is whether the site is still worth crawling.

## Filtering before indexing

`index build --input -` reads JSONL from stdin, one object per line with a `url` and the `text` to index, which is what a filtered `parse` produces:

```bash
ccrawl parse file.warc.wet.gz --lang eng -o jsonl \
  | jq -c 'select((.Text | length) > 1000)' \
  | ccrawl index build --dir idx/ --input -
```

The index is a reference implementation with a ceiling of roughly 600,000 documents on a 16 GB machine.
[How far each part goes](/reference/maturity/) has the numbers.

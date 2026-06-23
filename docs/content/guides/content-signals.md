---
title: "Content signals"
description: "Extract text, measure quality, and map outlinks from live pages or stored WARC records."
weight: 100
---

`ccrawl content` runs content-analysis operations against live pages or stored WARC/JSONL corpora.
These commands are useful for quality filtering before indexing and for building link graphs from your own crawl data.

## Extracting content from a live URL

`content extract` fetches a URL and returns one or more content views:

```bash
ccrawl content extract https://example.com --text        # clean readable text
ccrawl content extract https://example.com --markdown    # Markdown with headings/links
ccrawl content extract https://example.com --outlinks    # outbound links only
ccrawl content extract https://example.com --all -o json # all signals in one JSON
```

Flags:

| Flag | Default | Purpose |
|---|---|---|
| `--text` | false | Return readable plain text (boilerplate removed) |
| `--markdown` | false | Return page as Markdown |
| `--outlinks` | false | Return outbound links as a JSONL list |
| `--all` | false | Return all signals together |
| `--crawl` | latest | Use a specific CC crawl instead of fetching live |

Without a content flag, `content extract` returns the raw HTML.

## Measuring content quality

`content quality` computes a set of quality signals for a URL or a JSONL stream:

```bash
# single URL
ccrawl content quality https://example.com -o json

# batch from a crawl result
ccrawl crawl fetch seeds.jsonl -o jsonl | ccrawl content quality - -o jsonl
```

Output fields:

| Field | Description |
|---|---|
| `lang` | Detected language (BCP-47) |
| `lang_confidence` | Language detection confidence (0–1) |
| `text_length` | Character count of the extracted text |
| `word_count` | Word count of the extracted text |
| `link_density` | Ratio of link text to total text |
| `boilerplate_ratio` | Estimated fraction of the page that is boilerplate |
| `readability_score` | Flesch-Kincaid readability estimate |

Use these signals to filter low-quality pages before indexing:

```bash
ccrawl crawl fetch seeds.jsonl -o jsonl \
  | ccrawl content quality - -o jsonl \
  | jq 'select(.word_count > 200 and .lang == "en")' \
  | ccrawl index build --dir idx/ -
```

## Extracting outlinks

`content outlinks` focuses on the link graph.
It reads a JSONL stream and emits `(source_url, target_url, anchor_text)` triples:

```bash
ccrawl crawl fetch seeds.jsonl -o jsonl | ccrawl content outlinks - -o jsonl > links.jsonl
```

This is cheaper than running the full quality pipeline when you only need the link graph.
Each output row is a JSON object with `src`, `dst`, and `text` fields.

## Processing stored WARC files

All three subcommands (`extract`, `quality`, `outlinks`) accept a `--warc` flag that reads from a local WARC file instead of fetching live:

```bash
ccrawl content extract --warc out/worker-0.warc --text
ccrawl content quality --warc out/worker-0.warc -o jsonl
ccrawl content outlinks --warc out/worker-0.warc -o jsonl > links.jsonl
```

This is the standard way to post-process the output of `crawl fetch -f warc`.

## Language filtering

All three commands pass language metadata through.
Filter to a single language at the shell:

```bash
ccrawl content quality - -o jsonl | jq 'select(.lang == "vi")'
```

Or use the `--lang` flag directly (where supported):

```bash
ccrawl content quality --lang en - -o jsonl
```

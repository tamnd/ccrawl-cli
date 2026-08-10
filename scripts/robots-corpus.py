#!/usr/bin/env python3
"""Build a real robots.txt corpus and the verdicts a reference parser gives for it.

The Go side is TestRobotsAgainstRealSites, which is skipped unless
CCRAWL_ROBOTS_CORPUS points at the directory this writes. Hand written test
cases prove the parser matches the RFC as we read it; this proves it matches
what the live web actually serves and what another implementation of the same
spec concludes about it.

    python3 -m venv .venv && .venv/bin/pip install protego
    .venv/bin/python scripts/robots-corpus.py --out /tmp/robots-corpus
    CCRAWL_ROBOTS_CORPUS=/tmp/robots-corpus go test ./ccrawl -run RealSites -v

protego is the parser Scrapy uses and it follows the same Google derived rules
RFC 9309 was written from, so a disagreement is worth looking at in both
directions rather than assumed to be ours.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request

# Sites picked for variety rather than popularity: news, commerce, code hosting,
# government, and a few that are known to write long or unusual files.
SITES = """
wikipedia.org nytimes.com github.com reddit.com amazon.com apple.com
microsoft.com cloudflare.com mozilla.org python.org golang.org arxiv.org
nasa.gov europa.eu gov.uk imdb.com etsy.com wordpress.org theguardian.com
cnn.com booking.com craigslist.org archive.org quora.com linkedin.com
pinterest.com ebay.com target.com walmart.com espn.com un.org
""".split()

USER_AGENT = "CCrawl/2.0 (+https://ccrawl.tamnd.com/bot)"
TOKEN = "ccrawl"

# Paths every file is probed with, so a file with no rules of its own still
# contributes something to compare.
COMMON = [
    "/",
    "/index.html",
    "/search?q=test",
    "/admin/",
    "/api/v1/items",
    "/images/logo.gif",
    "/page.php?id=1",
    "/a/b/c/d/e.html",
]


def reliable(path: str) -> bool:
    """Report whether protego's verdict on a path is worth comparing against.

    protego runs a URL through urlparse and percent encodes the result before
    matching, and it does not do the same to the pattern, so three kinds of path
    come back with an answer that contradicts the file's own text:

      - an = in the path segment is escaped in the URL and left alone in the
        pattern, so Disallow: /gp/aw/help/id=sss does not match /gp/aw/help/id=sss
        and neither does a %3D spelling of the same character
      - a trailing ? is an empty query that urlparse drops from the URL and a
        corner case in protego preserves in the pattern
      - a leading // is read as a network location, which empties the path

    Every one of these is protego reading a path as if it were a URL. They are
    filtered here rather than papered over in the Go test so that the thing
    being skipped, and the reason, sit next to the tool they are about.
    """
    if path.endswith("?") or path.startswith("//"):
        return False
    return "=" not in urllib.parse.unquote(path.split("?", 1)[0])


def fetch(site: str) -> tuple[str, int, str]:
    req = urllib.request.Request(
        f"https://{site}/robots.txt", headers={"User-Agent": USER_AGENT}
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return site, resp.status, resp.read(2_000_000).decode("utf-8", "replace")
    except urllib.error.HTTPError as err:
        return site, err.code, ""
    except Exception:
        return site, 0, ""


def probes(body: str) -> list[str]:
    """Concrete paths derived from the file's own rules, plus the common set.

    A rule is only interesting if something is tested against it, and the paths
    a file cares about are the ones it names. Each pattern becomes the path it
    was written for and a couple of near misses around it.
    """
    paths = list(COMMON)
    for line in body.splitlines():
        line = line.split("#", 1)[0].strip()
        match = re.match(r"(?i)^(allow|disallow)\s*:\s*(\S.*)$", line)
        if not match:
            continue
        pattern = match.group(2).strip()
        base = pattern.rstrip("$").replace("*", "x")
        if not base.startswith("/"):
            base = "/" + base.lstrip("/")
        paths.extend([base, base + "extra", base.rstrip("/") + "/deep/page.html", base + "?q=1"])
    seen, out = set(), []
    for p in paths:
        if p not in seen and len(p) < 300:
            seen.add(p)
            out.append(p)
    return out[:400]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/tmp/robots-corpus")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    from protego import Protego

    verdicts: dict[str, dict[str, object]] = {}
    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        for site, status, body in pool.map(fetch, SITES):
            if status != 200 or not body.strip():
                print(f"skip {site}: HTTP {status}")
                continue
            with open(os.path.join(args.out, f"{site}.txt"), "w") as fh:
                fh.write(body)
            parser = Protego.parse(body)
            paths = probes(body)
            allowed = {p: bool(parser.can_fetch(p, TOKEN)) for p in paths if reliable(p)}
            delay = parser.crawl_delay(TOKEN)
            verdicts[site] = {
                "allowed": allowed,
                "skipped": [p for p in paths if not reliable(p)],
                "crawl_delay": float(delay) if delay else 0.0,
                "sitemaps": sorted(parser.sitemaps),
            }
            print(f"{site}: {len(body)} bytes, {len(allowed)} probes")

    path = os.path.join(args.out, "verdicts.json")
    with open(path, "w") as fh:
        json.dump({"user_agent": TOKEN, "sites": verdicts}, fh, indent=1, sort_keys=True)
    total = sum(len(v["allowed"]) for v in verdicts.values())
    skipped = sum(len(v["skipped"]) for v in verdicts.values())
    print(f"wrote {path}: {len(verdicts)} sites, {total} probes, {skipped} skipped")


if __name__ == "__main__":
    main()

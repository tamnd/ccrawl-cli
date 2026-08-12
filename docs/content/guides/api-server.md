---
title: "API server"
description: "Serve a local index and the host graph over HTTP, on loopback, for your own use."
weight: 110
---

`ccrawl api` starts a small HTTP server over your local index and the web-graph host table.

It is a local exploration tool. It has no authentication, no rate limiting, no request log and no pagination, and it has never been run against hostile traffic. It binds `127.0.0.1` by default and warns on stderr if you point it at an address other machines can reach. If it has to be reachable, put it behind a reverse proxy that adds auth, and treat everything below as an internal API.

## Starting the server

```bash
ccrawl api
ccrawl api --index-dir idx/                        # with full-text search
ccrawl api --addr 127.0.0.1:9090 --index-dir idx/  # a different port
```

Flags:

| Flag | Default | Purpose |
|---|---|---|
| `--addr` | `127.0.0.1:8080` | Listen address |
| `--index-dir` | none | Index directory built by `index build`; without it `/v2/search` answers 503 |

On startup the server reads the top million hosts of the web-graph rank table into memory. That takes a minute or two on a good connection and happens on every start, because nothing is persisted. A rank table that fails to load is fatal: the server exits rather than answering host queries from half a table.

## Endpoints

### `GET /v2/search`

Run a BM25 query against the local index. Requires `--index-dir`, otherwise 503.

```
GET /v2/search?q=golang+concurrency&k=10
```

Parameters:

| Parameter | Default | Description |
|---|---|---|
| `q` | required | Query string; missing or empty is a 400 |
| `k` | 10 | Results to return, capped at 100 |

BM25 k1 and b are fixed at 1.2 and 0.75 and are not settable over HTTP. Query terms are ORed, as in `ccrawl index search`.

Response:

```json
{
  "query": "golang concurrency",
  "results": [
    { "doc_id": 123, "url": "https://...", "host": "...", "title": "...", "snippet": "...", "score": 12.4, "language": "eng" }
  ]
}
```

### `GET /v2/host/{host}`

Look up a single host record. Returns the same `HostRecord` structure `host enrich` emits, though over this server it carries only what the rank table holds. A host that is not in the top million is a 404.

```
GET /v2/host/golang.org
```

### `GET /v2/hosts`

The top hosts by harmonic centrality, in rank order.

```
GET /v2/hosts?tld=gov&n=20
```

Parameters:

| Parameter | Default | Description |
|---|---|---|
| `tld` | — | Restrict to one top-level domain |
| `n` | 100 | Hosts to return, capped at 10000 |

There is no cursor. Asking for more than the cap means restarting from the top with a different filter.

### `GET /v2/health`

Always 200 while the process is up, and says which stores are loaded:

```json
{"status":"ok","hosts":true,"search":false}
```

`search: false` means no `--index-dir` was given, so `/v2/search` will answer 503. Health is a liveness check, not a readiness check: the server does not start serving until both stores are settled, so there is no window where it is up and still loading.

## Example: curl

```bash
ccrawl api --index-dir ~/cc-index/ &

curl "http://127.0.0.1:8080/v2/search?q=machine+learning&k=5" | jq .
curl "http://127.0.0.1:8080/v2/host/arxiv.org" | jq .
curl "http://127.0.0.1:8080/v2/health"
```

## Example: integrate with a script

```python
import requests

BASE = "http://127.0.0.1:8080/v2"

def search(q, k=10):
    r = requests.get(f"{BASE}/search", params={"q": q, "k": k})
    r.raise_for_status()
    return r.json()["results"]

for hit in search("python async programming"):
    print(hit["score"], hit["url"])
```

## What it is not

No auth, no rate limiting, no request log, no pagination cursor, no persistence, and the search side inherits the [corpus ceiling of the index](/guides/search-index/): a few hundred thousand documents on an ordinary machine. It is the right tool for poking at a corpus you just built, and the wrong tool for anything with users on it.

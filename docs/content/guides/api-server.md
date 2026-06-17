---
title: "API server"
description: "Serve your local ccrawl index over HTTP with a simple REST API."
weight: 110
---

`ccrawl api` starts a lightweight HTTP server that exposes your local index over a REST API.
Use it to integrate ccrawl search into your own applications, or to share a local index with teammates on the same network.

## Starting the server

```bash
ccrawl api --index-dir idx/
ccrawl api --index-dir idx/ --addr :9090    # custom port
ccrawl api --index-dir idx/ --addr 0.0.0.0:8080  # listen on all interfaces
```

Flags:

| Flag | Default | Purpose |
|---|---|---|
| `--index-dir` | required | Path to the index directory built by `index build` |
| `--addr` | `:8080` | Listen address |

The server loads the index on startup and holds it in memory.
For a large index (hundreds of thousands of documents), allow a few seconds for loading.

## Endpoints

### `GET /v2/search`

Run a BM25 query against the local index.

```
GET /v2/search?q=golang+concurrency&n=10
```

Parameters:

| Parameter | Default | Description |
|---|---|---|
| `q` | required | Query string |
| `n` | 10 | Number of results to return |
| `k1` | 1.2 | BM25 k1 parameter |
| `b` | 0.75 | BM25 b parameter |

Response:

```json
{
  "query": "golang concurrency",
  "total": 42,
  "results": [
    { "url": "https://...", "title": "...", "snippet": "...", "score": 12.4 }
  ]
}
```

### `GET /v2/host/{host}`

Look up a single host record (rank, degree, CDX stats).

```
GET /v2/host/golang.org
```

Returns the same `HostRecord` structure that `host enrich` emits.

### `GET /v2/hosts`

List hosts, optionally filtered and sorted.

```
GET /v2/hosts?q=golang&sort=rank&n=20
```

Parameters:

| Parameter | Default | Description |
|---|---|---|
| `q` | — | Substring filter on hostname |
| `sort` | `rank` | Sort field: `rank`, `url_count` |
| `n` | 20 | Number of results to return |

### `GET /v2/health`

Health check.
Returns `{"status":"ok"}` with HTTP 200 when the server is ready.
Returns HTTP 503 if the index has not finished loading.

## Example: curl

```bash
# start the server
ccrawl api --index-dir ~/cc-index/ &

# search
curl "http://localhost:8080/v2/search?q=machine+learning&n=5" | jq .

# host lookup
curl "http://localhost:8080/v2/host/arxiv.org" | jq .

# health
curl "http://localhost:8080/v2/health"
```

## Example: integrate with a script

```python
import requests

BASE = "http://localhost:8080/v2"

def search(q, n=10):
    r = requests.get(f"{BASE}/search", params={"q": q, "n": n})
    r.raise_for_status()
    return r.json()["results"]

for hit in search("python async programming"):
    print(hit["score"], hit["url"])
```

## Authentication and security

The server has no built-in authentication.
If you expose it beyond localhost, put it behind a reverse proxy (nginx, Caddy) and add HTTP basic auth or an API key at that layer.

#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["huggingface_hub>=1.7.1", "hf-xet>=1.4.2"]
# ///
"""
Minimal HuggingFace commit helper for ccrawl.
Reads a JSON payload from stdin, performs the commit, prints the commit URL.

Input JSON (stdin):
{
  "token":       "hf_...",
  "repo_id":     "open-index/cc-host-dataset",
  "message":     "Add crawl=CC-MAIN-2026-21/subset=urls/prefix=a (68M rows)",
  "num_threads": 8,
  "ops": [
    {"local_path": "/abs/path/to/hosts-a.parquet", "path_in_repo": "data/crawl=CC-MAIN-2026-21/subset=urls/hosts-a.parquet"},
    ...
  ]
}

Output JSON (stdout):
{"commit_url": "https://huggingface.co/datasets/open-index/cc-host-dataset/commit/abc123"}
"""

import json
import sys
import os
import logging
import signal
import time

_COMMIT_TIMEOUT_S = 10 * 60


def _timeout_handler(signum, frame):
    print(
        f"[hf_commit.py] TIMEOUT: create_commit did not return within {_COMMIT_TIMEOUT_S}s — self-terminating",
        file=sys.stderr,
    )
    print(json.dumps({"error": f"commit timed out after {_COMMIT_TIMEOUT_S}s"}))
    sys.stdout.flush()
    os._exit(1)


signal.signal(signal.SIGALRM, _timeout_handler)

from huggingface_hub import HfApi, CommitOperationAdd, CommitOperationDelete
from huggingface_hub.errors import HfHubHTTPError

logging.basicConfig(
    level=logging.WARNING,
    format="[hf_commit.py] %(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
for _logger_name in ("httpx", "urllib3", "huggingface_hub", "hf_xet"):
    _lg = logging.getLogger(_logger_name)
    _lg.setLevel(logging.WARNING)
    _lg.handlers.clear()
    _lg.propagate = True


def main():
    payload = json.load(sys.stdin)
    token = payload["token"]
    repo_id = payload["repo_id"]
    message = payload["message"]
    ops_raw = payload["ops"]
    num_threads = payload.get("num_threads", 8)

    api = HfApi(token=token)

    operations = []
    total_size = 0
    skipped = 0
    for op in ops_raw:
        if op.get("delete", False):
            operations.append(CommitOperationDelete(path_in_repo=op["path_in_repo"]))
            continue
        local = op["local_path"]
        repo_path = op["path_in_repo"]
        if not os.path.isfile(local):
            print(f"[hf_commit.py] WARNING: file not found: {local}", file=sys.stderr)
            skipped += 1
            continue
        fsize = os.path.getsize(local)
        total_size += fsize
        print(f"[hf_commit.py] add: {repo_path} ({fsize / 1024 / 1024:.1f} MB)", file=sys.stderr)
        operations.append(CommitOperationAdd(path_in_repo=repo_path, path_or_fileobj=local))

    if skipped > 0:
        print(f"[hf_commit.py] WARNING: {skipped} file(s) missing", file=sys.stderr)
        data_ops = [o for o in operations if isinstance(o, CommitOperationAdd) and o.path_in_repo.endswith(".parquet")]
        if len(data_ops) == 0:
            message = f"Update README ({skipped} parquet(s) missing locally)"

    if not operations:
        print(json.dumps({"commit_url": "", "error": "no files to commit", "uploaded": 0}))
        sys.exit(1)

    print(f"[hf_commit.py] committing {len(operations)} ops ({total_size / 1024 / 1024:.1f} MB) to {repo_id}", file=sys.stderr)
    t0 = time.monotonic()

    uploaded = sum(1 for o in operations if isinstance(o, CommitOperationAdd))
    global _COMMIT_TIMEOUT_S
    size_timeout = max(600, int(total_size / (2 * 1024 * 1024)) + 300)
    _COMMIT_TIMEOUT_S = min(size_timeout, 40 * 60)
    print(f"[hf_commit.py] timeout set to {_COMMIT_TIMEOUT_S}s for {total_size / 1024 / 1024:.0f} MB", file=sys.stderr)
    signal.alarm(_COMMIT_TIMEOUT_S)
    try:
        commit_info = api.create_commit(
            repo_id=repo_id,
            repo_type="dataset",
            operations=operations,
            commit_message=message,
            num_threads=num_threads,
        )
        elapsed = time.monotonic() - t0
        print(f"[hf_commit.py] committed in {elapsed:.1f}s: {commit_info.commit_url}", file=sys.stderr)
        print(json.dumps({"commit_url": commit_info.commit_url, "uploaded": uploaded}))
    except HfHubHTTPError as e:
        elapsed = time.monotonic() - t0
        retry_after = 0
        if getattr(e, "response", None) is not None and e.response.status_code == 429:
            ra = e.response.headers.get("Retry-After", "")
            try:
                retry_after = int(ra)
            except (ValueError, TypeError):
                pass
        print(f"[hf_commit.py] HF error after {elapsed:.1f}s: {e}", file=sys.stderr)
        print(json.dumps({"error": str(e), "retry_after": retry_after}))
        sys.exit(1)
    except (OSError, ConnectionError) as e:
        elapsed = time.monotonic() - t0
        print(f"[hf_commit.py] network error after {elapsed:.1f}s: {e}", file=sys.stderr)
        print(json.dumps({"error": f"network: {e}"}))
        sys.exit(1)
    except Exception as e:
        elapsed = time.monotonic() - t0
        print(f"[hf_commit.py] error after {elapsed:.1f}s: {e}", file=sys.stderr)
        print(json.dumps({"error": str(e)}))
        sys.exit(1)


if __name__ == "__main__":
    main()

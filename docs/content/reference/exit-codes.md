---
title: "Exit codes"
description: "What ccrawl returns to the shell, and which codes are worth branching on."
weight: 35
---

`ccrawl` uses a small set of exit codes so scripts can tell the difference between a query that failed and a query that simply found nothing.

| Code | Meaning | Where it comes from |
|---|---|---|
| `0` | Success | Any command that finished |
| `1` | Error | Anything not classified below |
| `2` | Usage error, a missing or invalid argument | Argument checks, and a command that needs `duckdb` on a host without it |
| `3` | The query ran and matched nothing | `search`, `news search`, `columnar`, and the other read commands |
| `4` | A credential is needed and is not set | The commands that push to HuggingFace, listed in [what each command needs](/reference/requirements/) |
| `8` | Transport failure, the bytes did not arrive | `download`, when a file could not be read from the source |
| `75` | Temporary failure, run it again | The publish pipelines |

Those are the seven codes ccrawl returns. The shared taxonomy behind them reserves `5` for rate limiting, `6` for a missing entity, and `7` for an unsupported capability; no ccrawl command returns those today, and a script does not need to handle them.

## Exit 3 is not a failure

Exit 3 means the command worked, talked to Common Crawl, and got nothing back. That is a normal outcome when you search for a URL that was never crawled or list a month of CC-NEWS that does not exist.

```bash
if ccrawl search "$url" -o jsonl > captures.jsonl; then
  echo "found $(wc -l < captures.jsonl) captures"
elif [ $? -eq 3 ]; then
  echo "nothing crawled for $url"   # fine, keep going
else
  echo "search failed" >&2; exit 1
fi
```

Without the exit 3 check that script cannot distinguish "this URL is not in the index" from "the index is down", and both come back as an empty file.

## Exit 4 means set a token

Every command that writes to HuggingFace checks for `HF_TOKEN` (or `HUGGINGFACE_TOKEN`) before it does any work, and exits 4 when there is none:

```bash
env -u HF_TOKEN ccrawl urls recount -c CC-MAIN-2026-30; echo $?
# no HuggingFace token; set HF_TOKEN (or HUGGINGFACE_TOKEN), or pass --no-push
# 4
```

The check happens up front on purpose, so a run that would push at the end does not spend hours first. Every one of these commands also takes an opt out (`--no-push`, or `--push=false` for the markdown pipelines) which turns the same run into a local one and never asks for a token.

## Exit 8 means the bytes did not arrive

`download` returns 8 when a file could not be read from the source: the network went away, the source refused, or `--source s3` was asked for without credentials. It is separate from exit 1 because it says the request was fine and the transport was not, which is the case worth retrying automatically.

## Exit 75 means restart the run

`EX_TEMPFAIL` from `sysexits.h`. The publish pipelines return it in two situations:

- **Commit stall.** No commit landed within `--max-stall` (45 minutes by default). The stall clock cancels the run rather than letting it hang forever.
- **Incomplete run.** The run made progress but did not finish the crawl, usually because a source went away partway through.

Both are recoverable by running the same command again. Every pipeline resumes from what is already on the hub rather than from local state, so a restart costs one `paths-info` round trip and picks up where it left off.

A run that made **no** progress at all does not exit 75. That is deliberate: a permanently dead source would otherwise spin a supervisor forever.

### Supervising a publish run

This is what exit 75 is designed for. The unit restarts on 75 and stops on anything else:

```ini
[Unit]
Description=ccrawl markdown export

[Service]
Type=simple
ExecStart=/usr/local/bin/ccrawl markdown export --crawl CC-MAIN-2026-17 --repo you/your-dataset
Restart=on-failure
RestartForceExitStatus=75
RestartSec=60
Environment=HF_TOKEN=...

[Install]
WantedBy=multi-user.target
```

The same thing in a shell loop, for a run you are babysitting by hand:

```bash
until ccrawl urls publish --crawl CC-MAIN-2026-17; do
  code=$?
  [ $code -eq 75 ] || exit $code
  echo "stalled, restarting in 60s"
  sleep 60
done
```

## Caveat

An unrecognised flag comes back as `1` rather than `2`, because that error is raised by the flag parser before ccrawl sees the command. Everything ccrawl rejects itself, a missing required flag or an argument it cannot parse, exits `2`.

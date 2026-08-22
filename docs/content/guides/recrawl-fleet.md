---
title: "Running the recrawl fleet"
description: "Install ccrawl on three machines, run a shard of the work list on each under systemd, publish as it goes, and know what to do when one falls behind."
weight: 75
---

A recrawl of the published domain list is 121 million rows, and of the URL list about 2.1 billion.
At any rate one machine can hold, that is months of running, which makes this an operations problem rather than a command to type.
This page is the runbook: what gets installed, how it is started and stopped, what happens across a reboot, and what to do when one of the three machines stops keeping up.

The engine itself is described in [building a recrawl engine](../recrawl-engine/).
This page assumes it and talks only about running it on more than one box.

## The shape

Each machine takes one shard of the work list and publishes into a shared dataset repo.
Two units run per machine per work list, both templated on the name of the list:

| Unit | Does |
|---|---|
| `ccrawl-recrawl@domains` | Fetches this machine's third of the domain list and writes Parquet shards to local disk |
| `ccrawl-publish@domains` | Watches that directory and commits each shard to the hub as it closes, then deletes it |
| `ccrawl-recrawl.target` | One handle for everything this machine crawls |

The publisher runs beside the crawl and not after it.
A run of this length that published at the end would need months of disk, and none of these machines has that.
It also means the dataset is readable from the first hour rather than the last day.

The partition key is the registered domain, so a site and everything under it stays on one machine.
That is what keeps the politeness clock meaningful: two machines crawling the same site would each think they were spacing their requests properly, and the site would see twice the rate.

## Installing

From a checkout, on a machine that can ssh to all three:

```bash
deploy/install.sh                                  # all three, domain list
deploy/install.sh --servers server1 --kind urls    # one machine, url list
deploy/install.sh --start                          # install and turn it on
```

It builds one binary, copies it to each server, checks the hash on the far side before moving it into place, installs the units, and enables them.
It does not start anything unless asked, because starting a crawl is a decision about somebody else's bandwidth.

The reason it builds rather than copying whatever is on the box is the state the fleet was found in.
server3 was running 0.5.0 from July, and the other two were dev builds from two different afternoons.
A fleet running three binaries is a fleet where a rate difference between two machines means nothing, so the script prints the version off all three when it finishes and that output is the thing to read.

Two files are left alone by every install after the first:

`/etc/ccrawl/recrawl-<kind>.env` holds the shard number and the tuning.
It is written once, from the example in `deploy/env`, with the shard and the server name filled in.
A deploy that reset it would silently undo whatever the last person measured.

`/etc/ccrawl/hf.env` holds the token, mode 0600, read by the publisher and not by the crawl.
It is created empty, so the first commit fails loudly rather than the hundredth failing quietly.
Put the token in before starting a publisher.

## Starting and stopping

```bash
systemctl start ccrawl-recrawl.target     # everything this machine crawls
systemctl stop ccrawl-recrawl.target      # all of it, cleanly
systemctl status ccrawl-recrawl@domains   # one crawl
journalctl -fu ccrawl-recrawl@domains     # watch it
```

A stop is safe at any moment.
The checkpoint is written at a batch boundary and holds a part number and a row offset, so a run that is stopped and started again refetches the batch it was in and whatever the pool had in the air behind it, which is a few hundred rows.
It never skips.
The unit allows ninety seconds to shut down, which is far more than a batch takes to drain.

A reboot needs nothing done to it.
The target is enabled, the units come back with the machine, and each one reads its checkpoint and carries on.
This is worth testing once per machine rather than finding out during one.

## What the numbers in the env file mean

`CCRAWL_WORKERS` is the width of the pool, and it is the only one of these that moves the rate.

The rate is workers times yield over time per page, and on the domain corpus the yield is about 55 percent.
Everything else in this file has been measured against the live list and none of it changed the rate, which is the section below.
Workers is also what decides the memory, so read the memory section before raising it.

`CCRAWL_WRITERS` is how many output files are open at once, each with its own encoder.
Raise it only when a run reports the writer busy in the high eighties.
At four writers the share drops to about a third each, and the page rate moves by a few percent, so it is headroom rather than speed.

`CCRAWL_DNS_LOOKUPS` bounds the lookups in flight.
Every row of a domain corpus is a name nobody has looked up yet, so DNS is a per page cost there rather than a detail of the fetch.
The end of a run prints the peak against the bound.
A peak well under the bound means DNS is not the constraint and raising it will do nothing.
A peak sitting on the bound does not mean the opposite, which is the trap and is measured below.

`CCRAWL_SHARD_SIZE` is how much payload goes into a shard before it is sealed, counted uncompressed.
It decides how many files land on the hub and how much work a crash replays, and it does not decide the memory even though a shard buffers in memory until it is sealed.

`CCRAWL_TIMEOUT` is the budget for one fetch and its retries together.
Leave it at 30 seconds, for the reason below.

## Three dials that look like the ceiling and are not

Each of these was measured on server3 against the live domain list, 20000 rows a run from a fresh row offset, runs alternated so a drift in the box load falls on both settings rather than on one.
They are written down because all three are things the summary line invites you to turn, and turning any of them wastes a day.

**The DNS bound.** The peak pegs at whatever the bound is set to, so pegging proves there is demand and not that the queue costs anything.

| dns-lookups | pages a second | seconds an item | peak | idle |
|---|---|---|---|---|
| 32 | 19.2 | 5.498 | 32 | 27% |
| 128 | 19.3 | 5.289 | 128 | 29% |
| 512 | 20.7 | 4.986 | 216 | 28% |
| 512 | 23.9 | 4.144 | 200 | 31% |

Four times the bound moved the rate by half a percent.
The two runs at 512 differ from each other by 15 percent, which is the box, so even the gain at 512 is inside the noise.

**The fetch timeout.** Roughly 1250 rows in every 20000 time out at 30 seconds, which looks like a quarter of the pool held for the whole run and returning nothing.
Cutting it does make each item faster and it does not make the run faster.

| timeout | pages a second | seconds an item | timeouts per 20000 |
|---|---|---|---|
| 30s | 29.8 | 4.069 | 1246 |
| 10s | 28.1 | 4.418 | 1438 |
| 5s | 25.9 | 3.828 | 3779 |
| 5s | 28.8 | 3.673 | 3170 |
| 10s | 27.1 | 4.529 | 1573 |
| 30s | 25.6 | 4.759 | 1246 |

At five seconds the time per item drops about 15 percent and the timeouts nearly triple, so the yield drops about as much as the item got faster and the two cancel.
The averages are 27.7, 27.6 and 27.4 pages a second, and the spread inside the 30 second setting alone is 25.6 to 29.8.
A short timeout does not skip slow pages, it converts them into failures.

**The shard size.** A shard buffers in memory until it is sealed, so a 512 MB target across four writers looks like 2 GB of buffer before a worker holds anything.
Measured on server2 at 48 workers and 2 writers, alternated:

| shard size | peak resident |
|---|---|
| 20 MB | 1.042 GB |
| 512 MB | 1.177 GB |
| 512 MB | 1.190 GB |
| 20 MB | 1.299 GB |

The smallest setting produced the highest peak of the four.
The spread inside a setting is 25 percent and the difference between settings is 1 percent, so the rows are compressed as they are buffered and the buffer is not where the memory goes.
The memory is on the worker side, which is the next section.

The summary at the end of a run is written to be read in this order.
The failure breakdown says what the corpus is costing: on the domain list about 5500 rows in 20000 are names that do not resolve, 1400 are broken TLS handshakes and 1200 time out, and none of that is the machine's fault or something a wider pool fixes.
The timing line says what the machine is costing: how much of the run the pool spent idle, and how much of it each writer was busy.

## The URL list is sorted by host, and that is its ceiling

The two work lists need different things from the same crawler, and the difference is the order they are in.

The domain list is one row per host, so a batch of it is two thousand different sites and every worker has something to fetch.
The URL list comes out of Common Crawl's index in SURT order, which is sorted by host and then by path, so a batch of it is one site.
The politeness delay gives a host one request per second no matter how many workers are free, so a pool reading that list in order runs at one page a second and the rest of the pool waits.

Measured on server3 at 32 workers against the live URL list, before the fix: 596 pages in ten minutes, and all 596 of them from `vkbn.ru`.

The crawler now reads the URL list through a reorder buffer that keeps reading until it holds one host per worker, then hands rows out one host at a time.
Each site keeps its own pages in work list order, so this is a rotation and not a shuffle, and two runs over the same list hand out the same sequence.
The domain list is unaffected, because a read of it already satisfies the buffer on the first batch.

The cost is replay after a crash.
The checkpoint can only name the oldest row that has not finished, so rows held back in the buffer hold the checkpoint back with them, bounded by the buffer at sixteen batches.
That is a few minutes of refetching on a run measured in months.

Measured on server3 against the live URL list, with the domain run going on the same box:

| workers | distinct hosts in a two minute window | fetched pages a second |
|---|---|---|
| 32, before the buffer | 1 | 1.0 |
| 32 | 80 | 6.3 |
| 96 | | 17.6 |
| 256 | | 23.3 |

The rate follows the worker count because the worker count is what the buffer reads ahead for, and each worker holding its own host is what turns the one request per second per host into one request per second per worker.
It stops following it somewhere before 256, where the box runs out of whatever it runs out of first, and 256 workers on the URL list also halved the domain run beside it.
96 is where server3 is left, as the most rate per unit of memory rather than the most rate.

## Memory is the binding constraint, not CPU

This is the thing to know before tuning anything.

| | server1 | server2 | server3 |
|---|---|---|---|
| CPU | 4 | 6 | 8 |
| RAM | 5 GB | 11 GB | 23 GB |
| Available RAM | under 1 GB | about 1 GB | about 3 GB |
| Free disk | 156 GB | 20 GB | 27 GB |

These are shared machines with other work on them, and the available column is what is actually free rather than what is installed.

Measured peaks, all on the live domain list:

| workers | writers | shard size | peak resident |
|---|---|---|---|
| 48 | 2 | 20 MB to 512 MB | 1.0 to 1.3 GB |
| 64 | 2 | 20 MB | 1.1 GB |
| 256 | 1 | 512 MB | 2.7 GB |
| 256 | 4 | 512 MB | 4.5 GB |

Neither of the 256 worker numbers fits in what server1 has free today.

The rule of thumb between the two ends is roughly 15 to 20 MB of resident memory per worker, over a floor of about a gigabyte that does not move much below 64 workers.
It is a rule of thumb and not a formula: it is fitted to a handful of points on one corpus, and the page sizes on a different slice of the work list would move it.
Use it to pick a starting width and then watch the run rather than trusting the arithmetic.

So the width has to be set against the memory the box actually has at the moment, and that means checking `free -g` before raising `CCRAWL_WORKERS` rather than copying a number from another machine.
`install.sh` writes a `MemoryHigh` of 70 percent of installed RAM into a drop-in per machine.
`MemoryHigh` throttles rather than kills, which is the right end for a crawler: a run that slows down is one that carries on, and a run that is OOM killed repeats its batch and may do it again.

The logs are the other thing on the disk, and they were the surprise.
The run writes a JSON line per page to stdout, which is right for somebody watching a crawl and wrong for a unit that is up for months: an hour of it on server3 is about 390 MB of journal, so a machine would spend half a gigabyte a day recording lines nobody reads.
The crawl unit sends stdout to `/dev/null` for that reason.
Nothing an operator uses is lost, because the release the run picked, the failure breakdown, the timing line and every error are on stderr, and `journalctl -u ccrawl-recrawl@domains` still answers the questions it is asked.
Run the binary by hand when a per page trace is what you want.

Disk is the constraint people expect and it is currently the one that is fine, because the publisher deletes each shard after committing it.
The number to watch is not the free space, it is whether the free space is flat.
A capture directory that grows is a publisher that has stopped, and that is the failure below.

## When one machine falls behind

The three machines have different CPU, different memory and different neighbours, so they will not run at the same rate and are not meant to.
Falling behind matters when it is a fault rather than a difference.

**The publisher stopped and the crawl did not.**
Free disk drops steadily and the capture directory fills.
`journalctl -u ccrawl-publish@domains` will usually say the hub rejected a commit or the token is wrong.
The crawl can be left running while this is fixed if there is disk to spare, since the publisher picks up every closed shard it finds when it comes back.
Shards are named by a hash of their contents, so a shard that was uploaded and not deleted is republished as a no-op rather than a duplicate.

**The crawl is restarting in a loop.**
`systemctl status` shows a recent start and a low uptime, repeatedly.
The unit gives up after ten starts in ten minutes and stays down, which is deliberate: at that point it is the binary or the config rather than the network, and a machine that sits still is one that gets noticed.

**The crawl is running and the rate is much lower than the other two.**
Read the summary lines rather than the rate.
A writer share in the high eighties is the sink, and `CCRAWL_WRITERS` is the answer.
A DNS peak well under the bound rules the resolver out.
A high idle share with a low writer share is the pool waiting on something outside the machine, and on these machines that is usually the network or the neighbours rather than a setting, so read the load average before changing anything.
Do not reach for `CCRAWL_DNS_LOOKUPS` or `CCRAWL_TIMEOUT`, which were both measured and neither moved the rate.

**A machine has to be taken out of the fleet.**
Stop its target, then leave it.
Do not repartition the remaining two, because the shard number is what decides which rows a machine owns, and changing `CCRAWL_SHARDS` from three to two moves every row on every machine and invalidates all three checkpoints.
The right move is to fix the machine and let it resume, or to accept that a third of the work list is paused.

## Where the output goes

`open-index/ccrawl-recrawl-domains` and `open-index/ccrawl-recrawl-urls`, one repo each, because the two lists finish on completely different schedules and nobody wants a card that averages them.

Each machine writes exactly one ledger file, `ledger/<server>-shard<i>of<n>.csv`, and never touches another machine's.
Three machines committing at the same moment therefore cannot lose each other's numbers.
The dataset card is generated from the union of every ledger on the hub, so it corrects itself on the next commit from any machine, and a machine that was down for a day does not leave a permanently wrong card behind.

Every row carries the extracted text and Markdown as well as the body, rendered as the page was fetched, so a published shard is usable without a second pass over the corpus.

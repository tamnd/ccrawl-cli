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

Only three of them are dials, and the run tells you which one is in the way.

`CCRAWL_WORKERS` is the width of the pool.
The rate is workers times yield over time per page, and on the domain corpus the yield is about 55 percent, so this is the number that moves the rate.
It is also the number that decides the memory, see below.

`CCRAWL_WRITERS` is how many output files are open at once, each with its own encoder.
Raise it only when a run reports the writer busy in the high eighties.
At four writers the share drops to about a quarter each, and the page rate moves by a few percent, so it is headroom rather than speed.

`CCRAWL_DNS_LOOKUPS` bounds the lookups in flight.
Every row of a domain corpus is a name nobody has looked up yet, so DNS is a per page cost there rather than a detail of the fetch.
The end of a run prints the peak against the bound, and a peak sitting exactly on the bound for a whole run means workers are queueing for a lookup slot and this is the thing to raise.

The summary at the end of a run is written to be read in this order.
The failure breakdown says what the corpus is costing: on the domain list about 5500 rows in 20000 are names that do not resolve, 1400 are broken TLS handshakes and 1200 time out, and none of that is the machine's fault or something a wider pool fixes.
The timing line says what the machine is costing: how much of the run the pool spent idle, and how much of it each writer was busy.

## Memory is the binding constraint, not CPU

This is the thing to know before tuning anything.

| | server1 | server2 | server3 |
|---|---|---|---|
| CPU | 4 | 6 | 8 |
| RAM | 5 GB | 11 GB | 23 GB |
| Available RAM | under 1 GB | about 1 GB | about 3 GB |
| Free disk | 156 GB | 20 GB | 27 GB |

These are shared machines with other work on them, and the available column is what is actually free rather than what is installed.
A recrawl at 256 workers and 4 writers peaked at 4.5 GB resident, and at 256 workers and 1 writer at 2.7 GB.
Neither of those fits in what server1 has free today.

So the width has to be set against the memory the box actually has at the moment, and that means checking `free -g` before raising `CCRAWL_WORKERS` rather than copying a number from another machine.
`install.sh` writes a `MemoryHigh` of 70 percent of installed RAM into a drop-in per machine.
`MemoryHigh` throttles rather than kills, which is the right end for a crawler: a run that slows down is one that carries on, and a run that is OOM killed repeats its batch and may do it again.

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
A high idle share with a low writer share is the pool waiting on something outside the machine, usually DNS or the network.
A writer share in the high eighties is the sink, and `CCRAWL_WRITERS` is the answer.
A DNS peak sitting on the bound is the resolver, and `CCRAWL_DNS_LOOKUPS` is the answer.
If none of those is it, the box has neighbours and the load average will say so.

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

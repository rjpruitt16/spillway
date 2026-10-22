# Soak run 3l

One Spillway node (Fly performance-2x: 2 CPUs, 4GB, 10GB volume, the RocksDB
store, WebSocket transcripts in a local Valkey capped at 600MB, admission
memory limit 1,200MB) under Aquifer's soak harness
([`benchmark/soak`](https://github.com/rjpruitt16/aquifer/tree/main/benchmark/soak)
in the Aquifer repo; the target config is `fly.spillway-target.toml`), HTTP
jobs and WebSocket sessions together, with chaos on. Built from `main` at
bb09f8f (#69: webhook retries default to 10, about 13 minutes). Started
2026-10-17 19:03 UTC. These files were fetched at 2026-10-22 11:43 UTC, after
112 hours and the third scheduled machine reboot, and the run was stopped
after that.

Load: 500 jobs/s from 50 users, a 2,000/s burst every hour at :00, a
1,000-session WebSocket burst every hour at :30, a daily ramp at 04:00 UTC
(HTTP and WebSocket on alternate days), and a combined peak at 16:15 UTC.
Every few minutes the target API, the webhook receiver and the WebSocket
backend switch between healthy, slow, 503s, 429s, dropping and down. A
`kill -9` comes every 3-5 hours, and the first one after each 36 hours is a
full machine reboot instead.

## Totals

| | |
|---|---|
| Submitted | 213.2M |
| Accepted | 185.3M |
| Turned away when full (429/503) | 26.5M |
| Requests that errored on the client side | 1.4M |
| Webhooks received (completed / failed) | 185.2M / 64k |
| Restarts caused by the harness (`kill -9` and machine reboots) | 28 |
| Full machine reboots | 3 (Oct 19 07:10, Oct 20 20:15, Oct 22 11:16 UTC) |
| **Lost** (accepted, never finished) | **0** at the final snapshot; 234 jobs were still in flight then, the oldest 31 seconds old |
| Lost inside the flush window of a kill | 0 |
| Duplicate webhooks, all within 10 minutes of a restart | 9,956 |
| Finished without a webhook | 262 |
| Stuck over 24 hours | 0 |
| WebSocket commands sent / completed | 83.7M / 83.7M |
| WebSocket commands not completed within 5 minutes | 7,801 (0.009%) |

Run 3j, before #69, had 36,316 jobs finished without a webhook.

## What held

- **No accepted job lost** across 28 restarts, three of them full machine
  reboots, and none inside the flush window either.
- **#69 worked.** 262 jobs finished without a webhook, against 36,316 in
  run 3j. Each is a webhook that used up its 10 retries during a receiver
  outage.
- **Memory and disk level off; open files crept up.**
  - RSS at the end of each hour: median 908MB, highest 1,159MB. The daily
    maximum stayed between 1,031MB and 1,199MB, under the 1,200MB admission
    limit.
  - The store at the end of each hour: median 1.26GB, between 0.88GB and
    1.53GB.
  - Volume free space stayed between 60% and 67%.
  - Open file descriptors at the end of each hour: median 1,115, highest
    1,302. The daily peaks crept up, but within each process lifetime the
    count was flat (about 1,000-1,100), and each restart started near the
    first day's level. One-minute peaks of 2,600-3,550 came during bursts
    and webhook outages and fell back the next minute.
- **Restarts recover.** After each kill or reboot the backlog drained and
  accepts returned to 500/s within minutes.
- **Throughput.** Hourly accepts: median 460/s, lowest 374/s, in an
  outage-heavy hour. Accept p99 median 148ms. The HTTP ramp sustained
  500/s (Oct 21).
- **Duplicates only around restarts.** All 9,956 duplicate webhooks arrived
  within 10 minutes of a kill or reboot, which at-least-once delivery
  allows. The bigger batches (about 750) came at restarts with a large
  backlog.
- **WebSocket ordering.** 0 out-of-order events, 0 messages for unknown
  commands and 0 replay gaps across 251M events.

## What didn't hold

### WebSocket commands not completed within 5 minutes: 7,801

| Day | Commands | When |
|---|---:|---|
| Oct 18 | 97 | 04:00-04:36 WebSocket ramp, during a dropping backend |
| Oct 19 | 5,296 | 17:00 and 17:31, when Valkey timeouts crashed WebSocket session handlers |
| Oct 20 | 19 | 13:00-16:00 and 21:00-22:00 |
| Oct 21 | 680 | 18:00-20:59, the backend's dropping slots and a kill |
| Oct 22 | 1,709 | the 04:00 WebSocket ramp, which overlapped a kill |

The largest jump (Oct 19) coincided with Valkey write timeouts after HTTP
bursts, which crashed WebSocket session handlers. The run couldn't match
the affected command IDs to those errors, so this is a correlation, not a
proven cause. Reproducing it locally showed the mechanism: a timed-out
write closed the session with 1011 and dropped the backend message being
recorded. #78 now retries that write (idempotently, so a write that landed
late isn't recorded twice). #78 merged after this run started, so the run
measured the old behavior.

Separately, 58,605 WebSocket messages arrived after their command's
5-minute cutoff over the whole run. They couldn't be matched to these
7,801 commands, so it isn't known how many of them eventually completed.

The Oct 22 ramp window isn't fully explained: Spillway's logger was dropping
lines at the time, and no Valkey errors appear for those minutes. #70 tracks
it.

### Jobs stranded until the next restart

A job runner that died mid-job (for example during a burst) left its job
marked in flight in the store but no longer in the queue, and kept its
concurrency slot, so nothing ran the job until the next kill or reboot
recovered it. Between restarts unfinished jobs piled up: after the Oct 18
combined peak about 1,200-1,900 jobs sat unfinished, and the 18:34 kill
logged recovering 2,053 queued jobs, which brought outstanding back to
30-170 (the run didn't confirm job by job that these were the same jobs).
At other checks the oldest outstanding job was 112-165 minutes old. After these files were fetched, between the
14:35 kill and 17:43 on Oct 22, the stranded set grew to about 5,500 jobs
(the oldest 163 minutes) in steps at the hourly bursts. None were counted lost.
Restarts with a large recovery also had the larger batches of duplicate
webhooks (about 750), though the run didn't confirm that link. #72
(merged after this run's build) monitors job runners so a dead one frees
its slot and requeues its job.

### Ramp ceilings

The WebSocket ramp sustained 400 sessions on Oct 18 and 200 on Oct 20 and
22. The HTTP ramp sustained 500/s on Oct 21; on Oct 19 its first step
wasn't held. Both overlap the hourly bursts and the upstream's chaos modes,
so they understate a clean ceiling (see benchmark.md section 8 for that).

### Not measured

Heap and process counts read 0 throughout: the harness reads Go runtime
fields that Spillway doesn't report.

## Files

`status.json`, `hourly.json` (112 hours), `daily.json`, `minutes.json` (the
last 3 hours), `events.json` (the last 500 events) and `reports.json` (the
monitoring agent's 18 reports), all fetched from the driver before it
stopped. `reports.json` is kept as written; some of its elapsed-hour
figures are wrong (for example "78h" at 00:50 Oct 20, about 54 hours in),
so go by the timestamps.

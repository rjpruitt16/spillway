# Pacing and per-tenant fairness

By default all traffic for a destination flows through one shared queue. Turn on per-tenant isolation via a response header (or set it on submission if you already know a tenant needs it) and each `user_id` + API key gets its own independently paced queue: a heavy user no longer blocks everyone else, and each tenant can run at a genuinely different pace.

![Traditional circuit breaking rejecting requests outright, forcing clients to retry independently into a thundering herd on reopen, versus Aqueduct's per-client queues pacing release via X-Aqueduct-Rps until the backend catches up](images/circuit-breaking-vs-queue.gif)

**By your API's response**: respond to a dispatched request with:

```
X-Aqueduct-Account-Queue: enabled
```

(or `X-EZTHROTTLE-ACCOUNT-QUEUE: enabled`; both are read, `X-Aqueduct-*` first if both are present)

**By the client submitting the job**: set the same header on the `POST /jobs` request itself, if you already know at submission time that this tenant needs isolation:

```
X-Aqueduct-Account-Queue: enabled
```

Either way, Spillway switches to per-user isolation: each `user_id` + API key gets its own queue. A heavy user no longer blocks everyone else.

Above 70% of the shared upstream backlog budget, queues holding more than their current fair share receive progressively more `429` responses while smaller queues continue entering, even once the budget is full (up to 125% of it). One active queue can still use the entire budget. See [API.md](../API.md#fair-queue-admission) for the formula and observability headers.

Critically, each user can run at a **different pace**, and your backend decides it on every response. If your service processes requests from user A faster than user B, because of their tier, their data size, or just load at that moment, it advertises a different `X-Aqueduct-Rps` to each. When the upstream is busy, its total is split in proportion to those rates: a premium user advertised `50` next to a free-tier user advertised `5` gets 10 slots for every 1 of theirs. Note that literal pace (RPS/max-concurrent) can only ever be set by the upstream's own response headers, never by the client submitting the job, so a client can ask for isolation but never for a faster rate than the upstream gives it.

Disable it any time by responding with `X-Aqueduct-Account-Queue: disabled`.

Isolation between tenants doesn't give each one its own copy of the full rate. Every tenant queue for an upstream takes its dispatch slots from one shared pace, so the upstream sees at most its total however many tenants are active. The upstream sets that total too: it is the highest `X-Aqueduct-Rps` (or ORCA rate) it is currently advertising to any active tenant (one that dispatched in the last 3 seconds). The configured rate (`SPILLWAY_DEFAULT_RPS`, 2 if unset) applies only while it advertises nothing, and a pool-backed upstream's total is its live aggregate capacity. A backend that stamps one number on every response gets that number in total, not per tenant.

Tenants with equal rates take equal turns, and a tenant with a steady stream of new jobs can't crowd out one with a static backlog. When the upstream advertises different rates to different tenants, the turns stop being equal: see [Dynamic priority](#dynamic-priority). In a cluster, a background check also scales the queues on every node down to the same total.

`X-Aqueduct-*` is read first if both namespaces are present: the shared protocol namespace also spoken by [Aquifer](https://github.com/rjpruitt16/aquifer), so a backend already emitting Aqueduct headers works against either implementation unchanged.

**ORCA fallback**, for backends that speak neither namespace but already report load some other way: Spillway sends `endpoint-load-metrics-format: text` on every dispatch, and a response carrying an `endpoint-load-metrics` header is used to pace down (full rate below 70% utilization, 2 RPS at 70-90%, 0.5 RPS at 90-97%, 0.25 RPS above that) only when no explicit Aqueduct/Spillway header is present. Two backends verified directly against their own source: **vLLM** (metric name `kv_cache_usage_perc`, case-insensitive opt-in) and **Triton/TensorRT-LLM** (metric name `kv_cache_utilization`, case-sensitive lowercase-only opt-in; Spillway sends lowercase specifically so both work).

Long-term protocol goal: if more services emit `X-Aqueduct-*`, agents can respond to capacity signals instead of independently guessing retry and concurrency behavior. Spillway works today without ecosystem adoption; broader protocol adoption is the longer-term goal.

**Webhook delivery uses this same pacing, not a separate fire-and-forget path.** A webhook POST is enqueued as its own job, keyed by the receiver's domain, and dispatched through the identical AccountQueue machinery described above, so a webhook receiver can slow Spillway down with `X-Aqueduct-Rps`/`X-Aqueduct-Max-Concurrent` response headers exactly the way a real upstream can, instead of just getting hammered on a fixed retry schedule. It's also crash-durable: a webhook still pending when the node restarts is recovered and retried the same way a queued job is. Retries trigger on connection errors, `5xx`, `408` and `429`, with exponential backoff capped at `SPILLWAY_RETRY_MAX_BACKOFF_SECONDS` (300). The default is 10 retries, about 13 minutes: enough to ride out a receiver deploy or restart, and a receiver that stays down longer fails its webhooks rather than piling them up (the result stays readable at `GET /jobs/:id`). Set `"webhook_max_retries"` on the job (or the `X-Aqueduct-Webhook-Max-Retries` header, which wins) to change that for this job's webhook only, separately from `max_retries` for the upstream call: `0` sends it once, `4` gives up after about 15 seconds, and `-1` keeps retrying until the receiver accepts it, for up to 24 hours. Every retryable failure halves the queue's pace; an `X-Aqueduct-Rps` on a failed response can only lower it further, never raise it, so a receiver that stamps the same rate on every response, `503`s included, still gets backed off while it's down. Drain mode's own ledger-flush webhook is unaffected: it stays synchronous, confirming delivery before clearing the local idempotency ledger.

## Dynamic priority

Most queues that support priority read it from a static file: a tier per customer, or a list of user IDs that go first. That works until the important user changes faster than someone can edit the file. Spillway has no priority setting. Every tenant takes equal turns until the upstream says otherwise, and the upstream says otherwise on any response, with the same `X-Aqueduct-Rps` header it already uses to set its pace.

When several tenants are backlogged, the upstream's total is split in proportion to the rate it last sent each one. Advertise `600` to one tenant, `60` to a second and `6` to a third, and the total is 600/s, split 90%, 9% and 1%. A tenant that asks for less than its share keeps its own pace, and the rest goes to the others.

**That allows deliberately unfair processing when it matters.** Fair turns are the right default, but some moments aren't fair:

- A flood warning goes out and one county's alert jobs have to reach phones before anything else does.
- A security team is responding to a breach, and its log queries and key rotations can't wait behind routine scans.
- A hospital's monitoring alerts arrive during a surge, and a patient's alert can't sit behind billing exports.

In each case the backend already knows which tenant is in trouble, often before any human does. It advertises that tenant a much higher rate, say `1000` next to `10` for each of ten others, and from the next response on that tenant gets 1000/1100 of the upstream, about 91%, while everyone else slows down but keeps moving. When the emergency ends, the backend advertises that tenant its normal rate again: from its next response the turns are back to normal, and the total falls to the highest rate still advertised. Send the lower rate rather than dropping the header, because an advertised rate stays in force until the upstream sends a new one or a failure clears it; a response without the header keeps it. A tenant that goes idle stops counting toward the total after 3 seconds. Nothing was configured, and nobody had to deploy anything.

The rate a client submits never counts: only the upstream's responses can set pace or priority, so a tenant can't make itself the emergency.

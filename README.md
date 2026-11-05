# Spillway

![How engineers respond to congestion: pacing and backpressure everywhere else in infrastructure, versus retrying everything immediately at the API layer](docs/images/how-engineers-respond-to-congestion.jpg)

A durable queue that paces traffic into a service at the rate it can take, built on Elixir and the BEAM. Put it in front of your webhook handlers, a rate-limited third-party API, a GPU or your own backend: when a burst arrives faster than the target can handle, Spillway holds the excess on disk instead of letting callers retry into it, and releases it at the rate the target asks for.

A spillway is how a dam lets out water it can't hold, at a rate the river below can take. This one works in both directions: inbound, it holds the burst that agents and clients send your API; outbound, it holds the calls your app makes to rate-limited APIs.

- **Retry storm mitigation.** Callers wait in a durable queue instead of hammering a struggling service with retries, and the queue drains as capacity comes back.
- **Fairness.** Every tenant can get its own queue, paced on its own. In the fairness test, a quiet tenant's jobs finished in 1–5 s while another tenant flooded the same target; stuck behind the flood in one shared queue, they took 35–52 s.
- **Delivery however the caller wants it:** results on the same connection (`POST /proxy`), a live event stream, or a durable webhook that's paced and retried like any other job, plus WebSocket sessions proxied with a transcript that replays anything missed after a reconnect.

![Traditional load balancing collapsing under a spike, round-robin flickering faster as instances die, versus Aqueduct pacing that keeps the fleet stable while an autoscaler brings real capacity online](docs/images/fleet-degradation.gif)

## Why not Envoy, Redis, SQS or Hookdeck?

**Envoy** protects an overloaded service by rejecting the excess. A rejected request is gone, so every client retries on its own and they all compete for whatever capacity frees up next. Circuit breaking says "stop." Spillway says "wait here, and I'll tell you when to go."

![Circuit breaking rejecting requests and clients retrying into a thundering herd when it reopens, versus Aqueduct's per-client queues releasing work at the pace the backend asks for](docs/images/circuit-breaking-vs-queue.gif)

**Redis or SQS plus a rate-limited worker** can do the core of this. Spillway gives you the rest without writing it: a queue per tenant with fair admission, a pace the target sets live (`X-Aqueduct-*` response headers, or vLLM's and Triton's load reports) instead of a fixed worker rate, `POST /proxy` falling back to the queue on the same connection, idempotency keys, and results by stream or webhook.

**Hookdeck** queues inbound webhooks as a hosted service, billed by usage. Spillway runs on your own machines: there's no per-event fee, but you run and pay for the infrastructure. It also paces outbound API calls, agent tool calls and WebSocket sessions.

**Temporal** makes workflows durable and can cap how fast a task queue runs, but that cap is a number you set, not what the downstream reports it can take right now.

They combine: Envoy in front as the gateway, Hookdeck delivering webhooks into Spillway, and Temporal activities calling Spillway's `POST /proxy`.

## Spillway or Aquifer?

[Aquifer](https://github.com/rjpruitt16/aquifer) is the Go sibling. Both speak the same Aqueduct protocol and take the same `POST /jobs` body, so a backend that sends `X-Aqueduct-*` headers works with either. Pick Spillway if your team runs Elixir or wants a BEAM cluster where any node can take any user's request: connected nodes give each queue one live owner. Pick Aquifer for a single Go binary, a per-host rate file, and the larger set of end-to-end benchmarks.

## Results

- **112 hours under chaos, 0 jobs lost.** One performance-2x Fly machine took 185.3M jobs at 500 jobs/s from 50 users, with a target, webhook receiver and WebSocket backend that kept switching between healthy, slow, erroring and down, a `kill -9` every 3–5 hours and a machine reboot every 36. Across 28 restarts nothing was lost; 9,956 webhooks were delivered twice, all within 10 minutes of a restart ([write-up](benchmark/soak/results/run-3l/README.md)).
- **vLLM's queue: 447 waiting requests direct, 0 through Spillway,** at the same 40 requests a second. Spillway sent vLLM one request at a time here, because its concurrency starts at 1 and vLLM doesn't send a header to raise it ([#7](https://github.com/rjpruitt16/spillway/issues/7)), so this run didn't exercise its load-report pacing end to end.
- **Capacity per machine,** accepting, dispatching and delivering a webhook for every job: about 700 jobs/s on a Fly performance-1x, 1,300–1,500 on a 2x, and 3,000 on a 4x, where it sheds with `429` past the ceiling and drains back to zero.

Methods and caveats: [benchmark.md](benchmark.md), and the end-to-end tests of the Aqueduct protocol in [aqueduct-benchmarks](https://github.com/rjpruitt16/aqueduct-benchmarks).

## Quick start

With Docker:

```bash
docker build -t spillway .
docker run -p 4000:4000 \
  -e SECRET_KEY_BASE="$(openssl rand -hex 64)" \
  -e SPILLWAY_DEFAULT_RPS=5 \
  -v spillway-data:/data \
  spillway
```

Or from source, with Elixir installed: `mix setup && mix phx.server`.

`SPILLWAY_DEFAULT_RPS` sets how many requests a second each target gets (2 if unset), one at a time. The target can change its pace live with `X-Aqueduct-Rps` and `X-Aqueduct-Max-Concurrent` response headers.

Send it a job and watch it go through the line:

```bash
curl -s localhost:4000/jobs -H 'Content-Type: application/json' -d '{
  "user_id": "me", "idempotent_key": "first-job",
  "url": "https://httpbin.org/post", "method": "POST", "body": "{\"hello\":\"spillway\"}"
}'
# {"job_id":"…","status":"queued"}

curl -N localhost:4000/jobs/<job_id>/stream
# event: queued
# event: completed   (data carries the target API's response_status and body)
```

Fly.io and Kubernetes setups are in [Running and deploying](docs/deployment.md).

Keep Spillway on a private network behind a gateway you trust: it sends real HTTP requests to the URLs it's given, so it must never take them from untrusted callers. `SPILLWAY_ALLOWED_URL_DOMAINS` limits which hosts it will call.

## Durability and running more than one node

Each accepted job is written to RocksDB on the local disk of the node that accepted it.

- **A killed process loses nothing.** Every write reaches RocksDB's write-ahead log before Spillway answers. The log is synced to disk every 100 ms, so a machine or kernel crash can lose about the last 100 ms of accepted jobs. New jobs get `429` once the sync falls more than 1 second behind.
- **Recovering a lost job is a resubmit.** Send it again with the same `user_id` and `idempotent_key`: `200` with `"duplicate": true` means Spillway still has it, and `201` means it was lost and has been queued again. Spillway isn't meant to be the record of whether work succeeded; a payment service, for one, should keep its own database.
- **A restarted node picks its queue back up** from its volume and dispatches it again. Delivery is at least once, so make the target and your webhook handler idempotent on the job's key.
- **Queued jobs are not replicated.** If a node's disk is lost, the jobs queued on it are lost with it.
- **More than one node:** unconnected nodes are independent, each with its own queue and pacing, so several of them in front of one target multiply the rate it sees. Either give each protected target its own node, or connect the nodes in one region (`DNS_CLUSTER_QUERY`) so each queue has one owner across them ([Partitioning](docs/partitioning.md)).

## Documentation

The [docs](docs/README.md) folder covers use cases, pacing, pools, streaming, durability, partitioning, configuration, L8 and deployment. The full API is in [API.md](API.md); [DRAIN_MODE.md](DRAIN_MODE.md) and [REGISTRATION.md](REGISTRATION.md) cover handing nodes between tenants and reporting to a control plane.

For coding agents, the [Aquifer engineer skill](https://github.com/rjpruitt16/aquifer/blob/main/skills/aquifer-engineer/SKILL.md) covers benchmarking and tuning an Aqueduct deployment, and its guidance on pacing headers applies here too.

Formerly EZThrottle Local; see [Upgrading from EZThrottle Local](docs/deployment.md#upgrading-from-ezthrottle-local).

## Writing

- [Eliminate GPU Waste by Cutting the Retry Tax](https://rahmipruitt.me/content/gpu-retry-tax/)
- [GitHub Outages Show the Limits of Reactive Scaling](https://rahmipruitt.me/content/github-outage-reactive-scaling/)

## License

[Apache License 2.0](LICENSE). Releases before this change were MIT.

Built by [Rahmi Pruitt](https://rahmipruitt.me). Open to AI infra consulting, founding engineer, and contract work.

# Use cases

## Use cases

**Protect your API**
```
agents / clients  →  POST /jobs to Spillway  →  your backend (at controlled RPS)
```
Agents or clients hammering your API over HTTP? Spillway queues their requests and drains them to your backend at a pace it can handle. Your backend returns `X-Aqueduct-Rps` headers to signal how fast it wants traffic in real time.

**A durable checkpoint in front of a rate-limited resource**
```
your app  →  POST /jobs to Spillway  →  database / CI runner / OpenAI / Stripe / any rate-limited API
```
Calling something with its own capacity limit (a database read replica, a CI runner, a third-party API)? Spillway queues the calls durably and dispatches them at your configured rate, so a burst from your own side never becomes the thing that takes the downstream down. Works especially well closed-loop: if the downstream already speaks `X-Aqueduct-*` headers, it can tell Spillway to back off in real time instead of you guessing a static rate.

**Edge load balancer → gateway — pace and route at the edge**
```
your users  →  POST /proxy to Spillway  →  your resources (paced, routed, at the speed you can handle)
```
Point Spillway at your resources like a normal reverse proxy, close to the caller. It tries the request directly first (a healthy resource sees no queue at all) and only falls back to durable queuing when something's actually overloaded, on the same connection, staying in queue mode until that domain's backlog is genuinely drained (not just until a cooldown timer expires). See [`POST /proxy`](../API.md#post-proxy) for the details, including the header an upstream can use to request queuing proactively. For low-latency cross-region failover on top of that: Fly.io's own [`fly-replay`](https://fly.io/docs/networking/dynamic-request-routing/) is a response header *your* app returns to tell Fly's edge "redeliver this request in a different region." Your own logic in front of Spillway can watch for the same overload signal the circuit breaker already uses (429, 5xx, an ORCA threshold) and respond with `fly-replay` instead of just falling back locally, rerouting at Fly's edge rather than adding a round trip through your own infrastructure. Not something Spillway implements itself; the same overload classification just composes naturally with it.

In all three, the upstream can lower the dispatch pace via response headers. See [Per-tenant fairness](pacing.md) for how the ceiling, backoff, and recovery actually work.

**Durable WebSocket proxy**
```
clients  →  trusted gateway  →  GET /websocket  →  WebSocket backend
```
The same `aqueduct.v1` protocol as Aquifer, backed by the node's local store: durable ordered messages, cursor replay, paced upstream connection openings, and automatic reconnect. Authentication remains the gateway's job. See [`GET /websocket`](../API.md#get-websocket).

## How it works

```
Client → POST /jobs → Spillway → paced outbound requests → Your API
              ↓                                                          ↓
     GET /jobs/:id/stream                                        response body
              ↓                                                          ↓
     live event stream ←─────────────────────────────────────────────────
              ↓
     (or webhook if stream drops)
```

1. **Submit a job**: POST the request you want forwarded, with a `webhook_url` for the response.
2. **Spillway queues it**: jobs are held in memory and dispatched at the configured RPS.
3. **Your API responds**: Spillway reads pacing headers from the response and adjusts automatically, falling back to the real [ORCA](https://github.com/cncf/xds/blob/main/xds/data/orca/v3/orca_load_report.proto) standard for backends that report load a different way (vLLM and Triton/TensorRT-LLM both work today). See [Per-tenant fairness](pacing.md) for the full header reference.
4. **Stay on the line or hang up**: open `GET /jobs/:id/stream` to receive live events as the job moves through the queue, or disconnect and the result is delivered to your `webhook_url` when ready.

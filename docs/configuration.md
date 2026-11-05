# Configuration and admission control

## Admission control

Memory/DB-size ceilings that shed new (non-duplicate) jobs with a `429` once exceeded, mirroring [Aquifer's](https://github.com/rjpruitt16/aquifer) admission control.

| Env var | Default | Description |
|---|---|---|
| `SPILLWAY_VALKEY_URL` | *(unset)* | Keep WebSocket transcripts in Valkey/Redis streams (`XADD` with `MAXLEN`, expiring after `SPILLWAY_WS_STREAM_TTL_SECONDS`), as Aquifer does. Unset, they stay in the node's local store. |
| `SPILLWAY_STORE_MAX_BACKLOG` | `5000` | Refuse new jobs with 429 (`store_backlog`) once this many store writes are queued, instead of letting them pile up. `0` disables. Queue depths are on `GET /health` under `admission`. |
| `SPILLWAY_MEMORY_LIMIT_MB` | 70% of the container's memory limit, or of the machine's memory | Reject new jobs once the process's resident memory exceeds this many MB. Retained jobs live on disk, so memory doesn't grow with how many are kept. The default is logged at startup and is only a guess: if anything else runs on the machine (Valkey, a sidecar) or the machine is resized, set it yourself. `0` disables it. Off by default where memory can't be read (macOS). |
| `SPILLWAY_MAX_SYNC_LAG_MS` | `1000` | Reject new jobs with 429 (`sync_lag`) while the store's log hasn't been synced to disk for this long. Writes reach the OS as they're made, but disk only on the `SPILLWAY_STORE_SYNC_INTERVAL_MS` timer, so a machine crash or host reboot loses what came after the last sync. On a saturated node the timer can fall seconds behind. `0` disables it. `GET /health` shows `admission.sync_lag_ms`. |
| `SPILLWAY_ALLOWED_URL_DOMAINS` | *(unset, unrestricted)* | Comma-separated hostnames that `url`-routed jobs, `POST /proxy` and WebSocket upstreams may target (subdomains included); see [API.md](../API.md#submit-a-job) |
| `SPILLWAY_NODE_ID` | `FLY_MACHINE_ID`, else the host name | This node's id, sent to backends as `X-Aqueduct-Node-Id` and shown on `GET /metrics` ([API.md](../API.md#get-metrics)) |
| `SPILLWAY_MAX_BODY_BYTES` | `1048576` (1MB) | Reject oversized request bodies with `413` |
| `SPILLWAY_DB_MAX_BYTES` | `838860800` (800MB) | Reject new jobs once the store directory exceeds this many bytes |
| `SPILLWAY_MAX_EXPIRY_LAG_SECONDS` | `300` | Refuse new jobs with 429 (`expiry_lag`) while cleanup of expired jobs is more than this many seconds behind, so intake can't grow the store faster than it's cleaned. `0` disables. The lag and cleanup timings are on `GET /health` under `admission.expiry`. |
| `SPILLWAY_MAX_PENDING_PER_USER` | `10000` | Reject new client work for one user once its queued plus in-flight jobs reach this ceiling; `0` disables it |
| `SPILLWAY_MAX_PENDING_PER_UPSTREAM` | `10000` | Shared per-upstream backlog budget used by fair admission; `0` disables it |
| `SPILLWAY_RETRY_AFTER_SECONDS` | `5` | Base `Retry-After` on a `429`: doubles per consecutive rejection (capped at 60s), resets the moment a request is allowed again |

Body-size and DB-size admission are **on by default**, sized off the infrastructure this project is
actually benchmarked against (a single 512MB Fly.io instance with a 1GB volume; see
[benchmark.md](../benchmark.md)); set an explicit `0` to disable a check, or raise it for a bigger
deployment. Memory is the exception: there's no safe one-size-fits-all default since it depends on
your own deployment's memory budget, so it stays disabled until set explicitly; Spillway logs a
warning on startup if it isn't (benchmarked safe at 400MB on a 512MB instance, as a starting point).
`GET /health` reports a live `admission` snapshot.

## Configuration

Runtime config lives in `config/runtime.exs`: default RPS, account-queue mode, idempotency TTL, and a pluggable metrics adapter.

```elixir
# config/runtime.exs
config :spillway,
  default_rps: 2.0,
  account_queue_enabled: false,   # opt-in per-tenant isolation
  idempotent_ttl: 86_400,         # seconds
  metrics_adapter: MyApp.Metrics  # optional; defaults to no-op
```

### Metrics adapter

Spillway emits lifecycle events through a configurable metrics adapter.
Adapters implement the `Spillway.Metrics` behaviour:

```elixir
defmodule MyApp.Metrics do
  @behaviour Spillway.Metrics

  def job_queued(user_id, upstream), do: :ok
  def job_dispatched(user_id, upstream), do: :ok
  def job_completed(user_id, upstream, duration_ms), do: :ok
  def job_failed(user_id, upstream, reason), do: :ok
  def webhook_delivered(url, attempt), do: :ok
  def webhook_failed(url, attempts), do: :ok
  def queue_depth(upstream, depth), do: :ok
  def flow_rate(upstream, rps), do: :ok
end
```

The default adapter is `Spillway.Metrics.Noop`, so existing deployments do not change.

### Region adapter (cross-region /proxy redirect)

Off by default. Set `SPILLWAY_FLY_REGIONS` (comma-separated Fly region codes this app is deployed to) to enable `/proxy`'s cross-region redirect on Fly. See [API.md's "Cross-region redirect" section](../API.md#cross-region-redirect-flyio) for the full behavior. Direct port of Aquifer's `RegionAdapter`/`FlyRegionAdapter`.

| Env var | Default | Description |
|---|---|---|
| `SPILLWAY_FLY_REGIONS` | _(none, feature off)_ | Comma-separated Fly region codes this app is deployed to |
| `SPILLWAY_FLY_POLL_INTERVAL_SECONDS` | `30` | How often to poll sibling regions over Fly's private network for liveness |
| `SPILLWAY_REDIRECT_GATE_COOLDOWN_SECONDS` | `500` | How long to stop attempting cross-region redirect after a tour finds no reachable region at all: internal probe throttling, not what's told to the caller |
| `SPILLWAY_REDIRECT_EXHAUSTED_RETRY_AFTER_SECONDS` | `900` (15min) | `Retry-After` sent to the caller when cross-region redirect is configured but exhausted: request is rejected (429), not queued locally |

Custom implementations plug in via `:region_adapter`, implementing the `Spillway.RegionAdapter` behaviour (`live_regions/0`, `self_region/0`), same pattern as the metrics adapter above.

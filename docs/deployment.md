# Running and deploying

## Running locally

```bash
mix setup
mix phx.server
```

Integration tests (requires `hurl` and Python 3 with Flask):

```bash
make integration-test
```

L8 protocol tests (verifies handshake, signed delivery, and cryptographic signature; requires `pip install cryptography`):

```bash
make l8-test
```

## Docker

```bash
docker build -t spillway .
docker run -p 4000:4000 spillway
```

## Do not expose this directly to untrusted callers

`POST /jobs` takes a `url` field and dispatches a real HTTP request to it. If an arbitrary or untrusted party can set that field, Spillway becomes an open relay/SSRF vector: it can be pointed at your internal network, cloud metadata endpoints (`169.254.169.254`), or anything else the machine it runs on can reach, using its own network position and identity. The intended caller is **your own trusted backend or gateway code**, dispatching to a specific microservice or third-party API it already knows about, not an agent, end user, or any other untrusted party choosing the destination itself. Run it on a private network or internal service mesh, not bound to a public address, and if agents need to reach it, put your own authorization and destination allow-listing in front rather than letting them call this API directly.

## Deployment

Run standalone, partition nodes by upstream, or connect a region-local cluster with `DNS_CLUSTER_QUERY`. Each node persists to its own RocksDB store; Syn coordinates live queue ownership but does not replicate that disk state.

### Deploy to Fly.io

```bash
fly launch --config fly.toml
fly volumes create ezthrottle_data --region iad --size 1
fly deploy
```

The volume is required: jobs are stored on disk. Without it, `SPILLWAY_STORE_DIR` lives on the machine's ephemeral filesystem and durability is lost on every redeploy.

For maximum queue capacity, use the largest available machine. A 32GB RAM machine can hold 3–32 million jobs in memory, enough buffer for hours of traffic at typical agentic workloads, giving your autoscaler time to catch up before a single request is dropped.

```toml
[[vm]]
  memory = "32gb"
  cpu_kind = "performance"
  cpus = 16
```

### Deploy to Kubernetes

A fourth deployment shape alongside sidecar, standalone, and embedded library: Spillway as a normal Deployment, reached through a Gateway API proxy (Envoy Gateway) instead of a sidecar. See [`examples/kubernetes/`](../examples/kubernetes/), verified end-to-end against a real `kind` cluster, including a real pod restart (verified while the store was still Mnesia, which also needed a stable `RELEASE_NODE`).

### Scaling by partitioning

See [Partitioning strategies](partitioning.md) above for how to assign tenants to nodes, statically or dynamically.

### Zero-downtime updates

Spillway is built on the BEAM (Erlang VM), which supports hot code reloading. Updates to rate limiting logic, routing behavior, or configuration can be deployed to a running node without restarting the process, and the in-memory queue is preserved across deploys, with no jobs lost.

## Upgrading from EZThrottle Local

The project was renamed; behavior is unchanged.

- **Environment variables** are now `SPILLWAY_*`. The old `EZTHROTTLE_*` names still work: at boot each is copied to its new name unless that is already set, and a line on stderr lists the ones still in use.
- **Release and modules:** the release is `bin/spillway` and the OTP app is `:spillway`.
- **Wire headers are unchanged.** `X-EZTHROTTLE-*` request and response headers are shared with the managed EZThrottle service and its SDKs, and `X-Aqueduct-*` is the protocol both implementations speak.

## Upgrading from the Mnesia store

Versions before the RocksDB store kept jobs in Mnesia under `MNESIA_DIR`. That data is not migrated: let the node finish its queued jobs (stop sending traffic until `GET /health` shows none pending), then upgrade. `MNESIA_DIR`, `SPILLWAY_STORAGE` and `SPILLWAY_MNESIA_FLUSH_INTERVAL_MS` are no longer read; set `SPILLWAY_STORE_DIR` and `SPILLWAY_STORE_SYNC_INTERVAL_MS` instead. The store isn't tied to the Erlang node name, so a pinned `RELEASE_NODE` is no longer needed to keep it.

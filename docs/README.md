# Spillway documentation

Start with the [README](../README.md) for what Spillway is and a quick start.

- [Use cases](use-cases.md): protecting your API, pacing calls to a rate-limited resource, `POST /proxy` at the edge, and the WebSocket proxy.
- [Pacing and per-tenant fairness](pacing.md): `X-Aqueduct-*` headers, AccountQueue mode, the ORCA fallback for vLLM and Triton, and webhook pacing.
- [Pools](pools.md): registering backend instances and dispatching by capacity and reputation.
- [Streaming](streaming.md): live job events and queue position for agent fleets.
- [Idempotency and durability](durability.md): keys, shared scope, delivery semantics and the RocksDB store.
- [Partitioning](partitioning.md): static partitions, drain mode, external registration and region-local clustering.
- [Configuration and admission control](configuration.md): every admission limit, runtime config, the metrics adapter and cross-region redirect.
- [L8](l8.md): signed and encrypted webhook delivery, and request schemas.
- [Running and deploying](deployment.md): local runs and tests, Docker, Fly.io, Kubernetes, and upgrading from EZThrottle Local or the Mnesia store.

Reference: [API.md](../API.md), [DRAIN_MODE.md](../DRAIN_MODE.md), [REGISTRATION.md](../REGISTRATION.md), [benchmark.md](../benchmark.md).

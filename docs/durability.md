# Idempotency and durability

## Idempotency

Every job requires an `idempotent_key`. Submitting the same key twice **for the same `user_id`** returns the original job ID without re-executing the request; different users can safely use the same `idempotent_key` without colliding with each other. Keys expire after 24 hours (configurable).

**Shared scope.** With `SPILLWAY_SHARED_IDEMPOTENCY_ENABLED=true`, a job can set `"idempotency_scope": "shared"` to dedup on `idempotent_key` alone, across every `user_id`. Many agents asking for the same resource then coalesce onto one upstream call: later callers get `200 + "duplicate": true` with the first job's ID and can poll or stream it; they don't get their own webhook. Anyone who knows a shared key can read its result, so use keys that name a public resource (`weather:sf:2026-10-05`), never a user's private data. Shared keys live in the same RocksDB store as every other job, so they survive a crash.

**Delivery semantics:** Spillway provides at-least-once dispatch and webhook delivery, not exactly-once execution. If the node crashes after a dispatch succeeds but before it records that completion, the recovered job dispatches to the upstream again on restart, so it's not just the webhook that can repeat, the upstream call itself can. Make both your upstream endpoint and your webhook handler idempotent on `job_id` (or `idempotent_key`) anywhere duplicate execution isn't safe, the same contract Stripe and GitHub webhooks already ask of you.

## Durability (RocksDB)

Jobs, idempotency keys and results are written to RocksDB on the node's disk, called directly from Elixir. Spillway used Mnesia until soak run 3; its transaction layer and the Mnesia-to-RocksDB adapter were where the cost and most of the bugs were.

Two things matter for what "durable" actually means here:

- `SPILLWAY_STORE_DIR`: where the database lives. **Must point at a persistent volume in production** (`/data/spillway` by default, mirroring Aquifer's `DB_PATH`). Without a real volume mount, this data lives on the machine's ephemeral filesystem and durability is lost on every redeploy.
- `SPILLWAY_STORE_SYNC_INTERVAL_MS` (default `100`): every write reaches RocksDB's write-ahead log before it returns, so a killed process loses nothing. The log is synced to disk on this timer, which bounds what a machine or kernel crash can lose to about this many milliseconds of writes. `0` syncs on every write (no loss on a machine crash, a disk flush per request).
- `SPILLWAY_STORE_CACHE_MB` (default `128`): RocksDB's read cache, shared by every table. It sits outside the BEAM but counts toward resident memory, so leave room for it under `SPILLWAY_MEMORY_LIMIT_MB`. Each table also keeps a bloom filter, so looking up a key that isn't stored (a new idempotency key) rarely reads the disk.

A job's rows are written as one atomic batch. Each node's store is its own; nodes don't replicate it.

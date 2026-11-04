# Pools (agent-native load balancing)

Instead of dispatching to a fixed `url`, a job can target a named **pool**: a group of registered service instances Spillway picks from at dispatch time, weighted by declared capacity and live reputation. Ported from [Aquifer](https://github.com/rjpruitt16/aquifer)'s Go implementation, built and proven out there first.

**Registering a member:**

```bash
curl -X POST https://your-spillway/pools/writers/members \
  -d '{"member_id": "writer-1", "address": "http://10.0.1.5:8080", "capacity_rps": 20, "heartbeat_interval_seconds": 30}'
```

The same call is both initial registration and heartbeat: call it again periodically (at roughly your declared `heartbeat_interval_seconds`) to stay in the pool. Missing several consecutive expected heartbeats evicts a member. A member can register under more than one pool id.

**Dispatching to a pool:**

```json
{
  "user_id": "user-123",
  "idempotent_key": "job-1",
  "pool_id": "writers",
  "method": "POST",
  "webhook_url": "https://yourapp.com/webhooks/spillway"
}
```

`pool_id` and `url` are mutually exclusive: a job sets exactly one.

**How a member gets picked:** proportional to `capacity_rps × reputation`, not equal-split round robin: a member declaring 100 RPS gets roughly 4x the dispatches of one declaring 25. The pool's aggregate ceiling is the live sum of every member's current effective rate, so it grows and shrinks automatically as members register, degrade, or drop out, with no need to reconfigure Spillway as your fleet autoscales.

**Reputation**: a dispatch failure halves a member's effective share; a success nudges it back up. A member isn't evicted on one bad response, only once its reputation has stayed at the floor continuously, with no interrupting success, for a sustained window. This avoids flapping a member in and out of the pool over a single transient error.

**Set `capacity_rps` conservatively, not at your true theoretical max.** Spillway only learns a member died via a failed dispatch or a missed heartbeat, both of which lag the actual failure, so leaving headroom in what you declare gives real slack for that detection delay. Reputation decay is a second line of defense on top of this.

**A given `pool_id` should belong to exactly one Spillway instance**: pool membership is not distributed by Syn. If the same member registers the same `pool_id` with two different instances, each one independently believes it owns that member's full declared capacity. If a member genuinely needs to register with more than one instance, divide its declared `capacity_rps` across however many it's registered with.

`GET /health` reports every pool's current members, their declared capacity, and current reputation.

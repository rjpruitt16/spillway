# Why Spillway

The longer case for Spillway, from the README before it was shortened.


![How engineers respond to congestion: pacing and backpressure everywhere else in infrastructure, versus retrying everything immediately at the API layer](images/how-engineers-respond-to-congestion.jpg)

**TL;DR — why not Envoy?**

Envoy circuit breaking protects an overloaded service by rejecting excess work outright. That keeps the backend alive, but a rejected request is just gone. Nothing durable survives it, and the client is left to retry independently, creating uncoordinated competition for whatever capacity opens up next and wasting cycles on repeated attempts.

Spillway takes a different approach: persist the excess work, then pace its release.

The backend is also closer to the truth about its own capacity than a static proxy limit ever is. If the fleet loses half its usable capacity, the admission rate should fall; if capacity expands, the release rate should rise. Instead of leaning on static limits like max connections or concurrent requests, Spillway can ask the backend directly, continuously: *how fast should we send work right now?* (See [Per-tenant fairness](pacing.md) for how that ceiling, backoff, and recovery actually work.)

Circuit breaking protects the service by saying "stop." Spillway coordinates demand by saying "wait here, and I'll tell you when to go."

The name: a spillway is how a dam releases water it can't hold, at a rate the river below can take, so the dam doesn't fail. This one works in both directions. Inbound, it holds the burst agents and clients send your API. Outbound, it holds the calls your app makes to rate-limited APIs. Either way, the excess waits and is released at a pace the other side can handle.

**Increase your rate limit without DDoSing your backend.**

Kubernetes and modern orchestrators are great at scaling compute, but they weren't designed for spiky traffic or tenant fairness. When a burst of requests arrives, your pods get hammered, queues back up unevenly, and one noisy tenant crowds out everyone else. Horizontal scaling helps eventually, but the spike hits before a new pod is ready, so the burden falls on clients retrying uncoordinated: [wasted utilization and higher cost](https://rahmipruitt.me/content/gpu-retry-tax/) on one end, [outages reactive autoscaling alone can't prevent](https://rahmipruitt.me/content/github-outage-reactive-scaling/) on the other.

The usual fix for that is more hardware: double the fleet, triple the GPU budget, eat the bill. It works, but it's expensive, it leaves capacity idle most of the time, and it doesn't actually fix the burst, just buys enough headroom to survive the next one:

![How teams handle traffic spikes: overprovisioning the fleet versus queueing the burst and pacing the flow while capacity catches up](images/how-teams-handle-traffic-spikes.png)

Spillway is what I built to actually fix it: a self-hosted load balancer that absorbs bursts into a durable queue, dispatches at a controlled rate, and spreads traffic across a pool of backend instances. Your API sheds load by talking back, not by getting hammered until it falls over.

What's usually behind that API can't scale instantly either: a GPU, a database, a CI runner. Spillway buys time for more of it to come online; it's overkill if that ceiling is fixed for good.

Real numbers (durability, throughput ceiling, admission shedding, multi-tenant fairness, a real GPU under load, and a [112-hour soak with 0 lost jobs across 28 restarts](../benchmark/soak/results/run-3l/README.md)) are in [benchmark.md](../benchmark.md), including a head-to-head against [Aquifer](https://github.com/rjpruitt16/aquifer), the Go/SQLite sibling this project mirrors. Longer, end-to-end tests of the Aqueduct protocol in front of real open-source systems, starting with Hatchet behind Aquifer, are in [aqueduct-benchmarks](https://github.com/rjpruitt16/aqueduct-benchmarks).

![Traditional load balancing collapsing under a spike, round-robin flickering faster as instances die, versus Aqueduct pacing that keeps the fleet stable while an autoscaler brings real capacity online](images/fleet-degradation.gif)

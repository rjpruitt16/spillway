# Streaming

Most queue systems force you to choose: poll for status or hope the webhook lands. Spillway gives you a third option: an open SSE stream that tells you exactly what's happening in real time, or a `webhook_url` fallback if you disconnect. Coordination built into the infrastructure, the same way TCP handled it at Layer 4. At Layer 7, for API workflows, the queue is the protocol.

```bash
# Submit a job
curl -X POST http://localhost:4000/jobs -d '{...}'
# → { "job_id": "abc123" }

# Stay on the line
curl -N http://localhost:4000/jobs/abc123/stream
```

```
event: queued
data: {"job_id":"abc123","status":"queued"}

event: position
data: {"job_id":"abc123","position":4}

event: position
data: {"job_id":"abc123","position":2}

event: dispatching
data: {"job_id":"abc123"}

event: completed
data: {"job_id":"abc123","response_status":200,"body":"..."}
```

**If the stream drops before completion, the result is delivered to your `webhook_url` automatically.** Terminal results are also retained for `GET /jobs/:id`, so polling later returns the same completed/failed payload.

## Queue position for agent fleets

When multiple agents share the same API key, they share the same queue. The `position` event tells each agent where it stands:

```
position: 12 → the line is long, go work on something else
position: 3  → getting close, stay on the line
dispatching  → your turn
```

An agent that sees position 12 can disconnect, pick up other work, and trust the webhook will arrive when the job is done. An agent at position 1 stays on the line and streams the response as tokens arrive.

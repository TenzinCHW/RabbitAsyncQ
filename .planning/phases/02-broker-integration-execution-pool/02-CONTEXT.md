# Phase 02: Broker Integration & Execution Pool - Context

**Gathered:** 2026-03-28
**Status:** Ready for planning

<domain>
## Phase Boundary

The RabbitMQ consumer dispatches incoming jobs to a process pool and safely forwards yielded intermediate results to the broker without dropping the connection or starving the heartbeat.
</domain>

<decisions>
## Implementation Decisions

### Prefetching & Pool Size
- **D-01:** Set RabbitMQ `prefetch_count` exactly equal to the number of processes in the pool to prevent idle workers and prevent massive local queue buildup.
- **D-02:** Let `JobManager` manage the pool sizing automatically, with a user override option.
- **D-03:** Use `os.sched_getaffinity(0)` on Linux to respect Docker/cgroups CPU limits rather than relying solely on the naive `os.cpu_count()`.

### Connection Ownership & Lifecycle
- **D-04:** Run the RabbitMQ `start_consuming()` loop in the main thread (blocking) instead of a background thread. This aligns with standard Python daemon patterns.
- **D-05:** Register `SIGINT` / `SIGTERM` handlers inside the `JobManager` to safely shut down the process pool, join the IPC thread, and gracefully close the `pika` connection loop.

### Error & Retry Policies
- **D-06:** If a worker throws a normal Python exception, the system should Ack the incoming job message and publish the error dictionary back to the result queue (keeping the current Phase 1 behavior).
- **D-07:** Handling unexpected hard process crashes (e.g., OOM kills, segfaults) is explicitly deferred to Phase 3 (Resilience & Crash Recovery).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

No external specs referenced — requirements fully captured in decisions above.
</canonical_refs>

<code_context>
## Existing Code Insights

### Established Patterns
- **IPC Thread safe callbacks**: The background IPC consumer thread correctly uses `self.conn.add_callback_threadsafe()` to communicate. This pattern is robust and must be maintained when we shift the `pika` IO loop to block the main thread.

### Integration Points
- `JobManager` in `rabbitasyncq/manager.py` currently calls `start_consuming` in `JobManager.__init__` via a thread. This must be refactored so initialization is separate from execution (e.g., adding a `JobManager.start()` blocking method).
</code_context>

<specifics>
## Specific Ideas

No specific UI/UX requirements — system is purely backend.
</specifics>

<deferred>
## Deferred Ideas

- Detecting hard worker crashes via `Future.add_done_callback` and NACKing jobs was discussed but deferred to Phase 3.
</deferred>

---
*Phase: 02-broker-integration-execution-pool*
*Context gathered: 2026-03-28*

# Phase 03: Resilience & Crash Recovery - Context

**Gathered:** 2026-03-28
**Status:** Ready for planning

<domain>
## Phase Boundary

The system detects and recovers from worker process crashes (OOM, segfaults), NACKing the affected message and safely handling process pool degradation without leaving hanging messages or zombie processes.
</domain>

<decisions>
## Implementation Decisions

### Crash Detection Strategy
- **D-01:** Detect hard crashes by attaching `future.add_done_callback()` to the `ProcessPoolExecutor` futures. When a future completes unexpectedly with an exception (like `TerminatedWorkerError` or `BrokenProcessPool`), we can catch the crash in the callback.

### Message Disposition
- **D-02:** When a hard crash is detected, the `JobManager` must execute `basic_nack(requeue=False)` on the affected job's delivery tag. This routes the poison-pill message to the dead-letter exchange rather than requeuing it in an infinite crash loop.

### Pool Recovery Strategy
- **D-03:** If `ProcessPoolExecutor` raises a `BrokenProcessPool` exception and becomes unusable, the `JobManager` will adopt a "Fail Fast" strategy. It must log the critical failure, clean up the RabbitMQ connection, and call `sys.exit()` to terminate the process, allowing an external supervisor (like Docker) to restart the service cleanly.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

No external specs referenced — requirements fully captured in decisions above.
</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `self.conn.add_callback_threadsafe()` is heavily utilized in the `JobManager._consume_ipc()` and will be required inside the future callback, as `add_done_callback` functions are invoked by a background thread within `ProcessPoolExecutor`.

### Established Patterns
- We have the `ProcessJobContext` in `self.jobs` which holds the job metadata (`job_id`, `method.delivery_tag`, `ch`). This context must be cleaned up (`del self.jobs[job_id]`) when a future fails.

### Integration Points
- The `JobManager.accept_job()` method creates the future. The `add_done_callback` should be attached here.
- The `future.exception()` method inside the callback can be used to distinguish between a clean exit (returns `None`) and a worker crash.
</code_context>

<specifics>
## Specific Ideas

No specific UI/UX requirements — system is purely backend.
</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---
*Phase: 03-resilience-crash-recovery*
*Context gathered: 2026-03-28*

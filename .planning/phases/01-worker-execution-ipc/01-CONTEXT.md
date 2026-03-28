# Phase 1: Worker Execution & IPC - Context

**Gathered:** 2026-03-28
**Status:** Ready for planning

<domain>
## Phase Boundary

Executing a single job generator in an isolated process, capturing its yielded results via IPC, and cleanly cancelling it.
</domain>

<decisions>
## Implementation Decisions

### IPC Mechanism
- **D-01:** Use `multiprocessing.Queue` to stream yielded results from the worker process back to the main process.

### Cancellation Signaling
- **D-02:** Use `multiprocessing.Event` to signal cancellation from the main process to the worker process, keeping the pattern similar to the existing `threading.Event`.

### Exception Serialization
- **D-03:** Serialize exceptions as a dictionary (e.g., `{type: 'ValueError', msg: '...', traceback: '...'}`) in the worker before sending via IPC to avoid pickling issues and support structured logging.

### Worker Abstraction
- **D-04:** Use `concurrent.futures.ProcessPoolExecutor` or `multiprocessing.pool.Pool` instead of bare `multiprocessing.Process` if possible, even for Phase 1, to make Phase 2 (Execution Pool) easier.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

No external specs — requirements fully captured in decisions above.
</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `StoppableJob` (in `rabbitasyncq/job.py`): The existing thread-based class can serve as a template for the new process-based worker abstraction.
- `JobManager` (in `rabbitasyncq/manager.py`): Contains the logic for parsing incoming RabbitMQ messages, which will need to interface with the new process pool/worker abstraction.

### Established Patterns
- **Yield-based status updates:** The worker expects the user's `job_fn` to yield dictionaries, which are currently appended with `job_id` and `status="RUNNING"`. This pattern must be maintained in the new IPC mechanism.
- **Graceful cancellation via Event:** The existing `_stop_event` pattern should map well to the new `multiprocessing.Event`.

### Integration Points
- The boundary between `JobManager` (which holds the RabbitMQ connection) and the worker process. The worker MUST NOT interact with `pika` directly; all communication must go through the IPC `Queue`.

</code_context>

<specifics>
## Specific Ideas

No specific requirements — open to standard approaches.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---

*Phase: 01-worker-execution-ipc*
*Context gathered: 2026-03-28*

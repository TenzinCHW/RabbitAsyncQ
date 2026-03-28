# Requirements

## v1 Requirements

### Execution
- [x] **EXEC-01**: System uses a Multi-Process Execution Pool to run jobs, bypassing the Python GIL for compute-heavy tasks.
- [x] **EXEC-02**: System implements Robust IPC to send results from worker processes back to the main process safely.
- [ ] **EXEC-03**: Main process maintains Broker Heartbeat during long-running tasks without blocking.
- [ ] **EXEC-04**: System supports Crash Recovery to detect dead worker processes and nack jobs/respawn as needed.

### Job Lifecycle
- [x] **LIFE-01**: System supports Yield-Based Intermediate State, allowing workers to stream yielded data back via IPC.
- [x] **LIFE-02**: System supports Graceful Generator Cancellation by signaling workers to stop and allowing cleanup via `finally`.

## v2 Requirements (Deferred)

### Lifecycle
- [ ] **LIFE-03**: Hard Cancellation (SIGTERM) to forcefully terminate processes that hang and refuse to yield.
- [ ] **LIFE-04**: Zero-State-Backend Architecture (formalized explicitly).

## Out of Scope

- **External Result Backend (Redis)**: Instances must remain independent; all state is passed via RabbitMQ.
- **Worker AMQP Connections**: Pika connections are not process-safe; workers cannot interact directly with RabbitMQ.
- **Asyncio Rewriting**: Existing synchronous `yield` API must be maintained for compatibility.
- **Complex Process Topologies**: Maintain a flat topology of one main Pika process and N isolated worker processes.

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| EXEC-01 | Phase 2 | Complete |
| EXEC-02 | Phase 1 | Complete |
| EXEC-03 | Phase 2 | Pending |
| EXEC-04 | Phase 3 | Pending |
| LIFE-01 | Phase 1 | Complete |
| LIFE-02 | Phase 1 | Complete |
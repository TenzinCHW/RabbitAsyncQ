# RabbitAsyncQ

## What This Is

RabbitAsyncQ is a Python library for queuing, starting, and gracefully stopping asynchronous jobs via RabbitMQ. It is being upgraded from a thread-based execution model to a multi-processing model to better handle compute-intensive workloads.

## Core Value

Reliable, interruptible execution of compute-intensive Python jobs driven by RabbitMQ messages.

## Requirements

### Validated

- ✓ Accept jobs via RabbitMQ input queues
- ✓ Execute job functions that yield intermediate results
- ✓ Gracefully cancel running jobs via RabbitMQ stop messages
- ✓ Publish job status updates and final results back to RabbitMQ
- ✓ Maintain message broker connections and process AMQP events
- ✓ Execute jobs using a process pool or process-based executor instead of threads (Validated in Phase 01: worker-execution-ipc)
- ✓ Ensure job cancellation works correctly across process boundaries (Validated in Phase 01: worker-execution-ipc)
- ✓ Maintain the existing `yield`-based job function interface (Validated in Phase 01: worker-execution-ipc)
- ✓ Safely communicate intermediate results from worker processes back to the main RabbitMQ consumer process (Validated in Phase 01: worker-execution-ipc)
- ✓ Prevent zombie processes and manage process lifecycle safely (Validated in Phase 01: worker-execution-ipc)
- ✓ Execute jobs using a Multi-Process Execution Pool and match RabbitMQ prefetch limits (Validated in Phase 02: broker-integration-execution-pool)
- ✓ Main process maintains Broker Heartbeat during long-running tasks without blocking (Validated in Phase 02: broker-integration-execution-pool)

### Active

### Out of Scope

- Distributed worker state beyond RabbitMQ queues (keep instances independent)
- Asyncio rewriting (maintaining the synchronous/yield-based interface is preferred for existing workloads unless necessary)

## Context

The library currently uses `threading.Thread` and a `threading.Event` to run and cancel jobs. This works for I/O bound tasks but is blocked by the Python GIL for compute-intensive tasks, reducing throughput. 
The main process runs the Pika consumer which listens to RabbitMQ. Transitioning to processes requires careful handling of IPC (Inter-Process Communication) because Pika connections are not thread-safe, let alone process-safe, meaning worker processes must pass messages back to the parent process to publish results.

## Constraints

- **Execution Model**: Must use multiple processes (e.g., `multiprocessing.Pool`, `concurrent.futures.ProcessPoolExecutor`) to bypass the GIL.
- **Dependency**: `pika` for RabbitMQ communication.
- **Compatibility**: Should minimize changes to the public API (how users define and start jobs).

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Move to Processes | Threads block the GIL for compute-bound tasks, limiting scalability. | ✓ Validated in Phase 01 |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-03-28 after Phase 02 completion*

# Feature Landscape

**Domain:** Python AMQP Job Queuing / Compute Execution
**Researched:** 2026-03-28

## Table Stakes

Features users expect. Missing = product feels incomplete.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| **Multi-Process Execution Pool** | Bypasses Python GIL for CPU-heavy jobs, which threads cannot do. | Medium | Core goal. Replace `threading.Thread` with `multiprocessing` processes to isolate tasks. |
| **Robust IPC for Results** | Pika connections are not process-safe; workers can't ack directly. | High | Must use `multiprocessing.Queue` or `Pipe` back to the parent process's Pika loop. |
| **Broker Heartbeat Maintenance** | Main process must keep AMQP connection alive during long, compute-heavy tasks. | Medium | Pika event loop runs continuously in the main process, delegating execution. |
| **Crash Recovery** | If a worker dies (e.g., OOM, segfault), the job must be nacked and the pool restored. | Medium | Requires detecting dead child processes and respawning them safely. |

## Differentiators

Features that set product apart. Not expected, but valued.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| **Yield-Based Intermediate State** | Most queues (Celery/RQ) require external Redis for state. We stream `yield`s straight to RabbitMQ. | High | The worker loop calls `next()` on the job's generator and ferries yielded data over IPC. |
| **Graceful Generator Cancellation** | Sending a RabbitMQ stop message triggers `generator.close()` in the worker, allowing clean up via `finally`. | High | The wrapper checks a cancel flag between `yield`s and injects `GeneratorExit`. |
| **Hard Cancellation (SIGTERM)** | If a job is cancelled and hangs, it must be forcefully terminated without crashing the main process. | Medium | Requires tracking worker PIDs directly; `concurrent.futures` obscures PIDs so custom pool may be needed. |
| **Zero-State-Backend Architecture** | Removes the need for Redis/Memcached. All state & results are passed strictly via RabbitMQ queues. | Low | Reduces infrastructure footprint massively compared to traditional Python queues. |

## Anti-Features

Features to explicitly NOT build.

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| **External Result Backend (Redis)** | Violates "Out of Scope" rule (instances must be independent). | Stream intermediate & final results back to the RabbitMQ broker via IPC. |
| **Worker AMQP Connections** | Connecting to RabbitMQ from child processes is unsafe (causes duplicate frames, connection resets). | Send results to the main process via `multiprocessing.Queue` to publish safely. |
| **Asyncio Rewriting** | Forces users to rewrite legacy synchronous CPU-bound code to `async def`. | Keep the standard synchronous `yield` API; use processes for non-blocking concurrency. |
| **Complex Process Topologies** | Sub-pools or nested workers complicate lifecycle and signal handling. | Keep a flat topology: One main Pika process, N isolated worker processes. |

## Feature Dependencies

```text
Multi-Process Execution Pool → Robust IPC for Results (workers must communicate with parent)
Robust IPC for Results → Yield-Based Intermediate State (reuses the same IPC pipe for streaming)
Graceful Generator Cancellation → Multi-Process Execution Pool (requires cross-process signaling like multiprocessing.Event)
Hard Cancellation (SIGTERM) → Multi-Process Execution Pool (requires direct tracking of process objects)
```

## MVP Recommendation

Prioritize:
1. **Multi-Process Execution Pool**: Swap threads for processes.
2. **Robust IPC for Results**: Route final return values through a `Queue` to the main Pika loop.
3. **Yield-Based Intermediate State**: Expand the IPC to ferry yielded items.
4. **Graceful Generator Cancellation**: Pass a `multiprocessing.Event` to signal cancellation and trigger `generator.close()`.

Defer:
**Hard Cancellation (SIGTERM)**: Start with graceful cancellation via `yield` checking. Hard kills can be added later if tasks refuse to yield and hang indefinitely.

## Sources

- .planning/PROJECT.md (Project Context & Constraints)
- Python `multiprocessing` documentation (IPC patterns, safe state sharing)
- Pika documentation (Connection thread/process safety warnings)
- Ecosystem Analysis (Celery vs Dramatiq standard practices)

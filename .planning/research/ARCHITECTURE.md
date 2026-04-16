# Architecture Research

**Domain:** RabbitMQ Python Job Queuing (Multiprocessing)
**Researched:** 2026-03-28
**Confidence:** HIGH

## Standard Architecture

The architecture transitions from thread-based to process-based execution to bypass the Python GIL for compute-bound tasks, while maintaining the non-thread-safe Pika connection safely in the main process.

### System Overview

```text
┌─────────────────────────────────────────────────────────────┐
│                     External Systems                        │
├─────────────────────────────────────────────────────────────┤
│                       ┌──────────┐                          │
│                       │ RabbitMQ │                          │
│                       └────┬─────┘                          │
│                            │ (AMQP over TCP)                │
├───────┴────────────────────┴────────────────────┴───────────┤
│                   Main Process (Controller)                 │
├─────────────────────────────────────────────────────────────┤
│  ┌────────────────┐     ┌──────────────┐    ┌────────────┐  │
│  │  Pika Consumer │ ──> │ Job Manager  │    │ IPC Reader │  │
│  │  (Event Loop)  │ <── │ (Lifecycle)  │ <──│ (Thread)   │  │
│  └────────────────┘     └──────┬───────┘    └──────▲─────┘  │
│         │ (thread-safe         │                   │        │
│         │  callbacks)          │ (spawns/signals)  │ (reads)│
├─────────┼──────────────────────┼───────────────────┼────────┤
│         │                      │                   │        │
│  ┌──────▼──────┐        ┌──────▼──────┐     ┌──────┴─────┐  │
│  │ Pika Publ.  │        │ Cancel Event│     │Result Queue│  │
│  └─────────────┘        └──────┬──────┘     └──────▲─────┘  │
│                                │ (reads)           │ (puts) │
├───────┴────────────────────────┴───────────────────┴────────┤
│                 Worker Processes (Executors)                │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   Worker Runner                     │    │
│  │  ┌───────────────┐               ┌───────────────┐  │    │
│  │  │ User Job Func │ ──(yields)──> │ IPC Publisher │  │    │
│  │  └───────────────┘               └───────────────┘  │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Responsibility | Typical Implementation |
|-----------|----------------|------------------------|
| **Pika Consumer / Publisher** | Maintains RabbitMQ connection, processes incoming jobs/cancellations, sends status updates. | `pika.BlockingConnection` or `SelectConnection` in main process. |
| **Job Manager** | Maintains registry of active jobs mapping `job_id` to its process handle and cancel event. Handles timeouts and zombie process cleanup. | Python `dict` + `multiprocessing.Process` lifecycle tracking. |
| **IPC Reader** | Listens for results/statuses from workers without blocking the Pika event loop. Forwards events to Pika. | A `threading.Thread` blocking on `queue.get()` calling `add_callback_threadsafe`. |
| **Cancel Event** | Signals a specific worker to gracefully abort its operation. | `multiprocessing.Event` (one per job/worker). |
| **Result Queue** | Aggregates all yield events, completion statuses, and errors from all workers back to the main process. | `multiprocessing.Queue` (shared across all workers). |
| **Worker Runner** | Wraps user logic. Executes generator, intercepts yielded values and errors, routes them to the Result Queue. | Target function for `multiprocessing.Process`. |

## Recommended Project Structure

```text
src/
├── core/                  # Core abstractions and domain models
│   ├── job.py             # Job request/response dataclasses
│   └── exceptions.py      # Custom library exceptions
├── broker/                # RabbitMQ interaction layer
│   ├── connection.py      # Pika connection management
│   ├── consumer.py        # Queue consumption and ack logic
│   └── publisher.py       # Result and status publishing
├── execution/             # Process pool and IPC layer
│   ├── manager.py         # Job registry and process lifecycle (JobManager)
│   ├── worker.py          # The isolated Worker Runner execution wrapper
│   └── ipc.py             # Queue handling, IPC message definitions
└── app.py                 # Main entrypoint orchestrating components
```

### Structure Rationale

- **`broker/`:** Isolates all Pika-specific code. If the Pika logic blocks or throws connection errors, it is localized here, preventing it from polluting execution logic.
- **`execution/`:** Separates the complex IPC and multiprocessing logic from the broker. This makes it possible to unit test the execution engine without an active RabbitMQ connection.
- **`core/`:** Contains pure data structures (like standard IPC message formats) that both the `broker/` and `execution/` packages rely on, breaking cyclic dependencies.

## Architectural Patterns

### Pattern 1: Event-Loop Integration via Thread-Safe Callbacks

**What:** Because the Pika connection is strictly bound to the main thread/process, any external thread or process wanting to publish a message must use a thread-safe callback queue provided by Pika.
**When to use:** Crucial when retrieving data from the `multiprocessing.Queue`.
**Trade-offs:** Introduces slight latency as the IPC message has to hop from Worker → Result Queue → Background Thread → Pika Event Loop → Network.

**Example:**
```python
def ipc_listener_thread(result_queue, connection, publish_func):
    while True:
        msg = result_queue.get()
        if msg == "STOP": break
        
        # Schedule the publish function to run safely on the Pika thread
        connection.add_callback_threadsafe(
            lambda: publish_func(msg.job_id, msg.payload)
        )
```

### Pattern 2: Process Registry over Process Pool

**What:** Instead of `multiprocessing.Pool.apply_async`, manually manage a `dict` of `multiprocessing.Process` objects mapped to job IDs.
**When to use:** When you need fine-grained control over individual job cancellation (passing unique `multiprocessing.Event`s) and hard-termination (`process.terminate()`).
**Trade-offs:** Requires manual cleanup of dead processes and zombies (via `process.join()` sweeps), but gives maximum control over lifecycle which is essential for a robust job queue.

### Pattern 3: Yield Wrapping (Generator Adapter)

**What:** A wrapper function executed in the new process that continuously consumes the user's generator and forwards values to IPC, checking cancellation at each step.
**When to use:** To preserve the existing `yield`-based synchronous API for job authors.
**Trade-offs:** User code blocking for too long between `yield`s will delay cancellation unless they manually check a cancel flag.

## Data Flow

### Request Flow (Job Execution)

```text
[RabbitMQ Queue] 
    ↓ (consume)
[Pika Consumer] → (Job ID, Payload) → [Job Manager]
                                            ↓ (spawns)
[Cancel Event] ← (injects) ─┐               ↓
                            │       [Worker Process]
                            │               ↓ (runs)
                            └──── [Worker Runner (Wrapper)]
                                            ↓ (calls)
                                  [User Generator Function]
```

### Response Flow (Yields & Completion)

```text
[User Generator Function]
    ↓ (yields data)
[Worker Runner] → (wraps in IPC Message) → [Result Queue]
                                                ↓ (queue.get())
[RabbitMQ Topic] ← (publishes) ← [Pika Publ.] ← [IPC Listener Thread]
```

## Scaling Considerations

| Scale | Architecture Adjustments |
|-------|--------------------------|
| Default (Per Instance) | Max concurrent processes equal to CPU cores. Rely on RabbitMQ prefetch count (`basic_qos`) to prevent over-scheduling the instance. |
| High Compute Loads | Run multiple instances of the application on separate VMs/Pods. RabbitMQ naturally load-balances across consumers. |
| High Frequency Yields | Batch IPC messages. If a job yields 10,000 times a second, reading from the IPC queue and invoking `add_callback_threadsafe` will bottleneck the main loop. |

### Scaling Priorities

1. **First bottleneck:** CPU exhaustion and GIL contention (solved by this Multiprocessing architecture).
2. **Second bottleneck:** Event loop starvations. If workers yield too fast, the main process spends all its time processing IPC and drops the RabbitMQ heartbeat. **Fix:** Throttle yields or batch intermediate results in the worker before putting them in the `multiprocessing.Queue`.

## Anti-Patterns

### Anti-Pattern 1: Sharing Pika Connections

**What people do:** Passing the `pika.BlockingConnection` object or channel to the worker process to let it publish its own yields.
**Why it's wrong:** Pika connections are strictly not thread-safe, nor process-safe. Using them across processes will result in interleaved AMQP frames, socket errors, and immediate connection closures by the RabbitMQ server.
**Do this instead:** Strictly use the IPC Result Queue to forward messages back to the main process, which owns the single Pika connection.

### Anti-Pattern 2: Zombie Processes on Disconnect

**What people do:** The main process crashes or loses its RabbitMQ connection and exits, but worker processes continue running the compute-heavy tasks indefinitely.
**Why it's wrong:** Leaks CPU and memory resources severely over time.
**Do this instead:** Run worker processes as `daemon=True` so they die when the main process dies, OR implement signal handlers (`SIGTERM`, `SIGINT`) in the main process to explicitly iterate over the Job Manager registry and call `process.terminate()` on all active workers before exiting.

### Anti-Pattern 3: Unbounded IPC Queues

**What people do:** Using an unbounded `multiprocessing.Queue()` and allowing workers to yield results endlessly even if the main process is slow to publish them.
**Why it's wrong:** If RabbitMQ is slow (network latency), the IPC queue fills up RAM until the system OOMs.
**Do this instead:** Set a `maxsize` on the `multiprocessing.Queue`. The worker will block on `queue.put()` if the main process falls behind, creating natural backpressure that correctly slows down the compute job.

## Integration Points

### Internal Boundaries

| Boundary | Communication | Notes |
|----------|---------------|-------|
| Main ↔ Worker (Commands) | `multiprocessing.Process(args=...)` | Pass initial payload. Must be Picklable. |
| Main ↔ Worker (Cancel) | `multiprocessing.Event` | Checked iteratively by worker. |
| Worker ↔ Main (Results) | `multiprocessing.Queue(maxsize=X)` | Safe, ordered, blocks on full (backpressure). |

## Build Order Implications

To safely implement this architecture and transition from threads to processes, the components should be built in the following dependency order:

1. **Phase 1: Core IPC Models & Data Structures**
   - Create standard, picklable `dataclass` representations of IPC Messages (e.g., YieldMessage, ResultMessage, CancelMessage).
   - This serves as the communication contract between the execution engine and the main process without entangling either.
2. **Phase 2: The Worker Runner (Execution Engine)**
   - Build the standalone function/process that accepts a user generator, a `Queue`, and an `Event`.
   - Iterate over the generator, emitting values to the queue and checking the event.
   - *Validation*: Can be tested completely independently of RabbitMQ.
3. **Phase 3: IPC Reader & Main Process Loop**
   - Integrate a background thread into the Pika consumer process.
   - Configure the thread to read from the IPC `Queue` and invoke `connection.add_callback_threadsafe(...)` back to the Pika event loop.
4. **Phase 4: Job Manager Integration**
   - Replace the existing `threading.Thread` spawner with `multiprocessing.Process`.
   - Update job registration to store the `Process` and `Event` references.
   - Bind the Pika incoming job and cancel messages to the Job Manager's process-handling APIs.
5. **Phase 5: Process Lifecycle & Cleanup Hooks**
   - Add graceful shutdown (`SIGTERM` handling, `daemon=True`) to reap zombie processes when the main Pika consumer exits or crashes.

## Sources

- RabbitMQ & Python Multiprocessing patterns
- Pika Thread-Safety Documentation (`add_callback_threadsafe`)
- Python `multiprocessing` library official documentation
- standard IPC architecture for message queues

---
*Architecture research for: RabbitMQ Python Job Queuing (Multiprocessing)*
*Researched: 2026-03-28*

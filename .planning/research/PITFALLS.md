# Domain Pitfalls

**Domain:** Python RabbitMQ Job Queuing (Multiprocessing & IPC)
**Researched:** 2026-03-28

## Critical Pitfalls

Mistakes that cause rewrites or major issues.

### Pitfall 1: RabbitMQ Heartbeat Drops (Blocking the Event Loop)
**What goes wrong:** RabbitMQ closes the connection, logging "Missed heartbeats from client". The consumer stops receiving messages.
**Why it happens:** The main process runs Pika's `BlockingConnection` but blocks waiting for results from workers (e.g., `multiprocessing.Queue.get()` or `process.join()`). Because the thread is blocked, Pika cannot process or send AMQP heartbeats (default 60s timeout).
**Consequences:** Complete loss of RabbitMQ connection; library fails to acknowledge messages or receive new ones.
**Prevention:** Never block indefinitely on IPC in the thread handling Pika. Use `queue.get(timeout=0.5)` in a loop and periodically call `connection.process_data_events()`, or read from the queue in a separate background thread that relays updates to Pika using `connection.add_callback_threadsafe()`.
**Detection:** Connection reset errors from Pika or heartbeat timeout logs in RabbitMQ.

### Pitfall 2: IPC Queue Deadlocks on Process Cancellation
**What goes wrong:** The main process hangs forever when attempting to read from the IPC queue, or future jobs fail to start/communicate.
**Why it happens:** Using `process.terminate()` or `process.kill()` to stop a worker while it is writing to a `multiprocessing.Queue`. This corrupts the queue's internal locks or leaves the background flushing thread in a broken state.
**Consequences:** Complete system deadlock requiring a hard restart of the main consumer.
**Prevention:** Avoid `process.terminate()` if using `multiprocessing.Queue`. Implement graceful cancellation by passing a `multiprocessing.Event` (e.g., `stop_event.set()`) that the worker checks periodically. If hard termination is strictly required, use `multiprocessing.Pipe` or `multiprocessing.Manager().Queue()`, or create a completely fresh Queue for every job.
**Detection:** Main process stuck on `queue.get()` indefinitely after a cancellation event.

### Pitfall 3: Loss of Generator (Yield) Semantics Across Processes
**What goes wrong:** `TypeError: can't pickle generator objects` or intermediate results are never sent back to the main process.
**Why it happens:** Standard process pools (`ProcessPoolExecutor`, `multiprocessing.Pool`) do not support returning generator objects or streaming yields. They expect to serialize the final return value. If a user function yields, the pool tries to pickle the generator itself.
**Consequences:** Existing jobs that rely on `yield` for intermediate status updates break entirely.
**Prevention:** Do not submit the user's generator directly to the process pool. Create a worker wrapper function that iterates over the user's generator (`for item in job_func(): ...`) and explicitly pushes each yielded item into a `multiprocessing.Queue`, followed by a final return sentinel.
**Detection:** Pickling errors during job submission or workers returning immediately without executing the loop.

## Moderate Pitfalls

### Pitfall 4: Sharing Pika Connections Across Processes
**What goes wrong:** Frame interleaving errors, dropped channels, or immediate crashes when a job starts.
**Prevention:** Pika connections are strictly not thread-safe or process-safe. Never pass the `pika.BlockingConnection` or `pika.adapters.blocking_connection.BlockingChannel` to worker processes. Workers must only communicate via Python IPC mechanisms (Queues/Pipes), leaving the main process to handle all AMQP publishing/acking.

### Pitfall 5: Zombie Processes from Unflushed Queues
**What goes wrong:** Worker processes remain in a `<defunct>` state after completion, leaking memory and process IDs.
**Prevention:** A worker process will not exit if it has pushed items to a `multiprocessing.Queue` that haven't been consumed yet (the background thread blocks waiting for the pipe to drain). The main process must fully drain the IPC queue before calling `process.join()`, even if the job was cancelled.

## Phase-Specific Warnings

| Phase Topic | Likely Pitfall | Mitigation |
|-------------|---------------|------------|
| Process Pool Setup | Loss of yield semantics | Write a wrapper that iterates the generator and puts items in an IPC queue. |
| Job Cancellation | IPC queue deadlock | Use `multiprocessing.Event` for graceful exit instead of `terminate()`. |
| RabbitMQ Integration | Heartbeat timeouts | Read IPC queue with short timeouts; interleave with `connection.process_data_events()`. |
| Architecture | Pika object sharing | Keep Pika strictly isolated to the main consumer process. |

## Sources

- Pika Documentation: `add_callback_threadsafe` and `process_data_events` (HIGH)
- Python `multiprocessing` Documentation: Queue corruption on `terminate()` (HIGH)
- Python `concurrent.futures` Documentation: Serialization constraints on return values (HIGH)
- Community wisdom: Deadlocks in Python multiprocessing queues (MEDIUM)
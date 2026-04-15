# Architecture

**Analysis Date:** 2026-04-15

## Pattern Overview

**Overall:** Message-Driven Asynchronous Multiprocessing Job Queue

**Key Characteristics:**
- **Producer-Consumer via Message Broker:** Relies on RabbitMQ to distribute inputs, return outputs, and handle commands (like stopping jobs).
- **Multiprocessing Concurrency:** Uses `concurrent.futures.ProcessPoolExecutor` to run compute-intensive jobs in separate processes, bypassing the GIL.
- **Generator-Based Intermediary Results:** Job functions yield multiple times instead of returning once, allowing the system to capture intermediate processing progress.
- **IPC Communication:** A `multiprocessing.Queue` and a dedicated `_consume_ipc` thread relay messages (results, done, error, stopped) from worker processes back to the main thread's RabbitMQ connection safely.
- **Graceful Cancellation:** Long-running jobs check a shared `multiprocessing.Manager().Event()` (`_stop_event`) on each iteration to stop gracefully when a cancellation message is received.

## Layers

**Job Management (`rabbitasyncq/manager.py`):**
- Purpose: Orchestrates queues, listens to RabbitMQ, handles IPC messages, and spins up or stops worker processes.
- Location: `rabbitasyncq/manager.py`
- Contains: `JobManager` class
- Depends on: `pika`, `multiprocessing`, `concurrent.futures`, `rabbitasyncq/job.py`, `rabbitasyncq/messaging.py`
- Used by: External clients setting up a job consumer.

**Job Execution (`rabbitasyncq/job.py`):**
- Purpose: Executes the user-provided logic in a dedicated worker process. Handles yielding results, checking stop events, and sending IPC messages back to the manager.
- Location: `rabbitasyncq/job.py`
- Contains: `process_worker` function, `ProcessJobContext` class
- Depends on: `multiprocessing`, user's `job_fn`
- Used by: `JobManager` via `ProcessPoolExecutor`

**Messaging / Transport (`rabbitasyncq/messaging.py`):**
- Purpose: Provides an abstraction layer over `pika` channels to standardize publishing messages, acks, and standardized states (STOPPED, SUCCESS).
- Location: `rabbitasyncq/messaging.py`
- Contains: `Messenger` class
- Depends on: `pika`
- Used by: `JobManager`, `ProcessJobContext`

## Data Flow

**Accepting a Job:**
1. A client publishes a JSON payload with a `job_id` to the `{name} input job` queue.
2. `JobManager.accept_job` consumes the message, parses the JSON, and creates a `ProcessJobContext` with a stop event.
3. The job is submitted to `ProcessPoolExecutor`, adding the job context to `jobs[job_id]`.

**Job Execution & Results:**
1. `process_worker` executes the user's `job_fn` which is expected to yield dictionary results.
2. For each yielded result, `process_worker` adds `job_id` and `status="RUNNING"`.
3. The result is pushed to the `ipc_queue` as a dict with `type="result"`.
4. The main thread's `_consume_ipc` thread picks up the IPC message and schedules a threadsafe callback on the RabbitMQ connection to publish it to `{name} result`.
5. `JobManager.handle_result` consumes from `{name} result` and calls the user's `result_fn`.
6. Upon completion, a `done` msg is sent to IPC, triggering SUCCESS publishing and cleanup.

**Stopping a Job:**
1. A client publishes a JSON payload with the `job_id` to `{name} stop job` queue.
2. `JobManager.stop_job` retrieves the context from `jobs` and calls `.stop()` which sets the `multiprocessing.Event()`.
3. The `process_worker` checks `stop_event.is_set()` during its iteration loop, sends a `stopped` IPC message, and returns early.
4. The `_consume_ipc` thread processes `stopped`, publishes a STOPPED message to `{name} result`, and acknowledges the original message.

## Key Abstractions

**`ProcessJobContext`:**
- Purpose: Stores the state of a job (method, channel, stop event, future) in the main thread.
- Examples: `rabbitasyncq/manager.py`, `rabbitasyncq/job.py`
- Pattern: State tracking for asynchronous processes.

**`ProcessPoolExecutor` & `IPC Queue`:**
- Purpose: Isolates work to bypass GIL and communicates safely across processes.
- Examples: `rabbitasyncq/manager.py` (instantiation), `rabbitasyncq/job.py` (worker usage).
- Pattern: Multiprocessing worker pool with message queue communication.

**`Messenger`:**
- Purpose: Standardizes RabbitMQ publishing operations.
- Examples: `rabbitasyncq/messaging.py`
- Pattern: Adapter/Facade over `pika.channel.Channel`

## Entry Points

**`JobManager.__init__`:**
- Location: `rabbitasyncq/manager.py`
- Triggers: Instantiated by a client script.
- Responsibilities: Declares RabbitMQ queues (`{name} input job`, `{name} stop job`, `{name} result`), starts the `ProcessPoolExecutor`, starts the `_consume_ipc` thread.

**`JobManager.start`:**
- Location: `rabbitasyncq/manager.py`
- Triggers: Called by client to begin consuming.
- Responsibilities: Starts pika `start_consuming` loop.

## Error Handling

**Strategy:** IPC messaging and exception catching at the process boundaries.

**Patterns:**
- **Job Exceptions:** `process_worker` wraps the user's `job_fn` in a `try...except`. If it crashes, it pushes an `error` message with `status="ERROR"` to the `ipc_queue`. The main thread publishes it and acks the original message.
- **Process Crashes:** A future callback (`_handle_future_done`) detects if a process crashes (e.g. `BrokenProcessPool`, `TerminatedWorkerError`), sends a NACK to RabbitMQ without requeue, and exits the consumer.
- **Parsing Errors:** `JobManager` uses bare `except:` blocks when parsing incoming JSON. If a message is malformed, it silently returns.

## Cross-Cutting Concerns

**Concurrency Safety:** `pika` channels are not thread-safe and cannot be shared across processes. Thus, workers do no RabbitMQ interaction; they only push to `ipc_queue`. The `_consume_ipc` thread reads the queue and uses `self.conn.add_callback_threadsafe(...)` to schedule publishing on the main pika thread.
**Logging:** Uses raw `print()` statements throughout the code for debugging (e.g., `print(f"Starting job {job_id}")`).

---

*Architecture analysis: 2026-04-15*
# Architecture

**Analysis Date:** 2026-03-28

## Pattern Overview

**Overall:** Message-Driven Asynchronous Job Queue

**Key Characteristics:**
- **Producer-Consumer via Message Broker:** Relies on RabbitMQ to distribute inputs, return outputs, and handle commands (like stopping jobs).
- **Thread-Based Concurrency:** Uses Python's `threading` module to run multiple jobs concurrently within a single consumer instance.
- **Generator-Based Intermediary Results:** Job functions are expected to yield multiple times instead of returning once, allowing the system to capture intermediate processing progress.
- **Graceful Cancellation:** Long-running jobs check a shared threading event (`_stop_event`) on each iteration to stop gracefully when a cancellation message is received.

## Layers

**Job Management (`rabbitasyncq/manager.py`):**
- Purpose: Orchestrates queues, listens to RabbitMQ, and spins up or stops job threads.
- Location: `rabbitasyncq/manager.py`
- Contains: `JobManager` class
- Depends on: `pika`, `rabbitasyncq/job.py`, `rabbitasyncq/messaging.py`
- Used by: External clients setting up a job consumer.

**Job Execution (`rabbitasyncq/job.py`):**
- Purpose: Wraps the user-provided logic in a dedicated stoppable thread. Handles yielding results and catching job-level exceptions.
- Location: `rabbitasyncq/job.py`
- Contains: `StoppableThread`, `StoppableJob` classes
- Depends on: `threading`, user's `job_fn`
- Used by: `JobManager`

**Messaging / Transport (`rabbitasyncq/messaging.py`):**
- Purpose: Provides an abstraction layer over `pika` channels to standardize publishing messages, acks, and standardized states (STOPPED, SUCCESS).
- Location: `rabbitasyncq/messaging.py`
- Contains: `Messenger` class
- Depends on: `pika`
- Used by: `JobManager`, `StoppableJob`

## Data Flow

**Accepting a Job:**
1. A client publishes a JSON payload with a `job_id` to the `{name} input job` queue.
2. `JobManager.accept_job` consumes the message, parses the JSON, and instantiates a `StoppableJob`.
3. The `StoppableJob` is stored in `JobManager.jobs[job_id]` and `.start()` is called to run it in a new thread.

**Job Execution & Results:**
1. `StoppableJob.run` executes the user's `job_fn` which is expected to yield dictionary results.
2. For each yielded result, `StoppableJob` adds `job_id` and `status="RUNNING"`.
3. The result is serialized to JSON and published to `{name} result` using `conn.add_callback_threadsafe` (via `Messenger`).
4. `JobManager.handle_result` consumes from `{name} result` and calls the user's `result_fn`.
5. Upon completion, a `status="SUCCESS"` message and a stop request are sent.

**Stopping a Job:**
1. A client publishes a JSON payload with the `job_id` to `{name} stop job` queue.
2. `JobManager.stop_job` retrieves the job from the `jobs` dictionary and calls `.stop()`.
3. The `StoppableJob` checks `self.stopped` during its iteration loop, publishes a stop confirmation, acknowledges the original queue message, and returns early.

## Key Abstractions

**`StoppableThread` / `StoppableJob`:**
- Purpose: Encapsulates long-running work inside a thread that can be canceled mid-execution.
- Examples: `rabbitasyncq/job.py`
- Pattern: Subclassing `threading.Thread` with a `threading.Event()` for cancellation signaling.

**`Messenger`:**
- Purpose: Standardizes RabbitMQ publishing operations and encapsulates channel checks.
- Examples: `rabbitasyncq/messaging.py`
- Pattern: Adapter/Facade over `pika.channel.Channel`

## Entry Points

**`JobManager.__init__`:**
- Location: `rabbitasyncq/manager.py`
- Triggers: Instantiated by a client script.
- Responsibilities: Declares RabbitMQ queues (`{name} input job`, `{name} stop job`, `{name} result`), binds them if an exchange is provided, and starts a background thread to consume messages.

## Error Handling

**Strategy:** Exception catching at the thread boundaries.

**Patterns:**
- **Job Exceptions:** `StoppableJob.run` wraps the user's `job_fn` in a `try...except`. If it crashes, it constructs a JSON payload with `status="ERROR"` and the exception string, publishes it to the result queue, acks the original message so it isn't requeued endlessly, and re-raises the exception.
- **Parsing Errors:** `JobManager` uses bare `except:` blocks when parsing incoming JSON. If a message is malformed or lacks a `job_id`, it silently swallows the error and fails to execute/stop the job (which can lead to hanging un-acked messages).

## Cross-Cutting Concerns

**Concurrency Safety:** `pika` channels are not thread-safe. `StoppableJob` (which runs in a background thread) uses `self.conn.add_callback_threadsafe(...)` to schedule publishing operations back on the connection's main thread, avoiding thread corruption in the broker connection.
**Logging:** Uses raw `print()` statements throughout the code for debugging (e.g., `print(f"Starting job {self.job_id}")`). There is a TODO comment indicating a desire for a callback-based logging setup.

---

*Architecture analysis: 2026-03-28*
# Codebase Concerns

**Analysis Date:** 2026-03-28

## Tech Debt

**Pokemon Exception Handling:**
- Issue: Using empty `except:` blocks indiscriminately, obscuring critical errors like JSON parsing failures and missing dictionary keys.
- Files: `rabbitasyncq/manager.py`
- Impact: Unhandled internal errors lead to `UnboundLocalError`, potentially crashing the main Pika consumer thread and causing unacknowledged messages to pile up indefinitely.
- Fix approach: Catch specific exceptions (e.g., `json.JSONDecodeError`, `KeyError`), return early to halt processing of bad messages, and ensure deterministic message `ack` or `nack`.

## Known Bugs

**Hardcoded Default Exchange:**
- Symptoms: Although `JobManager` accepts an `exchange_opt` configuration to declare and bind queues, job status updates and replies are hardcoded to use the default `""` exchange.
- Files: `rabbitasyncq/messaging.py`, `rabbitasyncq/manager.py`
- Trigger: Instantiating `JobManager` with a custom `exchange_opt`.
- Workaround: Consumers must rely on the default direct routing using the queue name and ignore their custom exchange.

## Security Considerations

**Untrusted Job Payloads:**
- Risk: `JobManager` parses arbitrary JSON from queues and blindly uses it as input for tasks without any schema validation.
- Files: `rabbitasyncq/manager.py`
- Current mitigation: None. The library assumes the message broker connection is secure and only trusted services are writing to the queue.
- Recommendations: Recommend or implement schema validation for input messages (e.g., using `pydantic` or `jsonschema`) before passing them to the job functions.

## Performance Bottlenecks

**Blocking Consumer Thread:**
- Problem: The `stop_job` function waits for the worker thread to finish.
- Files: `rabbitasyncq/manager.py`
- Cause: Calling `job_thread.join()` directly inside the message consumption callback blocks the Pika consumer thread, preventing it from processing heartbeats or other messages.
- Improvement path: Remove synchronous `.join()` calls inside Pika callbacks. The worker thread already uses safe thread callbacks to acknowledge completion and clean up after itself asynchronously.

## Fragile Areas

**Implicit Thread Management and Resource Leaks:**
- Files: `rabbitasyncq/manager.py`
- Why fragile: `JobManager.__init__` implicitly instantiates and starts a background `consume_thread`. However, there is no corresponding `stop()` or `close()` method provided to gracefully shut down this thread, close Pika channels, or disconnect from RabbitMQ cleanly.
- Safe modification: Introduce a teardown/stop method for the `JobManager` class to cleanly stop consumption, unbind channels, and join the internal thread.
- Test coverage: None. Tests currently terminate abruptly leaving dangling threads.

## Scaling Limits

**In-Memory State restricts Horizontal Scaling:**
- Current capacity: Single-instance process only.
- Limit: Running jobs are tracked in a local memory dictionary (`self.jobs`). If there are multiple `JobManager` worker instances listening to the same shared queues, a "stop job" message could be routed round-robin to a worker instance that doesn't own that job, silently failing to cancel the task.
- Scaling path: Route "stop" messages to worker-specific queues via unique worker IDs, or manage a shared distributed state (e.g., using Redis) for job cancellation signaling.

## Test Coverage Gaps

**Broken Test Synchronization:**
- What's not tested: Assertions within tests are executing blindly. Test functions exit instantly without waiting for actual completion.
- Files: `test/test_jobmanager.py`
- Risk: Assertions (like `assert body["nyaa"] % 5 == 0` in `handle_result`) are evaluated asynchronously on a background thread. If they fail, they raise an exception in the background, but the pytest runner still reports a success because the main thread already finished.
- Priority: High. Tests must use synchronization primitives like `threading.Event` to block until success/failure callbacks are invoked before exiting.

---

*Concerns audit: 2026-03-28*

---
phase: 02-broker-integration-execution-pool
plan: 02
subsystem: JobManager
tags: [rabbitmq, lifecycle, signals]

requires: [EXEC-03]
provides: "start() method and sig_handler"
affects: [rabbitasyncq/manager.py, test/test_jobmanager.py]

tech-stack.added: []
tech-stack.patterns: [Main thread blocking I/O loop, OS signal catching for graceful shutdown]

key-files.modified: [rabbitasyncq/manager.py, test/test_jobmanager.py]
key-files.created: []

key-decisions:
  - "Run `start_consuming` in the main thread inside a `start()` method instead of `__init__`."
  - "Register `SIGINT` and `SIGTERM` signals inside `start()` to cleanly stop consumption."
  - "Catch `ValueError` when registering signals if `start()` runs in a background thread (as in the updated pytest fixtures)."

requirements-completed: [EXEC-03]

duration: 3 min
completed: "2026-03-28T16:40:00Z"
---
# Phase 02 Plan 02: JobManager Lifecycle Refactor Summary

JobManager execution is now centralized in the main thread using a blocking `start()` method, replacing the previous background consumption thread.

## Execution Metrics
- **Duration**: 3 min
- **Tasks**: 2
- **Files Modified**: 2

## Deviations from Plan
- **Rule 3 (Blocking)**: The `test_jobmanager.py` fixture calls `jm.start()` in a background thread. `signal.signal` raises a `ValueError` if not called from the main thread, causing tests to fail with an unhandled exception. **Fix**: Wrapped the signal registration in a `try...except ValueError` block to silently ignore it when running tests.

## Next Steps
Phase complete, ready for next step.

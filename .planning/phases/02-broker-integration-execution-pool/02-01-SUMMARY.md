---
phase: 02-broker-integration-execution-pool
plan: 01
subsystem: JobManager
tags: [rabbitmq, process-pool, prefetch]

requires: [EXEC-01]
provides: "Prefetch-configured Channel and sized ProcessPoolExecutor"
affects: [rabbitasyncq/manager.py]

tech-stack.added: []
tech-stack.patterns: [Resource-aware process pool sizing, RabbitMQ basic_qos prefetch]

key-files.modified: [rabbitasyncq/manager.py]
key-files.created: []

key-decisions:
  - "Use `os.sched_getaffinity(0)` to respect Docker CPU constraints for max_workers when possible."
  - "Map process pool size to RabbitMQ `prefetch_count` to ensure exact capacity consumption."

requirements-completed: [EXEC-01]

duration: 2 min
completed: "2026-03-28T16:35:00Z"
---
# Phase 02 Plan 01: Process Pool Sizing Summary

JobManager now automatically calculates process pool capacity via cgroups-aware CPU checks and configures RabbitMQ channel prefetch to match, preventing uncontrolled local queue stockpiling.

## Execution Metrics
- **Duration**: 2 min
- **Tasks**: 2
- **Files Modified**: 1

## Deviations from Plan
None - plan executed exactly as written.

## Next Steps
Ready for Plan 02: Blocking JobManager start method.

# Phase 02: Broker Integration & Execution Pool - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-03-28
**Phase:** 02-broker-integration-execution-pool
**Areas discussed:** Prefetching & Pool Size, Connection Ownership, Error & Retry Policies

---

## Prefetching & Pool Size

| Option | Description | Selected |
|--------|-------------|----------|
| Exact Match (Recommended) | Set prefetch_count equal to number of processes (Prevents idle workers and prevents local queue buildup) | ✓ |
| Buffer Match | Set prefetch higher to keep a local buffer (e.g. processes + 2) | |
| Infinite | Do not set prefetch (RabbitMQ sends all messages instantly) | |

**User's choice:** Exact Match (Recommended)

---

## Pool Size

| Option | Description | Selected |
|--------|-------------|----------|
| JobManager Defaults (Recommended) | Let JobManager manage it, defaulting to os.cpu_count() with override option | ✓ |
| User Injected | User passes an already created ProcessPoolExecutor instance to JobManager | |
| Safe default | Hardcode to 1 for safety and easy debugging, requiring user to bump it | |

**User's choice:** JobManager Defaults (Recommended)

---

## CPU Discovery

| Option | Description | Selected |
|--------|-------------|----------|
| Respect cgroups (Recommended) | Use os.sched_getaffinity(0) on Linux to respect Docker/cgroups CPU limits | ✓ |
| Standard OS Count | Use os.cpu_count() and let the user override if needed | |
| Fixed fallback | Fallback to a fixed number like 4 if not overridden | |

**User's choice:** Respect cgroups (Recommended)

---

## Main Loop

| Option | Description | Selected |
|--------|-------------|----------|
| Block Main Thread (Recommended) | Run start_consuming() in the main thread (Standard CLI pattern, allows proper shutdown signals) | ✓ |
| Background Thread | Keep it in a background threading.Thread (Caller can do other tasks, but makes shutdown harder) | |

**User's choice:** Block Main Thread (Recommended)

---

## Signals

| Option | Description | Selected |
|--------|-------------|----------|
| Auto-handle (Recommended) | Register SIGINT/SIGTERM handlers inside JobManager.start() to gracefully stop workers and loop | ✓ |
| Manual handling | Let the user catch KeyboardInterrupt and call shutdown() manually | |
| Hard exit | Exit immediately with os._exit() on signal | |

**User's choice:** Auto-handle (Recommended)

---

## Error Disposition

| Option | Description | Selected |
|--------|-------------|----------|
| Ack & Log (Recommended) | Ack the message and publish the error dictionary (Current behavior) | ✓ |
| Retry (Nack) | Nack (requeue=True) to allow RabbitMQ to retry the job | |
| Dead Letter (Nack) | Nack (requeue=False) to route to Dead Letter Exchange | |

**User's choice:** Ack & Log (Recommended)

---

## Crash Recovery

| Option | Description | Selected |
|--------|-------------|----------|
| Defer to Phase 3 (Recommended) | Leave out of scope for Phase 2 (Phase 3 is dedicated to Resilience & Crash Recovery) | ✓ |
| Detect now | Attempt basic crash detection via ProcessPoolExecutor futures now | |

**User's choice:** Defer to Phase 3 (Recommended)

---

## Deferred Ideas

- Detecting hard worker crashes via `Future.add_done_callback` and NACKing jobs was discussed but deferred to Phase 3.

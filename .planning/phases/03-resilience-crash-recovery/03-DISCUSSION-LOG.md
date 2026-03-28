# Phase 03: Resilience & Crash Recovery - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-03-28
**Phase:** 03-resilience-crash-recovery
**Areas discussed:** Crash Detection Strategy, Message Disposition, Pool Recovery Strategy

---

## Crash Detection Strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Future Callbacks (Recommended) | Attach add_done_callback to the ProcessPoolExecutor future and check for exceptions like BrokenProcessPool | ✓ |
| Polling Loop | Use a background loop to poll futures in self.jobs and check their status | |

**User's choice:** Future Callbacks (Recommended)

---

## Message Disposition

| Option | Description | Selected |
|--------|-------------|----------|
| Dead-letter (Recommended) | basic_nack(requeue=False) to send it to a DLQ and avoid a poison-pill loop (OOM loop) | ✓ |
| Requeue | basic_nack(requeue=True) to immediately requeue the message for another worker | |
| Ack & Ignore | basic_ack() to consume the message, but log a severe error | |

**User's choice:** Dead-letter (Recommended)

---

## Pool Recovery Strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Fail Fast (Recommended) | Throw a fatal error, log loudly, and call sys.exit(), letting Docker/supervisor restart the service | ✓ |
| Auto-Heal Pool | Try to recreate the ProcessPoolExecutor and continue accepting jobs | |

**User's choice:** Fail Fast (Recommended)

---

## Deferred Ideas

None

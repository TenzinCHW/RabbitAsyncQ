# Phase 1: Worker Execution-IPC - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-03-28
**Phase:** 01-worker-execution-ipc
**Areas discussed:** IPC Mechanism, Cancellation Signaling, Exception Serialization, Worker Abstraction

---

## IPC Mechanism

| Option | Description | Selected |
|--------|-------------|----------|
| multiprocessing.Queue | Standard, safe for multiple readers/writers, easiest to poll | ✓ |
| multiprocessing.Pipe | Faster for strict 1:1 communication between main and worker | |
| Other | | |

**User's choice:** multiprocessing.Queue
**Notes:** 

---

## Cancellation Signaling

| Option | Description | Selected |
|--------|-------------|----------|
| multiprocessing.Event | Direct swap for existing `threading.Event`, familiar pattern | ✓ |
| Control message via IPC | Send a "STOP" command through the Queue/Pipe | |
| Other | | |

**User's choice:** multiprocessing.Event
**Notes:** 

---

## Exception Serialization

| Option | Description | Selected |
|--------|-------------|----------|
| Formatted String | Extract traceback string in worker, send as string. Safe, no pickling issues | |
| Serialized Dict | Send `{type: 'ValueError', msg: '...', traceback: '...'}`. Better for structured logging later | ✓ |
| Pickle | Standard `multiprocessing` behavior, but can fail on complex custom exceptions | |
| Other | | |

**User's choice:** Serialized Dict
**Notes:** 

---

## Worker Abstraction

| Option | Description | Selected |
|--------|-------------|----------|
| Yes, use `multiprocessing.Process` | Keep Phase 1 simple, swap to a Pool in Phase 2 | |
| No, start building the Pool structure now | More upfront work, but Phase 2 will be easier | ✓ |
| Other | | |

**User's choice:** Use multiprocessing.pool.Pool or concurrent.futures.ProcessPoolExecutor if possible, whichever is more suitable.
**Notes:** 

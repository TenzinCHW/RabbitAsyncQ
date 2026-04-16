# Stack Research

**Domain:** Python RabbitMQ Job Queuing Library (Multiprocessing & IPC layer)
**Researched:** 2026-03-28
**Confidence:** HIGH

## Recommended Stack

### Core Technologies

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| Pebble | 5.2.0 | Process pool management and execution | Python's standard `ProcessPoolExecutor` cannot forcefully terminate running tasks. `Pebble` provides `ProcessPool` with native timeouts and a `future.cancel()` method that terminates the specific worker process and transparently replaces it, which is essential for hard-canceling stuck jobs without poisoning the entire pool. |
| multiprocessing.SimpleQueue | Python 3.9+ | IPC for yielding intermediate results | Standard `multiprocessing.Queue` uses background threads and locks, which severely deadlock if a worker process is hard-killed (e.g., via Pebble cancellation). `SimpleQueue` is a lock-free (on POSIX) C implementation backed by OS pipes, making it safe for IPC even during abrupt worker termination. |
| multiprocessing.Event | Python 3.9+ | Graceful cancellation signaling | Provides a process-safe boolean flag. The main process sets the event upon receiving a RabbitMQ stop message. The worker function can periodically check `event.is_set()` between yields to perform cleanup and exit gracefully before a hard-kill is enforced. |

### Supporting Libraries

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| cloudpickle | 3.1.2 | Enhanced serialization for IPC | Standard `pickle` fails on closures, lambda functions, and dynamically defined classes. If your users submit complex Python objects or nested functions as job arguments/results, `cloudpickle` (used by Dask, Ray, PySpark) guarantees they can be safely sent across the process boundary. |
| threading.Thread | Python 3.9+ | Bridging IPC to Pika's event loop | Pika is strictly not thread-safe or process-safe. To publish yielded results from the `SimpleQueue` to RabbitMQ, use a background thread to block on `SimpleQueue.get()`, and then use `pika.connection.add_callback_threadsafe` to safely hand the message to the main Pika I/O loop. |

## Installation

```bash
# Core execution & serialization
pip install Pebble==5.2.0 cloudpickle==3.1.2

# (Pika is assumed to be already installed based on project context)
```

## Alternatives Considered

| Recommended | Alternative | When to Use Alternative |
|-------------|-------------|-------------------------|
| **Pebble** | **loky** (3.5.6) | If workloads are heavily data-science focused (numpy/scikit-learn) and you need advanced memmapping. However, `loky` cancels jobs by restarting the *entire* pool, which disrupts other running jobs. Pebble isolates termination to the single affected worker. |
| **multiprocessing.SimpleQueue** | **ZeroMQ (pyzmq)** | If the application needs to scale IPC across network boundaries or requires ultra-high throughput (millions of msgs/sec). Overkill for a single-node local process pool and adds a heavy C dependency (`libzmq`). |

## What NOT to Use

| Avoid | Why | Use Instead |
|-------|-----|-------------|
| `multiprocessing.Queue` | Prone to unrecoverable deadlocks if a child process is forcefully terminated while holding the queue's internal lock. | `multiprocessing.SimpleQueue` |
| `concurrent.futures.ProcessPoolExecutor` | Cannot cancel running jobs. Calling `.cancel()` only works if the job hasn't started yet. If a job is stuck in an infinite loop, you cannot kill it without tearing down the whole pool. | `Pebble` |
| `billiard` | Celery's multiprocessing fork. It contains fixes for older Python versions but introduces immense complexity and hasn't seen major architectural updates recently. Standard Python + Pebble is much more maintainable. | `multiprocessing` + `Pebble` |
| Calling Pika directly from workers | Pika connections are tied to the process/thread that created them. Attempting to share a Pika connection across process boundaries will immediately corrupt the AMQP frame state and crash the broker connection. | IPC to Main Process -> `add_callback_threadsafe` |

## Stack Patterns by Variant

**If the job workload is cooperative:**
- Use `multiprocessing.Event` to signal cancellation.
- Because it allows the worker to catch the signal, finish its current iteration, yield a final status, and cleanly close resources (database connections, file handles).

**If the job workload is third-party/untrusted (can get stuck in C-extensions or infinite loops):**
- Use `Pebble`'s `future.cancel()` or `timeout` arguments.
- Because the worker will not check the `Event` flag. Pebble will send a SIGTERM/SIGKILL, terminating the OS process, and automatically spawn a fresh worker to maintain pool capacity.

## Version Compatibility

| Package A | Compatible With | Notes |
|-----------|-----------------|-------|
| Pebble@5.2.0 | Python >= 3.8 | Fully compatible with modern `asyncio` and `multiprocessing` context methods (`spawn`, `fork`, `forkserver`). |
| cloudpickle@3.1.2 | Python >= 3.8 | Seamlessly replaces `pickle` in IPC pipelines. |

## Sources

- Official PyPI JSON API — Verified current versions of Pebble (5.2.0), cloudpickle (3.1.2), loky (3.5.6) (HIGH confidence)
- Pebble Documentation (https://pebble.readthedocs.io) — Verified `ProcessPool` `.cancel()` and `timeout` process-replacement behavior (HIGH confidence)
- Python `multiprocessing` Documentation — Verified `SimpleQueue` lock-free guarantees and thread-safety vs standard `Queue` deadlocks (HIGH confidence)
- Pika Documentation — Verified `add_callback_threadsafe` pattern for bridging blocking queues to the I/O loop (HIGH confidence)

---
*Stack research for: Python RabbitMQ Job Queuing Library (Multiprocessing & IPC layer)*
*Researched: 2026-03-28*
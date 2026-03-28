# Coding Conventions

**Analysis Date:** 2026-03-28

## Naming Patterns

**Files:**
- Snake case: `job.py`, `manager.py`, `messaging.py`
- Test files prefixed with `test_`: `test_jobmanager.py`

**Functions:**
- Snake case for both standalone functions and class methods: `accept_job`, `handle_result`, `send_msg`, `dummy_run`.

**Variables:**
- Snake case: `job_data`, `input_job_qname`, `consume_thread`.

**Types & Classes:**
- Pascal case for classes: `StoppableThread`, `StoppableJob`, `JobManager`, `Messenger`.

## Code Style

**Formatting:**
- No explicit formatting tool (e.g., Black, Ruff) configuration detected in the codebase.
- Indentation uses 4 spaces.

**Linting:**
- No linting configuration (e.g., Flake8, Ruff) found.

## Import Organization

**Order:**
1. Standard library imports (e.g., `import json`, `import threading`, `import os`).
2. Type hinting imports (e.g., `from typing import Callable`).
3. Third-party library imports (e.g., `import pika`).
4. Relative local imports (e.g., `from .messaging import Messenger`, `from .job import StoppableJob`).

**Path Aliases:**
- Standard relative imports are used within the package (e.g., `from .job import StoppableJob`).

## Error Handling

**Patterns:**
- **Silent failure for parsing/lookup errors:** The codebase frequently uses bare `except:` blocks with `...` (pass) to silently ignore errors, particularly for JSON parsing failures or missing keys in message bodies (e.g., in `rabbitasyncq/manager.py`).
- **Callback-based error bubbling:** In worker threads (`rabbitasyncq/job.py`), exceptions during job execution are caught, packaged into a JSON dictionary with `"status": "ERROR"`, and published back to the result queue using `self.conn.add_callback_threadsafe`. After publishing the error, the original exception is re-raised.

## Typing

**Patterns:**
- **Partial type hinting:** Type hints are consistently used for function and method arguments (e.g., `method: pika.frame.Method`, `job_fn: Callable`, `name: str`).
- Return types are generally omitted across the codebase.
- Variables are rarely explicitly typed.

## Logging

**Framework:** `console` (standard `print` statements)

**Patterns:**
- Standard `print()` statements are used to log lifecycle events (e.g., `print(f"Starting job {self.job_id}")`, `print(f"Stopped job with ID: {job_id}.")`).
- A comment (`# TODO add callback for logging` in `rabbitasyncq/job.py`) indicates an intent to move to a structured logging approach in the future.

## Comments

**When to Comment:**
- Comments are sparse and primarily used for explaining non-obvious failure modes (e.g., `# data not json, job_id not found or job_id not in job_thread, just fail`).
- `TODO` comments are used to note missing implementations (e.g., `# TODO why is exchange an empty string?`).

**Docstrings:**
- Docstrings are currently missing from modules, classes, and methods.

## Function Design

**Size:** Small, focused functions. Most methods are under 20 lines.

**Parameters:** Prefer passing exact dependencies (e.g., `conn: pika.connection.Connection`, `ch: pika.channel.Channel`) rather than relying on global state. 

**Return Values:** Handlers and void functions return `None`. Job functions are expected to be generators that `yield` intermediate results.

## Module Design

**Exports:** 
- Explicit imports are used. `rabbitasyncq/__init__.py` likely exposes the public API classes like `JobManager`, though standard module-level imports are also used across tests.

---

*Convention analysis: 2026-03-28*
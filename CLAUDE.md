<!-- GSD:project-start source:PROJECT.md -->
## Project

**RabbitAsyncQ**

RabbitAsyncQ is a Python library for queuing, starting, and gracefully stopping asynchronous jobs via RabbitMQ. It is being upgraded from a thread-based execution model to a multi-processing model to better handle compute-intensive workloads.

**Core Value:** Reliable, interruptible execution of compute-intensive Python jobs driven by RabbitMQ messages.

### Constraints

- **Execution Model**: Must use multiple processes (e.g., `multiprocessing.Pool`, `concurrent.futures.ProcessPoolExecutor`) to bypass the GIL.
- **Dependency**: `pika` for RabbitMQ communication.
- **Compatibility**: Should minimize changes to the public API (how users define and start jobs).
<!-- GSD:project-end -->

<!-- GSD:stack-start source:codebase/STACK.md -->
## Technology Stack

## Languages
- Python >=3.12 - Core library logic (`rabbitasyncq/*.py`) and tests (`test/*.py`).
- Not detected
## Runtime
- Python 3.12+
- uv (via `uv.lock` file presence)
- Lockfile: present (`uv.lock`)
## Frameworks
- Standard Python libraries (`threading`, `json`) - Asynchronous task handling is implemented via native threading rather than an async framework like `asyncio`.
- pytest (>=8.4.2) - Unit test execution and fixtures (`test/test_jobmanager.py`).
- poetry-core (>=2.0.0,<3.0.0) - Build backend defined in `pyproject.toml`.
## Key Dependencies
- pika (>=1.3.2,<2.0.0) - The core external library providing the AMQP 0-9-1 protocol implementation for RabbitMQ interaction.
- RabbitMQ - The underlying message broker the library depends on for queuing, job distribution, and message routing.
## Configuration
- Configured programmatically by passing a `pika.connection.Connection` instance to the `JobManager` (`rabbitasyncq/manager.py`).
- `pyproject.toml` handles packaging and dependency declarations.
## Platform Requirements
- Python >= 3.12
- Local RabbitMQ instance running on `localhost:5672` (default port) for executing the test suite (`test/test_jobmanager.py`).
- Any Python 3.12+ environment with access to a RabbitMQ broker instance.
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

## Naming Patterns
- Snake case: `job.py`, `manager.py`, `messaging.py`
- Test files prefixed with `test_`: `test_jobmanager.py`
- Snake case for both standalone functions and class methods: `accept_job`, `handle_result`, `send_msg`, `dummy_run`.
- Snake case: `job_data`, `input_job_qname`, `consume_thread`.
- Pascal case for classes: `StoppableThread`, `StoppableJob`, `JobManager`, `Messenger`.
## Code Style
- No explicit formatting tool (e.g., Black, Ruff) configuration detected in the codebase.
- Indentation uses 4 spaces.
- No linting configuration (e.g., Flake8, Ruff) found.
## Import Organization
- Standard relative imports are used within the package (e.g., `from .job import StoppableJob`).
## Error Handling
- **Silent failure for parsing/lookup errors:** The codebase frequently uses bare `except:` blocks with `...` (pass) to silently ignore errors, particularly for JSON parsing failures or missing keys in message bodies (e.g., in `rabbitasyncq/manager.py`).
- **Callback-based error bubbling:** In worker threads (`rabbitasyncq/job.py`), exceptions during job execution are caught, packaged into a JSON dictionary with `"status": "ERROR"`, and published back to the result queue using `self.conn.add_callback_threadsafe`. After publishing the error, the original exception is re-raised.
## Typing
- **Partial type hinting:** Type hints are consistently used for function and method arguments (e.g., `method: pika.frame.Method`, `job_fn: Callable`, `name: str`).
- Return types are generally omitted across the codebase.
- Variables are rarely explicitly typed.
## Logging
- Standard `print()` statements are used to log lifecycle events (e.g., `print(f"Starting job {self.job_id}")`, `print(f"Stopped job with ID: {job_id}.")`).
- A comment (`# TODO add callback for logging` in `rabbitasyncq/job.py`) indicates an intent to move to a structured logging approach in the future.
## Comments
- Comments are sparse and primarily used for explaining non-obvious failure modes (e.g., `# data not json, job_id not found or job_id not in job_thread, just fail`).
- `TODO` comments are used to note missing implementations (e.g., `# TODO why is exchange an empty string?`).
- Docstrings are currently missing from modules, classes, and methods.
## Function Design
## Module Design
- Explicit imports are used. `rabbitasyncq/__init__.py` likely exposes the public API classes like `JobManager`, though standard module-level imports are also used across tests.
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

## Pattern Overview
- **Producer-Consumer via Message Broker:** Relies on RabbitMQ to distribute inputs, return outputs, and handle commands (like stopping jobs).
- **Thread-Based Concurrency:** Uses Python's `threading` module to run multiple jobs concurrently within a single consumer instance.
- **Generator-Based Intermediary Results:** Job functions are expected to yield multiple times instead of returning once, allowing the system to capture intermediate processing progress.
- **Graceful Cancellation:** Long-running jobs check a shared threading event (`_stop_event`) on each iteration to stop gracefully when a cancellation message is received.
## Layers
- Purpose: Orchestrates queues, listens to RabbitMQ, and spins up or stops job threads.
- Location: `rabbitasyncq/manager.py`
- Contains: `JobManager` class
- Depends on: `pika`, `rabbitasyncq/job.py`, `rabbitasyncq/messaging.py`
- Used by: External clients setting up a job consumer.
- Purpose: Wraps the user-provided logic in a dedicated stoppable thread. Handles yielding results and catching job-level exceptions.
- Location: `rabbitasyncq/job.py`
- Contains: `StoppableThread`, `StoppableJob` classes
- Depends on: `threading`, user's `job_fn`
- Used by: `JobManager`
- Purpose: Provides an abstraction layer over `pika` channels to standardize publishing messages, acks, and standardized states (STOPPED, SUCCESS).
- Location: `rabbitasyncq/messaging.py`
- Contains: `Messenger` class
- Depends on: `pika`
- Used by: `JobManager`, `StoppableJob`
## Data Flow
## Key Abstractions
- Purpose: Encapsulates long-running work inside a thread that can be canceled mid-execution.
- Examples: `rabbitasyncq/job.py`
- Pattern: Subclassing `threading.Thread` with a `threading.Event()` for cancellation signaling.
- Purpose: Standardizes RabbitMQ publishing operations and encapsulates channel checks.
- Examples: `rabbitasyncq/messaging.py`
- Pattern: Adapter/Facade over `pika.channel.Channel`
## Entry Points
- Location: `rabbitasyncq/manager.py`
- Triggers: Instantiated by a client script.
- Responsibilities: Declares RabbitMQ queues (`{name} input job`, `{name} stop job`, `{name} result`), binds them if an exchange is provided, and starts a background thread to consume messages.
## Error Handling
- **Job Exceptions:** `StoppableJob.run` wraps the user's `job_fn` in a `try...except`. If it crashes, it constructs a JSON payload with `status="ERROR"` and the exception string, publishes it to the result queue, acks the original message so it isn't requeued endlessly, and re-raises the exception.
- **Parsing Errors:** `JobManager` uses bare `except:` blocks when parsing incoming JSON. If a message is malformed or lacks a `job_id`, it silently swallows the error and fails to execute/stop the job (which can lead to hanging un-acked messages).
## Cross-Cutting Concerns
<!-- GSD:architecture-end -->

<!-- GSD:workflow-start source:GSD defaults -->
## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:
- `/gsd:quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd:debug` for investigation and bug fixing
- `/gsd:execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->



<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd:profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->

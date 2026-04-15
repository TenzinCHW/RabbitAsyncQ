# Codebase Structure

**Analysis Date:** 2026-04-15

## Directory Layout

```
[project-root]/
├── rabbitasyncq/       # Core library package
│   ├── __init__.py     # Package exports
│   ├── job.py          # Process worker logic and job context
│   ├── manager.py      # Main entry point, IPC handling, and queue management
│   └── messaging.py    # RabbitMQ channel abstraction
├── test/               # Unit and integration tests
│   ├── __init__.py     # Test package indicator
│   └── test_jobmanager.py # Main test suite
├── pyproject.toml      # Poetry package manifest and dependencies
├── uv.lock             # uv dependency lockfile
└── README.md           # Documentation
```

## Directory Purposes

**`rabbitasyncq/`:**
- Purpose: The main source code directory for the package. Contains all execution logic for handling async multiprocessing jobs from RabbitMQ.
- Contains: Python modules representing the components of the library.
- Key files: `rabbitasyncq/manager.py`, `rabbitasyncq/job.py`

**`test/`:**
- Purpose: Testing suite verifying the functionality of the `rabbitasyncq` package.
- Contains: Pytest test files.
- Key files: `test/test_jobmanager.py`

## Key File Locations

**Entry Points:**
- `rabbitasyncq/manager.py`: Contains `JobManager`, which is the primary class instantiated by a user to start consuming and executing jobs.

**Configuration:**
- `pyproject.toml`: The standard modern Python configuration file using Poetry. Declares `pika` as the main dependency.
- `uv.lock`: Specifies pinned dependency versions.

**Core Logic:**
- `rabbitasyncq/job.py`: Houses the `process_worker` function that loops over the user's generator logic in a separate process.
- `rabbitasyncq/manager.py`: Handles IPC messages from workers and interacts with RabbitMQ.
- `rabbitasyncq/messaging.py`: Manages basic communication with the RabbitMQ broker via the `pika` library.

**Testing:**
- `test/test_jobmanager.py`: Tests error handling, process crashes, successful job executions, and job cancellations.

## Naming Conventions

**Files:**
- lowercase, descriptive: `job.py`, `manager.py`
- Test files prefixed with `test_`: `test_jobmanager.py`

**Classes:**
- PascalCase: `JobManager`, `ProcessJobContext`, `Messenger`

**Functions & Variables:**
- snake_case: `accept_job()`, `process_worker()`, `send_stop()`, `job_id`, `job_fn`

## Where to Add New Code

**New Feature (Library Core):**
- Primary code: `rabbitasyncq/`
- Tests: `test/test_*.py` (add a new test file for new modules)

**New Component/Module:**
- Implementation: Add a new `.py` file inside `rabbitasyncq/` and export it in `rabbitasyncq/__init__.py` if it should be part of the public API.

**Utilities:**
- Shared helpers: Currently handled in `rabbitasyncq/messaging.py` but could be placed in a `utils.py` if broader functionality is needed.

## Special Directories

**`__pycache__/`, `.pytest_cache/`, `.ruff_cache/`:**
- Purpose: Contains compiled Python bytecode and cache files for tests/linting.
- Generated: Yes
- Committed: No

---

*Structure analysis: 2026-04-15*
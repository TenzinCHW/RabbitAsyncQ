# Codebase Structure

**Analysis Date:** 2026-03-28

## Directory Layout

```
[project-root]/
├── rabbitasyncq/       # Core library package
│   ├── __init__.py     # Package exports
│   ├── job.py          # Stoppable job thread logic
│   ├── manager.py      # Main entry point and queue management
│   └── messaging.py    # RabbitMQ channel abstraction
├── test/               # Unit and integration tests
│   ├── __init__.py     # Test package indicator
│   └── test_jobmanager.py # Main test suite
├── pyproject.toml      # Poetry package manifest and dependencies
└── README.md           # Documentation
```

## Directory Purposes

**`rabbitasyncq/`:**
- Purpose: The main source code directory for the package. Contains all execution logic for handling async threaded jobs from RabbitMQ.
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

**Core Logic:**
- `rabbitasyncq/job.py`: Houses the `StoppableJob` thread that loops over the user's generator logic.
- `rabbitasyncq/messaging.py`: Manages basic communication with the RabbitMQ broker via the `pika` library.

**Testing:**
- `test/test_jobmanager.py`: Tests error handling, successful job executions, and job cancellations.

## Naming Conventions

**Files:**
- lowercase, descriptive: `job.py`, `manager.py`
- Test files prefixed with `test_`: `test_jobmanager.py`

**Classes:**
- PascalCase: `JobManager`, `StoppableJob`, `Messenger`

**Functions & Variables:**
- snake_case: `accept_job()`, `send_stop()`, `job_id`, `job_fn`

## Where to Add New Code

**New Feature (Library Core):**
- Primary code: `rabbitasyncq/`
- Tests: `test/test_*.py` (add a new test file for new modules)

**New Component/Module:**
- Implementation: Add a new `.py` file inside `rabbitasyncq/` and export it in `rabbitasyncq/__init__.py` if it should be part of the public API.

**Utilities:**
- Shared helpers: Currently handled in `rabbitasyncq/messaging.py` but could be placed in a `utils.py` if broader functionality is needed.

## Special Directories

**`__pycache__/`:**
- Purpose: Contains compiled Python bytecode (`.pyc` files) to speed up subsequent executions.
- Generated: Yes
- Committed: No

---

*Structure analysis: 2026-03-28*
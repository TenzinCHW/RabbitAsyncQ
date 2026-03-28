# Technology Stack

**Analysis Date:** 2026-03-28

## Languages

**Primary:**
- Python >=3.12 - Core library logic (`rabbitasyncq/*.py`) and tests (`test/*.py`).

**Secondary:**
- Not detected

## Runtime

**Environment:**
- Python 3.12+

**Package Manager:**
- uv (via `uv.lock` file presence)
- Lockfile: present (`uv.lock`)

## Frameworks

**Core:**
- Standard Python libraries (`threading`, `json`) - Asynchronous task handling is implemented via native threading rather than an async framework like `asyncio`.

**Testing:**
- pytest (>=8.4.2) - Unit test execution and fixtures (`test/test_jobmanager.py`).

**Build/Dev:**
- poetry-core (>=2.0.0,<3.0.0) - Build backend defined in `pyproject.toml`.

## Key Dependencies

**Critical:**
- pika (>=1.3.2,<2.0.0) - The core external library providing the AMQP 0-9-1 protocol implementation for RabbitMQ interaction.

**Infrastructure:**
- RabbitMQ - The underlying message broker the library depends on for queuing, job distribution, and message routing.

## Configuration

**Environment:**
- Configured programmatically by passing a `pika.connection.Connection` instance to the `JobManager` (`rabbitasyncq/manager.py`).

**Build:**
- `pyproject.toml` handles packaging and dependency declarations.

## Platform Requirements

**Development:**
- Python >= 3.12
- Local RabbitMQ instance running on `localhost:5672` (default port) for executing the test suite (`test/test_jobmanager.py`).

**Production:**
- Any Python 3.12+ environment with access to a RabbitMQ broker instance.

---

*Stack analysis: 2026-03-28*
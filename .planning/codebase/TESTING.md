# Testing Patterns

**Analysis Date:** 2026-03-28

## Test Framework

**Runner:**
- `pytest` >= 8.4.2
- No explicit `pytest.ini` or `jest.config.*` configuration detected in the codebase (configured via `pyproject.toml` dependency block).

**Assertion Library:**
- Standard Python `assert` statements.
- Direct output validation via `print` logging instead of assertions is very common (e.g., `print(f"Received results: {body}")`).

**Run Commands:**
```bash
pytest                 # Run all tests
```

## Test File Organization

**Location:**
- Located inside the `test/` directory at the project root.
- Not co-located alongside source code.

**Naming:**
- Files are prefixed with `test_` (e.g., `test_jobmanager.py`).

**Structure:**
```
[project-root]/
├── rabbitasyncq/
└── test/
    └── test_jobmanager.py
```

## Test Structure

**Suite Organization:**
```python
def test_job(job_manager):
    # test implementation

def test_cancel(job_manager):
    # test implementation
```

**Patterns:**
- **Setup Pattern:** Initialization and configuration heavily rely on `pytest` fixtures (e.g., `@fixture`, `@fixture(scope="session")`).
- **Dependency Pattern:** `JobManager` instances are yielded via fixtures rather than manually constructed inside individual tests.
- **Teardown Pattern:** No explicit teardown logic aside from fixture scoping boundaries. Threading stop mechanisms inside fixtures are currently commented out.

## Mocking

**Framework:** None detected (e.g., `unittest.mock`, `pytest-mock` are not utilized).

**Patterns:**
- **Real Infrastructure Required:** Testing currently requires a live, local instance of RabbitMQ (e.g., `pika.ConnectionParameters(host='localhost')`).
- **Dummy Functions:** Real callback logic is simulated through dummy functions like `dummy_run`, `exception_run`, and `handle_result`.
```python
def dummy_run(body):
    for i in range(body["var"]):
        time.sleep(1)
        yield {"nyaa": i * 5}
```

## Fixtures and Factories

**Test Data:**
- Simple dictionaries serialized to JSON for mock messages.
- Uses `os.urandom(15).hex()` to generate non-deterministic job identifiers.

**Location:**
- Stored directly inside test files (e.g., `test/test_jobmanager.py`). There is no separate `conftest.py` setup at this time.

## Coverage

**Requirements:** None enforced.
**View Coverage:** No explicit tool (e.g., `pytest-cov`) is configured in the codebase.

## Test Types

**Unit Tests:**
- Small scope validations missing. Existing tests function more closely as integration tests due to the dependency on the real RabbitMQ queue connection over `localhost`.

**Integration Tests:**
- Handled through `test_jobmanager.py`. It tests full queue publishing (`channel.basic_publish`), connection handshaking (`JobManager` background threads consuming messages), and exception handling.

## Common Patterns

**Async Testing:**
- Simulated using blocking `time.sleep()` statements to delay task completion and allow background threads enough time to process.
```python
time.sleep(0.5)
channel.basic_publish(exchange="", routing_key="test_job_name stop job", body=json.dumps({"job_id": job_id}))
```

**Error Testing:**
- Injecting payloads specifically designed to fail into a dedicated worker and monitoring the handler callback.
```python
def test_exception_run(job_manager_exception):
    # trigger fail_test_job_name input job
```

---

*Testing analysis: 2026-03-28*
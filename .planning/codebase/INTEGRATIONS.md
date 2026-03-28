# External Integrations

**Analysis Date:** 2026-03-28

## APIs & External Services

**Not detected:**
- The library does not interact with external HTTP APIs directly.

## Data Storage

**Message Brokers:**
- RabbitMQ - Serves as the primary broker for the asynchronous job queue. The library interacts with it to declare queues, route messages, and process job lifecycles (input, stop, results).
  - Client: `pika` Python package.
  - Connection details: Managed by the consumer codebase passing a `pika` Connection instance to the `JobManager`.

**Databases:**
- None.

**File Storage:**
- Local filesystem only. No external file storage APIs are integrated.

**Caching:**
- None.

## Authentication & Identity

**Auth Provider:**
- Custom / Inherited. Authentication is delegated to the host application providing the configured `pika.Connection` instance, which itself may handle AMQP credentials (e.g., `pika.PlainCredentials`). The `rabbitasyncq` library itself does not enforce or manage auth.

## Monitoring & Observability

**Error Tracking:**
- None out-of-the-box. Handled locally via `try/except` blocks in `rabbitasyncq/manager.py` which catch exceptions, mark jobs as "ERROR", and publish the result via the configured result queue.
  
**Logs:**
- Not natively integrated with external observability tools. Uses standard printing or custom callbacks for logging in tests.

## CI/CD & Deployment

**Hosting:**
- Packaged as a standard Python library (installable via pip/uv/poetry). No deployment specifics found in the repository.

**CI Pipeline:**
- Not detected (no `.github/workflows`, `.gitlab-ci.yml`, etc. found).

## Environment Configuration

**Required env vars:**
- None. The library relies on programmatic dependency injection of the RabbitMQ connection (`pika.connection.Connection`).

**Secrets location:**
- Delegated entirely to the parent application consuming `rabbitasyncq`.

## Webhooks & Callbacks

**Incoming:**
- Consumes AMQP messages from declared RabbitMQ queues: `{name} input job` and `{name} stop job` (`rabbitasyncq/manager.py`).

**Outgoing:**
- Publishes execution states and outcomes to the `{name} result` RabbitMQ queue (`rabbitasyncq/manager.py`).

---

*Integration audit: 2026-03-28*
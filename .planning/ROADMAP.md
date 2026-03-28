# Project Roadmap

## Phases

- [x] **Phase 1: Worker Execution & IPC** - Execute generator jobs in isolated processes and safely stream results via IPC. (completed 2026-03-28)
- [ ] **Phase 2: Broker Integration & Execution Pool** - Dispatch RabbitMQ jobs to the process pool and publish results while maintaining connection heartbeats.
- [ ] **Phase 3: Resilience & Crash Recovery** - Handle worker crashes gracefully by detecting failures and NACKing messages.

## Phase Details

### Phase 1: Worker Execution & IPC
**Goal**: Jobs can execute in isolated processes and safely stream results back via IPC.
**Depends on**: None
**Requirements**: EXEC-02, LIFE-01, LIFE-02
**Success Criteria** (what must be TRUE):
  1. A Python generator executes in an isolated worker process without blocking the main process.
  2. The main process receives yielded values from the worker in real-time via a lock-free IPC queue.
  3. The main process can signal cancellation, and the worker cleanly exits after running `finally` cleanup.
**Plans**: 1 plans

Plans:
- [x] 01-01-PLAN.md — Refactor JobManager to execute jobs in isolated processes via IPC queue

### Phase 2: Broker Integration & Execution Pool
**Goal**: The RabbitMQ consumer can dispatch jobs to a process pool and publish results without dropping connections.
**Depends on**: Phase 1
**Requirements**: EXEC-01, EXEC-03
**Success Criteria** (what must be TRUE):
  1. Incoming RabbitMQ messages automatically trigger job execution in an available pool process.
  2. Intermediate yielded results from workers are forwarded to RabbitMQ without blocking the main event loop.
  3. The RabbitMQ connection remains active (heartbeat maintained) while CPU-intensive tasks run in the background.
**Plans**: 2 plans

Plans:
- [x] 02-01-PLAN.md — Process Pool Optimization & Prefetch Limit
- [ ] 02-02-PLAN.md — Blocking Start and Signal Handling

### Phase 3: Resilience & Crash Recovery
**Goal**: The system recovers cleanly from worker crashes and maintains stable process lifecycles.
**Depends on**: Phase 2
**Requirements**: EXEC-04
**Success Criteria** (what must be TRUE):
  1. The main process detects when a worker process terminates unexpectedly (e.g., system kill or crash).
  2. The system NACKs the corresponding RabbitMQ message upon worker failure to allow queue retries.
  3. The system safely cleans up process handles and prevents zombie processes.
**Plans**: TBD

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Worker Execution & IPC | 1/1 | Complete   | 2026-03-28 |
| 2. Broker Integration & Execution Pool | 0/2 | Not started | - |
| 3. Resilience & Crash Recovery | 0/0 | Not started | - |
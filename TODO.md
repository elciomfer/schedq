# schedq - Evolution Roadmap

List of features currently under development and future milestones.

## Completed Milestones (v0.1.0)

- [x] **Registry Pattern Architecture:** Extracted decorators to the global scope, enabling lazy loading and eliminating circular imports.
- [x] **Data Context Isolation:** Implemented `contextvars` to safely pass Task IDs (TID) and Execution IDs (EID) across steps without explicit parameter passing, preventing data leaks.
- [x] **Graceful Shutdown:** Validated async termination procedure that protects running threads and coroutines from sudden application kills.
- [x] **Stress-Tested Engine:** Proven resilience processing millions of asynchronous and synchronous tasks concurrently with 0 memory leaks.
- [x] **Optimized Custom Heap:** Replaced raw `heapq` with a custom pointer-based heap structure for O(1) updates and fast `invoke()`.
- [x] **Code-as-Workflows (DAGs):** Introduced `@step` to compose modular and resilient pipelines inside flows.
- [x] **Fault Tolerance & Circuit Breaker:** Implemented Exponential Backoff retry policies and automated circuit-breaking for unstable tasks.
- [x] **Concurrency Control:** Added `max_instances` throttling to prevent execution overlap.

---

## Upcoming Roadmap (Next Steps)

### 1. Persistence Module (Resilience)

_Currently, tasks reside exclusively in the process's volatile memory._

- **Goal:** Add optional storage adapters for state persistence (e.g., embedded SQLite or Redis).
- **Feature:** A **Misfire** mechanism to handle edge cases where the server restarts and misses a task's exact execution window.

### 2. Cron Expressions & Timezone Support

_Currently, the engine only supports relative intervals (`timedelta`)._

- **Goal:** Integrate lightweight Cron parsers for human-readable scheduling (e.g., "Every Monday at 08:00").
- **Feature:** Native timezone handling to prevent schedule shifts caused by server time zone differences (UTC) or Daylight Saving Time (DST).

### 3. Distributed Locking / Cluster Mode (Future)

- **Goal:** Allow multiple instances of `schedq` to run behind a load balancer safely without duplicating scheduled executions.

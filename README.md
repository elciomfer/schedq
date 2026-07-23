# schedq

**schedq** is an asynchronous task scheduling and workflow orchestration engine in Python designed to be **extremely lightweight, performant, and dependency-free**. Relying exclusively on native language primitives and high-performance custom data structures, it eliminates the need for heavy infrastructure in concurrent scenarios.

---

## Current Features (What it already does)

- **Registry Pattern & Lazy Loading:** Define your workflows globally in any file without circular imports. The engine automatically discovers and registers them when started.
- **Code-as-Workflows (DAGs via `@step`):** Native support for building sequential or parallel pipelines using Python's native `async/await`.
- **Safe Data Pipelining:** Steps exchange data with built-in concurrency safety. Powered by `contextvars`, execution contexts are isolated, preventing data leaks between parallel executions.
- **Asynchronous & Non-Blocking Concurrency:** Built on top of `asyncio`. Synchronous (CPU-bound) tasks are automatically pushed to a ThreadPool, ensuring the main Event Loop never blocks.
- **Fault Tolerance & Circuit Breaker:** Automated **Exponential Backoff** retry policies with increasing delays, coupled with an automatic **Circuit Breaker** that pauses unstable flows after definitive failures to protect the system.
- **Graceful Shutdown:** Ensures that running tasks finish cleanly before the application exits, preventing data loss even under extreme loads.
- **Custom Optimized Heap:** Internal organization powered by a custom-built, pointer-aware `Heap` class using `__slots__` and an O(1) index dictionary lookup.
- **Concurrency Control (Instance Throttling):** Built-in `max_instances` property to prevent overlapping executions by skipping or throttling heavy jobs.
- **Dynamic Control (Runtime Management):** Programmatic API with fast in-memory access methods (O(1)) to manipulate flows in real time, such as `schedq.pause(tid)`, `schedq.resume(tid)`, and `schedq.invoke(tid)` (forces immediate execution).
- **Observability:** Native separation between Task ID (TID) and Execution ID (EID), injected directly into logs using Python's native `logging` module.

---

## How to Use

### FastAPI Integration with Workflows (Recommended for Web)

Here is how you can set up a resilient ETL pipeline using the global `@flow` and `@step` decorators inside a FastAPI application lifecycle (`lifespan`).

```python
import asyncio
import datetime
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from schedq import Schedq, step, flow

# Configure logging for schedq
logger = logging.getLogger("schedq")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logger.addHandler(handler)

schedq = Schedq(workers=10)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Starts the engine and registers all @flow decorators
    schedq.start()
    app.state.resource = schedq

    yield

    # Safely waits for active tasks to finish before shutting down
    await schedq.stop()
    app.state.resource = None

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    # Returns a rich list of scheduled flows and their states
    return {"flows": app.state.resource.list()}

# 1. Resilient step with built-in retries and exponential backoff
@step(name="External API Call", maxretries=2, retrydelay=10)
async def fetch(endpoint: str):
    await asyncio.sleep(1)
    return {"status": "success", "data": [1, 2, 3]}

# 2. Main flow scheduled by the core engine
@flow(
    interval=datetime.timedelta(seconds=300),
    name="ETL Pipeline",
    maxinstances=1,
    args=("[https://api.schedq.dev](https://api.schedq.dev)",)
)
async def pipeline(tid: str, eid: str, name: str, url: str):
    print(f"Running pipeline with target: {url}")
    result = await fetch(url)
    print(f"Pipeline finished with result: {result}")
```

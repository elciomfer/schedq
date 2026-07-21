# schedq

**schedq** is an asynchronous task scheduling and workflow orchestration engine in Python designed to be **extremely lightweight, performant, and dependency-free**. Relying exclusively on native language primitives and high-performance custom data structures, it eliminates the need for heavy infrastructure in concurrent scenarios.

---

## Current Features (What it already does)

- **Custom Optimized Heap:** Internal organization powered by a custom-built, pointer-aware `Heap` class using `__slots__` and an O(1) index dictionary lookup. Evaluates only the root and sleeps for the exact time remaining, resulting in **0% idle CPU usage**.
- **Asynchronous & Non-Blocking Concurrency:** Built on top of `asyncio`. Long-running tasks and flows are managed cleanly without hijacking the main thread, enabling seamless integration with web frameworks.
- **Code-as-Workflows (DAGs via `@step`):** Native support for building sequential or parallel pipelines using Python's native `async/await`. Steps encapsulate retries, backoffs, and error handling seamlessly.
- **Dynamic Arguments Support:** Full flexibility to pass custom positional (`*args`) and keyword (`**kwargs`) parameters directly into your workflows.
- **Fault Tolerance & Circuit Breaker:** Automated **Exponential Backoff** retry policies with increasing delays, coupled with an automatic **Circuit Breaker** that pauses unstable flows after definitive failures to protect the system.
- **Concurrency Control (Instance Throttling):** Built-in `max_instances` property to prevent overlapping executions by skipping or throttling heavy jobs.
- **Dynamic Control (Runtime Management):** Programmatic API with fast in-memory access methods (O(1)) to manipulate flows in real time, such as `schedq.pause(tid)`, `schedq.resume(tid)`, and `schedq.invoke(tid)` (forces immediate execution).
- **Tracking & Logs (Observability):** Native separation between **TID** (Task/Flow ID) and **EID** (Execution ID). Silent log emission using Python's native `logging` module (via `NullHandler`), which automatically adapts to the host application's configurations.

---

## How to Use

### FastAPI Integration with Workflows (Recommended for Web)

Here is how you can set up a resilient ETL pipeline using `@schedq.flow` and `@schedq.step` inside a FastAPI application lifecycle (`lifespan`)[cite: 6].

```python
import asyncio
import datetime
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from schedq import Schedq

# Configure logging for schedq
logger = logging.getLogger("schedq")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logger.addHandler(handler)

schedq = Schedq()

@asynccontextmanager
async def lifespan(app: FastAPI):
    schedq.start()
    app.state.resource = schedq
    yield
    schedq.stop()
    app.state.resource = None

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"flows": list(app.state.resource.taskmap.keys())}

# 1. Resilient step with built-in retries and exponential backoff
@schedq.step(name="External API Call", maxretries=2, retrydelay=1)
async def fetch_data(endpoint: str):
    await asyncio.sleep(1)
    return {"status": "success", "data": [1, 2, 3]}

# 2. Main flow scheduled in O(1) by the core engine
@schedq.flow(
    interval=datetime.timedelta(seconds=5),
    name="ETL Pipeline",
    maxinstances=1,
    args=("[https://api.schedq.dev](https://api.schedq.dev)",)
)
async def pipeline(tid: str, eid: str, name: str, url: str):
    print(f"Running pipeline with target: {url}")
    result = await fetch_data(url)
    print(f"Pipeline finished with result: {result}")
```

---

## Design Principles

1. **Zero Blocking:** Thread-safe execution using thread pools (`run_in_executor`) to prevent synchronous operations from starving the main event loop.
2. **Optional Dependencies:** Heavier features (such as databases for persistence) must be pluggable and optional to keep the core engine lightweight at all times.
3. **Developer Experience (DX) First:** Concurrency and scheduling complexity should always remain hidden under the engine's hood.

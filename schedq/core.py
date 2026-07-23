from __future__ import annotations

import asyncio
import contextvars
import dataclasses
import datetime
import functools
import logging
import uuid
import typing

log = logging.getLogger("schedq")
log.addHandler(logging.NullHandler())

storage: contextvars.ContextVar[typing.Optional[dict[str, str]]] = contextvars.ContextVar("storage", default=None)
registry: list[dict] = []

def flow(
    interval: datetime.timedelta,
    name: str,
    maxinstances: int = 0,
    maxretries: int = 0,
    retrydelay: int = 1,
    args: tuple = (),
    kwargs: typing.Optional[dict] = None,
):
    def decorator(func: typing.Callable[..., typing.Any]):
        registry.append({
            "name": name,
            "func": func,
            "interval": interval,
            "maxinstances": maxinstances,
            "maxretries": maxretries,
            "retrydelay": retrydelay,
            "args": args,
            "kwargs": kwargs
        })
        return func
    return decorator

def step(name: typing.Optional[str] = None, maxretries: int = 0, retrydelay: int = 1):
    def decorator(func: typing.Callable[..., typing.Any]):
        stepname = name or func.__name__

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            strg = storage.get()
            if strg is None:
                raise PermissionError(
                    f"[ERROR] O step '{stepname}' obrigatoriamente deve rodar dentro de um @flow!"                    )

            sid = f"step-{uuid.uuid4().hex[:8]}" 
            eid = strg.get("eid")
            name = strg.get("name")

            attempt = 0
            while attempt <= maxretries:
                try:
                    if attempt == 0:
                        log.info("[STEP RUNNING] Flow: %s - EID: %s - Step: %s (%s)", name, eid, stepname, sid)
                    else:
                        log.warning("[STEP RETRY %d/%d] Flow: %s - EID: %s - Step: %s", attempt, maxretries, name, eid, stepname)

                    if asyncio.iscoroutinefunction(func):
                        result = await func(*args, **kwargs)
                    else:
                        loop = asyncio.get_running_loop()
                        call = functools.partial(func, *args, **kwargs)
                        result = await loop.run_in_executor(None, call)

                    log.info("[STEP SUCCESS] Flow: %s - EID: %s - Step: %s", name, eid, stepname)
                    return result
                        
                except Exception as err:
                    attempt += 1
                    if attempt > maxretries:
                        log.error("[STEP FAILED DEFINITIVELY] Flow: %s - Step: %s - Error: %s", name, stepname, err)
                        raise err
                    else:
                        backoff = retrydelay * (2 ** (attempt - 1))
                        log.warning("[STEP FAILED] Step: %s - Waiting %ss - Error: %s", stepname, backoff, err)
                        await asyncio.sleep(backoff)
        return wrapper
    return decorator

@dataclasses.dataclass(order=True)
class Task:
    exectime: datetime.datetime
    tid: str = dataclasses.field(compare=False)
    name: str = dataclasses.field(compare=False)
    interval: datetime.timedelta = dataclasses.field(compare=False)
    taskfunc: typing.Callable[..., typing.Any] = dataclasses.field(compare=False)
    args: tuple = dataclasses.field(compare=False, default_factory=tuple)
    kwargs: dict = dataclasses.field(compare=False, default_factory=dict)
    ispaused: bool = dataclasses.field(compare=False, default=False)
    activeruns: int = dataclasses.field(compare=False, default=0)
    maxretries: int = dataclasses.field(compare=False, default=0)
    maxinstances: int = dataclasses.field(compare=False, default=0)
    retrydelay: int = dataclasses.field(compare=False, default=1)


class Heap:
    __slots__ = ("_data", "_pos")

    def __init__(self) -> None:
        self._data: list[Task] = []
        self._pos: dict[str, int] = {}

    def __len__(self) -> int:
        return len(self._data)

    def __bool__(self) -> bool:
        return bool(self._data)

    def peek(self) -> Task:
        return self._data[0]

    def push(self, task: Task) -> None:
        self._data.append(task)
        self._pos[task.tid] = len(self._data) - 1
        self._siftup(len(self._data) - 1)

    def pop(self) -> Task:
        return self._pop(0)

    def remove(self, tid: str) -> typing.Optional[Task]:
        idx = self._pos.get(tid)
        if idx is None:
            return None
        return self._pop(idx)

    def dkey(self, tid: str) -> None:
        idx = self._pos.get(tid)
        if idx is not None:
            self._siftdown(idx)
            self._siftup(idx)

    def _pop(self, idx: int) -> Task:
        last = len(self._data) - 1
        self._swap(idx, last)
        task = self._data.pop()
        del self._pos[task.tid]
        if idx < len(self._data):
            self._siftdown(idx)
            self._siftup(idx)
        return task

    def _swap(self, i: int, j: int) -> None:
        self._data[i], self._data[j] = self._data[j], self._data[i]
        self._pos[self._data[i].tid] = i
        self._pos[self._data[j].tid] = j

    def _siftup(self, i: int) -> None:
        while i > 0:
            parent = (i - 1) // 2
            if self._data[i] < self._data[parent]:
                self._swap(i, parent)
                i = parent
            else:
                break

    def _siftdown(self, i: int) -> None:
        n = len(self._data)
        while True:
            left, right = 2 * i + 1, 2 * i + 2
            smallest = i
            if left < n and self._data[left] < self._data[smallest]:
                smallest = left
            if right < n and self._data[right] < self._data[smallest]:
                smallest = right
            if smallest == i:
                break
            self._swap(i, smallest)
            i = smallest


class Schedq:
    def __init__(self, workers: int = 10, maxqueue: int = 0) -> None:
        self.mainloop: typing.Optional[asyncio.AbstractEventLoop] = None
        self.minheap = Heap()
        self.runntask = None
        self.taskmap: dict[str, Task] = {}
        self.trigger = asyncio.Event()
        self.actives: set[asyncio.Task] = set()
        self.workqueue: asyncio.Queue = asyncio.Queue(maxsize=maxqueue)
        self.workercount = workers

    def _runonloop(self, fn: typing.Callable[[], None]) -> None:
        if self.mainloop is None:
            fn()
            return

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is self.mainloop:
            fn()
        else:
            self.mainloop.call_soon_threadsafe(fn)

    def _add(
        self,
        tid: str,
        name: str,
        interval: datetime.timedelta,
        function: typing.Callable[..., typing.Any],
        maxinstances: int = 0,
        maxretries: int = 0,
        retrydelay: int = 1,
        args: tuple = (),
        kwargs: typing.Optional[dict] = None,
    ):
        if interval.total_seconds() < 1:
            raise ValueError(
                f"The minimum allowed interval is 1 second. "
                f"Provided: {interval.total_seconds()}s for task '{name}'"
            )

        newtask = Task(
            exectime=datetime.datetime.now() + interval,
            tid=tid,
            name=name,
            interval=interval,
            taskfunc=function,
            args=tuple(args),
            kwargs=dict(kwargs or {}),
            maxinstances=maxinstances,
            maxretries=maxretries,
            retrydelay=retrydelay,
        )

        def _do():
            self.minheap.push(newtask)
            self.taskmap[tid] = newtask
            if self.mainloop:
                self.trigger.set()

        self._runonloop(_do)

    def start(self):
        self.mainloop = asyncio.get_running_loop()

        for f in registry:
            if not any(t.name == f["name"] for t in self.taskmap.values()):
                self._add(
                    tid=str(uuid.uuid4()),
                    name=f["name"],
                    interval=f["interval"],
                    function=f["func"],
                    maxinstances=f["maxinstances"],
                    maxretries=f["maxretries"],
                    retrydelay=f["retrydelay"],
                    args=f["args"],
                    kwargs=f["kwargs"],
                )

        for worker in range(self.workercount):
            workertask = asyncio.create_task(self._worker(worker))
            self.actives.add(workertask)
            workertask.add_done_callback(self.actives.discard)

        self.runntask = asyncio.create_task(self._run())

    async def stop(self, wait: bool = True, timeout: typing.Optional[float] = None) -> None:
        if self.runntask:
            self.runntask.cancel()
            try:
                await self.runntask
            except asyncio.CancelledError:
                pass
            self.runntask = None

        if not self.actives:
            return

        if wait:
            try:
                if timeout is None:
                    await self.workqueue.join()
                else:
                    await asyncio.wait_for(self.workqueue.join(), timeout=timeout)
            except asyncio.TimeoutError:
                pass

        pending = list(self.actives)
        for w in pending:
            w.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    def pause(self, tid: str):
        def _do():
            task = self.taskmap.get(tid)
            if task is not None:
                task.ispaused = True
                log.info("[PAUSED] Flow: %s - TID: %s", task.name, tid)
            else:
                log.warning("Attempted to pause unknown TID: %s", tid)
        self._runonloop(_do)

    def resume(self, tid: str):
        def _do():
            task = self.taskmap.get(tid)
            if task is not None:
                task.ispaused = False
                log.info("[RESUMED] Flow: %s - TID: %s", task.name, tid)
            else:
                log.warning("Attempted to resume unknown TID: %s", tid)
        self._runonloop(_do)

    def invoke(self, tid: str):
        def _do():
            task = self.taskmap.get(tid)
            if task is None:
                log.warning("Attempted to invoke unknown TID: %s", tid)
                return

            if task.ispaused:
                log.warning("Cannot invoke paused Flow: %s (TID: %s). Resume it first.", task.name, tid)
                return

            task.exectime = datetime.datetime.now()
            self.minheap.dkey(tid)
            if self.mainloop:
                self.trigger.set()

        self._runonloop(_do)

    def remove(self, tid: str):
        def _do():
            task = self.taskmap.pop(tid, None)
            if task is not None:
                self.minheap.remove(tid)
                log.info("[REMOVED] Flow: %s - TID: %s", task.name, tid)
            else:
                log.warning("Attempted to remove unknown TID: %s", tid)
        self._runonloop(_do)

    def list(self) -> list[dict]:
        return [
            {
                "tid": task.tid,
                "name": task.name,
                "next_run": task.exectime.isoformat(),
                "interval_seconds": task.interval.total_seconds(),
                "is_paused": task.ispaused,
                "active_runs": task.activeruns,
                "max_instances": task.maxinstances,
            }
            for task in self.taskmap.values()
        ]

    def qsize(self) -> int:
        return self.workqueue.qsize()

    async def _callback(self, task: Task, eid: str):
        token = storage.set({ "tid": task.tid, "eid": eid, "name": task.name })
        
        try:
            if asyncio.iscoroutinefunction(task.taskfunc):
                return await task.taskfunc(task.tid, eid, task.name, *task.args, **task.kwargs)

            loop = asyncio.get_running_loop()
            call = functools.partial(task.taskfunc, task.tid, eid, task.name, *task.args, **task.kwargs)
            return await loop.run_in_executor(None, call)
        finally:
            storage.reset(token)

    async def _worker(self, worker: int) -> None:
        while True:
            task, eid = await self.workqueue.get()
            try:
                await self._execute(task, eid)
            finally:
                self.workqueue.task_done()

    async def _execute(self, t: Task, e: str) -> None:
        attempt = 0
        try:
            while attempt <= t.maxretries:
                try:
                    if attempt == 0:
                        log.info("[FLOW RUNNING] Flow: %s - TID: %s - EID: %s", t.name, t.tid, e)
                    else:
                        log.warning(
                            "[FLOW RETRY %d/%d] Flow: %s - TID: %s - EID: %s",
                            attempt, t.maxretries, t.name, t.tid, e,
                        )

                    await self._callback(t, e)

                    log.info("[FLOW SUCCESS] Flow: %s - EID: %s", t.name, e)
                    break

                except Exception as err:
                    attempt += 1
                    if attempt > t.maxretries:
                        log.error(
                            "[FLOW FAILED DEFINITIVELY] Flow: %s - EID: %s - Error: %s",
                            t.name, e, err,
                        )

                        t.ispaused = True
                        log.critical(
                            "[CIRCUIT BREAKER] Flow: %s (TID: %s) It was paused to protect the system.",
                            t.name, t.tid,
                        )
                    else:
                        backoff = t.retrydelay * (2 ** (attempt - 1))
                        log.warning(
                            "[FLOW FAILED] Flow: %s - Waiting %ss - Error: %s",
                            t.name, backoff, err,
                        )
                        await asyncio.sleep(backoff)
        finally:
            t.activeruns -= 1

    async def _run(self):
        while True:
            self.trigger.clear()

            if not self.minheap:
                await self.trigger.wait()
                continue

            now = datetime.datetime.now()
            headtask = self.minheap.peek()

            if now >= headtask.exectime:
                task = self.minheap.pop()

                if not task.ispaused:
                    if task.maxinstances > 0 and task.activeruns >= task.maxinstances:
                        log.warning(
                            "[SKIPPED] Flow: %s - TID: %s - Reason: Limit reached (%d/%d)",
                            task.name, task.tid, task.activeruns, task.maxinstances,
                        )
                    else:
                        eid = str(uuid.uuid4())
                        task.activeruns += 1
                        await self.workqueue.put((task, eid))

                task.exectime = now + task.interval
                self.minheap.push(task)

                await asyncio.sleep(0)
            else:
                leftover = (headtask.exectime - now).total_seconds()

                if leftover > 0:
                    try:
                        await asyncio.wait_for(self.trigger.wait(), timeout=leftover)
                    except asyncio.TimeoutError:
                        pass
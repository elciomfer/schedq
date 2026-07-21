from __future__ import annotations

import asyncio
import dataclasses
import datetime
import functools
import logging
import uuid
import typing

log = logging.getLogger("schedq")
log.addHandler(logging.NullHandler())


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
    def __init__(self) -> None:
        self.mainloop: typing.Optional[asyncio.AbstractEventLoop] = None
        self.minheap = Heap()
        self.runntask = None
        self.taskmap: dict[str, Task] = {}
        self.trigger = asyncio.Event()

    def _runonloop(self, fn: typing.Callable[[], None]) -> None:
        if self.mainloop is None:
            fn()
            return

        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

        if running_loop is self.mainloop:
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

    def step(self, name: typing.Optional[str] = None, maxretries: int = 0, retrydelay: int = 1):
        def decorator(func: typing.Callable[..., typing.Any]):
            stepname = name or func.__name__

            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                attempt = 0
                while attempt <= maxretries:
                    try:
                        if attempt == 0:
                            log.info(f"[STEP RUNNING] {stepname}")
                        else:
                            log.warning(f"[STEP RETRY {attempt}/{maxretries}] {stepname}")

                        if asyncio.iscoroutinefunction(func):
                            result = await func(*args, **kwargs)
                        else:
                            loop = asyncio.get_running_loop()
                            call = functools.partial(func, *args, **kwargs)
                            result = await loop.run_in_executor(None, call)

                        log.info(f"[STEP SUCCESS] {stepname}")
                        return result
                    except Exception as err:
                        attempt += 1
                        if attempt > maxretries:
                            log.error(f"[STEP FAILED DEFINITIVELY] {stepname} - Error: {err}")
                            raise err
                        else:
                            backoff = retrydelay * (2 ** (attempt - 1))
                            log.warning(f"[STEP FAILED] {stepname} - Waiting {backoff}s - Error: {err}")
                            await asyncio.sleep(backoff)
            return wrapper
        return decorator

    def flow(
        self,
        interval: datetime.timedelta,
        name: typing.Optional[str] = None,
        maxinstances: int = 0,
        maxretries: int = 0,
        retrydelay: int = 1,
        args: tuple = (),
        kwargs: typing.Optional[dict] = None,
    ):
        def decorator(func: typing.Callable[..., typing.Any]):
            self._add(
                str(uuid.uuid4()), name or func.__name__, interval, func,
                maxinstances, maxretries, retrydelay, args, kwargs,
            )
            return func
        return decorator

    def start(self):
        self.mainloop = asyncio.get_running_loop()
        self.runntask = asyncio.create_task(self._run())

    def stop(self):
        if self.runntask:
            self.runntask.cancel()

    def pause(self, tid: str):
        if tid in self.taskmap:
            self.taskmap[tid].ispaused = True

    def resume(self, tid: str):
        if tid in self.taskmap:
            self.taskmap[tid].ispaused = False

    def invoke(self, tid: str):
        def _do():
            task = self.taskmap.get(tid)
            if task is None:
                return
            task.exectime = datetime.datetime.now()
            self.minheap.dkey(tid)
            if self.mainloop:
                self.trigger.set()

        self._runonloop(_do)

    async def _callback(self, task: Task, eid: str):
        if asyncio.iscoroutinefunction(task.taskfunc):
            return await task.taskfunc(task.tid, eid, task.name, *task.args, **task.kwargs)

        loop = asyncio.get_running_loop()
        call = functools.partial(task.taskfunc, task.tid, eid, task.name, *task.args, **task.kwargs)
        return await loop.run_in_executor(None, call)

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
                        log.warning(f"[SKIPPED] Flow: {task.name} - TID: {task.tid} - Reason: Limit reached ({task.activeruns}/{task.maxinstances})")
                    else:
                        eid = str(uuid.uuid4())

                        async def taskrun(t=task, e=eid):
                            t.activeruns += 1
                            attempt = 0

                            try:
                                while attempt <= t.maxretries:
                                    try:
                                        if attempt == 0:
                                            log.info(f"[FLOW RUNNING] Flow: {t.name} - TID: {t.tid} - EID: {e}")
                                        else:
                                            log.warning(f"[FLOW RETRY {attempt}/{t.maxretries}] Flow: {t.name} - TID: {t.tid} - EID: {e}")

                                        await self._callback(t, e)

                                        log.info(f"[FLOW SUCCESS] Flow: {t.name} - EID: {e}")
                                        break

                                    except Exception as err:
                                        attempt += 1
                                        if attempt > t.maxretries:
                                            log.error(f"[FLOW FAILED DEFINITIVELY] Flow: {t.name} - EID: {e} - Error: {err}")

                                            t.ispaused = True
                                            log.critical(f"[CIRCUIT BREAKER] Flow: {t.name} (TID: {t.tid}) It was paused to protect the system.")
                                        else:
                                            backoff = t.retrydelay * (2 ** (attempt - 1))
                                            log.warning(f"[FLOW FAILED] Flow: {t.name} - Waiting {backoff}s - Error: {err}")
                                            await asyncio.sleep(backoff)
                            finally:
                                t.activeruns -= 1

                        asyncio.create_task(taskrun())

                task.exectime = now + task.interval
                self.minheap.push(task)
            else:
                leftover = (headtask.exectime - now).total_seconds()

                if leftover > 0:
                    try:
                        await asyncio.wait_for(self.trigger.wait(), timeout=leftover)
                    except asyncio.TimeoutError:
                        pass
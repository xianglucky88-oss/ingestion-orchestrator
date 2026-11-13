"""Injectable clocks used by the scheduler and by local task executors.

The scheduler never calls ``asyncio.sleep`` directly.  It hands wait
deadlines to a :class:`Clock`, which lets tests inject a deterministic
virtual clock and keeps production on the monotonic real clock.
"""
from __future__ import annotations

import asyncio
import time
from typing import Iterable


class Clock:
    """Protocol for time and waiting."""

    def now(self) -> float:  # pragma: no cover - interface
        raise NotImplementedError

    async def sleep(self, seconds: float) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    async def wait_until(self, when: float) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    async def wait_for(self, futures: Iterable["asyncio.Future"], deadline: float | None) -> set["asyncio.Future"]:
        """Wait until one of ``futures`` is done or clock time reaches ``deadline``."""
        raise NotImplementedError


class RealClock(Clock):
    """Monotonic wall-clock backed clock."""

    def now(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        if seconds > 0:
            await asyncio.sleep(seconds)

    async def wait_until(self, when: float) -> None:
        delay = when - self.now()
        if delay > 0:
            await asyncio.sleep(delay)

    async def wait_for(self, futures, deadline):
        futures = set(futures)
        timeout = None if deadline is None else max(0.0, deadline - self.now())
        if futures:
            done, _ = await asyncio.wait(futures, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
            return done
        if timeout:
            await asyncio.sleep(timeout)
        return set()


class ManualClock(Clock):
    """Deterministic virtual clock.

    Sleeping coroutines register a wake-up time; :meth:`wait_for` advances
    virtual time to the earliest pending wake-up or the supplied deadline.
    Relative ordering of long tasks, back-offs and dependency completions is
    fully reproducible and instant.
    """

    def __init__(self, start: float = 0.0) -> None:
        self._now = float(start)
        self._sleepers: list[tuple[float, asyncio.Future]] = []

    def now(self) -> float:
        return self._now

    def pending_wake(self) -> float | None:
        live = [target for target, future in self._sleepers if not future.done()]
        return min(live) if live else None

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            await asyncio.sleep(0)
            return
        await self._sleep_until(self._now + seconds)

    async def wait_until(self, when: float) -> None:
        if when <= self._now:
            await asyncio.sleep(0)
            return
        await self._sleep_until(float(when))

    async def _sleep_until(self, target: float) -> None:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        entry = (target, future)
        self._sleepers.append(entry)
        try:
            await future
        finally:
            if entry in self._sleepers:
                self._sleepers.remove(entry)

    async def wait_for(self, futures, deadline):
        futures = set(futures)
        # Tasks dispatched in the current turn have not reached their first
        # clock wait yet.  Yield a bounded number of times so their execution
        # windows open at dispatch time rather than after an unrelated sleeper
        # has already advanced virtual time.
        for _ in range(8):
            if any(future.done() for future in futures):
                break
            await asyncio.sleep(0)
        while True:
            done = {future for future in futures if future.done()}
            if done:
                return done
            if deadline is not None and self._now >= deadline:
                return {future for future in futures if future.done()}

            sleepers = [target for target, future in self._sleepers if not future.done()]
            candidates = sleepers[:]
            if deadline is not None:
                candidates.append(deadline)
            if not candidates:
                done, _ = await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)
                return done

            target = min(candidates)
            if target > self._now:
                self._now = target
                for wake_at, future in self._sleepers:
                    if not future.done() and wake_at <= self._now:
                        future.set_result(None)
            # Even when the deadline is not later than "now", yield once so
            # coroutines that were just resolved can run before we re-check.
            await asyncio.sleep(0)

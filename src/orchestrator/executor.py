"""Local, injectable asynchronous task executors.

The scheduler accepts any callable with signature ``(spec, attempt)`` or
``(spec,)`` that returns ``bool``/``AttemptResult``/``None`` directly or via an
awaitable.  :class:`LocalExecutor` is the default: it sleeps for the task
``duration`` on the injected clock and succeeds once the attempt number
exceeds ``fail_until`` (the same policy the original synchronous executor
implemented).
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable

from .clock import Clock
from .models import AttemptResult, TaskSpec


@dataclass(frozen=True)
class ExecutedAttempt:
    """A recorded execution window, useful for verifying slot limits."""

    task_id: str
    attempt: int
    resource_group: str
    started_at: float
    finished_at: float
    succeeded: bool
    cancelled: bool = False


class LocalExecutor:
    """Simulates controlled local collection work on the injected clock."""

    def __init__(self, clock: Clock):
        self.clock = clock
        self.windows: list[ExecutedAttempt] = []

    async def __call__(self, spec: TaskSpec, attempt: int) -> AttemptResult:
        started = self.clock.now()
        cancelled = False
        try:
            await self.clock.sleep(spec.duration)
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            succeeded = not cancelled and attempt > spec.fail_until
            self.windows.append(
                ExecutedAttempt(
                    task_id=spec.task_id,
                    attempt=attempt,
                    resource_group=spec.resource_group,
                    started_at=started,
                    finished_at=self.clock.now(),
                    succeeded=succeeded,
                    cancelled=cancelled,
                )
            )
        if attempt <= spec.fail_until:
            return AttemptResult(False, f"simulated failure on attempt {attempt}")
        return AttemptResult(True, "collected")


def wrap(executor: Callable[[TaskSpec, int], Any], clock: Clock) -> Callable[[TaskSpec, int], Any]:
    """Accept both async and sync callables; the scheduler awaits awaitables."""
    del clock
    return executor

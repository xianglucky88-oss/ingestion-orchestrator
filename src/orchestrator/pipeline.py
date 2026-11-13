"""Run-scoped configuration loading and the orchestration pipeline.

The task fixture stays in its original schema; run limits live in a separate
optional JSON file so old fixtures remain executable:

.. code-block:: json

    {"max_parallel": 2, "group_limits": {"telemetry": 1, "crawler": 2},
     "retry_backoff": 1.5, "cancellations": {"rpa-1": 3}}

``RunConfig.from_dict`` understands the subset shown above.  Tasks may also
carry per-task ``retry_backoff`` to override the run default.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from .clock import Clock, ManualClock, RealClock
from .executor import LocalExecutor
from .models import RunConfig
from .scheduler import CollectionScheduler, ExecutorFn, Reporter
from .storage import RunStore


class OrchestrationPipeline:
    """Connects the task source, scheduler, executor and run store.

    It owns only orchestration: loading, wiring the event sink, invoking the
    scheduler and closing out the run record.  All scheduling decisions stay
    in :class:`CollectionScheduler`.
    """

    def __init__(
        self,
        tasks: Iterable[Any],
        config: RunConfig | None = None,
        *,
        store: RunStore | None = None,
        clock: Clock | None = None,
        executor: ExecutorFn | None = None,
        max_retries: int = 2,
        reporter: Reporter | None = None,
    ):
        self.tasks = list(tasks)
        self.config = config or RunConfig()
        self.store = store
        self.clock = clock or RealClock()
        self.executor = executor or LocalExecutor(self.clock)
        self.max_retries = max_retries
        self.reporter = reporter

    def build(self) -> CollectionScheduler:
        def report(event: Mapping[str, Any]) -> None:
            if self.store is not None:
                self.store.apply_event(event)
            if self.reporter is not None:
                self.reporter(dict(event))

        return CollectionScheduler(
            self.tasks,
            self.config,
            self.executor,
            self.clock,
            max_retries=self.max_retries,
            report=report,
        )

    async def run_async(self) -> dict[str, int]:
        scheduler = self.build()
        if self.store is not None:
            self.store.start_run(self.config, scheduler.clock.now())
        started = scheduler.clock.now()
        try:
            summary = await scheduler.run()
        finally:
            finished = scheduler.clock.now()
        if self.store is not None:
            self.store.finish_run({"summary": summary, "duration": finished - started}, finished)
        return summary

    def run(self) -> dict[str, int]:
        return asyncio.run(self.run_async())


def load_config(path: str | Path | None) -> RunConfig:
    if path is None:
        return RunConfig()
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return RunConfig.from_dict(raw)


def run_files(
    fixture: str,
    database: str,
    *,
    config_path: str | None = None,
    config: RunConfig | None = None,
    max_retries: int = 2,
    clock: Clock | None = None,
    executor: ExecutorFn | None = None,
    virtual_time: bool = False,
) -> dict[str, int]:
    """One-shot run over files (used by the CLI).

    ``virtual_time=True`` uses the deterministic :class:`ManualClock`; with
    local executors whose durations/back-offs come from the fixture,
    ``retry_backoff`` and ``duration`` are interpreted as virtual seconds.
    """
    from .sources import TaskSource

    resolved = config or load_config(config_path)
    chosen_clock = clock or (ManualClock() if virtual_time else RealClock())
    store = RunStore(database)
    try:
        tasks = TaskSource().read(fixture)
        pipeline = OrchestrationPipeline(
            tasks,
            resolved,
            store=store,
            clock=chosen_clock,
            executor=executor,
            max_retries=max_retries,
        )
        return pipeline.run()
    finally:
        store.close()

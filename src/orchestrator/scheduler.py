"""Dependency-aware, resource-limited collection scheduler.

Responsibilities are kept separate:

* :class:`DependencyGraph` validates ``requires`` and turns missing references
  and cycles into explicit blocked reasons.
* :class:`PriorityScheduler` only orders tasks that are already ready:
  priority first, then fairness ageing for tasks that have been waiting.
* :class:`RetryExecutor` only owns the attempt budget and back-off delay.
* :class:`ResourceGovernor` only owns total and per-group slot accounting.
* :class:`CollectionScheduler` is the event loop connecting all of them and
  emitting one event per state transition.

Scheduling rules
----------------
1. A task is *ready* only after every id in ``requires`` reached ``completed``.
   Priority can never let a dependent overtake a prerequisite.
2. A ready task starts only when the run-wide slot budget **and** its resource
   group budget both have capacity.  An execution slot is held only while an
   attempt is actually running.
3. A failed attempt releases its slot immediately.  The task then enters
   ``backoff`` (a clock wait that holds no slot) and later re-enters the ready
   set, competing for resources again like any other ready task.
4. Each scheduler round: (a) apply cancellation requests, (b) apply
   published attempt outcomes, (c) propagate bad predecessors to blocked,
   (d) promote dependents of completed tasks to ready, (e) dispatch.
   Consequences:
     - cancellation is evaluated before this round's completions, so a
       dependent that is cancelled while its prerequisite completes at the
       same instant is **cancelled**, never starts, and its own dependents are
       blocked as ``dependency_cancelled:<id>``;
     - the same rule applies to a task whose own attempt finished at that
       instant: the task is cancelled, its dependents are blocked, while the
       attempt row keeps the executor's real outcome (the attempt did run to
       completion; the cancellation decision belongs to the task);
     - tasks already terminal in an earlier round are never affected by a
       later-seen cancellation;
     - a dependent never starts after its prerequisite failed or was
       cancelled: it is blocked with an explicit reason;
     - terminal predecessors propagate explicit reasons transitively so no
       task ever remains silently waiting.
5. Blocked reasons produced by the scheduler:
   ``dependency_failed:<id>`` (retry budget exhausted),
   ``dependency_cancelled:<id>``,
   ``dependency_blocked:<id>`` (nearest blocked predecessor),
   ``dependency_missing:<id>`` (this task, or a predecessor, references an
   unknown id), ``dependency_cycle`` (this task, or a predecessor, belongs to
   a dependency cycle).  The root cause is propagated through
   ``dependency_blocked`` chains.
"""
from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping

from .clock import Clock
from .models import AttemptResult, RunConfig, TaskRun, TaskSpec

WAITING = "waiting"
READY = "ready"
RUNNING = "running"
BACKOFF = "backoff"
BLOCKED = "blocked"
COMPLETED = "completed"
FAILED = "failed"
CANCELLED = "cancelled"
TERMINAL = (COMPLETED, FAILED, CANCELLED)
BAD = (FAILED, CANCELLED, BLOCKED)

ExecutorFn = Callable[..., Any]
Reporter = Callable[[dict[str, Any]], None]


class GraphError(ValueError):
    """Fixture error that cannot be represented by a blocked task."""

    def __init__(self, message: str, *, task_id: str | None = None, code: str | None = None):
        super().__init__(message)
        self.task_id = task_id
        self.code = code


@dataclass
class TaskState:
    """Mutable scheduler bookkeeping for one task (not persisted)."""

    spec: TaskSpec
    status: str = WAITING
    attempts: int = 0
    blocked_reason: str | None = None
    started_at: float | None = None
    finished_at: float | None = None
    ready_since: float | None = None
    cancel_requested: bool = False
    retry_ready: bool = False
    pending_outcome: AttemptResult | None = None
    attempt_cancelled: bool = False
    action_future: "asyncio.Future | None" = None
    action_handle: "asyncio.Task | None" = None

    def as_run(self) -> TaskRun:
        return TaskRun(
            task_id=self.spec.task_id,
            status=self.status,
            attempts=self.attempts,
            mode=self.spec.mode,
            resource_group=self.spec.resource_group,
            blocked_reason=self.blocked_reason,
            started_at=self.started_at,
            finished_at=self.finished_at,
        )


class DependencyGraph:
    """Validates task ids, references and cycles."""

    def __init__(self, tasks: Iterable[Any]):
        specs = [TaskSpec.from_any(task) for task in tasks]
        counts: dict[str, int] = {}
        for spec in specs:
            counts[spec.task_id] = counts.get(spec.task_id, 0) + 1
        duplicates = sorted(task_id for task_id, count in counts.items() if count > 1)
        if duplicates:
            raise GraphError(f"duplicate task ids: {duplicates}", code="duplicate_task")

        self.specs: dict[str, TaskSpec] = {spec.task_id: spec for spec in specs}
        self.dependents: dict[str, set[str]] = {task_id: set() for task_id in self.specs}
        self.invalid: dict[str, str] = {}
        for spec in self.specs.values():
            for dependency in spec.requires:
                if dependency in self.specs:
                    self.dependents[dependency].add(spec.task_id)
                else:
                    self.invalid.setdefault(spec.task_id, f"dependency_missing:{dependency}")
        for task_id in self._cycle_members():
            self.invalid.setdefault(task_id, "dependency_cycle")

    def _cycle_members(self) -> set[str]:
        """Tarjan SCC: every member of a multi-node SCC or a self-loop is cyclic."""
        index = 0
        indices: dict[str, int] = {}
        low: dict[str, int] = {}
        stack: list[str] = []
        on_stack: set[str] = set()
        members: set[str] = set()

        def visit(node: str) -> None:
            nonlocal index
            indices[node] = low[node] = index
            index += 1
            stack.append(node)
            on_stack.add(node)
            for dependency in self.specs[node].requires:
                if dependency not in self.specs:
                    continue
                if dependency not in indices:
                    visit(dependency)
                    low[node] = min(low[node], low[dependency])
                elif dependency in on_stack:
                    low[node] = min(low[node], indices[dependency])
            if low[node] == indices[node]:
                component: list[str] = []
                while True:
                    popped = stack.pop()
                    on_stack.discard(popped)
                    component.append(popped)
                    if popped == node:
                        break
                if len(component) > 1:
                    members.update(component)
                elif component[0] in self.specs[component[0]].requires:
                    members.add(component[0])

        for task_id in self.specs:
            if task_id not in indices:
                visit(task_id)
        return members

    def downstream(self, task_id: str) -> set[str]:
        found: set[str] = set()
        stack = list(self.dependents.get(task_id, ()))
        while stack:
            current = stack.pop()
            if current in found:
                continue
            found.add(current)
            stack.extend(self.dependents.get(current, ()))
        return found


class PriorityScheduler:
    """Orders ready tasks; owns no clocks, slots or persistence.

    Effective priority = ``priority + aging_step * seconds_ready``.  Tasks that
    become ready together are therefore ordered by raw priority, while a task
    that keeps missing a slot slowly gains on higher-priority peers and cannot
    starve.  ``aging_step=0`` is strict priority ordering.
    """

    def __init__(self, aging_step: float = 1.0):
        if aging_step < 0:
            raise ValueError("aging_step must be non-negative")
        self.aging_step = aging_step

    def order(self, tasks: list[Any]) -> list[Any]:
        """Static priority ordering (legacy API)."""
        return sorted(tasks, key=lambda task: (-int(_task_value(task, "priority", 0)), str(_task_value(task, "task_id", ""))))

    def effective_priority(self, state: TaskState, now: float) -> float:
        waited = 0.0 if state.ready_since is None else max(0.0, now - state.ready_since)
        return float(state.spec.priority) + self.aging_step * waited

    def rank_now(self, candidates: Iterable[TaskState], now: float) -> list[TaskState]:
        return sorted(
            candidates,
            key=lambda state: (
                -self.effective_priority(state, now),
                -state.spec.priority,
                state.ready_since if state.ready_since is not None else float("inf"),
                state.spec.task_id,
            ),
        )


def _task_value(task: Any, key: str, default: Any) -> Any:
    if isinstance(task, Mapping):
        return task.get(key, default)
    return getattr(task, key, default)


class RetryExecutor:
    """Per-task retry policy: attempt budget and back-off between attempts."""

    def __init__(self, max_retries: int = 2, clock: Clock | None = None):
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        self.max_retries = max_retries
        self.clock = clock

    def attempts_allowed(self, task: TaskSpec) -> int:
        return self.max_retries + 1

    def backoff_seconds(self, task: Any, config: RunConfig, attempt: int) -> float:
        """Seconds to wait after failed attempt number ``attempt`` (1-based)."""
        spec = TaskSpec.from_any(task)
        base = config.retry_backoff if spec.retry_backoff is None else spec.retry_backoff
        return max(0.0, base * (config.backoff_multiplier ** max(0, attempt - 1)))

    def execute(self, task: Any) -> TaskRun:
        """Legacy synchronous helper: retry immediately until success."""
        spec = TaskSpec.from_any(task)
        attempts = 0
        while attempts <= self.max_retries:
            attempts += 1
            if spec.fail_until < attempts:
                return TaskRun(spec.task_id, COMPLETED, attempts, spec.mode, spec.resource_group)
        return TaskRun(spec.task_id, FAILED, attempts, spec.mode, spec.resource_group)


class ResourceGovernor:
    """Execution-slot accounting for the run total and for each group."""

    def __init__(self, config: RunConfig):
        self.config = config
        self.in_flight = 0
        self.in_flight_by_group: dict[str, int] = {}
        self.max_total_seen = 0
        self.max_group_seen: dict[str, int] = {}

    def can_start(self, group: str) -> bool:
        if self.in_flight >= self.config.max_parallel:
            return False
        return self.in_flight_by_group.get(group, 0) < self.config.group_limit(group)

    def acquire(self, group: str) -> None:
        if not self.can_start(group):
            raise RuntimeError(f"resource limit exceeded for group {group!r}")
        self.in_flight += 1
        self.in_flight_by_group[group] = self.in_flight_by_group.get(group, 0) + 1
        self.max_total_seen = max(self.max_total_seen, self.in_flight)
        self.max_group_seen[group] = max(self.max_group_seen.get(group, 0), self.in_flight_by_group[group])

    def release(self, group: str) -> None:
        held = self.in_flight_by_group.get(group, 0)
        if self.in_flight <= 0 or held <= 0:
            raise RuntimeError(f"released a slot that was not acquired for group {group!r}")
        self.in_flight -= 1
        if held == 1:
            del self.in_flight_by_group[group]
        else:
            self.in_flight_by_group[group] = held - 1

    def snapshot(self) -> dict[str, Any]:
        return {
            "total": self.in_flight,
            "groups": dict(self.in_flight_by_group),
            "max_total": self.max_total_seen,
            "max_groups": dict(self.max_group_seen),
        }


class CollectionScheduler:
    """Dependency-aware scheduler over an injectable clock and executor."""

    def __init__(
        self,
        tasks: Iterable[Any],
        config: RunConfig,
        executor: ExecutorFn,
        clock: Clock,
        *,
        max_retries: int = 2,
        report: Reporter | None = None,
        cancel: Iterable[str] | Callable[[], Iterable[str]] | None = None,
        priority: PriorityScheduler | None = None,
    ):
        self.graph = DependencyGraph(tasks)
        self.config = config
        self.executor = executor
        self.clock = clock
        self.retry = RetryExecutor(max_retries, clock)
        self.priority = priority or PriorityScheduler(config.aging_step)
        self.report = report or (lambda event: None)
        self._external_cancel = cancel or ()
        self._cancelled: set[str] = set()
        self._cancellations = dict(config.cancellations)
        self._executor_arity = _arity(executor)

        self.states: dict[str, TaskState] = {}
        for task_id, spec in self.graph.specs.items():
            reason = self.graph.invalid.get(task_id)
            self.states[task_id] = TaskState(
                spec,
                status=BLOCKED if reason else WAITING,
                blocked_reason=reason,
            )
        self.governor = ResourceGovernor(config)
        self.trace: list[dict[str, Any]] = []
        self.start_order: list[str] = []

        self._ready: set[str] = set()
        self._counts = {
            "scheduled": len(self.states),
            "completed": 0,
            "failed": 0,
            "cancelled": 0,
            "blocked": sum(1 for state in self.states.values() if state.status == BLOCKED),
            "retries": 0,
        }
        self._origin = 0.0

    # -- public ---------------------------------------------------------
    @property
    def counts(self) -> dict[str, int]:
        return dict(self._counts)

    def status(self, task_id: str) -> TaskState:
        return self.states[task_id]

    async def run(self) -> dict[str, int]:
        self._origin = self.clock.now()
        for state in self.states.values():
            if state.status == BLOCKED:
                self._emit("blocked", state, reason=state.blocked_reason)
            else:
                self._emit("waiting", state)

        while True:
            for task_id in self._pending_cancellations():
                self._request_cancel(task_id)
            self._apply_outcomes()
            self._propagate_blocks()
            self._promote_waiters()
            if self._dispatch():
                continue
            if not self._progress_possible():
                break
            await self._wait()

        # Defensive: no task may silently remain waiting when progress stopped.
        for state in self.states.values():
            if state.status == WAITING:
                self._block(state, "dependency_unreachable")
        self._emit_usage()
        summary = {
            "scheduled": self._counts["scheduled"],
            "completed": self._counts["completed"],
            "failed": self._counts["failed"],
            "retries": self._counts["retries"],
        }
        if self._counts["blocked"]:
            summary["blocked"] = self._counts["blocked"]
        if self._counts["cancelled"]:
            summary["cancelled"] = self._counts["cancelled"]
        return summary

    # -- events ---------------------------------------------------------
    def _elapsed(self) -> float:
        return self.clock.now() - self._origin

    def _emit(self, kind: str, state: TaskState, **extra: Any) -> None:
        event = {
            "kind": kind,
            "task_id": state.spec.task_id,
            "status": state.status,
            "attempts": state.attempts,
            "attempt": state.attempts,
            "mode": state.spec.mode,
            "resource_group": state.spec.resource_group,
            "blocked_reason": state.blocked_reason,
            "at": self._elapsed(),
            **extra,
        }
        self.trace.append(event)
        self.report(event)
        self.report({"kind": "usage", "at": self._elapsed(), **self.governor.snapshot()})

    def _emit_usage(self) -> None:
        self.report({"kind": "usage", "at": self._elapsed(), **self.governor.snapshot()})

    # -- dependency / readiness ------------------------------------------
    def _dependencies_ok(self, task_id: str) -> bool:
        specs = self.graph.specs[task_id]
        return all(self.states[dependency].status == COMPLETED for dependency in specs.requires)

    def _promote_waiters(self) -> None:
        now = self._elapsed()
        for task_id, state in self.states.items():
            if task_id in self._ready:
                continue
            if state.status == WAITING and self._dependencies_ok(task_id):
                state.status = READY
                state.ready_since = now
                state.blocked_reason = None
                self._ready.add(task_id)
                self._emit("ready", state)
            elif state.status == BACKOFF and state.retry_ready:
                state.retry_ready = False
                state.status = READY
                state.ready_since = now
                state.blocked_reason = None
                self._ready.add(task_id)
                self._emit("retry_ready", state)

    def _bad_reason(self, dependency: str) -> str | None:
        state = self.states[dependency]
        if state.status == FAILED:
            return f"dependency_failed:{dependency}"
        if state.status == CANCELLED:
            return f"dependency_cancelled:{dependency}"
        if state.status == BLOCKED:
            # Keep the root cause visible for descendants of cyclic/missing
            # tasks; otherwise point at the nearest blocked predecessor.
            root = state.blocked_reason or ""
            if root.startswith("dependency_missing:"):
                return root
            if root == "dependency_cycle":
                return "dependency_cycle"
            return f"dependency_blocked:{dependency}"
        return None

    def _propagate_blocks(self) -> None:
        changed = True
        while changed:
            changed = False
            for task_id, state in self.states.items():
                if state.status not in (WAITING, READY, BACKOFF):
                    continue
                for dependency in self.graph.specs[task_id].requires:
                    reason = self._bad_reason(dependency)
                    if reason is not None:
                        self._block(state, reason)
                        changed = True
                        break

    def _block(self, state: TaskState, reason: str) -> None:
        if state.status in TERMINAL:
            return
        self._ready.discard(state.spec.task_id)
        if state.action_handle is not None:
            state.action_handle.cancel()
            state.action_handle = None
        state.status = BLOCKED
        state.blocked_reason = reason
        self._counts["blocked"] += 1
        self._emit("blocked", state, reason=reason)

    # -- dispatch --------------------------------------------------------
    def _dispatch(self) -> int:
        if not self._ready:
            return 0
        now = self._elapsed()
        candidates = [self.states[task_id] for task_id in self._ready]
        started = 0
        for state in self.priority.rank_now(candidates, now):
            if not self.governor.can_start(state.spec.resource_group):
                continue
            self._ready.discard(state.spec.task_id)
            self._start_attempt(state)
            started += 1
        return started

    def _start_attempt(self, state: TaskState) -> None:
        loop = asyncio.get_running_loop()
        self.governor.acquire(state.spec.resource_group)
        state.attempts += 1
        if state.attempts > 1:
            self._counts["retries"] += 1
        state.status = RUNNING
        state.pending_outcome = None
        state.attempt_cancelled = False
        state.retry_ready = False
        state.blocked_reason = None
        if state.started_at is None:
            state.started_at = self._elapsed()
        self.start_order.append(state.spec.task_id)
        self._emit("started", state, attempt=state.attempts)
        future = loop.create_future()
        state.action_future = future
        state.action_handle = loop.create_task(self._run_attempt(state, future))

    async def _run_attempt(self, state: TaskState, done: asyncio.Future) -> None:
        outcome: AttemptResult | None = None
        cancelled = False
        try:
            try:
                result = self._invoke(state)
                if inspect.isawaitable(result):
                    result = await result
                outcome = AttemptResult.from_any(result)
            except asyncio.CancelledError:
                cancelled = True
            except Exception as exc:  # an executor error is a failed attempt
                outcome = AttemptResult(False, f"{type(exc).__name__}: {exc}")
        finally:
            self.governor.release(state.spec.resource_group)
            state.action_handle = None

        # Publish the outcome *before* waking the scheduler so the event loop
        # cannot observe a finished attempt it has no result for.
        state.attempt_cancelled = cancelled
        state.pending_outcome = outcome
        detail = "" if outcome is None else outcome.detail
        try:
            self._emit(
                "attempt_finished",
                state,
                attempt=state.attempts,
                succeeded=outcome is not None and outcome.succeeded,
                cancelled=cancelled,
                detail=detail,
            )
        finally:
            if not done.done():
                done.set_result(None)

    def _invoke(self, state: TaskState) -> Any:
        if self._executor_arity >= 2:
            return self.executor(state.spec, state.attempts)
        return self.executor(state.spec)

    def _apply_outcomes(self) -> None:
        for state in self.states.values():
            if state.status != RUNNING:
                continue
            handle = state.action_handle
            if handle is not None and not handle.done():
                # The attempt coroutine is still unwinding; its slot is
                # released in its own ``finally`` and the outcome published
                # before the wait future resolves.
                continue
            if state.cancel_requested or state.attempt_cancelled:
                self._finish_cancel(state)
                continue
            outcome = state.pending_outcome
            if outcome is None:
                continue
            if outcome.succeeded:
                self._complete(state)
                continue
            if state.attempts >= self.retry.attempts_allowed(state.spec):
                self._fail(state)
            else:
                self._schedule_backoff(state)

    def _complete(self, state: TaskState) -> None:
        state.status = COMPLETED
        state.finished_at = self._elapsed()
        state.blocked_reason = None
        self._counts["completed"] += 1
        self._emit("completed", state)

    def _fail(self, state: TaskState) -> None:
        state.status = FAILED
        state.finished_at = self._elapsed()
        state.blocked_reason = None
        self._counts["failed"] += 1
        self._emit("failed", state)

    def _schedule_backoff(self, state: TaskState) -> None:
        delay = self.retry.backoff_seconds(state.spec, self.config, state.attempts)
        state.status = BACKOFF
        state.retry_ready = False
        self._emit("backoff", state, attempt=state.attempts, delay=delay, wake_at=self._elapsed() + delay)
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        state.action_future = future
        state.action_handle = loop.create_task(self._wait_backoff(state, delay, future))

    async def _wait_backoff(self, state: TaskState, delay: float, done: asyncio.Future) -> None:
        cancelled = False
        try:
            await self.clock.sleep(delay)
        except asyncio.CancelledError:
            cancelled = True
        if not cancelled and not state.cancel_requested and state.status == BACKOFF:
            # Publish readiness *before* resolving the wait future: the
            # scheduler may be about to jump virtual time to a later sleeper,
            # and it must observe the woken task on this turn.
            state.retry_ready = True
        state.action_handle = None
        if not done.done():
            done.set_result(None)

    # -- cancellation ----------------------------------------------------
    def _requested_cancellations(self) -> set[str]:
        requested = {task_id for task_id, at in self._cancellations.items() if self._elapsed() >= at}
        external = self._external_cancel() if callable(self._external_cancel) else self._external_cancel
        requested.update(str(task_id) for task_id in external)
        return requested

    def _pending_cancellations(self) -> set[str]:
        return {
            task_id
            for task_id in self._requested_cancellations()
            if task_id in self.states
            and task_id not in self._cancelled
            and self.states[task_id].status not in TERMINAL
        }

    def _next_cancel_at(self) -> float | None:
        pending = [
            at
            for task_id, at in self._cancellations.items()
            if task_id in self.states
            and task_id not in self._cancelled
            and self.states[task_id].status not in TERMINAL
        ]
        return min(pending) if pending else None

    def _request_cancel(self, task_id: str) -> None:
        state = self.states.get(task_id)
        if state is None or task_id in self._cancelled or state.status in TERMINAL:
            return
        self._cancelled.add(task_id)
        state.cancel_requested = True
        self._emit("cancel_requested", state)
        if state.status == RUNNING:
            if state.action_handle is not None and not state.action_handle.done():
                state.action_handle.cancel()
            return
        if state.status == BACKOFF:
            if state.action_handle is not None and not state.action_handle.done():
                state.action_handle.cancel()
            state.retry_ready = False
        if state.status == READY:
            self._ready.discard(task_id)
        self._finish_cancel(state)

    def _finish_cancel(self, state: TaskState) -> None:
        if state.status in TERMINAL:
            return
        self._ready.discard(state.spec.task_id)
        state.status = CANCELLED
        state.finished_at = self._elapsed()
        self._counts["cancelled"] += 1
        self._emit("cancelled", state)

    # -- waiting ---------------------------------------------------------
    def _progress_possible(self) -> bool:
        if any(state.status in (RUNNING, BACKOFF) for state in self.states.values()):
            return True
        if self._ready:
            return True
        return self._next_cancel_at() is not None

    async def _wait(self) -> None:
        futures = [
            state.action_future
            for state in self.states.values()
            if state.status in (RUNNING, BACKOFF) and state.action_future is not None and not state.action_future.done()
        ]
        deadline = self._next_cancel_at()
        deadline_abs = None if deadline is None else self._origin + deadline
        if not futures and deadline_abs is None:
            await asyncio.sleep(0)
            return
        await self.clock.wait_for(futures, deadline_abs)


def _arity(fn: Any) -> int:
    """Count positional parameters accepted by an executor callable."""
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):  # some builtins / callables expose no signature
        return 2
    positional = 0
    for parameter in signature.parameters.values():
        if parameter.kind in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD):
            positional += 1
        if parameter.kind == parameter.VAR_POSITIONAL:
            return 2
    return positional


def build_scheduler(
    tasks: Iterable[Any],
    config: RunConfig,
    executor: ExecutorFn,
    clock: Clock,
    *,
    max_retries: int = 2,
    report: Reporter | None = None,
    cancel: Iterable[str] | Callable[[], Iterable[str]] | None = None,
    priority: PriorityScheduler | None = None,
) -> CollectionScheduler:
    return CollectionScheduler(
        tasks,
        config,
        executor,
        clock,
        max_retries=max_retries,
        report=report,
        cancel=cancel,
        priority=priority,
    )

"""Shared value objects for task definitions, run configuration and results."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class TaskSpec:
    """Normalised task definition.

    ``requires`` and ``resource_group`` are optional, so fixtures written
    against the original schema (``task_id``/``priority``/``mode``/``fail_until``)
    keep working unchanged.
    """

    task_id: str
    priority: int = 0
    mode: str = "incremental"
    fail_until: int = 0
    requires: tuple[str, ...] = ()
    resource_group: str = "default"
    retry_backoff: float | None = None
    duration: float = 0.0

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "TaskSpec":
        if not isinstance(raw, Mapping):
            raise ValueError("task definition must be an object")
        task_id = raw.get("task_id")
        if not isinstance(task_id, str) or not task_id:
            raise ValueError("task definition requires a non-empty string task_id")

        requires = raw.get("requires", ())
        if isinstance(requires, str):
            raise ValueError(f"requires for {task_id!r} must be a list of task ids")
        if not isinstance(requires, (list, tuple)):
            raise ValueError(f"requires for {task_id!r} must be a list of task ids")
        if any(not isinstance(item, str) or not item for item in requires):
            raise ValueError(f"requires for {task_id!r} contains an invalid task id")
        if len(set(requires)) != len(requires):
            raise ValueError(f"requires for {task_id!r} contains duplicate entries")

        group = raw.get("resource_group", "default")
        if not isinstance(group, str) or not group:
            raise ValueError(f"resource_group for {task_id!r} must be a non-empty string")

        retry_backoff = raw.get("retry_backoff")
        if retry_backoff is not None:
            retry_backoff = float(retry_backoff)
            if retry_backoff < 0:
                raise ValueError(f"retry_backoff for {task_id!r} must be non-negative")

        duration = float(raw.get("duration", 0.0))
        if duration < 0:
            raise ValueError(f"duration for {task_id!r} must be non-negative")

        return cls(
            task_id=task_id,
            priority=int(raw.get("priority", 0)),
            mode=str(raw.get("mode", "incremental")),
            fail_until=int(raw.get("fail_until", 0)),
            requires=tuple(requires),
            resource_group=group,
            retry_backoff=retry_backoff,
            duration=duration,
        )

    @classmethod
    def from_any(cls, raw: "TaskSpec | Mapping[str, Any]") -> "TaskSpec":
        return raw if isinstance(raw, TaskSpec) else cls.from_dict(raw)


@dataclass(frozen=True)
class RunConfig:
    """Run-wide scheduling limits.

    ``max_parallel`` is the total execution-slot budget.  ``group_limits``
    caps a source/resource group; groups not mentioned use
    ``default_group_limit`` (falling back to ``max_parallel``).  A group limit
    is never allowed to exceed the total budget.
    """

    max_parallel: int = 4
    group_limits: Mapping[str, int] = field(default_factory=dict)
    default_group_limit: int | None = None
    retry_backoff: float = 0.0
    backoff_multiplier: float = 1.0
    aging_step: float = 1.0
    cancellations: Mapping[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.max_parallel, int) or self.max_parallel < 1:
            raise ValueError("max_parallel must be a positive integer")
        limits = dict(self.group_limits)
        for group, limit in limits.items():
            if not isinstance(group, str) or not group:
                raise ValueError("group_limits keys must be non-empty strings")
            if not isinstance(limit, int) or limit < 1:
                raise ValueError(f"group limit for {group!r} must be a positive integer")
        if self.default_group_limit is not None:
            if not isinstance(self.default_group_limit, int) or self.default_group_limit < 1:
                raise ValueError("default_group_limit must be a positive integer or None")
        if self.retry_backoff < 0:
            raise ValueError("retry_backoff must be non-negative")
        if self.backoff_multiplier < 1:
            raise ValueError("backoff_multiplier must be at least 1")
        if self.aging_step < 0:
            raise ValueError("aging_step must be non-negative")
        cancel_map = dict(self.cancellations)
        for task_id, at in cancel_map.items():
            if not isinstance(task_id, str) or not task_id:
                raise ValueError("cancellations keys must be non-empty task ids")
            if float(at) < 0:
                raise ValueError(f"cancellation time for {task_id!r} must be non-negative")
        object.__setattr__(self, "group_limits", limits)
        object.__setattr__(self, "cancellations", {str(k): float(v) for k, v in cancel_map.items()})

    def group_limit(self, group: str) -> int:
        raw = self.group_limits.get(group, self.default_group_limit)
        if raw is None:
            raw = self.max_parallel
        return max(0, min(int(raw), self.max_parallel))

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "RunConfig":
        if not isinstance(raw, Mapping):
            raise ValueError("run configuration must be an object")
        allowed = {
            "max_parallel",
            "group_limits",
            "default_group_limit",
            "retry_backoff",
            "backoff_multiplier",
            "aging_step",
            "cancellations",
        }
        unknown = set(raw) - allowed
        if unknown:
            raise ValueError(f"unknown run configuration keys: {sorted(unknown)}")
        limits = raw.get("group_limits", {})
        if not isinstance(limits, Mapping):
            raise ValueError("group_limits must be an object")
        cancellations = raw.get("cancellations", {})
        if not isinstance(cancellations, Mapping):
            raise ValueError("cancellations must be an object of task_id -> seconds")
        return cls(
            max_parallel=int(raw.get("max_parallel", 4)),
            group_limits={str(key): int(value) for key, value in limits.items()},
            default_group_limit=(
                None if raw.get("default_group_limit") is None else int(raw["default_group_limit"])
            ),
            retry_backoff=float(raw.get("retry_backoff", 0.0)),
            backoff_multiplier=float(raw.get("backoff_multiplier", 1.0)),
            aging_step=float(raw.get("aging_step", 1.0)),
            cancellations={str(key): float(value) for key, value in cancellations.items()},
        )


@dataclass(frozen=True)
class TaskRun:
    """Persisted terminal (or latest known) state of one task."""

    task_id: str
    status: str
    attempts: int
    mode: str
    resource_group: str = "default"
    blocked_reason: str | None = None
    started_at: float | None = None
    finished_at: float | None = None


@dataclass(frozen=True)
class AttemptResult:
    """Outcome of one execution attempt returned by an async executor."""

    succeeded: bool
    detail: str = ""

    @classmethod
    def from_any(cls, value: Any) -> "AttemptResult":
        if isinstance(value, cls):
            return value
        if isinstance(value, bool):
            return cls(value)
        if value is None:
            return cls(True)
        raise TypeError("executor must return bool, AttemptResult or None")

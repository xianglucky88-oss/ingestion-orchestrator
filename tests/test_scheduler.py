import asyncio
import sqlite3
from pathlib import Path

import pytest

from orchestrator.application import run_fixture
from orchestrator.clock import ManualClock
from orchestrator.executor import LocalExecutor
from orchestrator.models import RunConfig, TaskSpec
from orchestrator.pipeline import OrchestrationPipeline, load_config
from orchestrator.scheduler import CollectionScheduler, GraphError, PriorityScheduler, RetryExecutor
from orchestrator.sources import TaskSource
from orchestrator.storage import RunStore

ROOT = Path(__file__).parents[1]


def run_virtual(tasks, config, store=None, max_retries=2):
    clock = ManualClock()
    executor = LocalExecutor(clock)
    scheduler = CollectionScheduler(tasks, config, executor, clock, max_retries=max_retries,
                                    report=(store.apply_event if store else None))
    if store is not None:
        store.start_run(config, clock.now())
    summary = asyncio.run(scheduler.run())
    return scheduler, executor, summary


# ---------------------------------------------------------------- legacy API
def test_source_scheduler_and_retry_policy():
    tasks = TaskSource().read(str(ROOT / "fixtures/tasks.json"))
    assert [task["task_id"] for task in PriorityScheduler().order(tasks)] == ["telemetry-1", "catalog-1", "rpa-1"]
    assert RetryExecutor(2).execute(tasks[0]).attempts == 2


def test_pipeline_persists_core_scheduling_result(tmp_path):
    root = ROOT
    result = run_fixture(str(root / "fixtures/tasks.json"), str(tmp_path / "runs.sqlite3"))
    assert result == {"scheduled": 3, "completed": 3, "failed": 0, "retries": 3}
    with sqlite3.connect(tmp_path / "runs.sqlite3") as conn:
        assert conn.execute("SELECT COUNT(*) FROM task_runs").fetchone()[0] == 3


def test_legacy_fixture_without_new_fields_still_runs(tmp_path):
    """Fixtures with only task_id/priority/mode/fail_until keep working."""
    fixture = tmp_path / "legacy.json"
    fixture.write_text('[{"task_id": "a", "priority": 3, "fail_until": 1}]', encoding="utf-8")
    result = run_fixture(str(fixture), str(tmp_path / "legacy.sqlite3"))
    assert result == {"scheduled": 1, "completed": 1, "failed": 0, "retries": 1}
    with sqlite3.connect(tmp_path / "legacy.sqlite3") as conn:
        row = conn.execute("SELECT status, attempts, resource_group, blocked_reason FROM task_runs").fetchone()
    assert row == ("completed", 2, "default", None)


def test_old_database_schema_is_migrated(tmp_path):
    database = tmp_path / "old.sqlite3"
    conn = sqlite3.connect(database)
    conn.execute("CREATE TABLE task_runs(task_id TEXT PRIMARY KEY, status TEXT, attempts INTEGER, mode TEXT)")
    conn.execute("CREATE TABLE task_attempts(task_id TEXT, attempt INTEGER, status TEXT, mode TEXT, "
                 "PRIMARY KEY(task_id, attempt))")
    conn.execute("INSERT INTO task_runs VALUES ('old', 'completed', 1, 'full')")
    conn.commit()
    conn.close()
    store = RunStore(str(database))
    try:
        columns = {row["name"] for row in store.conn.execute("PRAGMA table_info(task_runs)")}
        assert {"resource_group", "blocked_reason", "started_at", "finished_at"} <= columns
        assert store.task_run("old")["status"] == "completed"
    finally:
        store.close()


# ------------------------------------------------------- task definition / config
def test_task_spec_defaults_and_validation():
    spec = TaskSpec.from_dict({"task_id": "t"})
    assert spec.requires == () and spec.resource_group == "default" and spec.fail_until == 0
    with pytest.raises(ValueError):
        TaskSpec.from_dict({"task_id": "t", "requires": "not-a-list"})
    with pytest.raises(ValueError):
        TaskSpec.from_dict({"requires": ["x"]})


def test_run_config_parsing_limits_and_validation():
    config = RunConfig.from_dict({
        "max_parallel": 3,
        "group_limits": {"catalog": 1},
        "retry_backoff": 2,
        "backoff_multiplier": 2,
        "aging_step": 1,
        "cancellations": {"slow": 4},
    })
    assert config.group_limit("catalog") == 1
    assert config.group_limit("unknown") == 3
    assert config.group_limit("catalog") <= config.max_parallel
    with pytest.raises(ValueError):
        RunConfig(max_parallel=0)
    with pytest.raises(ValueError):
        RunConfig(max_parallel=2, group_limits={"g": 0})
    with pytest.raises(ValueError):
        RunConfig.from_dict({"max_parallel": 2, "mystery": 1})


def test_group_limit_never_exceeds_total_budget():
    config = RunConfig(max_parallel=2, group_limits={"g": 10})
    assert config.group_limit("g") == 2


# ------------------------------------------------------------ dependency graph
def test_cycle_is_reported_and_not_scheduled():
    store = RunStore(":memory:")
    scheduler, executor, summary = run_virtual(
        [
            {"task_id": "a", "requires": ["b"], "duration": 1},
            {"task_id": "b", "requires": ["a"], "duration": 1},
            {"task_id": "after", "requires": ["a"], "duration": 1},
        ],
        RunConfig(max_parallel=1),
        store=store,
    )
    assert executor.windows == []
    assert summary["blocked"] == 3
    assert store.blocked_reason("a") == "dependency_cycle"
    assert store.blocked_reason("b") == "dependency_cycle"
    assert store.blocked_reason("after") == "dependency_cycle"


def test_missing_reference_is_reported_transitively():
    store = RunStore(":memory:")
    scheduler, executor, summary = run_virtual(
        [
            {"task_id": "broken", "requires": ["ghost"], "duration": 1},
            {"task_id": "child", "requires": ["broken"], "duration": 1},
        ],
        RunConfig(max_parallel=1),
        store=store,
    )
    assert summary["blocked"] == 2
    assert store.blocked_reason("broken") == "dependency_missing:ghost"
    assert store.blocked_reason("child") == "dependency_missing:ghost"


def test_duplicate_task_ids_are_rejected():
    with pytest.raises(GraphError):
        CollectionScheduler(
            [{"task_id": "dup"}, {"task_id": "dup"}],
            RunConfig(),
            lambda spec, attempt: True,
            ManualClock(),
        )


# -------------------------------------------------------- dependencies & retries
def test_dependent_waits_for_successful_prerequisite():
    clock = ManualClock()
    scheduler = CollectionScheduler(
        [
            {"task_id": "full", "priority": 1, "duration": 4},
            {"task_id": "incremental", "priority": 99, "duration": 1, "requires": ["full"]},
        ],
        RunConfig(max_parallel=4),
        LocalExecutor(clock),
        clock,
    )
    summary = asyncio.run(scheduler.run())
    assert summary["completed"] == 2
    # Priority 99 cannot overtake the prerequisite.
    assert scheduler.start_order == ["full", "incremental"]


def test_predecessor_retries_while_dependent_waits():
    clock = ManualClock()
    store = RunStore(":memory:")
    scheduler = CollectionScheduler(
        [
            {"task_id": "root", "priority": 10, "duration": 1, "fail_until": 2, "retry_backoff": 1},
            {"task_id": "child", "priority": 9, "duration": 1, "requires": ["root"]},
        ],
        RunConfig(max_parallel=2),
        LocalExecutor(clock),
        clock,
        max_retries=2,
        report=store.apply_event,
    )
    store.start_run(RunConfig(), clock.now())
    summary = asyncio.run(scheduler.run())
    assert summary == {"scheduled": 2, "completed": 2, "failed": 0, "retries": 2}
    root_started = [e["at"] for e in store.events("root", kinds=["started"])]
    child_started = [e["at"] for e in store.events("child", kinds=["started"])]
    assert root_started == [0.0, 2.0, 4.0]
    assert child_started == [5.0]
    assert [row["status"] for row in store.attempts("root")] == ["failed", "failed", "succeeded"]


def test_exhausted_retries_block_dependents_with_reason():
    clock = ManualClock()
    store = RunStore(":memory:")
    scheduler = CollectionScheduler(
        [
            {"task_id": "root", "duration": 1, "fail_until": 99, "retry_backoff": 1},
            {"task_id": "child", "duration": 1, "requires": ["root"]},
            {"task_id": "grandchild", "duration": 1, "requires": ["child"]},
            {"task_id": "unrelated", "priority": 0, "duration": 1},
        ],
        RunConfig(max_parallel=2),
        LocalExecutor(clock),
        clock,
        max_retries=1,
        report=store.apply_event,
    )
    store.start_run(RunConfig(), clock.now())
    summary = asyncio.run(scheduler.run())
    assert summary["failed"] == 1
    assert summary["blocked"] == 2
    assert summary["completed"] == 1
    assert store.blocked_reason("child") == "dependency_failed:root"
    assert store.blocked_reason("grandchild") == "dependency_blocked:child"
    assert all(event["kind"] == "blocked" for event in store.events("child", kinds=["blocked"]))
    assert store.task_run("unrelated")["status"] == "completed"


def test_no_task_is_left_waiting_silently():
    store = RunStore(":memory:")
    scheduler, _, summary = run_virtual(
        [
            {"task_id": "root", "duration": 1, "fail_until": 99},
            {"task_id": "dep", "duration": 1, "requires": ["root"]},
        ],
        RunConfig(max_parallel=1),
        store=store,
        max_retries=0,
    )
    assert summary.get("blocked") == 1
    assert store.task_run("dep")["status"] == "blocked"
    assert store.blocked_reason("dep") == "dependency_failed:root"


# ------------------------------------------------------------- resource limits
def _overlap_windows(windows):
    for left, right in zip(windows, windows[1:]):
        start = max(left.started_at, right.started_at)
        end = min(left.finished_at, right.finished_at)
        if end > start:
            yield left, right


def test_total_and_group_limits_are_never_exceeded(tmp_path):
    store = RunStore(str(tmp_path / "limits.sqlite3"))
    clock = ManualClock()
    executor = LocalExecutor(clock)
    tasks = [
        {"task_id": "cat-a", "priority": 9, "duration": 5, "resource_group": "catalog"},
        {"task_id": "cat-b", "priority": 8, "duration": 2, "resource_group": "catalog"},
        {"task_id": "tel-a", "priority": 7, "duration": 3, "resource_group": "telemetry", "fail_until": 1, "retry_backoff": 2},
        {"task_id": "tel-b", "priority": 6, "duration": 1, "resource_group": "telemetry"},
        {"task_id": "merge", "priority": 5, "duration": 2, "requires": ["cat-a", "tel-a"], "resource_group": "merge"},
    ]
    config = RunConfig(max_parallel=3, group_limits={"catalog": 1, "telemetry": 2, "merge": 1}, retry_backoff=1)
    scheduler = CollectionScheduler(tasks, config, executor, clock, report=store.apply_event)
    store.start_run(config, clock.now())
    summary = asyncio.run(scheduler.run())
    assert summary["completed"] == 5
    assert store.usage_high_watermark()[0] <= config.max_parallel
    for group, count in store.usage_high_watermark()[1].items():
        assert count <= config.group_limit(group)
    # Independent verification from the executor's own execution windows.
    events = [(w.started_at, 1) for w in executor.windows] + [(w.finished_at, -1) for w in executor.windows]
    current = peak = 0
    for _, delta in sorted(events, key=lambda item: (item[0], item[1])):
        current += delta
        peak = max(peak, current)
    assert peak <= config.max_parallel
    catalog_peak = _peak(executor.windows, "catalog")
    assert catalog_peak <= config.group_limit("catalog")
    telemetry_peak = _peak(executor.windows, "telemetry")
    assert telemetry_peak <= config.group_limit("telemetry")


def _peak(windows, group):
    selected = [w for w in windows if group is None or w.resource_group == group]
    events = [(w.started_at, 1) for w in selected] + [(w.finished_at, -1) for w in selected]
    current = peak = 0
    for _, delta in sorted(events, key=lambda item: (item[0], item[1])):
        current += delta
        peak = max(peak, current)
    return peak


def test_backoff_does_not_hold_an_execution_slot():
    clock = ManualClock()
    executor = LocalExecutor(clock)
    tasks = [
        {"task_id": "flaky", "priority": 10, "duration": 1, "fail_until": 1,
         "retry_backoff": 5, "resource_group": "source-a"},
        {"task_id": "steady", "priority": 1, "duration": 1, "resource_group": "source-a"},
        {"task_id": "elsewhere", "priority": 0, "duration": 2, "resource_group": "source-b"},
    ]
    config = RunConfig(max_parallel=2, group_limits={"source-a": 1, "source-b": 1})
    scheduler = CollectionScheduler(tasks, config, executor, clock)
    summary = asyncio.run(scheduler.run())
    assert summary["completed"] == 3
    assert scheduler.start_order == ["flaky", "elsewhere", "steady", "flaky"]
    assert scheduler.governor.max_total_seen <= 2
    backoff = [e for e in scheduler.trace if e["kind"] == "backoff"][0]
    assert backoff["at"] == 1.0


# ------------------------------------------------------------------ fairness
def test_priority_wins_between_simultaneously_ready_tasks():
    clock = ManualClock()
    executor = LocalExecutor(clock)
    tasks = [
        {"task_id": "low", "priority": 1, "duration": 1},
        {"task_id": "high", "priority": 9, "duration": 1},
        {"task_id": "mid", "priority": 5, "duration": 1},
    ]
    scheduler = CollectionScheduler(tasks, RunConfig(max_parallel=1, aging_step=0), executor, clock)
    asyncio.run(scheduler.run())
    assert scheduler.start_order == ["high", "mid", "low"]


def test_ageing_prevents_starvation_within_group():
    clock = ManualClock()
    executor = LocalExecutor(clock)
    tasks = [
        {"task_id": "long", "priority": 10, "duration": 5, "resource_group": "g"},
        {"task_id": "starved", "priority": 0, "duration": 1, "resource_group": "g"},
        {"task_id": "newcomer", "priority": 3, "duration": 1, "resource_group": "g", "requires": ["long"]},
    ]
    scheduler = CollectionScheduler(tasks, RunConfig(max_parallel=1, aging_step=1.0), executor, clock)
    asyncio.run(scheduler.run())
    # ``starved`` has waited since t=0, so at t=5 its effective priority is
    # 5 versus 3 for the newcomer that only became ready at t=5.
    assert scheduler.start_order == ["long", "starved", "newcomer"]


def test_zero_ageing_gives_strict_priority():
    clock = ManualClock()
    executor = LocalExecutor(clock)
    tasks = [
        {"task_id": "long", "priority": 10, "duration": 5, "resource_group": "g"},
        {"task_id": "starved", "priority": 0, "duration": 1, "resource_group": "g"},
        {"task_id": "newcomer", "priority": 3, "duration": 1, "resource_group": "g", "requires": ["long"]},
    ]
    scheduler = CollectionScheduler(tasks, RunConfig(max_parallel=1, aging_step=0.0), executor, clock)
    asyncio.run(scheduler.run())
    assert scheduler.start_order == ["long", "newcomer", "starved"]


# ---------------------------------------------------------------- cancellation
def test_cancel_ready_task_blocks_dependents_of_its_count():
    clock = ManualClock()
    store = RunStore(":memory:")
    tasks = [
        {"task_id": "a", "duration": 3, "requires": []},
        {"task_id": "b", "duration": 1, "requires": ["a"]},
    ]
    scheduler = CollectionScheduler(tasks, RunConfig(max_parallel=2, cancellations={"a": 1}), LocalExecutor(clock), clock,
                                    report=store.apply_event)
    store.start_run(RunConfig(), clock.now())
    summary = asyncio.run(scheduler.run())
    assert summary.get("cancelled") == 1
    assert summary.get("blocked") == 1
    assert store.blocked_reason("b") == "dependency_cancelled:a"


def test_cancel_during_backoff_releases_the_task_permanently(tmp_path):
    store = RunStore(str(tmp_path / "cancel.sqlite3"))
    clock = ManualClock()
    executor = LocalExecutor(clock)
    tasks = [
        {"task_id": "slow", "priority": 5, "duration": 4, "fail_until": 1,
         "retry_backoff": 3, "resource_group": "g"},
        {"task_id": "peer", "priority": 3, "duration": 1, "resource_group": "g"},
    ]
    scheduler = CollectionScheduler(tasks, RunConfig(max_parallel=2, group_limits={"g": 1},
                                                    cancellations={"slow": 6}), executor, clock,
                                    report=store.apply_event)
    store.start_run(RunConfig(), clock.now())
    summary = asyncio.run(scheduler.run())
    assert summary["completed"] == 1
    assert summary["cancelled"] == 1
    # The peer ran immediately after the slow task released the slot, and the
    # cancelled task never re-appeared in back-off.
    starts = [e["at"] for e in store.events("peer", kinds=["started"])]
    assert starts == [4.0]
    assert store.task_run("slow")["attempts"] == 1
    assert store.task_run("slow")["status"] == "cancelled"


def test_cancellation_beats_dependency_completion_in_same_round():
    clock = ManualClock()
    store = RunStore(":memory:")
    tasks = [
        {"task_id": "root", "priority": 9, "duration": 5, "resource_group": "a"},
        {"task_id": "dep", "priority": 5, "duration": 1, "requires": ["root"], "resource_group": "b"},
        {"task_id": "dep-kid", "priority": 4, "duration": 1, "requires": ["dep"], "resource_group": "b"},
    ]
    config = RunConfig(max_parallel=2, group_limits={"a": 1, "b": 1}, cancellations={"dep": 5})
    scheduler = CollectionScheduler(tasks, config, LocalExecutor(clock), clock, report=store.apply_event)
    store.start_run(config, clock.now())
    asyncio.run(scheduler.run())
    assert scheduler.start_order == ["root"]
    assert store.task_run("dep")["status"] == "cancelled"
    assert store.blocked_reason("dep-kid") == "dependency_cancelled:dep"
    dep_events = store.events("dep")
    kinds = [e["kind"] for e in dep_events]
    assert "started" not in kinds
    assert kinds.index("cancel_requested") < kinds.index("cancelled")


def test_cancel_running_task_records_cancelled_attempt():
    clock = ManualClock()
    store = RunStore(":memory:")
    tasks = [{"task_id": "running", "duration": 10}]
    scheduler = CollectionScheduler(tasks, RunConfig(max_parallel=1, cancellations={"running": 2}),
                                    LocalExecutor(clock), clock, report=store.apply_event)
    store.start_run(RunConfig(), clock.now())
    summary = asyncio.run(scheduler.run())
    assert summary["cancelled"] == 1
    rows = store.attempts("running")
    assert rows and rows[0]["status"] == "cancelled"
    assert store.task_run("running")["status"] == "cancelled"


# ------------------------------------------------------------ persisted trace
def test_attempt_table_matches_execution_windows(tmp_path):
    store = RunStore(str(tmp_path / "attempts.sqlite3"))
    clock = ManualClock()
    executor = LocalExecutor(clock)
    tasks = [
        {"task_id": "a", "duration": 2, "fail_until": 1, "retry_backoff": 1, "resource_group": "g1"},
        {"task_id": "b", "duration": 1, "requires": ["a"], "resource_group": "g2"},
    ]
    config = RunConfig(max_parallel=2, group_limits={"g1": 1, "g2": 1})
    scheduler = CollectionScheduler(tasks, config, executor, clock, report=store.apply_event)
    store.start_run(config, clock.now())
    asyncio.run(scheduler.run())
    for window in executor.windows:
        rows = [row for row in store.attempts(window.task_id) if row["attempt"] == window.attempt]
        assert len(rows) == 1
        row = rows[0]
        assert row["resource_group"] == window.resource_group
        assert row["started_at"] == window.started_at
        assert row["finished_at"] == window.finished_at
        assert row["status"] == ("succeeded" if window.succeeded else "failed")
    started_events = store.events(kinds=["started"])
    assert [e["at"] for e in started_events] == [0.0, 3.0, 5.0]


def test_usage_high_watermark_matches_event_derived_peak(tmp_path):
    store = RunStore(str(tmp_path / "usage.sqlite3"))
    clock = ManualClock()
    executor = LocalExecutor(clock)
    tasks = [
        {"task_id": "g1-a", "priority": 5, "duration": 4, "resource_group": "g1"},
        {"task_id": "g1-b", "priority": 4, "duration": 2, "resource_group": "g1"},
        {"task_id": "g2-a", "priority": 3, "duration": 3, "resource_group": "g2"},
        {"task_id": "g2-b", "priority": 2, "duration": 1, "resource_group": "g2", "requires": ["g1-a"]},
    ]
    config = RunConfig(max_parallel=3, group_limits={"g1": 0 + 1, "g2": 1})
    scheduler = CollectionScheduler(tasks, config, executor, clock, report=store.apply_event)
    store.start_run(config, clock.now())
    asyncio.run(scheduler.run())
    total, groups = store.usage_high_watermark()
    assert total == _peak(executor.windows, None)
    for group in ("g1", "g2"):
        assert groups.get(group, 0) == _peak(executor.windows, group)
    assert total <= config.max_parallel


# ------------------------------------------------------------ full scenario
def test_dynamic_fixture_end_to_end(tmp_path):
    store = RunStore(str(tmp_path / "dynamic.sqlite3"))
    config = load_config(ROOT / "fixtures/run_config.json")
    clock = ManualClock()
    executor = LocalExecutor(clock)
    pipeline = OrchestrationPipeline(
        TaskSource().read(str(ROOT / "fixtures/dynamic_tasks.json")),
        config,
        store=store,
        clock=clock,
        executor=executor,
    )
    summary = pipeline.run()
    assert summary == {
        "scheduled": 10, "completed": 6, "failed": 1, "retries": 3, "blocked": 2, "cancelled": 1,
    }
    assert store.task_run("legacy-report")["blocked_reason"] == "dependency_failed:legacy-extract"
    assert store.task_run("legacy-report")["attempts"] == 0
    assert store.task_run("downstream-report")["blocked_reason"] == "dependency_cancelled:report"
    assert store.task_run("report")["status"] == "cancelled"
    assert store.task_run("merged")["status"] == "completed"
    total, groups = store.usage_high_watermark()
    assert total <= config.max_parallel
    for group, peak in groups.items():
        assert peak <= config.group_limit(group)
    assert config.group_limit("merge") == 1
    # Dependents only started after their prerequisites completed successfully.
    for task_id, requirements in (
        ("catalog-inc", ["catalog-full"]),
        ("merged", ["telemetry-a", "catalog-inc"]),
        ("report", ["merged"]),
    ):
        for requirement in requirements:
            completed_at = [e["at"] for e in store.events(requirement, kinds=["completed"])]
            started_at = [e["at"] for e in store.events(task_id, kinds=["started"])]
            if started_at:
                assert completed_at and completed_at[0] <= started_at[0]


def test_invalid_fixture_reports_every_blocked_task(tmp_path):
    store = RunStore(str(tmp_path / "invalid.sqlite3"))
    pipeline = OrchestrationPipeline(
        TaskSource().read(str(ROOT / "fixtures/invalid_tasks.json")),
        RunConfig(max_parallel=2),
        store=store,
        clock=ManualClock(),
    )
    summary = pipeline.run()
    assert summary["blocked"] == 5
    assert summary["completed"] == 1
    assert store.task_run("healthy")["status"] == "completed"
    assert store.task_run("cycle-a")["status"] == "blocked"
    assert store.blocked_reason("cycle-a") == "dependency_cycle"
    assert store.blocked_reason("missing-ref") == "dependency_missing:ghost-source"
    for task_id in ("after-missing", "after-cycle"):
        row = store.task_run(task_id)
        assert row["status"] == "blocked" and row["blocked_reason"]
        assert row["attempts"] == 0


def test_cli_reports_summary(tmp_path, capsys):
    from orchestrator.cli import main

    database = tmp_path / "cli.sqlite3"
    import sys
    argv = ["--fixture", str(ROOT / "fixtures/dynamic_tasks.json"), "--database", str(database),
            "--config", str(ROOT / "fixtures/run_config.json"), "--virtual-time"]
    old = sys.argv
    sys.argv = ["orchestrator"] + argv
    try:
        main()
    finally:
        sys.argv = old
    out = capsys.readouterr().out.strip()
    assert '"scheduled": 10' in out

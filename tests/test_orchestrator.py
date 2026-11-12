from pathlib import Path
import sqlite3
from orchestrator.application import run_fixture
from orchestrator.scheduler import PriorityScheduler, RetryExecutor
from orchestrator.sources import TaskSource


def test_source_scheduler_and_retry_policy():
    tasks = TaskSource().read(str(Path(__file__).parents[1] / "fixtures/tasks.json"))
    assert [task["task_id"] for task in PriorityScheduler().order(tasks)] == ["telemetry-1", "catalog-1", "rpa-1"]
    assert RetryExecutor(2).execute(tasks[0]).attempts == 2


def test_pipeline_persists_core_scheduling_result(tmp_path):
    root = Path(__file__).parents[1]
    result = run_fixture(str(root / "fixtures/tasks.json"), str(tmp_path / "runs.sqlite3"))
    assert result == {"scheduled": 3, "completed": 3, "failed": 0, "retries": 3}
    with sqlite3.connect(tmp_path / "runs.sqlite3") as conn:
        assert conn.execute("SELECT COUNT(*) FROM task_runs").fetchone()[0] == 3

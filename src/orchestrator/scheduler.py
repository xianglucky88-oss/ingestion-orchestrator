from typing import Any
from .models import TaskRun


class RetryExecutor:
    """只负责按失败预算执行一次任务的重试策略。"""

    def __init__(self, max_retries: int = 2):
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        self.max_retries = max_retries

    def execute(self, task: dict[str, Any]) -> TaskRun:
        attempts = 0
        fail_until = int(task.get("fail_until", 0))
        while attempts <= self.max_retries:
            attempts += 1
            if fail_until < attempts:
                return TaskRun(task["task_id"], "completed", attempts, task.get("mode", "incremental"))
        return TaskRun(task["task_id"], "failed", attempts, task.get("mode", "incremental"))


class PriorityScheduler:
    """只负责优先级排序，不持久化任务状态。"""

    def order(self, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(tasks, key=lambda item: (-int(item.get("priority", 0)), item["task_id"]))

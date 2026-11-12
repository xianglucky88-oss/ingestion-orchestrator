from typing import Any
from .scheduler import PriorityScheduler, RetryExecutor
from .storage import RunStore


class OrchestrationPipeline:
    """只负责任务排序、执行和保存之间的流程编排。"""

    def __init__(self, tasks: list[dict[str, Any]], scheduler: PriorityScheduler, executor: RetryExecutor, store: RunStore):
        self.tasks = tasks
        self.scheduler = scheduler
        self.executor = executor
        self.store = store

    def run(self) -> dict[str, int]:
        completed = failed = retries = 0
        for task in self.scheduler.order(self.tasks):
            result = self.executor.execute(task)
            self.store.save(result)
            retries += max(0, result.attempts - 1)
            if result.status == "completed":
                completed += 1
            else:
                failed += 1
        return {"scheduled": len(self.tasks), "completed": completed, "failed": failed, "retries": retries}

from .pipeline import OrchestrationPipeline
from .scheduler import PriorityScheduler, RetryExecutor
from .sources import TaskSource
from .storage import RunStore


def run_fixture(fixture: str, database: str, max_retries: int = 2) -> dict[str, int]:
    store = RunStore(database)
    try:
        tasks = TaskSource().read(fixture)
        return OrchestrationPipeline(tasks, PriorityScheduler(), RetryExecutor(max_retries), store).run()
    finally:
        store.close()

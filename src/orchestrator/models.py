from dataclasses import dataclass


@dataclass(frozen=True)
class TaskRun:
    task_id: str
    status: str
    attempts: int
    mode: str

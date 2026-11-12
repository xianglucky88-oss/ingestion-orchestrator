import json
from pathlib import Path
from typing import Any


class TaskSource:
    """只负责读取任务定义。"""

    def read(self, path: str) -> list[Any]:
        tasks = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(tasks, list):
            raise ValueError("task fixture must contain a JSON list")
        return tasks

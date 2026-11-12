import sqlite3
from .models import TaskRun


class RunStore:
    """只负责任务运行状态的 SQLite 持久化。"""

    def __init__(self, database: str):
        self.conn = sqlite3.connect(database)
        self.conn.execute("CREATE TABLE IF NOT EXISTS task_runs(task_id TEXT PRIMARY KEY, status TEXT, attempts INTEGER, mode TEXT)")
        self.conn.execute("""CREATE TABLE IF NOT EXISTS task_attempts(
            task_id TEXT NOT NULL, attempt INTEGER NOT NULL, status TEXT NOT NULL,
            mode TEXT NOT NULL, PRIMARY KEY(task_id, attempt)
        )""")
        self.conn.commit()

    def save(self, run: TaskRun) -> None:
        self.conn.execute("""INSERT INTO task_runs(task_id, status, attempts, mode) VALUES (?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET status=excluded.status,
            attempts=excluded.attempts, mode=excluded.mode""",
            (run.task_id, run.status, run.attempts, run.mode))
        self.conn.execute("INSERT OR IGNORE INTO task_attempts VALUES (?, ?, ?, ?)",
                          (run.task_id, run.attempts, run.status, run.mode))
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

from .pipeline import run_files
from .models import RunConfig


def run_fixture(
    fixture: str,
    database: str,
    max_retries: int = 2,
    *,
    config: RunConfig | None = None,
    config_path: str | None = None,
) -> dict[str, int]:
    """Backward-compatible entry point used by tests and the CLI.

    ``config``/``config_path`` are optional: without them a legacy fixture runs
    with the default limits exactly as before.
    """
    return run_files(
        fixture,
        database,
        config=config,
        config_path=config_path,
        max_retries=max_retries,
    )

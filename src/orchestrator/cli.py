import argparse
import json

from .pipeline import run_files


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a local ingestion task fixture")
    parser.add_argument("--fixture", required=True, help="task definitions (JSON list)")
    parser.add_argument("--database", required=True, help="SQLite run store path")
    parser.add_argument("--config", help="optional run limits (JSON object)")
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument(
        "--virtual-time",
        action="store_true",
        help="use the deterministic virtual clock (durations are virtual seconds)",
    )
    args = parser.parse_args()
    result = run_files(
        args.fixture,
        args.database,
        max_retries=args.max_retries,
        config_path=args.config,
        virtual_time=args.virtual_time,
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()

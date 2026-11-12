import argparse
import json
from .application import run_fixture


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--fixture", required=True)
    p.add_argument("--database", required=True)
    a = p.parse_args()
    print(json.dumps(run_fixture(a.fixture, a.database), sort_keys=True))


if __name__ == "__main__":
    main()

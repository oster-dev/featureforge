"""Smoke check of the feature service in the Compose stack (ADR-016)."""

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

from featureforge.spec.loader import load_spec


def main() -> int:
    parser = argparse.ArgumentParser(description="Check readiness and one lookup.")
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument(
        "--spec", type=Path, default=Path("feature_repo/specs/user_play_count_1h.yaml")
    )
    parser.add_argument("--entity", default="user_000001")
    args = parser.parse_args()
    spec = load_spec(args.spec)

    try:
        with urllib.request.urlopen(f"{args.url}/readyz", timeout=5) as response:
            ready = json.load(response)
    except urllib.error.URLError as error:
        print(f"readyz failed: {error}")
        return 1
    if ready.get("status") != "ready":
        print(f"service is not ready: {ready}")
        return 1

    body = {"features": [{"name": spec.name}], "entities": [{spec.keys[0]: args.entity}]}
    request = urllib.request.Request(
        f"{args.url}/v1/features/lookup",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            result = json.load(response)
    except urllib.error.URLError as error:
        print(f"lookup failed: {error}")
        return 1

    statuses = sorted({entry["status"] for entry in result["entries"]})
    print(
        f"readyz: ready. lookup {spec.name} for {args.entity}: "
        f"statuses {statuses}, elapsed_ms {result['elapsed_ms']}"
    )
    return 0 if statuses else 1


if __name__ == "__main__":
    sys.exit(main())

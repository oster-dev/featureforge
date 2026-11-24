"""Quickstart: write three demo values and read them through the feature service (ADR-016).

Run it with `make quickstart`. It writes a fresh value and an old value for
`user_play_count_1h` into Redis through the spec store, asks the feature service about both and
about a user that was never written, and prints one line per entry. With `--with-failure` it then
stops Redis and asks again: every entry comes back as `backend_error`, never as a zero.
"""

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from featureforge.spec.loader import load_spec
from featureforge.spec.model import FeatureSpec
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.contracts.feature_record import FeatureRecord

SPEC = Path("feature_repo/specs/user_play_count_1h.yaml")

# entity, age of the observation, plays, status that the service should report
DEMO: tuple[tuple[str, timedelta | None, int | None, str], ...] = (
    ("demo_user_fresh", timedelta(minutes=1), 7, "ok"),
    ("demo_user_old", timedelta(minutes=20), 4, "stale"),
    ("demo_user_unknown", None, None, "missing"),
)
EXPECTED = [row[3] for row in DEMO]


def build_records(spec: FeatureSpec, now: datetime) -> list[FeatureRecord]:
    """One record per demo user that has a value. The unknown user gets none."""
    records = []
    for entity, age, plays, _ in DEMO:
        if age is None or plays is None:
            continue
        counts = {a.name: plays for a in spec.aggregations}
        observed = now - age
        records.append(
            FeatureRecord(
                feature_name=spec.name,
                entity_key=spec.keys[0],
                entity_value=entity,
                observation_time=observed,
                window_start_exclusive=observed - spec.window.size,
                counts=counts,
                derived={n: evaluate_expression(e, counts) for n, e in spec.derived.items()},
            )
        )
    return records


def lookup_body(spec: FeatureSpec) -> dict[str, Any]:
    return {
        "features": [{"name": spec.name}],
        "entities": [{spec.keys[0]: entity} for entity, *_ in DEMO],
    }


def render(spec: FeatureSpec, entries: Sequence[dict[str, Any]]) -> str:
    counter = spec.aggregations[0].name
    lines = [f"{'entity':20} {'status':14} {counter:12} {'age_s':>6}  note"]
    for entry in entries:
        entity = entry["entity"][spec.keys[0]]
        value = (entry.get("values") or {}).get(counter, "-")
        age = entry.get("age_seconds")
        age_text = "-" if age is None else f"{age:.0f}"
        note = entry.get("freshness") or entry.get("error_type") or "-"
        lines.append(f"{entity:20} {entry['status']:14} {value!s:12} {age_text:>6}  {note}")
    return "\n".join(lines)


def check(entries: Sequence[dict[str, Any]], expected: Sequence[str]) -> list[str]:
    got = [entry["status"] for entry in entries]
    return [] if got == list(expected) else [f"expected statuses {list(expected)}, got {got}"]


def call(url: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="GET" if body is None else "POST",
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        result: dict[str, Any] = json.load(response)
    return result


def wait_until_ready(url: str, seconds: int = 60) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            if call(f"{url}/readyz").get("status") == "ready":
                return True
        except (OSError, ValueError):
            pass
        time.sleep(1)
    return False


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write three demo values and read them back.")
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--spec", type=Path, default=SPEC)
    parser.add_argument("--with-failure", action="store_true", help="stop Redis and ask again")
    args = parser.parse_args(argv)

    import redis

    from featureforge.streaming.spec_store import RedisSpecStore

    spec = load_spec(args.spec)
    store = RedisSpecStore(redis.Redis.from_url(args.redis_url), spec)
    for record in build_records(spec, datetime.now(UTC)):
        outcome = store.write(record)
        print(f"wrote {record.entity_value}: {getattr(outcome, 'value', outcome)}")

    if not wait_until_ready(args.url):
        print(f"the service at {args.url} did not become ready")
        return 1
    try:
        entries = call(f"{args.url}/v1/features/lookup", lookup_body(spec))["entries"]
    except (OSError, ValueError, KeyError) as error:
        print(f"lookup failed: {error}")
        return 1
    print()
    print(render(spec, entries))
    problems = check(entries, EXPECTED)

    if args.with_failure:
        print("\nStopping Redis and asking again ...")
        subprocess.run(["docker", "compose", "stop", "redis"], check=True)
        try:
            failed = call(f"{args.url}/v1/features/lookup", lookup_body(spec))["entries"]
            print()
            print(render(spec, failed))
            problems += check(failed, ["backend_error"] * len(DEMO))
        finally:
            subprocess.run(["docker", "compose", "start", "redis"], check=True)

    for problem in problems:
        print(f"PROBLEM: {problem}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())

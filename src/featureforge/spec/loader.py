from __future__ import annotations

import sys
from pathlib import Path

import yaml
from pydantic import ValidationError

from featureforge.spec.model import FeatureSpec
from featureforge.spec.reference import validate_event_fields


def load_spec(path: str | Path) -> FeatureSpec:
    """Load a spec and check its event fields against the event contract."""
    with open(path, encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    spec = FeatureSpec.model_validate(data)
    validate_event_fields(spec)
    return spec


def format_errors(exc: ValidationError) -> list[str]:
    lines = []
    for err in exc.errors():
        where = ".".join(str(p) for p in err["loc"]) or "<spec>"
        lines.append(f"{where}: {err['msg'].removeprefix('Value error, ')}")
    return lines


def main(argv: list[str] | None = None) -> int:
    paths = sys.argv[1:] if argv is None else argv
    if not paths:
        print("usage: python -m featureforge.spec.loader <spec.yaml>...", file=sys.stderr)
        return 2
    status = 0
    for path in paths:
        try:
            spec = load_spec(path)
        except FileNotFoundError:
            print(f"✗ {path}: file not found")
            status = 2
        except ValidationError as exc:
            print(f"✗ {path}")
            for line in format_errors(exc):
                print(f"    {line}")
            status = max(status, 1)
        except ValueError as exc:
            print(f"✗ {path}")
            print(f"    {exc}")
            status = max(status, 1)
        else:
            print(f"✓ {path}: {spec.name} v{spec.version}")
    return status


if __name__ == "__main__":
    raise SystemExit(main())

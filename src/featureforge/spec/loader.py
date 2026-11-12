from __future__ import annotations

import sys
from pathlib import Path

import yaml
from pydantic import ValidationError

from featureforge.spec.model import FeatureSpec


def load_spec(path: str | Path) -> FeatureSpec:
    with open(path, encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    return FeatureSpec.model_validate(data)


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
        else:
            print(f"✓ {path}: {spec.name} v{spec.version}")
    return status


if __name__ == "__main__":
    raise SystemExit(main())

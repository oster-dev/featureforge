from __future__ import annotations

import ast
import sys
from pathlib import Path
from typing import get_args

import yaml
from pydantic import ValidationError

from featureforge.spec.label_model import LabelSpec
from featureforge.spec.loader import format_errors
from featureforge.spec.reference import names_in
from featureforge.streaming.contracts.events import InteractionEventV1


def contract_event_types(model: type[InteractionEventV1] = InteractionEventV1) -> set[str]:
    return set(get_args(model.model_fields["event_type"].annotation))


def event_types_in_filter(expr: str) -> set[str]:
    """Return the event types a filter compares event_type against."""
    found: set[str] = set()
    for node in ast.walk(ast.parse(expr, mode="eval")):
        if not isinstance(node, ast.Compare):
            continue
        operands = [node.left, *node.comparators]
        if not any(isinstance(o, ast.Name) and o.id == "event_type" for o in operands):
            continue
        for operand in operands:
            if isinstance(operand, ast.Constant) and isinstance(operand.value, str):
                found.add(operand.value)
            elif isinstance(operand, (ast.List, ast.Tuple)):
                found |= {
                    item.value
                    for item in operand.elts
                    if isinstance(item, ast.Constant) and isinstance(item.value, str)
                }
    return found


def validate_label_against_contract(
    spec: LabelSpec, model: type[InteractionEventV1] = InteractionEventV1
) -> None:
    """Reject labels that use unknown event fields or event types outside the contract."""
    fields = set(spec.keys) | {spec.dedupe.key, "event_time"}
    if spec.outcome.filter:
        fields |= names_in(spec.outcome.filter)
    unknown_fields = fields - set(model.model_fields)
    if unknown_fields:
        raise ValueError(
            f"label '{spec.name}' references unknown event fields {sorted(unknown_fields)}; "
            f"available: {sorted(model.model_fields)}"
        )
    if spec.outcome.filter:
        allowed = contract_event_types(model)
        unknown_types = event_types_in_filter(spec.outcome.filter) - allowed
        if unknown_types:
            raise ValueError(
                f"label '{spec.name}' uses event types {sorted(unknown_types)} "
                f"outside the contract; allowed: {sorted(allowed)}"
            )


def load_label_spec(path: str | Path) -> LabelSpec:
    """Load a label spec and check it against the event contract."""
    with open(path, encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    spec = LabelSpec.model_validate(data)
    validate_label_against_contract(spec)
    return spec


def main(argv: list[str] | None = None) -> int:
    paths = sys.argv[1:] if argv is None else argv
    if not paths:
        print("usage: python -m featureforge.spec.label_loader <label.yaml>...", file=sys.stderr)
        return 2
    status = 0
    for path in paths:
        try:
            spec = load_label_spec(path)
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

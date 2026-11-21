"""Parity of the serving hot path: cached expressions and the whitespace check."""

import ast
import random
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from featureforge.spec.loader import load_spec
from featureforge.spec.reference import _eval, _parsed, evaluate_expression, names_in
from featureforge.streaming.spec_store import _WHITESPACE, check_text

SPECS_DIR = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"

SYNTHETIC = [
    "a + b",
    "a / b",
    "a / b if b else 0",
    "a - b * 2",
    "a > b and b > 0",
    "a == 1 or b == 2",
    "not a",
    "-a + b",
    "a in [1, 2, 3]",
    "a not in (1, 2)",
    "a < b < 10",
    "'x' == 'x'",
    "a / (b - b)",
    "missing + 1",
    "a @ b",
]


def _spec_expressions() -> list[str]:
    if not SPECS_DIR.is_dir():
        return []
    found: list[str] = []
    for path in sorted(SPECS_DIR.glob("*.yaml")):
        spec = load_spec(path)
        found.extend(spec.derived.values())
        found.extend(agg.filter for agg in spec.aggregations if agg.filter)
    return found


EXPRESSIONS = list(dict.fromkeys(SYNTHETIC + _spec_expressions()))


def _outcome(function: Callable[..., Any], *args: Any) -> tuple[Any, ...]:
    try:
        return ("ok", function(*args))
    except Exception as error:
        return ("error", type(error), str(error))


def _old_evaluate(expr: str, names: dict[str, Any]) -> Any:
    return _eval(ast.parse(expr, mode="eval").body, names)


def _old_names(expr: str) -> set[str]:
    tree = ast.parse(expr, mode="eval")
    return {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}


def _old_has_whitespace(value: str) -> bool:
    return any(character.isspace() for character in value)


@pytest.mark.parametrize("expr", EXPRESSIONS)
def test_cached_evaluation_matches_a_fresh_parse(expr: str) -> None:
    rng = random.Random(11)
    assert _outcome(names_in, expr) == _outcome(_old_names, expr)
    assert _outcome(evaluate_expression, expr, {}) == _outcome(_old_evaluate, expr, {})
    names = _old_names(expr) | {"a", "b"}
    for _ in range(200):
        values = {name: rng.choice([0, 0, 1, 2, 3, 7, 50, 2.5]) for name in names}
        assert _outcome(evaluate_expression, expr, values) == _outcome(_old_evaluate, expr, values)


def test_names_in_returns_a_fresh_set() -> None:
    first = names_in("a + b")
    first.add("injected")
    assert names_in("a + b") == {"a", "b"}


def test_a_syntax_error_is_raised_every_time() -> None:
    for _ in range(2):
        with pytest.raises(SyntaxError):
            evaluate_expression("a +", {})


def test_the_parse_is_cached() -> None:
    expr = "a + b + 12345"
    before = _parsed.cache_info()
    evaluate_expression(expr, {"a": 1, "b": 2})
    evaluate_expression(expr, {"a": 1, "b": 2})
    after = _parsed.cache_info()
    assert after.misses == before.misses + 1
    assert after.hits == before.hits + 1


def test_whitespace_pattern_matches_isspace_for_every_code_point() -> None:
    mismatches = [
        code
        for code in range(0x110000)
        if bool(_WHITESPACE.search(chr(code))) != chr(code).isspace()
    ]
    assert mismatches == []


def test_whitespace_pattern_matches_the_old_check_on_random_strings() -> None:
    rng = random.Random(5)
    alphabet = ["a", "Z", "0", "-", "_", ":", " ", "\t", "\n", "\u00a0", "\u2003", "\u200b", "é"]
    for _ in range(20000):
        value = "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 12)))
        assert bool(_WHITESPACE.search(value)) == _old_has_whitespace(value)


@pytest.mark.parametrize("value", ["", " ", "a b", "a\tb", "a\u00a0b"])
def test_check_text_rejects_empty_and_whitespace(value: str) -> None:
    with pytest.raises(ValueError, match="non-empty and contain no whitespace"):
        check_text(value, "entity_value")


def test_check_text_accepts_plain_text_and_rejects_other_types() -> None:
    check_text("user-1", "entity_value")
    with pytest.raises(TypeError, match="entity_value must be a string"):
        check_text(5, "entity_value")

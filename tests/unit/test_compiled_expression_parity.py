"""The compiled evaluator returns what the oracle returns, errors included."""

import random
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from featureforge.spec.compiled_expression import compile_expression
from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_expression, names_in

SPECS_DIR = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"

EDGE_CASES = [
    "a + b", "a / b", "a - b * 2", "a * b / c", "a / (b - b)", "missing + 1",
    "a if b else c", "1 if a else b % 2", "a if b else ~c", "a if b else f(c)",
    "a and b and c", "a or b or c", "a and (b or c)", "not a", "not not a", "-a",
    "-(-a)", "~a", "+a", "a % b", "a ** 2", "a // b", "a @ b", "a is b", "a is not b",
    "a < b < c", "a == b != c", "a in [1, 2, 3]", "a not in (1, 2)", "[a, b] == [a, b]",
    "(a, b)", "'s' == 'x'", "a.b", "a[0]", "f(a)", "{a: b}", "{a}",
    "a if b else c if a else b", "None", "True",
]  # fmt: skip

ATOMS = ["a", "b", "c", "0", "1", "2", "2.5", "True", "None", "'s'"]
VALUES: list[Any] = [0, 0, 1, 2, 3, 7, -1, 2.5, 0.0, True, False, None, "s", "abc"]


def _spec_expressions() -> list[str]:
    if not SPECS_DIR.is_dir():
        return []
    found: list[str] = []
    for path in sorted(SPECS_DIR.glob("*.yaml")):
        spec = load_spec(path)
        found.extend(spec.derived.values())
        found.extend(agg.filter for agg in spec.aggregations if agg.filter)
    return found


EXPRESSIONS = list(dict.fromkeys(EDGE_CASES + _spec_expressions()))


def _outcome(function: Callable[..., Any], *args: Any) -> tuple[Any, ...]:
    try:
        return ("ok", function(*args))
    except Exception as error:
        return ("error", type(error), str(error))


def _environment(rng: random.Random, names: set[str]) -> dict[str, Any]:
    env = {name: rng.choice(VALUES) for name in sorted(names | {"a", "b", "c"})}
    if rng.random() < 0.2:
        env.pop(rng.choice(sorted(env)))
    return env


def _random_expression(rng: random.Random, depth: int = 0) -> str:
    if depth >= 3 or rng.random() < 0.25:
        return rng.choice(ATOMS)
    kind = rng.choice(
        ["bin", "bin", "cmp", "chain", "bool", "not", "neg", "if", "in", "list", "other", "other"]
    )
    left = _random_expression(rng, depth + 1)
    right = _random_expression(rng, depth + 1)
    if kind == "bin":
        return f"({left} {rng.choice(['+', '-', '*', '/'])} {right})"
    if kind == "cmp":
        return f"({left} {rng.choice(['==', '!=', '<', '<=', '>', '>='])} {right})"
    if kind == "chain":
        return f"({left} < {right} < {_random_expression(rng, depth + 1)})"
    if kind == "bool":
        return f"({left} {rng.choice(['and', 'or'])} {right})"
    if kind == "not":
        return f"(not {left})"
    if kind == "neg":
        return f"(-{left})"
    if kind == "if":
        return f"({left} if {right} else {_random_expression(rng, depth + 1)})"
    if kind == "in":
        return f"({left} {rng.choice(['in', 'not in'])} [{right}, 1, 2])"
    if kind == "list":
        return f"[{left}, {right}]"
    return rng.choice(
        [
            f"({left} % {right})",
            f"({left} ** {right})",
            f"({left} // {right})",
            f"({left} @ {right})",
            f"(~{left})",
            f"(+{left})",
            f"({left} is {right})",
        ]
    )


@pytest.mark.parametrize("expr", EXPRESSIONS)
def test_compiled_matches_the_oracle(expr: str) -> None:
    rng = random.Random(3)
    compiled = compile_expression(expr)
    assert _outcome(compiled, {}) == _outcome(evaluate_expression, expr, {})
    for _ in range(300):
        env = _environment(rng, names_in(expr))
        assert _outcome(compiled, env) == _outcome(evaluate_expression, expr, env)


def test_compiled_matches_the_oracle_on_random_expressions() -> None:
    rng = random.Random(2026)
    for _ in range(3000):
        expr = _random_expression(rng)
        compiled = compile_expression(expr)
        for _ in range(5):
            env = _environment(rng, names_in(expr))
            assert _outcome(compiled, env) == _outcome(evaluate_expression, expr, env), expr


def test_compilation_is_cached() -> None:
    assert compile_expression("a + 1 + 98765") is compile_expression("a + 1 + 98765")


def test_a_syntax_error_is_raised_every_time() -> None:
    for _ in range(2):
        with pytest.raises(SyntaxError):
            compile_expression("a +")

"""Expressions compiled once into closures for the serving hot path (ADR-016).

compile_expression(expr)(names) returns what reference.evaluate_expression(expr, names)
returns, including the exceptions and the order in which they are raised. The oracle in
reference.py stays the definition of the semantics. The parity test enforces equality.
"""

from __future__ import annotations

import ast
from collections.abc import Callable, Mapping
from functools import lru_cache
from typing import Any

from featureforge.spec.reference import _ARITH, _COMPARE

Compiled = Callable[[Mapping[str, Any]], Any]


def _unsupported(node: ast.AST) -> Compiled:
    message = f"unsupported expression node {type(node).__name__}"

    def run(names: Mapping[str, Any]) -> Any:
        raise ValueError(message)

    return run


def _build(node: ast.AST) -> Compiled:
    if isinstance(node, ast.Constant):
        constant = node.value

        def run_constant(names: Mapping[str, Any]) -> Any:
            return constant

        return run_constant
    if isinstance(node, ast.Name):
        identifier = node.id
        missing = f"unknown name '{identifier}'"

        def run_name(names: Mapping[str, Any]) -> Any:
            if identifier not in names:
                raise ValueError(missing)
            return names[identifier]

        return run_name
    if isinstance(node, (ast.Tuple, ast.List)):
        items = tuple(_build(item) for item in node.elts)

        def run_sequence(names: Mapping[str, Any]) -> Any:
            return [item(names) for item in items]

        return run_sequence
    if isinstance(node, ast.UnaryOp):
        operand = _build(node.operand)
        if isinstance(node.op, ast.Not):

            def run_not(names: Mapping[str, Any]) -> Any:
                return not operand(names)

            return run_not
        if isinstance(node.op, ast.USub):

            def run_negate(names: Mapping[str, Any]) -> Any:
                return -operand(names)

            return run_negate
        rejected = _unsupported(node)

        def run_unary_unsupported(names: Mapping[str, Any]) -> Any:
            operand(names)
            return rejected(names)

        return run_unary_unsupported
    if isinstance(node, ast.BoolOp):
        parts = tuple(_build(part) for part in node.values)
        if isinstance(node.op, ast.And):

            def run_and(names: Mapping[str, Any]) -> Any:
                for part in parts:
                    if not part(names):
                        return False
                return True

            return run_and

        def run_or(names: Mapping[str, Any]) -> Any:
            for part in parts:
                if part(names):
                    return True
            return False

        return run_or
    if isinstance(node, ast.BinOp) and type(node.op) in _ARITH:
        operation = _ARITH[type(node.op)]
        left = _build(node.left)
        right = _build(node.right)

        def run_arithmetic(names: Mapping[str, Any]) -> Any:
            return operation(left(names), right(names))

        return run_arithmetic
    if isinstance(node, ast.Compare):
        first = _build(node.left)
        steps = tuple(
            (type(op), _build(comparator))
            for op, comparator in zip(node.ops, node.comparators, strict=True)
        )

        def run_compare(names: Mapping[str, Any]) -> Any:
            current = first(names)
            for op_type, comparator in steps:
                other = comparator(names)
                if not _COMPARE[op_type](current, other):
                    return False
                current = other
            return True

        return run_compare
    if isinstance(node, ast.IfExp):
        test = _build(node.test)
        body = _build(node.body)
        orelse = _build(node.orelse)

        def run_conditional(names: Mapping[str, Any]) -> Any:
            return (body if test(names) else orelse)(names)

        return run_conditional
    return _unsupported(node)


@lru_cache(maxsize=512)
def compile_expression(expr: str) -> Compiled:
    return _build(ast.parse(expr, mode="eval").body)

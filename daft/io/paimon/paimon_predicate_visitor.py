from __future__ import annotations

import datetime
import decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pypaimon.read.read_builder import ReadBuilder

    from daft.expressions import Expression

from pypaimon.common.predicate import Predicate

import daft.functions
from daft.expressions.visitor import PredicateVisitor
from daft.logical.schema import DataType


def _eval_expr_to_scalar(expr: Expression) -> Any:
    df = daft.from_pydict({"__dummy__": [0]})
    out = df.select(expr.alias("_v")).collect()

    d = out.to_pydict()
    return d["_v"][0]


def _cast_python_value(value: Any, dtype: DataType) -> Any:
    if value is None:
        return None

    if dtype == DataType.int8():
        return int(value)
    if dtype == DataType.int16():
        return int(value)
    if dtype == DataType.int32():
        return int(value)
    if dtype == DataType.int64():
        return int(value)

    if dtype == DataType.uint8():
        v = int(value)
        if v < 0:
            raise ValueError("cannot cast negative value to uint8")
        return v
    if dtype == DataType.uint16():
        v = int(value)
        if v < 0:
            raise ValueError("cannot cast negative value to uint16")
        return v
    if dtype == DataType.uint32():
        v = int(value)
        if v < 0:
            raise ValueError("cannot cast negative value to uint32")
        return v
    if dtype == DataType.uint64():
        v = int(value)
        if v < 0:
            raise ValueError("cannot cast negative value to uint64")
        return v

    if dtype == DataType.float32():
        return float(value)
    if dtype == DataType.float64():
        return float(value)

    if dtype == DataType.string():
        return str(value)

    if dtype == DataType.bool():
        if isinstance(value, str):
            s = value.strip().lower()
            if s in ("true", "1"):
                return True
            if s in ("false", "0"):
                return False
            raise ValueError(f"cannot cast string {value!r} to bool")
        return bool(value)

    if dtype == DataType.decimal128(38, 9):
        return decimal.Decimal(str(value))

    if dtype == DataType.date():
        if isinstance(value, datetime.date):
            return value
        return datetime.date.fromisoformat(value)

    if dtype == DataType.timestamp("us"):
        if isinstance(value, datetime.datetime):
            return value
        return datetime.datetime.fromisoformat(value)

    raise NotImplementedError(f"Unsupported cast to dtype: {dtype}")


class PaimonPredicateVisitor(PredicateVisitor[Predicate]):
    def __init__(self, read_builder: ReadBuilder):
        self._predicate_builder = read_builder.new_predicate_builder()

    def visit_col(self, name: str) -> str:
        return name

    def visit_lit(self, value: Any) -> Any:
        return value

    def visit_alias(self, expr: Expression, alias: str) -> Any:
        raise NotImplementedError("visit_alias not support for PaimonPredicateVisitor")

    def visit_cast(self, expr: Expression, dtype: DataType) -> Any:
        return _cast_python_value(self.visit(expr), dtype)

    def visit_list(self, items: list[Expression]) -> list[Any]:
        return [self.visit(expr) for expr in items]

    def visit_and(self, left: Expression, right: Expression) -> Predicate:
        return self._predicate_builder.and_predicates([self.visit(left), self.visit(right)])

    def visit_or(self, left: Expression, right: Expression) -> Predicate:
        return self._predicate_builder.or_predicates([self.visit(left), self.visit(right)])

    def visit_not(self, expr: Expression) -> Predicate:
        raise NotImplementedError("visit_not not support for PaimonPredicateVisitor")

    def visit_equal(self, left: Expression, right: Expression) -> Predicate:
        if left.is_column():
            return self._predicate_builder.equal(left.name(), self.visit(right))
        elif right.is_column():
            return self._predicate_builder.equal(right.name(), self.visit(left))
        raise NotImplementedError("col expr is needed in visit_equal for PaimonPredicateVisitor")

    def visit_not_equal(self, left: Expression, right: Expression) -> Predicate:
        if left.is_column():
            return self._predicate_builder.not_equal(left.name(), self.visit(right))
        elif right.is_column():
            return self._predicate_builder.not_equal(right.name(), self.visit(left))
        raise NotImplementedError("col expr is needed in visit_not_equal for PaimonPredicateVisitor")

    def visit_less_than(self, left: Expression, right: Expression) -> Predicate:
        if left.is_column():
            return self._predicate_builder.less_than(left.name(), self.visit(right))
        elif right.is_column():
            return self._predicate_builder.less_than(right.name(), self.visit(left))
        raise NotImplementedError("col expr is needed in visit_less_than for PaimonPredicateVisitor")

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> Predicate:
        if left.is_column():
            return self._predicate_builder.less_or_equal(left.name(), self.visit(right))
        elif right.is_column():
            return self._predicate_builder.less_or_equal(right.name(), self.visit(left))
        raise NotImplementedError("col expr is needed in visit_less_than_or_equal for PaimonPredicateVisitor")

    def visit_greater_than(self, left: Expression, right: Expression) -> Predicate:
        if left.is_column():
            return self._predicate_builder.greater_than(left.name(), self.visit(right))
        elif right.is_column():
            return self._predicate_builder.greater_than(right.name(), self.visit(left))
        raise NotImplementedError("col expr is needed in visit_greater_than for PaimonPredicateVisitor")

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> Predicate:
        if left.is_column():
            return self._predicate_builder.greater_or_equal(left.name(), self.visit(right))
        elif right.is_column():
            return self._predicate_builder.greater_or_equal(right.name(), self.visit(left))
        raise NotImplementedError("col expr is needed in visit_greater_than_or_equal for PaimonPredicateVisitor")

    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> Predicate:
        return self._predicate_builder.between(expr.name(), self.visit(lower), self.visit(lower))

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> Predicate:
        return self._predicate_builder.is_in(expr.name(), self.visit_list(items))

    def visit_is_null(self, expr: Expression) -> Predicate:
        return self._predicate_builder.is_null(expr.name())

    def visit_not_null(self, expr: Expression) -> Predicate:
        return self._predicate_builder.is_not_null(expr.name())

    def visit_function(self, name: str, args: list[Expression]) -> Any:
        f = getattr(daft.functions, name, None)
        if f is not None:
            func_expr = f(*args)
            return _eval_expr_to_scalar(func_expr)
        else:
            if len(args) != 2:
                raise NotImplementedError(f"{name} not support for PaimonPredicateVisitor with {len(args)} args")
            if name == "plus":
                return self.visit(args[0]) + self.visit(args[1])
            elif name == "minus":
                return self.visit(args[0]) - self.visit(args[1])
            elif name == "multiply":
                return self.visit(args[0]) * self.visit(args[1])
            elif name == "true_divide":
                return self.visit(args[0]) / self.visit(args[1])
            elif name == "floor_divide":
                return self.visit(args[0]) // self.visit(args[1])
            elif name == "modulus":
                return self.visit(args[0]) % self.visit(args[1])
            else:
                raise NotImplementedError(f"{name} not support for PaimonPredicateVisitor")
